package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.effect.implicits.concurrentParTraverseOps
import cats.mtl.Ask
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, AzureContainerService}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.LeoLenses.cloudContextToManagedResourceGroup
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{ToolDAO, _}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  CloudContextNotFoundException,
  WorkspaceNotFoundException
}
import org.broadinstitute.dsde.workbench.leonardo.http.{dbioToIO, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoMetric._
import org.broadinstitute.dsde.workbench.leonardo.util.{AppCreationException, KubernetesAlgebra}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials, Uri}
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/** Collects metrics about active Leo runtimes and apps. */
class LeoMetricsMonitor[F[_]](config: LeoMetricsMonitorConfig,
                              appDAO: AppDAO[F],
                              wdsDAO: WdsDAO[F],
                              cbasDAO: CbasDAO[F],
                              cbasUiDAO: CbasUiDAO[F],
                              cromwellDAO: CromwellDAO[F],
                              hailBatchDAO: HailBatchDAO[F],
                              samDAO: SamDAO[F],
                              wsmDAO: WsmDao[F],
                              kubeAlg: KubernetesAlgebra[F],
                              azureContainerService: AzureContainerService[F]
)(implicit
  F: Async[F],
  dbRef: DbReference[F],
  metrics: OpenTelemetryMetrics[F],
  logger: StructuredLogger[F],
  clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[F, RuntimeContainerServiceType],
  ec: ExecutionContext
) {
  private val parallelism = 25

  /** Entry point of this class; starts the async process */
  val process: Stream[F, Unit] =
    if (config.enabled) {
      (Stream.sleep[F](config.checkInterval) ++ Stream.eval(
        retrieveMetrics
          .handleErrorWith(e => logger.error(e)("Unexpected error occurred during metric monitoring"))
      )).repeat
    } else Stream.unit

  private[monitor] def retrieveMetrics: F[Unit] =
    for {
      now <- F.realTimeInstant
      traceId = TraceId(s"AppMetricsMonitor_${now.toEpochMilli}")
      implicit0(appContext: Ask[F, AppContext]) = Ask.const[F, AppContext](AppContext(traceId, now))
      _ <- retrieveAppMetrics
      _ <- retrieveRuntimeMetrics
    } yield ()

  /** Queries the DB for all active apps and collects metrics */
  private[monitor] def retrieveAppMetrics(implicit ev: Ask[F, AppContext]): F[Unit] = for {
    clusters <- KubernetesServiceDbQueries.listAppsForMetrics.transaction
    appDbStatus = countAppsByDbStatus(clusters)
    _ <- recordMetric(appDbStatus)
    _ <- logger.info(s"Recorded status metrics for ${appDbStatus.size} apps")
    appHealth <- countAppsByHealth(clusters)
    _ <- recordMetric(appHealth)
    _ <- logger.info(s"Recorded health metrics for ${appHealth.size} apps")
    appResources <- getAppK8sResources(clusters)
    _ <- recordMetric(appResources)
    _ <- logger.info(s"Recorded ${appResources.size} app k8s resources")
    nodepoolSize <- getNodepoolSize(clusters)
    _ <- recordMetric(nodepoolSize)
    _ <- logger.info(s"Recorded size for ${nodepoolSize.size} Azure nodepools")
  } yield ()

  /** Queries the DB for all active runtimes and collects metrics */
  private[monitor] def retrieveRuntimeMetrics(implicit ev: Ask[F, AppContext]): F[Unit] = for {
    runtimeSeq <- clusterQuery.listActiveForMetrics.transaction
    runtimes = runtimeSeq.toList
    runtimeDbStatus = countRuntimesByDbStatus(runtimes)
    _ <- recordMetric(runtimeDbStatus)
    _ <- logger.info(s"Recorded status metrics for ${runtimeDbStatus.size} runtimes")
    runtimeHealth <- countRuntimesByHealth(runtimes)
    _ <- recordMetric(runtimeHealth)
    _ <- logger.info(s"Recorded health metrics for ${runtimeHealth.size} runtimes")
  } yield ()

  /** Transforms DB apps to AppStatusMetric and computes counts. */
  private[monitor] def countAppsByDbStatus(
    allClusters: List[KubernetesCluster]
  ): Map[AppStatusMetric, Double] = {
    val allApps = for {
      c <- allClusters
      n <- c.nodepools
      a <- n.apps
    } yield Map(
      AppStatusMetric(c.cloudContext.cloudProvider,
                      a.appType,
                      a.status,
                      getRuntimeUI(a.labels),
                      getAzureCloudContext(c.cloudContext),
                      a.chart
      ) -> 1d
    )

    // combineAll folds a List[Map[metric -> 1]] structure to a Map[metric -> n]
    // using Monoid instances for List, Map, Int.
    allApps.combineAll
  }

  /** Transforms DB runtimes to RuntimeStatusMetric and computes counts. */
  private[monitor] def countRuntimesByDbStatus(allRuntimes: List[RuntimeMetrics]): Map[RuntimeStatusMetric, Double] = {
    val allContainers = for {
      r <- allRuntimes
      // Only care about Jupyter, RStudio, or Azure image types.
      // Assume every runtime has exactly 1 of these.
      imageTypes = Set(RuntimeImageType.Jupyter, RuntimeImageType.RStudio, RuntimeImageType.Azure)
      c <- r.images.filter(i => imageTypes.contains(i.imageType)).headOption
    } yield Map(
      RuntimeStatusMetric(r.cloudContext.cloudProvider,
                          c.imageType,
                          c.imageUrl,
                          r.status,
                          getRuntimeUI(r.labels),
                          getAzureCloudContext(r.cloudContext)
      ) -> 1d
    )
    allContainers.combineAll
  }

  /** Performs health checks for Running apps, and transforms to AppHealthMetric. */
  private[monitor] def countAppsByHealth(
    allClusters: List[KubernetesCluster]
  )(implicit ev: Ask[F, AppContext]): F[Map[AppHealthMetric, Double]] = {
    val allServices = for {
      c <- allClusters if c.asyncFields.isDefined
      n <- c.nodepools
      // Only care about Running apps for health check metrics
      a <- n.apps if a.status == AppStatus.Running
      s <- a.appResources.services
    } yield (c.cloudContext, c.asyncFields.get.loadBalancerIp, a, s.config.name)

    allServices
      .parTraverseN(parallelism) { case (cloudContext, baseUri, app, serviceName) =>
        for {
          ctx <- ev.ask
          // For GCP just test if the app is available through the Leo proxy.
          // For Azure impersonate the user and call the app's status endpoint via Azure Relay.
          isUp <- cloudContext match {
            case CloudContext.Gcp(project) =>
              appDAO.isProxyAvailable(project, app.appName, serviceName)
            case CloudContext.Azure(_) =>
              for {
                tokenOpt <- samDAO.getCachedArbitraryPetAccessToken(app.auditInfo.creator)
                token <- F.fromOption(
                  tokenOpt,
                  AppCreationException(s"Pet not found for user ${app.auditInfo.creator}", Some(ctx.traceId))
                )
                authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))
                relayPath = Uri
                  .unsafeFromString(baseUri.asString) / s"${app.appName.value}-${app.workspaceId.map(_.value.toString).getOrElse("")}"
                isUp <- serviceName match {
                  case ServiceName("cbas")     => cbasDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case ServiceName("cbas-ui")  => cbasUiDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case ServiceName("cromwell") => cromwellDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case ServiceName("wds")      => wdsDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case ServiceName("batch")    => hailBatchDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case s =>
                    logger.warn(ctx.loggingCtx)(
                      s"Unexpected app service encountered during health checks: ${s.value}"
                    ) >> F.pure(false)
                }
              } yield isUp
          }
          // In addition to collecting aggregate metrics, log a warning for any app that is down.
          _ <-
            if (isUp) F.unit
            else
              logger.error(ctx.loggingCtx)(
                s"App is DOWN with " +
                  s"name={${app.appName.value}}, " +
                  s"type={${app.appType.toString}}, " +
                  s"service={${serviceName.value}}, " +
                  s"workspace={${app.workspaceId.map(_.value.toString).getOrElse("")}}, " +
                  s"cloudContext={${cloudContext.asStringWithProvider}}"
              )
        } yield Map(
          AppHealthMetric(cloudContext.cloudProvider,
                          app.appType,
                          serviceName,
                          getRuntimeUI(app.labels),
                          getAzureCloudContext(cloudContext),
                          isUp,
                          app.chart
          ) -> 1d,
          AppHealthMetric(cloudContext.cloudProvider,
                          app.appType,
                          serviceName,
                          getRuntimeUI(app.labels),
                          getAzureCloudContext(cloudContext),
                          !isUp,
                          app.chart
          ) -> 0d
        )
      }
      .map(_.combineAll)
  }

  /** Performs health checks for Running runtimes, and transforms to RuntimeHealthMetric. */
  private[monitor] def countRuntimesByHealth(
    allRuntimes: List[RuntimeMetrics]
  )(implicit ev: Ask[F, AppContext]): F[Map[RuntimeHealthMetric, Double]] = {
    val allContainers = for {
      // Only care about Running runtimes for health checks
      r <- allRuntimes if r.status == RuntimeStatus.Running
      (i, c) <- r.images.flatMap(i =>
        RuntimeContainerServiceType.imageTypeToRuntimeContainerServiceType.get(i.imageType).map(c => (i, c))
      )
    } yield (r, i, c)

    allContainers
      .parTraverseN(parallelism) { case (runtime, image, container) =>
        for {
          ctx <- ev.ask
          isUp <- container.isProxyAvailable(runtime.cloudContext, runtime.runtimeName)
          // In addition to collecting aggregate metrics, log a warning for any runtime that is down.
          _ <-
            if (isUp) F.unit
            else
              logger.error(ctx.loggingCtx)(
                s"Runtime is DOWN with " +
                  s"name={${runtime.runtimeName.asString}}, " +
                  s"type={${container.imageType.toString}}, " +
                  s"cloudContext={${runtime.cloudContext.asStringWithProvider}}, " +
                  s"workspace={${runtime.workspaceId}}"
              )
        } yield Map(
          RuntimeHealthMetric(runtime.cloudContext.cloudProvider,
                              image.imageType,
                              image.imageUrl,
                              getRuntimeUI(runtime.labels),
                              getAzureCloudContext(runtime.cloudContext),
                              isUp
          ) -> 1d,
          RuntimeHealthMetric(runtime.cloudContext.cloudProvider,
                              image.imageType,
                              image.imageUrl,
                              getRuntimeUI(runtime.labels),
                              getAzureCloudContext(runtime.cloudContext),
                              !isUp
          ) -> 0d
        )
      }
      .map(_.combineAll)
  }

  private[monitor] def getNodepoolSize(
    allClusters: List[KubernetesCluster]
  )(implicit ev: Ask[F, AppContext]): F[Map[NodepoolSizeMetric, Double]] = {
    val allApps = for {
      // TODO: handle GCP
      c <- allClusters if c.asyncFields.isDefined && c.cloudContext.cloudProvider == CloudProvider.Azure
      n <- c.nodepools
      // Only care about Running apps for resource metrics
      a <- n.apps if a.status == AppStatus.Running
    } yield Map(c.cloudContext -> List(a))

    allApps.combineAll.toList
      .parTraverseN(parallelism) { case (cloudContext, apps) =>
        for {
          ctx <- ev.ask
          // Get LZ resources to obtain the AKS cluster name
          // TODO: seems silly to do all this to query the LZ; we should just store the name.
          headApp = apps.head
          userTokenOpt <- samDAO.getCachedArbitraryPetAccessToken(headApp.auditInfo.creator)
          userToken <- F.fromOption(
            userTokenOpt,
            AppCreationException(s"Pet not found for user ${headApp.auditInfo.creator}", Some(ctx.traceId))
          )
          authHeader = org.http4s.headers.Authorization(
            org.http4s.Credentials.Token(AuthScheme.Bearer, userToken)
          )
          workspaceId = headApp.workspaceId.get
          workspaceDescOpt <- wsmDAO.getWorkspace(workspaceId, authHeader)
          workspaceDesc <- F.fromOption(workspaceDescOpt, WorkspaceNotFoundException(workspaceId, ctx.traceId))
          leoAuth <- samDAO.getLeoAuthToken
          landingZoneResources <- wsmDAO.getLandingZoneResources(workspaceDesc.spendProfile, leoAuth).attempt
          clusterName = landingZoneResources.map(_.clusterName)
          res <- clusterName match {
            case Left(_) =>
              logger
                .info(s"Landing zone not found for MRG ${workspaceId}. Skipping")
                .as(Map.empty[NodepoolSizeMetric, Double])
            case Right(clusterName) =>
              for {
                azureCloudContext <- F.fromOption(cloudContextToManagedResourceGroup.get(cloudContext),
                                                  CloudContextNotFoundException(workspaceId, ctx.traceId)
                )
                cluster <- azureContainerService.getCluster(clusterName, azureCloudContext)
                res = cluster.agentPools().asScala.toList.map { case (name, pool) =>
                  Map(NodepoolSizeMetric(azureCloudContext, name) -> pool.count.doubleValue)
                }
                _ <- logger.info(s"YYY res is $res")
              } yield res.combineAll
          }
        } yield res
      }
      .map(_.combineAll)
  }

  private[monitor] def getAppK8sResources(allClusters: List[KubernetesCluster])(implicit
    ev: Ask[F, AppContext]
  ): F[Map[AppResourcesMetric, Double]] = {
    val allServices = for {
      // TODO: handle GCP
      c <- allClusters if c.asyncFields.isDefined && c.cloudContext.cloudProvider == CloudProvider.Azure
      n <- c.nodepools
      // Only care about Running apps for resource metrics
      a <- n.apps if a.status == AppStatus.Running
    } yield Map(c.cloudContext -> List(a))

    allServices.combineAll.toList
      .parTraverseN(parallelism) { case (cloudContext, apps) =>
        for {
          ctx <- ev.ask

          // Get LZ resources to obtain the AKS cluster name
          // TODO: seems silly to do all this to query the LZ; we should just store the name.
          headApp = apps.head
          userTokenOpt <- samDAO.getCachedArbitraryPetAccessToken(headApp.auditInfo.creator)
          userToken <- F.fromOption(
            userTokenOpt,
            AppCreationException(s"Pet not found for user ${headApp.auditInfo.creator}", Some(ctx.traceId))
          )
          authHeader = org.http4s.headers.Authorization(
            org.http4s.Credentials.Token(AuthScheme.Bearer, userToken)
          )
          workspaceId = headApp.workspaceId.get
          workspaceDescOpt <- wsmDAO.getWorkspace(workspaceId, authHeader)
          workspaceDesc <- F.fromOption(workspaceDescOpt, WorkspaceNotFoundException(workspaceId, ctx.traceId))
          leoAuth <- samDAO.getLeoAuthToken
          landingZoneResources <- wsmDAO.getLandingZoneResources(workspaceDesc.spendProfile, leoAuth).attempt
          clusterName = landingZoneResources.map(_.clusterName)
          res <- clusterName match {
            case Left(_) =>
              logger
                .info(s"Landing zone not found for MRG ${workspaceId}. Skipping")
                .as(Map.empty[AppResourcesMetric, Double])
            case Right(clusterName) =>
              for {
                // Build k8s client
                azureCloudContext <- F.fromOption(cloudContextToManagedResourceGroup.get(cloudContext),
                                                  CloudContextNotFoundException(workspaceId, ctx.traceId)
                )
                _ <- logger.info(s"XXX Got azure cloud context: ${azureCloudContext.asString}")
                client <- kubeAlg.createAzureClient(azureCloudContext, clusterName)
                _ <- logger.info("XXX Created kube client")

                // For each app, query pods with field selector
                // field selector leoAppName=...
                res <- apps.traverse { app =>
                  val namespace = app.appResources.namespace
                  val labelSelector = s"leoAppName=${app.appName.value}"
                  // (String namespace, String pretty, Boolean allowWatchBookmarks, String _continue, String fieldSelector, String labelSelector, Integer limit, String resourceVersion, String resourceVersionMatch, Integer timeoutSeconds, Boolean watch) throws ApiException {
                  for {
                    pods <- F.delay(
                      client.listNamespacedPod(namespace.name.value,
                                               null,
                                               null,
                                               null,
                                               null,
                                               labelSelector,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null
                      )
                    )
                    _ <- logger.info(s"XXX listed ${pods.getItems.size()} pods")
                    res = pods.getItems.asScala.flatMap { pod =>
                      pod.getMetadata.getLabels.asScala.get("leoServiceName").toList.flatMap { service =>
                        pod.getSpec.getContainers.asScala.flatMap { container =>
                          Option(container.getResources)
                            .flatMap(r => Option(r.getRequests))
                            .map(_.asScala.toList)
                            .getOrElse(List.empty)
                            .map { case (resource, quantity) =>
                              Map(
                                AppResourcesMetric(cloudContext.cloudProvider,
                                                   app.appType,
                                                   ServiceName(service),
                                                   getRuntimeUI(app.labels),
                                                   getAzureCloudContext(cloudContext),
                                                   "request",
                                                   resource
                                ) -> quantity.getNumber.doubleValue() // TODO are units consistent?
                              )
                            }
                        }
                      }
                    }
                    _ <- logger.info(s"XXX Result is ${res}")
                  } yield res.toList.combineAll
                }
              } yield res.combineAll

          }
        } yield res
      }
      .map(_.combineAll)
  }

  /** Records and logs a generic AppMetric */
  private[monitor] def recordMetric[T <: LeoMetric](
    appMetric: Map[T, Double]
  )(implicit ev: Ask[F, AppContext]): F[Unit] =
    appMetric.toList
      .parTraverseN(parallelism) { case (metric, count) =>
        for {
          ctx <- ev.ask
          _ <- metrics.gauge(
            metric.name,
            count,
            metric.tags
          )
          _ <- logger.debug(ctx.loggingCtx)(s"Recorded metric: ${metric.name}, tags: ${metric.tags}, value: ${count}")
        } yield ()
      }
      .void

  private def getRuntimeUI(labels: LabelMap): RuntimeUI =
    if (labels.contains(Config.uiConfig.terraLabel)) RuntimeUI.Terra
    else if (labels.contains(Config.uiConfig.allOfUsLabel)) RuntimeUI.AoU
    else RuntimeUI.Other

  private def getAzureCloudContext(cloudContext: CloudContext): Option[AzureCloudContext] =
    (config.includeAzureCloudContext, cloudContext) match {
      case (true, CloudContext.Azure(cc)) => Some(cc)
      case _                              => None
    }
}

case class LeoMetricsMonitorConfig(enabled: Boolean, checkInterval: FiniteDuration, includeAzureCloudContext: Boolean)

sealed trait LeoMetric {
  def name: String
  def tags: Map[String, String]
}
object LeoMetric {
  final case class AppStatusMetric(cloudProvider: CloudProvider,
                                   appType: AppType,
                                   status: AppStatus,
                                   runtimeUI: RuntimeUI,
                                   azureCloudContext: Option[AzureCloudContext],
                                   chart: Chart
  ) extends LeoMetric {
    override def name: String = "leoAppStatus"
    override def tags: Map[String, String] =
      Map(
        "cloudProvider" -> cloudProvider.asString,
        "appType" -> appType.toString,
        "status" -> status.toString,
        "uiClient" -> runtimeUI.asString,
        "azureCloudContext" -> azureCloudContext.map(_.asString).getOrElse(""),
        "chart" -> chart.toString
      )
  }

  final case class AppHealthMetric(cloudProvider: CloudProvider,
                                   appType: AppType,
                                   serviceName: ServiceName,
                                   runtimeUI: RuntimeUI,
                                   azureCloudContext: Option[AzureCloudContext],
                                   isUp: Boolean,
                                   chart: Chart
  ) extends LeoMetric {
    override def name: String = "leoAppHealth"
    override def tags: Map[String, String] = Map(
      "cloudProvider" -> cloudProvider.asString,
      "appType" -> appType.toString,
      "serviceName" -> serviceName.value,
      "uiClient" -> runtimeUI.asString,
      "isUp" -> isUp.toString,
      "azureCloudContext" -> azureCloudContext.map(_.asString).getOrElse(""),
      "chart" -> chart.toString
    )
  }

  final case class AppResourcesMetric(cloudProvider: CloudProvider,
                                      appType: AppType,
                                      serviceName: ServiceName,
                                      runtimeUI: RuntimeUI,
                                      azureCloudContext: Option[AzureCloudContext],
                                      requestOrLimit: String,
                                      k8sResource: String
  ) extends LeoMetric {
    override def name: String = "leoAppResources"
    override def tags: Map[String, String] = Map(
      "cloudProvider" -> cloudProvider.asString,
      "appType" -> appType.toString,
      "serviceName" -> serviceName.value,
      "uiClient" -> runtimeUI.asString,
      "azureCloudContext" -> azureCloudContext.map(_.asString).getOrElse(""),
      "requestOrLimit" -> requestOrLimit,
      "k8sResource" -> k8sResource
    )
  }

  final case class NodepoolSizeMetric(azureCloudContext: AzureCloudContext, nodepoolName: String) extends LeoMetric {
    override def name: String = "leoNodepoolSize"
    override def tags: Map[String, String] = Map(
      "azureCloudContext" -> azureCloudContext.asString,
      "nodepoolName" -> nodepoolName
    )
  }

  final case class RuntimeStatusMetric(cloudProvider: CloudProvider,
                                       imageType: RuntimeImageType,
                                       imageUrl: String,
                                       status: RuntimeStatus,
                                       runtimeUI: RuntimeUI,
                                       azureCloudContext: Option[AzureCloudContext]
  ) extends LeoMetric {
    override def name: String = "leoRuntimeStatus"
    override def tags: Map[String, String] =
      Map(
        "cloudProvider" -> cloudProvider.asString,
        "imageType" -> imageType.toString,
        "imageUrl" -> imageUrl,
        "status" -> status.toString,
        "uiClient" -> runtimeUI.asString,
        "azureCloudContext" -> azureCloudContext.map(_.asString).getOrElse("")
      )
  }

  final case class RuntimeHealthMetric(cloudProvider: CloudProvider,
                                       imageType: RuntimeImageType,
                                       imageUrl: String,
                                       runtimeUI: RuntimeUI,
                                       azureCloudContext: Option[AzureCloudContext],
                                       isUp: Boolean
  ) extends LeoMetric {
    override def name: String = "leoRuntimeHealth"
    override def tags: Map[String, String] =
      Map(
        "cloudProvider" -> cloudProvider.asString,
        "imageType" -> imageType.toString,
        "imageUrl" -> imageUrl,
        "uiClient" -> runtimeUI.asString,
        "isUp" -> isUp.toString,
        "azureCloudContext" -> azureCloudContext.map(_.asString).getOrElse("")
      )
  }

}
