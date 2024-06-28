package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.effect.implicits.concurrentParTraverseOps
import cats.mtl.Ask
import cats.syntax.all._
import fs2.Stream
import io.kubernetes.client.custom.Quantity
import org.broadinstitute.dsde.workbench.azure.{AKSClusterName, AzureCloudContext, AzureContainerService}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.LeoLenses.cloudContextToManagedResourceGroup
import org.broadinstitute.dsde.workbench.leonardo.config.{Config, KubernetesAppConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{ToolDAO, _}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference, KubernetesServiceDbQueries}
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
                              cromwellDAO: CromwellDAO[F],
                              hailBatchDAO: HailBatchDAO[F],
                              jupyterDAO: JupyterDAO[F],
                              listenerDAO: ListenerDAO[F],
                              samDAO: SamDAO[F],
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
  private val parallelism = 40

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
    _ <- logger.info(s"Retrieving app metrics...")
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
    _ <- logger.info(s"Retrieving runtime metrics...")
    runtimeSeq <- clusterQuery.listActiveForMetrics.transaction
    runtimes = runtimeSeq.toList
    runtimeDbStatus = countRuntimesByDbStatus(runtimes)
    _ <- recordMetric(runtimeDbStatus)
    _ <- logger.info(s"Recorded status metrics for ${runtimeDbStatus.size} runtimes")
    runtimeHealth <- countRuntimesByHealth(runtimes)
    _ <- recordMetric(runtimeHealth)
    _ <- logger.info(s"Recorded health metrics for ${runtimeHealth.size} runtimes")
  } yield ()

  /** Counts apps by (cloud, appType, status, chart) */
  private[monitor] def countAppsByDbStatus(
    allClusters: List[KubernetesCluster]
  ): Map[AppStatusMetric, Double] = {
    val allApps = for {
      c <- allClusters
      n <- c.nodepools
      a <- n.apps
    } yield Map(
      AppStatusMetric(
        c.cloudContext.cloudProvider,
        a.appType,
        a.status,
        getRuntimeUI(a.labels),
        getAzureCloudContext(c.cloudContext),
        a.chart,
        isUpgradeable(a.appType, c.cloudContext.cloudProvider, a.chart)
      ) -> 1d
    )

    // combineAll folds a List[Map[metric -> 1]] structure to a Map[metric -> n]
    // using Monoid instances for List, Map, Int.
    allApps.combineAll
  }

  /** Counts runtimes by (cloud, status, image) */
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

  /**
   * Performs health checks for Running apps, and counts healthy vs not-healthy
   * by (cloud, appType, chart).
   */
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
              appDAO.isProxyAvailable(project, app.appName, serviceName, ctx.traceId)
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
                  case ServiceName("cbas") => cbasDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case ServiceName("cromwell") | ServiceName("cromwell-reader") | ServiceName("cromwell-runner") =>
                    cromwellDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case ServiceName("wds")   => wdsDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case ServiceName("batch") => hailBatchDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case s if s == ConfigReader.appConfig.azure.listenerChartConfig.service.config.name =>
                    listenerDAO.getStatus(relayPath).handleError(_ => false)
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
              logger.debug(ctx.loggingCtx)(
                s"App is DOWN with " +
                  s"name={${app.appName.value}}, " +
                  s"type={${app.appType.toString}}, " +
                  s"service={${serviceName.value}}, " +
                  s"workspace={${app.workspaceId.map(_.value.toString).getOrElse("")}}, " +
                  s"cloudContext={${cloudContext.asStringWithProvider}}"
              )
        } yield Map(
          AppHealthMetric(
            cloudContext.cloudProvider,
            app.appType,
            serviceName,
            getRuntimeUI(app.labels),
            getAzureCloudContext(cloudContext),
            isUp,
            app.chart,
            isUpgradeable(app.appType, cloudContext.cloudProvider, app.chart)
          ) -> 1d,
          AppHealthMetric(
            cloudContext.cloudProvider,
            app.appType,
            serviceName,
            getRuntimeUI(app.labels),
            getAzureCloudContext(cloudContext),
            !isUp,
            app.chart,
            isUpgradeable(app.appType, cloudContext.cloudProvider, app.chart)
          ) -> 0d
        )
      }
      .map(_.combineAll)
  }

  /**
   * Performs health checks for Running runtimes, and counts healthy vs not-healthy by
   * (cloud, image).
   */
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
          isUp <- container.isProxyAvailable(runtime.cloudContext, runtime.runtimeName).handleError(_ => false)
          // In addition to collecting aggregate metrics, log a warning for any runtime that is down.
          _ <-
            if (isUp) F.unit
            else
              logger.debug(ctx.loggingCtx)(
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

  /**
   * Records the nodepool size per cluster.
   * Only AKS supported.
   */
  private[monitor] def getNodepoolSize(
    allClusters: List[KubernetesCluster]
  )(implicit ev: Ask[F, AppContext]): F[Map[NodepoolSizeMetric, Double]] = {
    // TODO: handle GCP
    val activeClusters = for {
      c <- allClusters
      // Filter out clusters whose apps have all been deleted
      if c.cloudContext.cloudProvider == CloudProvider.Azure && c.nodepools
        .flatMap(_.apps)
        .exists(a => a.status != AppStatus.Deleted)
    } yield List(c)

    activeClusters.combineAll
      .parTraverseN(parallelism) { case cluster =>
        for {
          ctx <- ev.ask
          azureCloudContext <- F.fromOption(
            cloudContextToManagedResourceGroup.get(cluster.cloudContext),
            new RuntimeException(s"Azure cloud context not found for cluster ${cluster}: ${ctx.traceId}")
          )
          clusterName = AKSClusterName(cluster.clusterName.value)
          cluster <- azureContainerService.getCluster(clusterName, azureCloudContext).attempt
          res <- cluster match {
            case Left(_) =>
              logger
                .warn(ctx.loggingCtx)(
                  s"Cluster ${azureCloudContext.asString} / ${clusterName} does not exist. Skipping metrics collection."
                )
                .as(List.empty[Map[NodepoolSizeMetric, Double]])
            case Right(c) =>
              F.delay(c.agentPools().asScala.toList.map { case (name, pool) =>
                Map(NodepoolSizeMetric(azureCloudContext, name) -> pool.count.doubleValue)
              })
          }
        } yield res.combineAll
      }
      .map(_.combineAll)
  }

  /**
   * Records memory/cpu requests/limits by (cloud, appType, service).
   * Only Azure apps supported.
   */
  private[monitor] def getAppK8sResources(allClusters: List[KubernetesCluster])(implicit
    ev: Ask[F, AppContext]
  ): F[Map[AppResourcesMetric, Double]] = {
    val allServices = for {
      // TODO: handle GCP
      c <- allClusters if c.cloudContext.cloudProvider == CloudProvider.Azure
      n <- c.nodepools
      // Only care about Running apps for resource metrics
      a <- n.apps if a.status == AppStatus.Running
    } yield Map((c.clusterName, c.cloudContext) -> List(a))

    allServices.combineAll.toList
      .parTraverseN(parallelism) { case ((clusterName, cloudContext), apps) =>
        for {
          ctx <- ev.ask

          // Build k8s client
          azureCloudContext <- F.fromOption(
            cloudContextToManagedResourceGroup.get(cloudContext),
            new RuntimeException(s"Azure cloud context not found for cluster ${clusterName}: ${ctx.traceId}")
          )
          aksClusterName = AKSClusterName(clusterName.value)
          client <- kubeAlg.createAzureClient(azureCloudContext, aksClusterName).attempt

          res <- client match {
            case Left(_) =>
              logger
                .warn(ctx.loggingCtx)(
                  s"Cluster ${azureCloudContext.asString} / ${clusterName} does not exist. Skipping metrics collection."
                )
                .as(List.empty[Map[AppResourcesMetric, Double]])
            case Right(client) =>
              // For each app, query pods by leoAppName label and services by leoServiceName label.
              // These labels are required for exposing Leo metrics.
              apps.traverse { app =>
                val namespace = app.appResources.namespace
                val labelSelector = s"leoAppName=${app.appName.value}"
                for {
                  pods <- F.blocking(
                    client.listNamespacedPod(namespace.value,
                                             null,
                                             null,
                                             null,
                                             null,
                                             labelSelector,
                                             null,
                                             null,
                                             null,
                                             null,
                                             null,
                                             null
                    )
                  )
                  res = pods.getItems.asScala.flatMap { pod =>
                    pod.getMetadata.getLabels.asScala.get("leoServiceName").toList.flatMap { service =>
                      pod.getSpec.getContainers.asScala.flatMap { container =>
                        val resources = Option(container.getResources)
                        val requests = resources.flatMap(r => Option(r.getRequests)).map(_.asScala.toList)
                        val limits = resources.flatMap(r => Option(r.getLimits)).map(_.asScala.toList)
                        val requestMetrics = buildResourcesMetric(cloudContext, app, service, "request", requests)
                        val limitMetrics = buildResourcesMetric(cloudContext, app, service, "limit", limits)
                        requestMetrics ++ limitMetrics
                      }
                    }
                  }
                } yield res.toList.combineAll
              }

          }

        } yield res.combineAll
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

  private def buildResourcesMetric(cloudContext: CloudContext,
                                   app: App,
                                   service: String,
                                   requestOrLimit: String,
                                   resources: Option[List[(String, Quantity)]]
  ): List[Map[AppResourcesMetric, Double]] =
    resources
      .map(_.map { case (resource, quantity) =>
        Map(
          AppResourcesMetric(
            cloudContext.cloudProvider,
            app.appType,
            ServiceName(service),
            getRuntimeUI(app.labels),
            getAzureCloudContext(cloudContext),
            requestOrLimit,
            resource,
            app.chart
          ) -> quantity.getNumber.doubleValue() // TODO are units consistent?
        )
      })
      .getOrElse(List.empty)

  private def isUpgradeable(appType: AppType, cloudProvider: CloudProvider, chart: Chart): Boolean =
    KubernetesAppConfig.configForTypeAndCloud(appType, cloudProvider).exists { config =>
      !config.chartVersionsToExcludeFromUpdates.contains(chart.version)
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
                                   chart: Chart,
                                   upgradeable: Boolean
  ) extends LeoMetric {
    override def name: String = "leoAppStatus"
    override def tags: Map[String, String] =
      Map(
        "cloudProvider" -> cloudProvider.asString,
        "appType" -> appType.toString,
        "status" -> status.toString,
        "uiClient" -> runtimeUI.asString,
        "azureCloudContext" -> azureCloudContext.map(_.asString).getOrElse(""),
        "chart" -> chart.toString,
        "upgradeable" -> upgradeable.toString
      )
  }

  final case class AppHealthMetric(cloudProvider: CloudProvider,
                                   appType: AppType,
                                   serviceName: ServiceName,
                                   runtimeUI: RuntimeUI,
                                   azureCloudContext: Option[AzureCloudContext],
                                   isUp: Boolean,
                                   chart: Chart,
                                   upgradeable: Boolean
  ) extends LeoMetric {
    override def name: String = "leoAppHealth"
    override def tags: Map[String, String] = Map(
      "cloudProvider" -> cloudProvider.asString,
      "appType" -> appType.toString,
      "serviceName" -> serviceName.value,
      "uiClient" -> runtimeUI.asString,
      "isUp" -> isUp.toString,
      "azureCloudContext" -> azureCloudContext.map(_.asString).getOrElse(""),
      "chart" -> chart.toString,
      "upgradeable" -> upgradeable.toString
    )
  }

  final case class AppResourcesMetric(cloudProvider: CloudProvider,
                                      appType: AppType,
                                      serviceName: ServiceName,
                                      runtimeUI: RuntimeUI,
                                      azureCloudContext: Option[AzureCloudContext],
                                      requestOrLimit: String,
                                      k8sResource: String,
                                      chart: Chart
  ) extends LeoMetric {
    override def name: String = "leoAppResources"
    override def tags: Map[String, String] = Map(
      "cloudProvider" -> cloudProvider.asString,
      "appType" -> appType.toString,
      "serviceName" -> serviceName.value,
      "uiClient" -> runtimeUI.asString,
      "azureCloudContext" -> azureCloudContext.map(_.asString).getOrElse(""),
      "requestOrLimit" -> requestOrLimit,
      "k8sResource" -> k8sResource,
      "chart" -> chart.toString
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
