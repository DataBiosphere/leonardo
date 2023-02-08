package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.effect.implicits.concurrentParTraverseOps
import cats.mtl.Ask
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.dao.{ToolDAO, _}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.leonardo.http.{dbioToIO, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoMetric._
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials, Uri}
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/** Monitors metrics about runtime and app deployments. */
class LeoMetricsMonitor[F[_]](config: LeoMetricsMonitorConfig,
                              appDAO: AppDAO[F],
                              wdsDAO: WdsDAO[F],
                              cbasDAO: CbasDAO[F],
                              cbasUiDAO: CbasUiDAO[F],
                              cromwellDAO: CromwellDAO[F],
                              samDAO: SamDAO[F]
)(implicit
  F: Async[F],
  dbRef: DbReference[F],
  metrics: OpenTelemetryMetrics[F],
  logger: StructuredLogger[F],
  clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[F, RuntimeContainerServiceType],
  ec: ExecutionContext
) {
  private val parallelism = 10

  /** Entry point of this class; starts the async process */
  val process: Stream[F, Unit] =
    (Stream.sleep[F](config.checkInterval) ++ Stream.eval(
      retrieveMetrics
        .handleErrorWith(e => logger.error(e)("Unexpected error occurred during metric monitoring"))
    )).repeat

  /** Queries for all apps and runtimes in the DB and collects metrics. */
  private[monitor] def retrieveMetrics: F[Unit] =
    for {
      now <- F.realTimeInstant
      traceId = TraceId(s"AppMetricsMonitor_${now.toEpochMilli}")
      implicit0(appContext: Ask[F, AppContext]) = Ask.const[F, AppContext](AppContext(traceId, now))
      _ <- retrieveAppMetrics
      _ <- retrieveRuntimeMetrics
    } yield ()

  /** Processes apps. */
  private[monitor] def retrieveAppMetrics(implicit ev: Ask[F, AppContext]): F[Unit] = for {
    clusters <- KubernetesServiceDbQueries.listAppsForMetrics.transaction
    appDbStatus = countAppsByDbStatus(clusters)
    _ <- recordMetric(appDbStatus)
    appHealth <- countAppsByHealth(clusters)
    _ <- recordMetric(appHealth)
  } yield ()

  /** Processes runtimes. */
  private[monitor] def retrieveRuntimeMetrics(implicit ev: Ask[F, AppContext]): F[Unit] = for {
    runtimeSeq <- clusterQuery.listActiveForMetrics.transaction
    runtimes = runtimeSeq.toList
    runtimeDbStatus = countRuntimesByDbStatus(runtimes)
    _ <- recordMetric(runtimeDbStatus)
    runtimeHealth <- countRuntimesByHealth(runtimes)
    _ <- recordMetric(runtimeHealth)
  } yield ()

  /** Counts apps by (cloudProvider, appType, dbStatus) */
  private[monitor] def countAppsByDbStatus(
    allClusters: List[KubernetesCluster]
  ): Map[AppDbStatus, Int] = {
    val allApps = for {
      c <- allClusters
      n <- c.nodepools
      a <- n.apps
    } yield Map(AppDbStatus(c.cloudContext.cloudProvider, a.appType, a.status) -> 1)

    allApps.combineAll
  }

  /** Performs health checks for Running apps, and counts apps by (cloudProvider, appType, serviceName, isUp) */
  private[monitor] def countAppsByHealth(
    allClusters: List[KubernetesCluster]
  )(implicit ev: Ask[F, AppContext]): F[Map[AppHealth, Int]] = {
    val allServices = for {
      c <- allClusters if c.asyncFields.isDefined
      n <- c.nodepools
      // Only care about Running apps for health check metrics
      a <- n.apps if a.status == AppStatus.Running
      s <- a.appResources.services
    } yield (c.cloudContext, c.asyncFields.get.loadBalancerIp, a.appName, a.appType, a.auditInfo.creator, s.config.name)

    allServices
      .parTraverseN(parallelism) { case (cloudContext, baseUri, appName, appType, userEmail, serviceName) =>
        for {
          ctx <- ev.ask
          // For GCP just test the app is available through the Leo proxy.
          // For Azure impersonate the user and call the app's status endpoint via Azure Relay.
          isUp <- cloudContext match {
            case CloudContext.Gcp(project) => appDAO.isProxyAvailable(project, appName, serviceName)
            case CloudContext.Azure(_) =>
              for {
                tokenOpt <- samDAO.getCachedArbitraryPetAccessToken(userEmail)
                token <- F.fromOption(tokenOpt,
                                      AppCreationException(s"Pet not found for user ${userEmail}", Some(ctx.traceId))
                )
                authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))
                relayPath = Uri.unsafeFromString(baseUri.asString) / appName.value
                isUp <- serviceName match {
                  case ServiceName("wds")      => wdsDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case ServiceName("cbas")     => cbasDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case ServiceName("cbas-ui")  => cbasUiDAO.getStatus(relayPath, authHeader).handleError(_ => false)
                  case ServiceName("cromwell") => cromwellDAO.getStatus(relayPath, authHeader).handleError(_ => false)
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
              logger.warn(ctx.loggingCtx)(
                s"App is DOWN with cloudContext={${cloudContext.asStringWithProvider}}, name={${appName.value}}, type={${appType.toString}}, service={${serviceName.value}}"
              )
        } yield Map(AppHealth(cloudContext.cloudProvider, appType, serviceName, isUp) -> 1,
                    AppHealth(cloudContext.cloudProvider, appType, serviceName, !isUp) -> 0
        )
      }
      .map(_.combineAll)
  }

  /** Counts runtimes by (cloudProvider, imageType, status). */
  private[monitor] def countRuntimesByDbStatus(allRuntimes: List[RuntimeContainers]): Map[RuntimeDbStatus, Int] = {
    val allContainers = for {
      r <- allRuntimes
      c <- r.containers if c != RuntimeContainerServiceType.WelderService
    } yield Map(RuntimeDbStatus(r.cloudContext.cloudProvider, c.imageType, r.status) -> 1)

    allContainers.combineAll
  }

  /** Performs health checks for Running runtimes, and counts runtimes by (cloudProvider, imageType, isUp) */
  private[monitor] def countRuntimesByHealth(
    allRuntimes: List[RuntimeContainers]
  )(implicit ev: Ask[F, AppContext]): F[Map[RuntimeHealth, Int]] = {
    val allContainers = for {
      // Only care about Running runtimes for health checks
      r <- allRuntimes if r.status == RuntimeStatus.Running
      c <- r.containers
    } yield (r, c)

    allContainers
      .parTraverseN(parallelism) { case (runtime, container) =>
        for {
          ctx <- ev.ask
          isUp <- container.isProxyAvailable(runtime.cloudContext, runtime.runtimeName)
          // In addition to collecting aggregate metrics, log a warning for any runtime that is down.
          _ <-
            if (isUp) F.unit
            else
              logger.warn(ctx.loggingCtx)(
                s"Runtime container is DOWN with cloudContext={${runtime.cloudContext.asStringWithProvider}}, name={${runtime.runtimeName.asString}}, type={${container.imageType.toString}}"
              )
        } yield Map(
          RuntimeHealth(runtime.cloudContext.cloudProvider, container.imageType, isUp) -> 1,
          RuntimeHealth(runtime.cloudContext.cloudProvider, container.imageType, !isUp) -> 0
        )
      }
      .map(_.combineAll)
  }

  /** Records and logs a generic AppMetric */
  private[monitor] def recordMetric[T <: LeoMetric](
    appMetric: Map[T, Int]
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
          _ <- logger.info(ctx.loggingCtx)(s"Recorded metric: ${metric.name}, tags: ${metric.tags}, value: ${count}")
        } yield ()
      }
      .void
}

case class LeoMetricsMonitorConfig(checkInterval: FiniteDuration)

sealed trait LeoMetric {
  def name: String
  def tags: Map[String, String]
}
object LeoMetric {
  final case class AppDbStatus(cloudProvider: CloudProvider, appType: AppType, status: AppStatus) extends LeoMetric {
    override def name: String = "appDbStatus"
    override def tags: Map[String, String] =
      Map("cloudProvider" -> cloudProvider.asString, "appType" -> appType.toString, "status" -> status.toString)
  }

  final case class AppHealth(cloudProvider: CloudProvider, appType: AppType, serviceName: ServiceName, isUp: Boolean)
      extends LeoMetric {
    override def name: String = "appHealth"
    override def tags: Map[String, String] = Map("cloudProvider" -> cloudProvider.asString,
                                                 "appType" -> appType.toString,
                                                 "serviceName" -> serviceName.value,
                                                 "isUp" -> isUp.toString
    )
  }

  final case class RuntimeDbStatus(cloudProvider: CloudProvider, imageType: RuntimeImageType, status: RuntimeStatus)
      extends LeoMetric {
    override def name: String = "runtimeDbStatus"
    override def tags: Map[String, String] =
      Map("cloudProvider" -> cloudProvider.asString, "imageType" -> imageType.toString, "status" -> status.toString)
  }

  final case class RuntimeHealth(cloudProvider: CloudProvider, imageType: RuntimeImageType, isUp: Boolean)
      extends LeoMetric {
    override def name: String = "runtimeHealth"
    override def tags: Map[String, String] =
      Map("cloudProvider" -> cloudProvider.asString, "imageType" -> imageType.toString, "isUp" -> isUp.toString)
  }
}
