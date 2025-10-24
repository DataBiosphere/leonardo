package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.effect.implicits.concurrentParTraverseOps
import cats.mtl.Ask
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoMetric._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/** Collects metrics about active Leo runtimes and apps. */
class LeoMetricsMonitor[F[_]](config: LeoMetricsMonitorConfig, appDAO: AppDAO[F])(implicit
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
        a.chart
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
      // Only care about Jupyter or RStudio image types.
      // Assume every runtime has exactly 1 of these.
      imageTypes = Set(RuntimeImageType.Jupyter, RuntimeImageType.RStudio)
      c <- r.images.filter(i => imageTypes.contains(i.imageType)).headOption
    } yield Map(
      RuntimeStatusMetric(r.cloudContext.cloudProvider, c.imageType, c.imageUrl, r.status, getRuntimeUI(r.labels)) -> 1d
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
    } yield (c.cloudContext, a, s.config.name)

    allServices
      .parTraverseN(parallelism) { case (cloudContext, app, serviceName) =>
        for {
          ctx <- ev.ask
          // For GCP just test if the app is available through the Leo proxy.
          isUp <- cloudContext match {
            case CloudContext.Gcp(project) =>
              appDAO.isProxyAvailable(project, app.appName, serviceName, ctx.traceId)
            case _ =>
              logger.warn(ctx.loggingCtx)(
                s"Unexpected cloud context encountered during health checks"
              ) >> F.pure(false)
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
            isUp,
            app.chart
          ) -> 1d,
          AppHealthMetric(
            cloudContext.cloudProvider,
            app.appType,
            serviceName,
            getRuntimeUI(app.labels),
            !isUp,
            app.chart
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
                              isUp
          ) -> 1d,
          RuntimeHealthMetric(runtime.cloudContext.cloudProvider,
                              image.imageType,
                              image.imageUrl,
                              getRuntimeUI(runtime.labels),
                              !isUp
          ) -> 0d
        )
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

}

case class LeoMetricsMonitorConfig(enabled: Boolean, checkInterval: FiniteDuration)

sealed trait LeoMetric {
  def name: String
  def tags: Map[String, String]
}
object LeoMetric {
  final case class AppStatusMetric(cloudProvider: CloudProvider,
                                   appType: AppType,
                                   status: AppStatus,
                                   runtimeUI: RuntimeUI,
                                   chart: Chart
  ) extends LeoMetric {
    override def name: String = "leoAppStatus"
    override def tags: Map[String, String] =
      Map(
        "cloudProvider" -> cloudProvider.asString,
        "appType" -> appType.toString,
        "status" -> status.toString,
        "uiClient" -> runtimeUI.asString,
        "chart" -> chart.toString
      )
  }

  final case class AppHealthMetric(cloudProvider: CloudProvider,
                                   appType: AppType,
                                   serviceName: ServiceName,
                                   runtimeUI: RuntimeUI,
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
      "chart" -> chart.toString
    )
  }

  final case class RuntimeStatusMetric(cloudProvider: CloudProvider,
                                       imageType: RuntimeImageType,
                                       imageUrl: String,
                                       status: RuntimeStatus,
                                       runtimeUI: RuntimeUI
  ) extends LeoMetric {
    override def name: String = "leoRuntimeStatus"
    override def tags: Map[String, String] =
      Map(
        "cloudProvider" -> cloudProvider.asString,
        "imageType" -> imageType.toString,
        "imageUrl" -> imageUrl,
        "status" -> status.toString
      )
  }

  final case class RuntimeHealthMetric(cloudProvider: CloudProvider,
                                       imageType: RuntimeImageType,
                                       imageUrl: String,
                                       runtimeUI: RuntimeUI,
                                       isUp: Boolean
  ) extends LeoMetric {
    override def name: String = "leoRuntimeHealth"
    override def tags: Map[String, String] =
      Map(
        "cloudProvider" -> cloudProvider.asString,
        "imageType" -> imageType.toString,
        "imageUrl" -> imageUrl,
        "uiClient" -> runtimeUI.asString,
        "isUp" -> isUp.toString
      )
  }

}
