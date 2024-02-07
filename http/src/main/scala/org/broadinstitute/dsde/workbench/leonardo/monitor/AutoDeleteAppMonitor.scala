package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.Show
import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.config.AutoDeleteConfig
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.model.SamResource.AppSamResource
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.DeleteAppMessage
import org.broadinstitute.dsde.workbench.leonardo.{
  AllowedChartName,
  AppId,
  AppName,
  AppStatus,
  CloudContext,
  SamResourceId
}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsp.ChartName
import org.typelevel.log4cats.StructuredLogger

import java.time.Instant
import scala.concurrent.ExecutionContext

/**
 * This monitor periodically sweeps the Leo database and autodelete App that have been running for too long.
 */
class AutoDeleteAppMonitor[F[_]](
  config: AutoDeleteConfig,
  publisherQueue: Queue[F, LeoPubsubMessage],
  authProvider: LeoAuthProvider[F]
)(implicit
  dbRef: DbReference[F],
  logger: StructuredLogger[F],
  ec: ExecutionContext,
  openTelemetry: OpenTelemetryMetrics[F]
) extends BackgroundProcess[F, AppToAutoDelete] {
  override def name: String =
    "autodeleteApp" //
  override def interval: scala.concurrent.duration.FiniteDuration = config.autodeleteCheckInterval

  override def getCandidates(now: Instant)(implicit
    F: Async[F],
    metrics: OpenTelemetryMetrics[F],
    logger: StructuredLogger[F]
  ): F[Seq[AppToAutoDelete]] = for {
    candidates <- appQuery.getAppsReadyToAutoDelete.transaction
  } yield candidates

  override def action(a: AppToAutoDelete, traceId: TraceId, now: Instant)(implicit F: Async[F]): F[Unit] =
    for {
      now <- F.realTimeInstant
      loggingCtx = Map("traceId" -> traceId.asString)
      _ <- a.cloudContext match {
        case CloudContext.Gcp(googleProject) =>
          if (a.appStatus == AppStatus.Error) {
            implicit val implicitTraceId: Ask[F, TraceId] = Ask.const[F, TraceId](traceId)
            for {
              // we only need to delete Sam record for clusters in Google. Sam record for Azure is managed by WSM
              _ <- authProvider.notifyResourceDeleted(
                a.samResourceId,
                a.creator,
                googleProject
              )
              _ <- appQuery.markAsDeleted(a.id, now).transaction
            } yield ()
          } else {
            for {
              _ <- KubernetesServiceDbQueries.markPreDeleting(a.id).transaction
              deleteMessage = DeleteAppMessage(
                a.id,
                a.appName,
                googleProject,
                None,
                Some(traceId)
              )
              trackUsage = AllowedChartName.fromChartName(a.chartName).exists(_.trackUsage)
              _ <- appUsageQuery.recordStop(a.id, now).whenA(trackUsage).recoverWith { case e: FailToRecordStoptime =>
                logger.error(loggingCtx)(e.getMessage)
              }
              _ <- publisherQueue.offer(deleteMessage)
            } yield ()
          }
        case CloudContext.Azure(_) => logger.info(loggingCtx)("Azure is not supported")
      }
    } yield ()
}

object AutoDeleteAppMonitor {
  def process[F[_]](config: AutoDeleteConfig,
                    publisherQueue: Queue[F, LeoPubsubMessage],
                    authProvider: LeoAuthProvider[F]
  )(implicit
    dbRef: DbReference[F],
    ec: ExecutionContext,
    F: Async[F],
    openTelemetry: OpenTelemetryMetrics[F],
    logger: StructuredLogger[F]
  ): Stream[F, Unit] = {
    val autodeleteAppMonitor = apply(config, publisherQueue, authProvider)

    implicit val appToAutoDeleteShowInstance: Show[AppToAutoDelete] =
      Show[AppToAutoDelete](appToAutoDelete => appToAutoDelete.projectNameString)
    autodeleteAppMonitor.process
  }

  private def apply[F[_]](config: AutoDeleteConfig,
                          publisherQueue: Queue[F, LeoPubsubMessage],
                          authProvider: LeoAuthProvider[F]
  )(implicit
    dbRef: DbReference[F],
    ec: ExecutionContext,
    logger: StructuredLogger[F],
    openTelemetry: OpenTelemetryMetrics[F]
  ): AutoDeleteAppMonitor[F] =
    new AutoDeleteAppMonitor(config, publisherQueue, authProvider)
}

final case class AppToAutoDelete(id: AppId,
                                 appName: AppName,
                                 appStatus: AppStatus,
                                 samResourceId: SamResourceId.AppSamResourceId,
                                 creator: WorkbenchEmail,
                                 chartName: ChartName,
                                 cloudContext: CloudContext
) {
  def projectNameString: String = s"${cloudContext}/${appName}"
}
