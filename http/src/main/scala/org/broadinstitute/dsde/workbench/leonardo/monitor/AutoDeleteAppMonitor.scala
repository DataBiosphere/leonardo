package org.broadinstitute.dsde.workbench.leonardo.monitor

import fs2.Stream
import cats.effect.Async
import cats.effect.std.Queue
import cats.syntax.all._
import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.leonardo.config.AutoDeleteConfig
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{ctxConversion, dbioToIO}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import cats.Show
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.http.ctxConversion
import org.broadinstitute.dsde.workbench.leonardo.model.SamResource.AppSamResource
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.DeleteAppMessage
import org.broadinstitute.dsde.workbench.leonardo.{AllowedChartName, AppContext, AppId, AppName, AppStatus, CloudContext, LeoLenses, SamResourceId}
import org.broadinstitute.dsp.ChartName

import java.time.Instant
import scala.concurrent.ExecutionContext

/**
 * This monitor periodically sweeps the Leo database and auto pause clusters that have been running for too long.
 */
class AutoDeleteAppMonitor[F[_]](
  config: AutoDeleteConfig,
  publisherQueue: Queue[F, LeoPubsubMessage],
  authProvider: LeoAuthProvider[F]
)(implicit
  dbRef: DbReference[F],
  logger: StructuredLogger[F],
  ec: ExecutionContext,
  F: Async[F],
  openTelemetry: OpenTelemetryMetrics[F]
) extends BackgroundProcess[F, AppToAutoDelete] {
  override def name: String =
    "autoDeleteApp" //
  override def interval: scala.concurrent.duration.FiniteDuration = config.autoDeleteCheckInterval

  override def getCandidates(now: Instant)(implicit
    F: Async[F],
    metrics: OpenTelemetryMetrics[F],
    logger: StructuredLogger[F]
  ): F[Seq[AppToAutoDelete]] = for {
    candidates <- appQuery.getAppsReadyToAutoDelete.transaction
  } yield candidates

  override def action(a: AppToAutoDelete, traceId: TraceId, now: Instant)(implicit as: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- as.ask[AppContext]
      _ <- a.cloudContext match {
        case CloudContext.Gcp(googleProject) => if (a.appStatus == AppStatus.Error) {
          for {
            // we only need to delete Sam record for clusters in Google. Sam record for Azure is managed by WSM
            _ <- authProvider.notifyResourceDeleted(
              a.samResourceId,
              a.creator,
              googleProject
            )
            _ <- appQuery.markAsDeleted(a.id, ctx.now).transaction
          } yield ()
        } else {
          for {
            _ <- KubernetesServiceDbQueries.markPreDeleting(a.id).transaction
            deleteMessage = DeleteAppMessage(
              a.id,
              a.appName,
              googleProject,
              None,
              Some(ctx.traceId)
            )
            trackUsage = AllowedChartName.fromChartName(a.chartName).exists(_.trackUsage)
            _ <- appUsageQuery.recordStop(a.id, ctx.now).whenA(trackUsage).recoverWith {
              case e: FailToRecordStoptime => logger.error(ctx.loggingCtx)(e.getMessage)
            }
            _ <- publisherQueue.offer(deleteMessage)
          } yield ()
        }
        case CloudContext.Azure(_) => logger.info(ctx.loggingCtx)("Azure is not supported")
      }
    } yield ()
}

object AutoDeleteAppMonitor {
  def process[F[_]](config: AutoDeleteConfig, publisherQueue: Queue[F, LeoPubsubMessage], authProvider: LeoAuthProvider[F])(
    implicit
    dbRef: DbReference[F],
    ec: ExecutionContext,
    F: Async[F],
    openTelemetry: OpenTelemetryMetrics[F],
    logger: StructuredLogger[F]
  ): Stream[F, Unit] = {
    val autoDeleteAppMonitor = apply(config,  publisherQueue, authProvider)

    implicit val appToAutoDeleteShowInstance: Show[AppToAutoDelete] =
      Show[AppToAutoDelete](appToAutoDelete => appToAutoDelete.projectNameString)
    autoDeleteAppMonitor.process
  }

  private def apply[F[_]](config: AutoDeleteConfig,
                          publisherQueue: Queue[F, LeoPubsubMessage],
                          authProvider: LeoAuthProvider[F]
  )(implicit
    dbRef: DbReference[F],
    ec: ExecutionContext, logger: StructuredLogger[F], F: Async[F],openTelemetry: OpenTelemetryMetrics[F]
  ): AutoDeleteAppMonitor[F] =
    new AutoDeleteAppMonitor(config, publisherQueue, authProvider)
}

final case class AppToAutoDelete(id: AppId, appName: AppName, appStatus: AppStatus, samResourceId: SamResourceId, creator: WorkbenchEmail,
                                 chartName: ChartName, cloudContext:CloudContext
) {
  def projectNameString: String = s"${CloudContext.Gcp}/${appName}"
}
