package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.{Async, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, patchQuery, DbReference, RuntimeConfigQueries}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext

class MonitorAtBoot[F[_]: Timer](publisherQueue: fs2.concurrent.Queue[F, LeoPubsubMessage])(
  implicit F: Async[F],
  dbRef: DbReference[F],
  logger: Logger[F],
  ec: ExecutionContext,
  metrics: OpenTelemetryMetrics[F]
) {
  val process: Stream[F, Unit] = {
    implicit val traceId = ApplicativeAsk.const[F, TraceId](TraceId("BootMonitoring"))
    val res = clusterQuery.listMonitored
      .transaction[F]
      .attempt
      .flatMap {
        case Right(clusters) =>
          clusters.toList.traverse_ {
            case c if c.status.isMonitored && c.status != RuntimeStatus.Unknown =>
              val r = for {
                tid <- traceId.ask
                message <- runtimeStatusToMessage(c, tid)
                // If a runtime is in transition status (Creating, Starting etc), then we're enqueue a pubsub message again
                _ <- message.traverse(m => publisherQueue.enqueue1(m))
                patchInProgress <- patchQuery.isInprogress(c.id).transaction
                _ <- if (patchInProgress) {
                  for {
                    statusOpt <- clusterQuery.getClusterStatus(c.id).transaction
                    s <- F.fromEither(
                      statusOpt
                        .toRight(new Exception(s"${tid} | ${c.id} not found after transition. This is very weird!"))
                    )
                    _ <- if (s != RuntimeStatus.Running) {
                      // There's slight chance where pubsub message is never published during a redeploy.
                      // In this case, user will see that the runtime doesn't get patched after clicking patch button.
                      // In the ideal case, patch is completed, and runtime has come back to Running.
                      metrics.incrementCounter("PatchInProgressFailed")
                    } else {
                      // If patch is in progress and we didn't finish patching, we don't really have a good way to recover;
                      // There is a chance that leonardo will be able to recover if the UpdateRuntimeEvent has already been sent to pubsub,
                      // we'll evaluate if this edge case is worth addressing based on PatchInProgressAtStartUp metrics
                      F.unit
                    }
                    _ <- patchQuery.updatePatchAsComplete(c.id).transaction
                    _ <- metrics.incrementCounter("PatchInProgressAtStartUp")
                  } yield ()
                } else F.unit
              } yield ()
              r.handleErrorWith(e => logger.error(e)(s"Error transitioning ${c.id}"))
          }
        case Left(e) => logger.error(e)("Error starting retrieve runtimes that need to be monitored during startup")
      }

    Stream.eval(res)
  }

  private def runtimeStatusToMessage(runtime: RuntimeToMonitor, traceId: TraceId): F[Option[LeoPubsubMessage]] =
    runtime.status match {
      case RuntimeStatus.Stopping =>
        F.pure(Some(LeoPubsubMessage.StopRuntimeMessage(runtime.id, Some(traceId))))
      case RuntimeStatus.Deleting =>
        F.pure(
          Some(
            LeoPubsubMessage.DeleteRuntimeMessage(
              runtime.id,
              None,
              Some(traceId)
            )
          )
        ) //If user specified `deleteDisk` being true in the original request, then we can't really recover; User will have to explicitly delete disk in UI again
      case RuntimeStatus.Starting =>
        F.pure(
          Some(
            LeoPubsubMessage.StartRuntimeMessage(
              runtime.id,
              Some(traceId)
            )
          )
        )
      case RuntimeStatus.Creating =>
        for {
          fullRuntime <- clusterQuery.getClusterById(runtime.id).transaction
          rt <- F.fromOption(fullRuntime, new Exception(s"can't find ${runtime.id} in DB"))
          rtConfig <- RuntimeConfigQueries.getRuntimeConfig(rt.runtimeConfigId).transaction
          r = rtConfig match {
            case x: RuntimeConfig.GceConfig =>
              for {
                bootDiskSize <- x.bootDiskSize.toRight(
                  s"disk Size field not found for ${rt.id}. This should never happen"
                ) //TODO: report error
              } yield RuntimeConfigInCreateRuntimeMessage.GceConfig(
                x.machineType,
                x.diskSize,
                bootDiskSize
              ): RuntimeConfigInCreateRuntimeMessage
            case x: RuntimeConfig.GceWithPdConfig =>
              for {
                diskId <- x.persistentDiskId.toRight(
                  s"disk id field not found for ${rt.id}. This should never happen"
                ) //TODO: report error
              } yield RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig(
                x.machineType,
                diskId,
                x.bootDiskSize
              ): RuntimeConfigInCreateRuntimeMessage
            case _: RuntimeConfig.DataprocConfig =>
              Right(
                LeoLenses.runtimeConfigPrism.getOption(rtConfig).get: RuntimeConfigInCreateRuntimeMessage
              )
          }
          rtConfigInMessage <- F.fromEither(r.leftMap(s => new RuntimeException(s)))
        } yield {
          Some(
            LeoPubsubMessage.CreateRuntimeMessage.fromRuntime(
              rt,
              rtConfigInMessage,
              Some(traceId)
            )
          )
        }
      case x => logger.info(s"${runtime.id} is in ${x} status. Do nothing").as(none[LeoPubsubMessage])
    }
}

final case class RuntimeToMonitor(
  id: Long,
  cloudService: CloudService,
  status: RuntimeStatus,
  patchInProgress: Boolean
)
