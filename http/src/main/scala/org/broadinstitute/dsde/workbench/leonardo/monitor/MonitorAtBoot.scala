package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.{Async, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, patchQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.leonardo.http.cloudServiceSyntax
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext

class MonitorAtBoot[F[_]: Timer](implicit F: Async[F],
                                 dbRef: DbReference[F],
                                 logger: Logger[F],
                                 ec: ExecutionContext,
                                 monitor: RuntimeMonitor[F, CloudService],
                                 metrics: OpenTelemetryMetrics[F]) {
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
                _ <- c.cloudService.process(c.id, c.status).compile.drain
                patchInProgress <- patchQuery.isInprogress(c.id).transaction
                _ <- if (patchInProgress) {
                  for {
                    statusOpt <- clusterQuery.getClusterStatus(c.id).transaction
                    tid <- traceId.ask
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
}

final case class RuntimeToMonitor(
  id: Long,
  cloudService: CloudService,
  status: RuntimeStatus,
  patchInProgress: Boolean
)
