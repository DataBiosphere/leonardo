package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.implicits._
import cats.mtl.ApplicativeAsk
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, clusterQuery}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.leonardo.db.patchQuery
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.RuntimeTransitionMessage

import scala.concurrent.ExecutionContext

class MonitorAtBoot[F[_]](gceRuntimeMonitor: GceRuntimeMonitor[F],
                          publisherQueue: fs2.concurrent.InspectableQueue[F, LeoPubsubMessage]
                         )(implicit F: Async[F],
                                                                   dbRef: DbReference[F],
                                                                   logger: Logger[F],
                                                                   ec: ExecutionContext
) {
  val process: Stream[F, Unit] = {
    implicit val traceId = ApplicativeAsk.const[F, TraceId](TraceId("BootMonitoring"))
    val res = clusterQuery.listMonitoredGceOnly
      .transaction[F]
      .attempt
      .flatMap {
        case Right(clusters) =>
          clusters.toList.traverse_ {
            case c if c.status.isMonitored && c.status != RuntimeStatus.Unknown =>
              val r = for {
                _ <- gceRuntimeMonitor.process(c.id).compile.drain
                patchInProgress <- patchQuery.isInprogress(c.id).transaction
                _ <- if(patchInProgress) {
                  for {
                    statusOpt <- clusterQuery.getClusterStatus(c.id).transaction
                    tid <- traceId.ask
                    s <- F.fromEither(statusOpt.toRight(new Exception(s"${c.id} not found after transition. This is very weird!")))
                    _ <- publisherQueue.enqueue1(RuntimeTransitionMessage(RuntimePatchDetails(c.id, s), Some(tid)))
                  } yield ()
                } else F.unit
              } yield ()
              r.handleErrorWith(e => logger.error(e)(s"Error transitioning ${c.projectNameString}"))
          }
        case Left(e) => logger.error(e)("Error starting retrieve runtimes that need to be monitored during startup")
      }

    Stream.eval(res)
  }
}
