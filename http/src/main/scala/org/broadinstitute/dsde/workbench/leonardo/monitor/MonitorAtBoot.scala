package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.implicits._
import cats.mtl.ApplicativeAsk
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.leonardo.db.patchQuery
import org.broadinstitute.dsde.workbench.leonardo.http.cloudServiceSyntax

import scala.concurrent.ExecutionContext

class MonitorAtBoot[F[_]](implicit F: Async[F],
                          dbRef: DbReference[F],
                          logger: Logger[F],
                          ec: ExecutionContext,
                          monitor: RuntimeMonitor[F, CloudService]) {
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
                      statusOpt.toRight(new Exception(s"${c.id} not found after transition. This is very weird!"))
                    )
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
