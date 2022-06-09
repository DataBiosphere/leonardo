package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.Async
import cats.effect.std.Queue
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import java.time.Instant
import scala.concurrent.duration._

final class AsyncTaskProcessor[F[_]](config: AsyncTaskProcessor.Config, asyncTasks: Queue[F, Task[F]])(implicit
  logger: StructuredLogger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) {
  def process: Stream[F, Unit] = {
    val asyncTaskStream = Stream
      .fromQueueUnterminated(asyncTasks)
      .parEvalMapUnordered(config.maxConcurrentTasks)(task => handler(task))

    Stream(asyncTaskStream, recordCurrentNumOfTasks).covary[F].parJoin(2)
  }

  private def handler(task: Task[F]): F[Unit] =
    for {
      now <- F.realTimeInstant
      latency = (now.toEpochMilli - task.metricsStartTime.toEpochMilli).millis
      tags = Map("taskName" -> task.taskName)
      _ <- recordLatency("asyncTaskLatency", latency, tags)
      _ <- logger.info(Map("traceId" -> task.traceId.asString))(
        s"Executing task with latency of ${latency.toSeconds} seconds"
      )
      _ <- task.op.handleErrorWith { case err =>
        task.errorHandler.traverse(cb => cb(err)) >> logger.error(Map("traceId" -> task.traceId.asString), err)(
          s"Error when executing async task"
        )
      }
      end <- F.realTimeInstant
      timeToFinishTask = (end.toEpochMilli - task.metricsStartTime.toEpochMilli).millis
      _ <- recordLatency("asyncTaskDuration", timeToFinishTask, tags)
    } yield ()

  private def recordCurrentNumOfTasks: Stream[F, Unit] = {
    val record = for {
      size <- asyncTasks.size
      _ <- metrics.gauge("asyncTasksCount", size)
    } yield ()

    (Stream.eval(record) ++ Stream.sleep_(30 seconds)).repeat
  }

  // record the latency between message being enqueued and task gets executed
  private def recordLatency(metricsName: String, latency: FiniteDuration, tags: Map[String, String]): F[Unit] =
    metrics.recordDuration(metricsName,
                           latency,
                           List(
                             10 seconds,
                             1 minutes,
                             2 minutes,
                             4 minutes,
                             8 minutes,
                             16 minutes,
                             32 minutes
                           ),
                           tags
    )
}

object AsyncTaskProcessor {
  def apply[F[_]: Async](
    config: Config,
    asyncTasks: cats.effect.std.Queue[F, Task[F]]
  )(implicit logger: StructuredLogger[F], metrics: OpenTelemetryMetrics[F]): AsyncTaskProcessor[F] =
    new AsyncTaskProcessor(config, asyncTasks)

  final case class Task[F[_]](traceId: TraceId,
                              op: F[Unit],
                              errorHandler: Option[Throwable => F[Unit]] = None,
                              metricsStartTime: Instant,
                              taskName: String
  )
  final case class Config(queueBound: Int, maxConcurrentTasks: Int)
}
