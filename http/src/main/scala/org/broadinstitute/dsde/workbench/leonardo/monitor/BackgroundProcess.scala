package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.Show
import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

/**
 * Defines a common interface that certain background processes should implement.
 * This type of background processes do the following periodically:
 * 1. Query the database for a list of candidates
 * 2. Perform some action on the candidates
 */
trait BackgroundProcess[F[_], A] {

  /**
   * The name of the background process. This is used mostly for log messages and metrics 
   */
  def name: String
  def interval: FiniteDuration

  /**
   * Get a collection of candidates from the database
   */
  def getCandidates(now: Instant)(implicit
    F: Async[F],
    metrics: OpenTelemetryMetrics[F],
    logger: StructuredLogger[F]
  ): F[Seq[A]]

  /**
   * Describes an action that'll happen to each candidate
   */
  def action(a: A, traceId: TraceId, now: Instant)(implicit F: Async[F]): F[Unit]

  /**
   * The main logic of the background process at each interval
   */
  private def check(implicit
    showA: Show[A],
    F: Async[F],
    metrics: OpenTelemetryMetrics[F],
    logger: StructuredLogger[F]
  ): F[Unit] = for {
    now <- F.realTimeInstant
    _ <- logger.info(
      s"[logAlert/${name}Heartbeat] Executing ${name} check at interval of ${interval}"
    )
    candidates <- getCandidates(now)
    _ <- metrics.gauge(s"${name}/numOfRuntimes", candidates.length) // TODO: update metrics once we add more monitors
    _ <- candidates.traverse_ { a =>
      for {
        uuid <- F.delay(UUID.randomUUID())
        traceId = TraceId(s"${name}_${uuid.toString}")
        loggingContext = Map("traceId" -> traceId.asString)
        _ <- metrics.incrementCounter(
          s"${name}/pauseRuntimeCounter"
        ) // TODO: update metrics once we add more monitors
        _ <- logger.info(loggingContext)(s"${name} | runtime ${a.show}")
        _ <- action(a, traceId, now).handleErrorWith { case e =>
          logger.error(loggingContext, e)(s"Failed to ${name} for ${a.show}")
        }
      } yield ()
    }
  } yield ()

  def process(implicit
    showA: Show[A],
    F: Async[F],
    openTelemetry: OpenTelemetryMetrics[F],
    logger: StructuredLogger[F]
  ): Stream[F, Unit] =
    (Stream.sleep[F](interval) ++ Stream.eval(
      check
        .handleErrorWith { case e =>
          logger.error(e)(s"Unexpected error occurred during ${name} monitoring. Recovering...")
        }
    )).repeat
}
