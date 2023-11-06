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
 * 2. Filter the candidates based on some criteria
 * 3. Perform some action on the filtered candidates
 */
trait BackgroundProcess[F[_], A] {
  def monitorType: String
  def interval: FiniteDuration

  /**
   * Given a candidate, determine whether it should be filtered out.
   * true: keep the candidate
   * false: filter out the candidate
   */
  def filterCriteria(a: A, now: Instant)(implicit
    F: Async[F],
    metrics: OpenTelemetryMetrics[F],
    logger: StructuredLogger[F]
  ): F[Boolean]

  /**
   * Get a collection of candidates from the database
   */
  def dbSource(): F[Seq[A]]

  /**
   * Perform some action on the candidate
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
      s"[logAlert/${monitorType}Heartbeat] Executing ${monitorType} check at interval of ${interval}"
    )
    candidates <- dbSource()
    filteredA <- candidates.toList.filterA { a =>
      filterCriteria(a, now)
    }
    _ <- metrics.gauge(s"${monitorType}/numOfRuntimes",
                       filteredA.length
    ) // TODO: update metrics once we add more monitors
    _ <- filteredA.traverse_ { a =>
      for {
        uuid <- F.delay(UUID.randomUUID())
        traceId = TraceId(s"${monitorType}_${uuid.toString}")
        _ <- metrics.incrementCounter(
          s"${monitorType}/pauseRuntimeCounter"
        ) // TODO: update metrics once we add more monitors
        _ <- logger.info(Map("traceId" -> traceId.asString))(s"${monitorType} | runtime ${a.show}")
        _ <- action(a, traceId, now)
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
        .handleErrorWith {
          case e: java.sql.SQLTransientConnectionException =>
            logger.error(e)("DB connection failed transiently") >> F.raiseError[Unit](e)
          case e =>
            logger.error(e)("Unexpected error occurred during auto-pause monitoring. Recovering...")
        }
    )).repeat
}
