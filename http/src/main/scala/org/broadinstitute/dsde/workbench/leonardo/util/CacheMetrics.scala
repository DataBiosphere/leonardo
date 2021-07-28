package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.Async
import cats.syntax.all._
import com.google.common.cache.CacheStats
import fs2.Stream
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

class CacheMetrics[F[_]] private (name: String, interval: FiniteDuration)(implicit F: Async[F],
                                                                          metrics: OpenTelemetryMetrics[F],
                                                                          logger: Logger[F]) {
  def process(sizeF: () => F[Long], statsF: () => F[CacheStats]): Stream[F, Unit] =
    (Stream.sleep[F](interval) ++ Stream.eval(recordMetrics(sizeF, statsF))).repeat

  private def recordMetrics(sizeF: () => F[Long], statsF: () => F[CacheStats]): F[Unit] =
    for {
      size <- sizeF()
      _ <- metrics.gauge(s"cache/$name/size", size.toDouble)
      _ <- logger.info(s"CacheMetrics: $name size: ${size.toDouble}")
      stats <- statsF()
      _ <- logger.info(s"CacheMetrics: $name stats: ${stats.toString}")
      _ <- metrics.gauge(s"cache/$name/hitCount", stats.hitCount.toDouble)
      _ <- metrics.gauge(s"cache/$name/missCount", stats.missCount.toDouble)
      _ <- metrics.gauge(s"cache/$name/loadSuccessCount", stats.loadSuccessCount.toDouble)
      _ <- metrics.gauge(s"cache/$name/loadExceptionCount", stats.loadExceptionCount.toDouble)
      _ <- metrics.gauge(s"cache/$name/totalLoadTime", stats.totalLoadTime.toDouble)
      _ <- metrics.gauge(s"cache/$name/evictionCount", stats.evictionCount.toDouble)
    } yield ()
}
object CacheMetrics {
  def apply[F[_]: Async: OpenTelemetryMetrics: Logger](name: String,
                                                       interval: FiniteDuration = 1 minute): CacheMetrics[F] =
    new CacheMetrics(name, interval)
}
