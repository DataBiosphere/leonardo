package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.{Effect, Timer}
import cats.implicits._
import com.google.common.cache.CacheStats
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.duration._

class CacheMetrics[F[_]: Timer: Effect] private (name: String, sizeF: F[Long], statsF: F[CacheStats])(
  implicit metrics: OpenTelemetryMetrics[F],
  logger: Logger[F]
) {
  val process: Stream[F, Unit] =
    (Stream.sleep[F](1 minute) ++ Stream.eval(updateMetrics)).repeat

  private def updateMetrics =
    for {
      size <- sizeF
      _ <- metrics.gauge(s"cache/$name/size", size)
      _ <- logger.info(s"CacheMetrics: $name size: $size")
      stats <- statsF
      _ <- logger.info(s"CacheMetrics: $name stats: ${stats.toString}")
      _ <- metrics.gauge(s"cache/$name/hitCount", stats.hitCount)
      _ <- metrics.gauge(s"cache/$name/missCount", stats.missCount)
      _ <- metrics.gauge(s"cache/$name/loadSuccessCount", stats.loadSuccessCount)
      _ <- metrics.gauge(s"cache/$name/loadExceptionCount", stats.loadExceptionCount)
      _ <- metrics.gauge(s"cache/$name/totalLoadTime", stats.totalLoadTime)
      _ <- metrics.gauge(s"cache/$name/evictionCount", stats.evictionCount)
    } yield ()
}
object CacheMetrics {
  def apply[F[_]: Timer: Effect: OpenTelemetryMetrics: Logger](name: String,
                                                               sizeF: F[Long],
                                                               statsF: F[CacheStats]): CacheMetrics[F] =
    new CacheMetrics(name, sizeF, statsF)
}
