package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant
import java.util.UUID

import cats.implicits._
import cats.effect.{Sync, Timer}
import org.broadinstitute.dsde.workbench.leonardo.http.nowInstant
import org.broadinstitute.dsde.workbench.model.TraceId

final case class AppContext(traceId: TraceId, now: Instant) {
  override def toString: String = s"${traceId.asString}"
}
object AppContext {
  def generate[F[_]: Sync](implicit timer: Timer[F]): F[AppContext] =
    for {
      traceId <- Sync[F].delay(UUID.randomUUID())
      now <- nowInstant[F]
    } yield AppContext(TraceId(traceId), now)
}
