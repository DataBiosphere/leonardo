package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant
import java.util.UUID

import cats.effect.{IO, Timer}
import org.broadinstitute.dsde.workbench.leonardo.http.nowInstant
import org.broadinstitute.dsde.workbench.model.TraceId

final case class AppContext(traceId: TraceId, now: Instant)
object AppContext {
  def generate(implicit timer: Timer[IO]): IO[AppContext] =
    for {
      traceId <- IO(UUID.randomUUID())
      now <- nowInstant[IO]
    } yield AppContext(TraceId(traceId), now)
}
