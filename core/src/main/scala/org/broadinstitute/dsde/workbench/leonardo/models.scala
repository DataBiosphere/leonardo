package org.broadinstitute.dsde.workbench.leonardo

import java.time.Instant
import java.util.UUID

import cats.effect.{Sync, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import io.opencensus.trace.Span
import org.broadinstitute.dsde.workbench.model.TraceId

final case class AppContext(traceId: TraceId, now: Instant, span: Option[Span] = None) {
  override def toString: String = s"${traceId.asString}"
}

object AppContext {
  def generate[F[_]: Sync](span: Option[Span] = None)(implicit timer: Timer[F]): F[AppContext] =
    for {
      traceId <- span.fold(Sync[F].delay(UUID.randomUUID().toString))(s =>
        Sync[F].pure(s.getContext.getTraceId.toLowerBase16())
      )
      now <- nowInstant[F]
    } yield AppContext(TraceId(traceId), now, span)

  def lift[F[_]: Sync: Timer](span: Option[Span] = None): F[ApplicativeAsk[F, AppContext]] =
    for {
      context <- AppContext.generate[F](span)
    } yield ApplicativeAsk.const[F, AppContext](
      context
    )

}
