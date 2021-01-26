package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{Sync, Timer}
import cats.mtl.Ask
import cats.syntax.all._
import io.opencensus.trace.Span
import org.broadinstitute.dsde.workbench.model.TraceId

import java.time.Instant
import java.util.UUID

final case class AppContext(traceId: TraceId, now: Instant, span: Option[Span] = None) {
  override def toString: String = s"${traceId.asString}"

  val loggingCtx = Map("traceId" -> traceId.asString)
}

object AppContext {
  def generate[F[_]: Sync](span: Option[Span] = None)(implicit timer: Timer[F]): F[AppContext] =
    for {
      traceId <- span.fold(Sync[F].delay(UUID.randomUUID().toString))(s =>
        Sync[F].pure(s.getContext.getTraceId.toLowerBase16())
      )
      now <- nowInstant[F]
    } yield AppContext(TraceId(traceId), now, span)

  def lift[F[_]: Sync: Timer](span: Option[Span] = None): F[Ask[F, AppContext]] =
    for {
      context <- AppContext.generate[F](span)
    } yield Ask.const[F, AppContext](
      context
    )

}

final case class LeonardoBaseUrl(asString: String) extends AnyVal
