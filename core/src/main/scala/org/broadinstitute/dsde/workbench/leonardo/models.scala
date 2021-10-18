package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.Sync
import cats.mtl.Ask
import cats.syntax.all._
import io.opencensus.trace.Span
import org.broadinstitute.dsde.workbench.model.TraceId

import java.time.Instant
import java.util.UUID

final case class AppContext(traceId: TraceId, now: Instant, requestUri: String = "", span: Option[Span] = None) {
  override def toString: String = s"${traceId.asString}"

  val loggingCtx = Map("traceId" -> traceId.asString)
}

object AppContext {
  def generate[F[_]: Sync](span: Option[Span] = None, requestUri: String): F[AppContext] =
    for {
      traceId <- span.fold(Sync[F].delay(UUID.randomUUID().toString))(s =>
        Sync[F].pure(s.getContext.getTraceId.toLowerBase16())
      )
      now <- Sync[F].realTimeInstant
    } yield AppContext(TraceId(traceId), now, requestUri, span)

  def lift[F[_]: Sync](span: Option[Span] = None, requestUri: String): F[Ask[F, AppContext]] =
    for {
      context <- AppContext.generate[F](span, requestUri)
    } yield Ask.const[F, AppContext](
      context
    )

}
