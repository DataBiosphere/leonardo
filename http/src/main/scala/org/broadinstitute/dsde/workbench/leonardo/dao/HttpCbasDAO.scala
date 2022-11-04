package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.http.scaladsl.model.StatusCode
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import io.circe._
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.typelevel.log4cats.StructuredLogger

import scala.util.control.NoStackTrace

object HttpCbasDAO {
  implicit val statusDecoder: Decoder[CbasStatusCheckResponse] = Decoder.instance { d =>
    for {
      ok <- d.downField("ok").as[Boolean]
    } yield CbasStatusCheckResponse(ok)
  }
}

class HttpCbasDAO[F[_]](httpClient: Client[F])(implicit
  logger: StructuredLogger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends CbasDAO[F]
    with Http4sClientDsl[F] {
  import HttpCbasDAO._

  override def getStatus(baseUri: Uri, authHeader: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      _ <- metrics.incrementCounter("cbas/status")
      cbasStatusUri = baseUri / "cbas" / "status"
      res <- httpClient.expectOr[CbasStatusCheckResponse](
        Request[F](
          method = Method.GET,
          uri = cbasStatusUri,
          headers = Headers(authHeader)
        )
      )(onError)
    } yield res.ok

  private def onError(response: Response[F])(implicit ev: Ask[F, AppContext]): F[Throwable] =
    for {
      ctx <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(ctx.loggingCtx)(s"Failed to get status from CBAS. Body: $body")
      _ <- metrics.incrementCounter("cbas/errorResponse")
    } yield CbasStatusCheckException(ctx.traceId, body, response.status.code)
}

// API response models
final case class CbasStatusCheckResponse(ok: Boolean) extends AnyVal

final case class CbasStatusCheckException(traceId: TraceId, msg: String, code: StatusCode)
    extends LeoException(message = s"CBAS error: $msg", statusCode = code, traceId = Some(traceId))
    with NoStackTrace
