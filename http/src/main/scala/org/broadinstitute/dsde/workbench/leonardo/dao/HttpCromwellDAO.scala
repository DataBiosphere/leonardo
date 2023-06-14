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
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization
import org.typelevel.log4cats.StructuredLogger

import scala.util.control.NoStackTrace

object HttpCromwellDAO {
  implicit val statusDecoder: Decoder[CromwellStatusCheckResponse] = Decoder.instance { d =>
    for {
      ok <- d.downField("ok").as[Boolean]
    } yield CromwellStatusCheckResponse(ok)
  }
}

class HttpCromwellDAO[F[_]](httpClient: Client[F])(implicit
  logger: StructuredLogger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends CromwellDAO[F]
    with Http4sClientDsl[F] {

  override def getStatus(baseUri: Uri, authHeader: Authorization)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      _ <- metrics.incrementCounter("cromwell/status")
      cromwellStatusUri = baseUri / "cromwell" / "engine" / "v1" / "status"
      // Note: cromwell-as-an-app /status returns '{}' in the response body for some reason.
      // For now just check the HTTP status code instead of parsing the response body.
      res <- httpClient.status(
        Request[F](
          method = Method.GET,
          uri = cromwellStatusUri,
          headers = Headers(authHeader)
        )
      )
//      res <- httpClient.expectOr[CromwellStatusCheckResponse](
//        Request[F](
//          method = Method.GET,
//          uri = cromwellStatusUri,
//          headers = Headers(authHeader)
//        )
//      )(onError)
    } yield res.isSuccess

  private def onError(response: Response[F])(implicit ev: Ask[F, AppContext]): F[Throwable] =
    for {
      ctx <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(ctx.loggingCtx)(s"Failed to get status from Cromwell. Body: $body")
      _ <- metrics.incrementCounter("cromwell/errorResponse")
    } yield CromwellStatusCheckException(ctx.traceId, body, response.status.code)
}

// API response models
final case class CromwellStatusCheckResponse(ok: Boolean) extends AnyVal

final case class CromwellStatusCheckException(traceId: TraceId, msg: String, code: StatusCode)
    extends LeoException(message = s"Cromwell error: $msg", statusCode = code, traceId = Some(traceId))
    with NoStackTrace
