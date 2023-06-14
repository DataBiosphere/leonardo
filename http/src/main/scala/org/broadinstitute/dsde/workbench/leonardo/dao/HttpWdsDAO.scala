package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.http.scaladsl.model.StatusCode
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import io.circe._
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppType}
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

object HttpWdsDAO {
  implicit val statusDecoder: Decoder[WdsStatusCheckResponse] = Decoder.instance { d =>
    for {
      ok <- d.downField("status").as[String]
    } yield WdsStatusCheckResponse(ok)
  }
}

class HttpWdsDAO[F[_]](httpClient: Client[F])(implicit
  logger: StructuredLogger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends WdsDAO[F]
    with Http4sClientDsl[F] {
  import HttpWdsDAO._

  override def getStatus(baseUri: Uri, authHeader: Authorization, appType: AppType)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      _ <- metrics.incrementCounter("wds/status")
      wdsStatusUri = baseUri / "status"
      res <- httpClient.expectOr[WdsStatusCheckResponse](
        Request[F](
          method = Method.GET,
          uri = wdsStatusUri,
          headers = Headers(authHeader)
        )
      )(onError)
    } yield res.status.equalsIgnoreCase("up")

  private def onError(response: Response[F])(implicit ev: Ask[F, AppContext]): F[Throwable] =
    for {
      ctx <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(ctx.loggingCtx)(s"Failed to get status from WDS. Body: $body")
      _ <- metrics.incrementCounter("wds/errorResponse")
    } yield WdsStatusCheckException(ctx.traceId, body, response.status.code)
}

// API response models
final case class WdsStatusCheckResponse(status: String) extends AnyVal

final case class WdsStatusCheckException(traceId: TraceId, msg: String, code: StatusCode)
    extends LeoException(message = s"WDS error: $msg", statusCode = code, traceId = Some(traceId))
    with NoStackTrace
