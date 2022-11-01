package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.http.scaladsl.model.StatusCode
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import io.circe._
import org.broadinstitute.dsde.workbench.azure.RelayNamespace
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NoStackTrace

object HttpCromwellDAO {
  implicit val statusDecoder: Decoder[CromwellStatusCheckResponse] = Decoder.instance { d =>
    for {
      ok <- d.downField("ok").as[Boolean]
    } yield CromwellStatusCheckResponse(ok)
  }
}

class HttpCromwellDAO[F[_]](httpClient: Client[F], samDAO: SamDAO[F])(implicit
  logger: Logger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends CromwellDAO[F]
    with Http4sClientDsl[F] {
  import HttpCromwellDAO._

  override def getStatus(relayNamespace: RelayNamespace, headers: Headers)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] = {
    val baseUri = Uri.unsafeFromString(s"https://${relayNamespace.value}.servicebus.windows.net")
    val cromwellStatusUri = baseUri / "cromwell" / "api" / "engine" / "v1" / "status"
    for {
      res <- metrics.incrementCounter("cromwell/status") >> httpClient.expectOr[CromwellStatusCheckResponse](
        Request[F](
          method = Method.GET,
          uri = cromwellStatusUri,
          headers = headers
        )
      )(onError) match {
        case Left(_) =>
          logger.error(s"Failed to get status from Cromwell for namespace ${relayNamespace.value}")
          F.pure(false)
        case Right(CromwellStatusCheckResponse(ok)) => F.pure(ok)
      }
    } yield res
  }

  private def onError(response: Response[F])(implicit ev: Ask[F, AppContext]): F[Throwable] =
    for {
      appContext <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(s"${appContext.traceId} | $body")
      _ <- metrics.incrementCounter("cromwell/errorResponse")
    } yield CromwellStatusCheckException(appContext.traceId, body, response.status.code)
}

// API response models
final case class CromwellStatusCheckResponse(ok: Boolean) extends AnyVal

final case class CromwellStatusCheckException(traceId: TraceId, msg: String, code: StatusCode)
    extends LeoException(message = s"Cromwell error: $msg", statusCode = code, traceId = Some(traceId))
    with NoStackTrace
