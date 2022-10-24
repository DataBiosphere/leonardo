package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.http.scaladsl.model.StatusCode
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import io.circe._
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.HostReady
import org.broadinstitute.dsde.workbench.leonardo.dns.RuntimeDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, CloudContext, RuntimeName}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.client.middleware.{Retry, RetryPolicy}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class HttpCromwellDAO[F[_]](val runtimeDnsCache: RuntimeDnsCache[F], httpClient: Client[F], samDAO: SamDAO[F])(implicit
  logger: Logger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends CromwellDao[F]
    with Http4sClientDsl[F] {

  private val retryable = (_: Request[F], resp: Either[Throwable, Response[F]]) =>
    resp match {
      case Left(_)               => true
      case Right(r: Response[F]) => ???
    }
  private val retryPolicy = RetryPolicy[F](RetryPolicy.exponentialBackoff(30 seconds, 5), retryable)
  private val httpClientWithRetry = Retry(retryPolicy)(httpClient)

  implicit val statusDecoder: Decoder[CromwellStatusCheckResponse] = Decoder.instance { d =>
    for {
      ok <- d.downField("ok").as[Boolean]
    } yield CromwellStatusCheckResponse(ok)
  }

  override def getStatus(cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      hostStatus <- Proxy.getRuntimeTargetHost[F](runtimeDnsCache, cloudContext, runtimeName)
      headers <- cloudContext match {
        case _: CloudContext.Azure =>
          samDAO.getLeoAuthToken.map(x => Headers(x))
        case _: CloudContext.Gcp =>
          F.pure(Headers.empty)
      }
      res <- hostStatus match {
        case host: HostReady =>
          metrics.incrementCounter("cromwell/status") >>
            httpClientWithRetry.expectOr[CromwellStatusCheckResponse](
              Request[F](
                method = Method.GET,
                uri = host.toUri.withPath(Uri.Path.unsafeFromString(s"api/engine/v1/status")),
                headers = headers
              )
            )(onError) match {
            case Left(_)                               => F.pure(CromwellStatusCheckResponse(false))
            case Right(r: CromwellStatusCheckResponse) => F.pure(r.ok)
          }
        case _ => F.pure(CromwellStatusCheckResponse(false))
      }
    } yield res

  private def onError(response: Response[F])(implicit ev: Ask[F, AppContext]): F[Throwable] =
    for {
      appContext <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(s"${appContext.traceId} | Cromwell call failed: $body")
      _ <- metrics.incrementCounter("cromwell/errorResponse")
    } yield CromwellException(appContext.traceId, body, response.status.code)

}

// API response models
final case class CromwellStatusCheckResponse(ok: Boolean) extends AnyVal

// Config
final case class HttpCromwellDaoConfig(cromwellUri: Uri)

final case class CromwellException(traceId: TraceId, msg: String, code: StatusCode)
    extends LeoException(message = s"Cromwell error: $msg", statusCode = code, traceId = Some(traceId))
    with NoStackTrace
