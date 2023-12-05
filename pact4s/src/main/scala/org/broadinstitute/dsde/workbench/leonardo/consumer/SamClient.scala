package org.broadinstitute.dsde.workbench.leonardo.consumer

import cats.effect.kernel.Concurrent
import cats.syntax.all._
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO._
import org.broadinstitute.dsde.workbench.leonardo.dao.ListResourceResponse
import org.broadinstitute.dsde.workbench.leonardo.{consumer, SamResourceType}
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse
import org.http4s.Credentials.Token
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.headers.Authorization

trait SamClient[F[_]] {

  def fetchResourcePolicies[R](
    authHeader: Authorization,
    resourceType: SamResourceType
  )(implicit
    decoder: Decoder[R]
  ): F[Iterable[ListResourceResponse[R]]]

  def fetchSystemStatus(): F[StatusCheckResponse]
}

/*
 This class represents the consumer (Leo) view of the Sam provider that implements the following endpoints:
 - GET /status
 - GET /api/resources/v2/$resourceType
 */
class SamClientImpl[F[_]: Concurrent](client: Client[F], baseUrl: Uri, bearer: Token) extends SamClient[F] {

  override def fetchSystemStatus(): F[StatusCheckResponse] = {
    val request = Request[F](uri = baseUrl / "status").withHeaders(
      org.http4s.headers.Accept(MediaType.application.json)
    )
    client.run(request).use { resp =>
      resp.status match {
        case Status.Ok                  => resp.as[StatusCheckResponse]
        case Status.InternalServerError => resp.as[StatusCheckResponse]
        case _                          => consumer.UnknownError.raiseError
      }
    }
  }

  override def fetchResourcePolicies[R](
    authHeader: Authorization,
    resourceType: SamResourceType
  )(implicit
    decoder: Decoder[R]
  ): F[Iterable[ListResourceResponse[R]]] = {
    val request = Request[F](uri = baseUrl / "api" / "resources" / "v2" / resourceType.asString).withHeaders(
      org.http4s.headers.Accept(MediaType.application.json),
      authHeader
    )
    client.run(request).use { resp =>
      resp.status match {
        case Status.Ok => resp.as[Iterable[ListResourceResponse[R]]]
        case _         => consumer.UnknownError.raiseError
      }
    }
  }
}
