package org.broadinstitute.dede.workbench.leonardo.consumer

import cats.effect.kernel.Concurrent
import cats.syntax.all._
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO._
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse
import org.http4s.Credentials.Token
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.client.Client
import org.http4s.headers.Authorization

trait SamClient[F[_]] {
  def fetchResource(id: String): F[Option[Resource]]

  def createResource(resource: Resource): F[Unit]

  def fetchSystemStatus(): F[Option[StatusCheckResponse]]
}

class SamClientImpl[F[_] : Concurrent](client: Client[F], baseUrl: Uri, bearer: Token)
  extends SamClient[F] {
  def fetchResource(id: String): F[Option[Resource]] = {
    val request = Request[F](uri = baseUrl / "resource" / id)
      .withHeaders(Authorization(bearer))
    client.run(request).use { resp =>
      resp.status match {
        case Status.Ok => resp.as[Resource].map(_.some)
        case Status.NotFound => none[Resource].pure[F]
        case Status.Unauthorized => InvalidCredentials.raiseError
        case _ => UnknownError.raiseError
      }
    }
  }

  def createResource(resource: Resource): F[Unit] = {
    val request = Request[F](method = Method.POST, uri = baseUrl / "resource")
      .withHeaders(Authorization(bearer))
      .withEntity(resource)
    client.run(request).use { resp =>
      resp.status match {
        case Status.NoContent => ().pure[F]
        case Status.Conflict => UserAlreadyExists.raiseError
        case _ => UnknownError.raiseError
      }
    }
  }

  override def fetchSystemStatus(): F[Option[StatusCheckResponse]] = {
    val request = Request[F](uri = baseUrl / "status").withHeaders(
      org.http4s.headers.Accept(MediaType.application.json)
    )
    client.run(request).use { resp =>
      resp.status match {
        case Status.Ok => resp.as[StatusCheckResponse].map(_.some)
        case Status.NotFound => none[StatusCheckResponse].pure[F]
        case _ => UnknownError.raiseError
      }
    }
  }
}

final case class Resource(id: String, value: Int)

object Resource {
  implicit val encoder: Encoder[Resource] = Encoder.instance { res =>
    Json.obj(
      "id" -> res.id.asJson,
      "value" -> res.value.asJson
    )
  }

  implicit def entityEncoder[F[_]]: EntityEncoder[F, Resource] = jsonEncoderOf[Resource]

  implicit val decoder: Decoder[Resource] = Decoder.forProduct2("id", "value")(Resource.apply)

  implicit def entityDecoder[F[_] : Concurrent]: EntityDecoder[F, Resource] = jsonOf[F, Resource]
}

case object InvalidCredentials extends Exception

case object UserAlreadyExists extends Exception

case object UnknownError extends Exception
