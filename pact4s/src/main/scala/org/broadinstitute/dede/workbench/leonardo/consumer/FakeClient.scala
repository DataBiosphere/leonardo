package org.broadinstitute.dede.workbench.leonardo.consumer

import cats.effect.kernel.Concurrent
import cats.implicits.{catsSyntaxApplicativeErrorId, catsSyntaxApplicativeId, catsSyntaxOptionId, toFunctorOps}
import cats.syntax.all.none
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import org.http4s.Credentials.Token
import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.{EntityDecoder, EntityEncoder, Method, Request, Status, Uri}

trait FakeClient[F[_]] {
  def fetchResource(id: String)(implicit decoder: Decoder[Resource]): F[Option[Resource]]

  def createResource(resource: Resource): F[Unit]
}

/*
 This class represents the consumer view of a fake provider that implements the following endpoints:
 - GET /resource/$id
 - POST /resource
 */
class FakeClientImpl[F[_]: Concurrent](client: Client[F], baseUrl: Uri, bearer: Token) extends FakeClient[F] {
  override def fetchResource(id: String)(implicit decoder: Decoder[Resource]): F[Option[Resource]] = {
    val request = Request[F](uri = baseUrl / "resource" / id)
      .withHeaders(Authorization(bearer))
    client.run(request).use { resp =>
      resp.status match {
        case Status.Ok           => resp.as[Resource].map(_.some)
        case Status.NotFound     => none[Resource].pure[F]
        case Status.Unauthorized => InvalidCredentials.raiseError
        case _                   => UnknownError.raiseError
      }
    }
  }

  override def createResource(resource: Resource): F[Unit] = {
    val request = Request[F](method = Method.POST, uri = baseUrl / "resource")
      .withHeaders(Authorization(bearer))
      .withEntity(resource)
    client.run(request).use { resp =>
      resp.status match {
        case Status.NoContent => ().pure[F]
        case Status.Conflict  => UserAlreadyExists.raiseError
        case _                => UnknownError.raiseError
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

  implicit def entityDecoder[F[_]: Concurrent]: EntityDecoder[F, Resource] = jsonOf[F, Resource]
}
