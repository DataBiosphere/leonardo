package org.broadinstitute.dsde.workbench.pipeline

import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
//import io.circe.generic.semiauto._

case class UserMetadata(email: String, `type`: UserType, bearer: String) {
  implicit val userMetadataDecoder: Decoder[UserMetadata] = new Decoder[UserMetadata] {
    final def apply(c: HCursor): Decoder.Result[UserMetadata] =
      for {
        email <- c.downField("email").as[String]
        userType <- c.downField("type").as[UserType]
        bearer <- c.downField("bearer").as[String]
      } yield UserMetadata(email, userType, bearer)
  }

  implicit val userMetadataEncoder: Encoder[UserMetadata] = new Encoder[UserMetadata] {
    final def apply(a: UserMetadata): Json = Json.obj(
      ("email", Json.fromString(a.email)),
      ("type", a.`type`.asJson),
      ("bearer", Json.fromString(a.bearer))
    )
  }

  def makeAuthToken: ProxyAuthToken =
    ProxyAuthToken(this, (new MockGoogleCredential.Builder()).build())
}

/**
 * Companion object containing some useful methods for UserMetadata.
 */
//object UserMetadata {
//  implicit val userMetadataDecoder: Decoder[UserMetadata] = deriveDecoder[UserMetadata]
//}
