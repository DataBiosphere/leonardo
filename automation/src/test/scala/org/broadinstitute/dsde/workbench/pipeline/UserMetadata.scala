package org.broadinstitute.dsde.workbench.pipeline

import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}

case class UserMetadata(email: String, `type`: UserType, bearer: String) {
  def makeAuthToken: ProxyAuthToken =
    ProxyAuthToken(this, new MockGoogleCredential.Builder().build())
}

object UserMetadata {
  implicit val userMetadataDecoder: Decoder[UserMetadata] = (c: HCursor) =>
    for {
      email <- c.downField("email").as[String]
      userType <- c.downField("type").as[UserType]
      bearer <- c.downField("bearer").as[String]
    } yield UserMetadata(email, userType, bearer)

  implicit val userMetadataEncoder: Encoder[UserMetadata] = (a: UserMetadata) =>
    Json.obj(
      ("email", Json.fromString(a.email)),
      ("type", a.`type`.asJson),
      ("bearer", Json.fromString(a.bearer))
    )

  implicit val seqUserMetadataDecoder: Decoder[Seq[UserMetadata]] = Decoder.decodeSeq[UserMetadata]
}
