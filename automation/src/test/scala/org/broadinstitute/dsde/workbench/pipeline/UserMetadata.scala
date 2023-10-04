package org.broadinstitute.dsde.workbench.pipeline

import cats.effect.IO
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.pipeline.PipelineInjector.BEE
import org.http4s.{AuthScheme, Credentials}
import org.http4s.headers.Authorization

case class UserMetadata(email: String, `type`: UserType, bearer: String) {
  def makeAuthToken: AuthToken =
    ProxyAuthToken(this, new MockGoogleCredential.Builder().build())
  def authToken(): IO[AuthToken] = IO(makeAuthToken)
  def authToken(scopes: Seq[String]): IO[AuthToken] = IO(makeAuthToken)
  def authorization(): IO[Authorization] =
    authToken().map(token => Authorization(Credentials.Token(AuthScheme.Bearer, token.value)))
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

  val Hermione: UserMetadata = BEE.Owners.getUserCredential("hermione").get
  val Ron: UserMetadata = BEE.Owners.getUserCredential("ron").get
}
