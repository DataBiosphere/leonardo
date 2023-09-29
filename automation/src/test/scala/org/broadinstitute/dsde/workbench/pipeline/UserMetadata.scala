package org.broadinstitute.dsde.workbench.pipeline

import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import io.circe.Decoder

case class UserMetadata (email: String, `type`: UserType, bearer: String) {
  def makeAuthToken: ProxyAuthToken =
    ProxyAuthToken(this, (new MockGoogleCredential.Builder()).build())
}

/**
 * Companion object containing some useful methods for UserMetadata.
 */
object UserMetadata {
  implicit val userMetadataDecoder: Decoder[UserMetadata] = deriveDecoder[UserMetadata]
}
