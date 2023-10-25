package org.broadinstitute.dsde.workbench.pipeline

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken

/**
 * Represents a proxy authentication token that combines user metadata and Google OAuth2 credentials,
 * extending a base `AuthToken` class.
 *
 * This class serves as an stand-in for `AuthToken` in
 * https://github.com/broadinstitute/workbench-libs/blob/develop/serviceTest/src/test/scala/org/broadinstitute/dsde/workbench/auth/AuthToken.scala#L26, allowing it to be enhanced
 * with user metadata and authentication credentials.
 *
 * This class enables us to encapsulate user metadata and authentication credentials obtained from
 * the pipeline through injection, providing a convenient way to interact with various APIs while asserting the user's
 * authorization.
 *
 * @param userData    The user metadata containing email, user type, and authorization information.
 * @param credential  The Google OAuth2 credentials builder to assert authorization for API calls.
 * @extends AuthToken An extension of a base `AuthToken` class provided by workbench library.
 */
case class ProxyAuthToken(userData: UserMetadata, credential: GoogleCredential) extends AuthToken with LazyLogging {
  override def buildCredential(): GoogleCredential = {
    logger.debug(s"ProxyAuthToken.buildCredential() has been called for user ${userData.email} ...")
    credential.setAccessToken(userData.bearer)
    logger.debug(
      "... with bearer token " + (if (credential.getAccessToken.length > 10)
                                    credential.getAccessToken.take(10 - 3) + "..."
                                  else credential.getAccessToken)
    )
    credential
  }
}
