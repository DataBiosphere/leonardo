package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives.{headerValueByName, optionalHeaderValueByName}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchUserId}

/**
 * Created by rtitle on 10/16/17.
 */
object StandardUserInfoDirectives extends UserInfoDirectives {
  // The OAUTH2_CLAIM_google_id header is populated when a user signs in to Google via B2C.
  // If present, use that value instead of the B2C id for backwards compatibility.
  override def requireUserInfo: Directive1[UserInfo] =
    (headerValueByName("OIDC_access_token") &
      headerValueByName("OIDC_CLAIM_user_id") &
      headerValueByName("OIDC_CLAIM_expires_in") &
      headerValueByName("OIDC_CLAIM_email") & optionalHeaderValueByName("OAUTH2_CLAIM_google_id")).tmap {
      case (token, userId, expiresIn, email, googleIdOpt) =>
        val user = googleIdOpt.getOrElse(userId)
        UserInfo(OAuth2BearerToken(token), WorkbenchUserId(user), WorkbenchEmail(email), expiresIn.toLong)
    }
}
