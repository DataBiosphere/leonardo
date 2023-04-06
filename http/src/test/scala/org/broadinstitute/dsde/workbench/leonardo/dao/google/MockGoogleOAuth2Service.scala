package org.broadinstitute.dsde.workbench.leonardo.dao.google

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.model.AuthenticationError
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchUserId}

import scala.concurrent.duration._

class MockGoogleOAuth2Service extends GoogleOAuth2Service[IO] {
  override def getUserInfoFromToken(accessToken: String)(implicit ev: Ask[IO, TraceId]): IO[UserInfo] =
    accessToken match {
      case "expired" =>
        IO(
          UserInfo(OAuth2BearerToken(accessToken),
                   WorkbenchUserId("1234567890"),
                   WorkbenchEmail("expiredUser@example.com"),
                   -10
          )
        )
      case "unauthorized" =>
        IO(
          UserInfo(OAuth2BearerToken(accessToken),
                   WorkbenchUserId("1234567890"),
                   WorkbenchEmail("non_allowlisted@example.com"),
                   (1 hour).toSeconds
          )
        )
      case "" =>
        IO.raiseError(AuthenticationError(extraMessage = "invalid token"))
      case _ =>
        IO(
          UserInfo(OAuth2BearerToken(accessToken),
                   WorkbenchUserId("1234567890"),
                   WorkbenchEmail("user1@example.com"),
                   (1 hour).toSeconds
          )
        )
    }
}
object MockGoogleOAuth2Service extends MockGoogleOAuth2Service
