package org.broadinstitute.dsde.workbench.leonardo.dao.google

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchUserId}

import scala.concurrent.duration._

class MockGoogleOAuth2Service extends GoogleOAuth2Service[IO] {
  override def getUserInfoFromToken(accessToken: String)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[UserInfo] =
    accessToken match {
      case "expired" =>
        IO(
          UserInfo(OAuth2BearerToken(accessToken),
                   WorkbenchUserId("1234567890"),
                   WorkbenchEmail("expiredUser@example.com"),
                   -10)
        )
      case "unauthorized" =>
        IO(
          UserInfo(OAuth2BearerToken(accessToken),
                   WorkbenchUserId("1234567890"),
                   WorkbenchEmail("non_whitelisted@example.com"),
                   (1 hour).toSeconds)
        )
      case _ =>
        IO(
          UserInfo(OAuth2BearerToken(accessToken),
                   WorkbenchUserId("1234567890"),
                   WorkbenchEmail("user1@example.com"),
                   (1 hour).toSeconds)
        )
    }
}
object MockGoogleOAuth2Service extends MockGoogleOAuth2Service
