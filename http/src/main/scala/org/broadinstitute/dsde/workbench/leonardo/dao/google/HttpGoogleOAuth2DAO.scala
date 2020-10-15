package org.broadinstitute.dsde.workbench.leonardo
package dao
package google

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.concurrent.Semaphore
import cats.effect.{Async, Blocker, ContextShift, Timer}
import cats.mtl.ApplicativeAsk
import com.google.api.services.oauth2.Oauth2
import io.chrisdavenport.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.withLogging
import org.broadinstitute.dsde.workbench.leonardo.http.api.AuthenticationError
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchUserId}

class HttpGoogleOAuth2DAO[F[_]: Async: Timer: StructuredLogger: ContextShift](client: Oauth2,
                                                                              blocker: Blocker,
                                                                              blockerBound: Semaphore[F])
    extends GoogleOAuth2DAO[F] {
  override def getUserInfoFromToken(accessToken: String)(implicit ev: ApplicativeAsk[F, TraceId]): F[UserInfo] =
    for {
      tokenInfo <- blockAndLogF(Async[F].delay(client.tokeninfo().setAccessToken(accessToken).execute()),
                                "com.google.api.services.oauth2.Oauth2.tokeninfo(<token>)").adaptError {
        case _ =>
          // Rethrow AuthenticationError if unable to look up the token
          AuthenticationError()
      }
      userInfo = UserInfo(OAuth2BearerToken(accessToken),
                          WorkbenchUserId(tokenInfo.getUserId),
                          WorkbenchEmail(tokenInfo.getEmail),
                          tokenInfo.getExpiresIn.toInt)
    } yield userInfo

  private def blockAndLogF[A](fa: F[A], action: String)(implicit ev: ApplicativeAsk[F, TraceId]): F[A] =
    for {
      traceId <- ev.ask
      res <- withLogging(blockerBound.withPermit(blocker.blockOn(fa)), Some(traceId), action)
    } yield res
}
