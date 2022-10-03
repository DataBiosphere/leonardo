package org.broadinstitute.dsde.workbench.leonardo
package dao
package google

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.std.Semaphore
import cats.effect.Async
import cats.syntax.all._
import cats.mtl.Ask
import com.google.api.services.oauth2.Oauth2
import com.google.api.services.oauth2.model.Tokeninfo
import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.util2.withLogging
import org.broadinstitute.dsde.workbench.leonardo.model.AuthenticationError
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchUserId}

class GoogleOAuth2Interpreter[F[_]](client: Oauth2, blockerBound: Semaphore[F])(implicit
  logger: StructuredLogger[F],
  F: Async[F]
) extends GoogleOAuth2Service[F] {
  override def getUserInfoFromToken(accessToken: String)(implicit ev: Ask[F, TraceId]): F[UserInfo] =
    for {
      ctx <- ev.ask
      tokenInfo <- blockAndLogF(
        Async[F].blocking(client.tokeninfo().setAccessToken(accessToken).execute()).recoverWith { case e =>
          // Rethrow AuthenticationError if unable to look up the token
          // Do this before logging the error because tokeninfo errors are verbose
          logger.error(Map("traceId" -> ctx.asString), e)("Failed to get token info") >> F
            .raiseError[Tokeninfo](AuthenticationError())
        },
        "com.google.api.services.oauth2.Oauth2.tokeninfo(<token>)"
      )
      userInfo = UserInfo(OAuth2BearerToken(accessToken),
                          WorkbenchUserId(tokenInfo.getUserId),
                          WorkbenchEmail(tokenInfo.getEmail),
                          tokenInfo.getExpiresIn.toInt
      )
    } yield userInfo

  private def blockAndLogF[A](fa: F[A], action: String)(implicit ev: Ask[F, TraceId]): F[A] =
    for {
      traceId <- ev.ask
      res <- withLogging(blockerBound.permit.use(_ => fa), Some(traceId), action)
    } yield res
}
