package org.broadinstitute.dsde.workbench.leonardo
package dao
package google

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleOAuth2Service.{tokenInfoDecoder, GOOGLE_OAUTH_SERVER}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}

class GoogleOAuth2Interpreter[F[_]](client: Client[F])(implicit
  F: Async[F]
) extends GoogleOAuth2Service[F] {
  override def getUserInfoFromToken(accessToken: String)(implicit ev: Ask[F, TraceId]): F[UserInfo] =
    for {
      tokenInfo <- client.expect[TokenInfo](
        Request[F](
          method = Method.GET,
          uri = Uri.unsafeFromString(
            GOOGLE_OAUTH_SERVER
          )
        )
      )

      userInfo = UserInfo(OAuth2BearerToken(accessToken), tokenInfo.userId, tokenInfo.email, tokenInfo.expiresIn)
    } yield userInfo
}
