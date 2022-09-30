package org.broadinstitute.dsde.workbench.leonardo
package dao
package google

import cats.mtl.Ask
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec.{workbenchEmailDecoder, workbenchUserIdDecoder}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchUserId}

trait GoogleOAuth2Service[F[_]] {
  def getUserInfoFromToken(accessToken: String)(implicit ev: Ask[F, TraceId]): F[UserInfo]
}
object GoogleOAuth2Service {
  val GOOGLE_OAUTH_SERVER = "https://www.googleapis.com/oauth2/v1/tokeninfo?access_token="

  implicit val tokenInfoDecoder: Decoder[TokenInfo] = Decoder.forProduct3(
    "user_id",
    "email",
    "expires_in"
  )(TokenInfo.apply)
}

final case class TokenInfo(userId: WorkbenchUserId, email: WorkbenchEmail, expiresIn: Long)
