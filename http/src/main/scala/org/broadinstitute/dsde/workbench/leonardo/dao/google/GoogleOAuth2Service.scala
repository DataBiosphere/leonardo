package org.broadinstitute.dsde.workbench.leonardo
package dao
package google

import cats.effect.concurrent.Semaphore
import cats.effect.{Async, Blocker, ContextShift, Resource, Sync, Timer}
import cats.mtl.Ask
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.services.oauth2.Oauth2
import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}

trait GoogleOAuth2Service[F[_]] {
  def getUserInfoFromToken(accessToken: String)(implicit ev: Ask[F, TraceId]): F[UserInfo]
}
object GoogleOAuth2Service {
  def resource[F[_]: Async: ContextShift: Timer: StructuredLogger](
    blocker: Blocker,
    blockerBound: Semaphore[F]
  ): Resource[F, GoogleOAuth2Service[F]] =
    for {
      httpTransport <- Resource.eval(Sync[F].delay(GoogleNetHttpTransport.newTrustedTransport))
      jsonFactory = com.google.api.client.json.gson.GsonFactory.getDefaultInstance
      // Credentials not needed for the tokeninfo API
      client = new Oauth2.Builder(httpTransport, jsonFactory, null)
        .setApplicationName("leonardo")
        .build()
    } yield new GoogleOAuth2Interpreter[F](client, blocker, blockerBound)
}
