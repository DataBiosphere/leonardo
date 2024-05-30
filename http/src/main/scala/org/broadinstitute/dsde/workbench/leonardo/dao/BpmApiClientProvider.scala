package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.profile.api.ProfileApi
import bio.terra.profile.model.ProfileModel
import bio.terra.profile.client.ApiClient
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.broadinstitute.dsde.workbench.leonardo.util.WithSpanFilter
import org.glassfish.jersey.client.ClientConfig
import org.http4s.Uri
import org.typelevel.log4cats.StructuredLogger

import java.util.UUID

trait BpmApiClientProvider[F[_]] {

  def getProfileApi(token: String)(implicit ev: Ask[F, AppContext]): F[ProfileApi]

  def getProfile(token: String, profileId: UUID)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ProfileModel]]

}

class HttpBpmClientProvider[F[_]](baseBpmUrl: Uri)(implicit F: Async[F]) extends BpmApiClientProvider[F] {
  private def getApiClient(token: String)(implicit ev: Ask[F, AppContext]): F[ApiClient] =
    for {
      ctx <- ev.ask
      client = new ApiClient() {
        override def performAdditionalClientConfiguration(clientConfig: ClientConfig): Unit = {
          super.performAdditionalClientConfiguration(clientConfig)
          ctx.span.foreach { span =>
            clientConfig.register(new WithSpanFilter(span))
          }
        }
      }
      _ = client.setBasePath(baseBpmUrl.renderString)
      _ = client.setAccessToken(token)
    } yield client

  override def getProfileApi(token: String)(implicit ev: Ask[F, AppContext]): F[ProfileApi] =
    getApiClient(token).map(apiClient => new ProfileApi(apiClient))

  override def getProfile(token: String, profileId: UUID)(implicit
    ev: Ask[F, AppContext],
    log: StructuredLogger[F]
  ): F[Option[ProfileModel]] = for {
    bpmApi <- getProfileApi(token)
    attempt <- F.delay(bpmApi.getProfile(profileId)).attempt
    profile = attempt match {
      case Right(result) => Some(result)
      case Left(_)       => None
    }
  } yield profile

}
