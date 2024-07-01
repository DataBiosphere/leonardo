package org.broadinstitute.dsde.workbench.leonardo.dao

import bio.terra.profile.api.ProfileApi
import bio.terra.profile.model.ProfileModel
import bio.terra.profile.client.ApiClient
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.AppContext
import org.http4s.Uri

import java.util.UUID

trait BpmApiClientProvider[F[_]] {

  def getProfileApi(token: String): F[ProfileApi]

  def getProfile(token: String, profileId: UUID)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[ProfileModel]]

}

class HttpBpmClientProvider[F[_]](baseBpmUrl: Uri)(implicit F: Async[F]) extends BpmApiClientProvider[F] {
  private def getApiClient(token: String): F[ApiClient] = {
    val client = new ApiClient()
    client.setBasePath(baseBpmUrl.renderString)
    client.setAccessToken(token)
    F.pure(client)
  }

  //    for {
//      ctx <- ev.ask
////      client = new ApiClient()
////    {
////        override def performAdditionalClientConfiguration(clientConfig: ClientConfig): Unit = {
////          super.performAdditionalClientConfiguration(clientConfig)
////          ctx.span.foreach { span =>
////            clientConfig.register(new WithSpanFilter(span))
////          }
////        }
////      }
//      _ = client.setBasePath(baseBpmUrl.renderString)
//      _ = client.setAccessToken(token)
//    } yield client

  override def getProfileApi(token: String): F[ProfileApi] =
    getApiClient(token).map(apiClient => new ProfileApi(apiClient))

  override def getProfile(token: String, profileId: UUID)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[ProfileModel]] = for {
    bpmApi <- getProfileApi(token)
    attempt <- F.delay(bpmApi.getProfile(profileId)).attempt
    profile = attempt match {
      case Right(result) => Some(result)
      case Left(_)       => None
    }
  } yield profile

}
