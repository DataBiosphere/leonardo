package org.broadinstitute.dsde.workbench.leonardo.auth

import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.CloudProvider
import org.broadinstitute.dsde.workbench.leonardo.config.{ApplicationConfig, AzureHostingModeConfig}
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}

trait CloudAuthTokenProvider[F[_]] {
  def getCloudProvider: CloudProvider
  def getAuthToken: F[Authorization]
}

/***
 * Base class for cloud service auth token providers. Handles token caching and expiration.
 * @param cloudProvider Cloud provider
 */
abstract class CloudServiceAuthTokenProvider[F[_]](cloudProvider: CloudProvider)(implicit F: Async[F])
    extends CloudAuthTokenProvider[F] {
  // A reference to the token, so it can be updated when it expires.
  private val tokenRef = Ref.unsafe(none[CloudToken])

  override def getCloudProvider: CloudProvider = cloudProvider

  def getCloudProviderAuthToken: F[CloudToken]

  override def getAuthToken: F[Authorization] =
    for {
      cloudTokenOpt <- tokenRef.get
      now <- F.realTimeInstant
      validAccessToken <- cloudTokenOpt match {
        case None =>
          for {
            newToken <- getCloudProviderAuthToken
            _ <- tokenRef.update(_ => Some(newToken))
          } yield newToken
        case Some(cloudToken) =>
          // this token needs to live long enough to complete a stairway which
          // can take 15 minutes so set to 30 minutes to be safe (30 * 60000)
          val tokenCacheTimeInMilli = now.toEpochMilli + 1800000

          if (cloudToken.expiration.toEpochMilli <= tokenCacheTimeInMilli) for {
            newToken <- getCloudProviderAuthToken
            _ <- tokenRef.update(_ => Some(newToken))
          } yield newToken
          else F.pure(cloudToken)
      }
    } yield {
      val token = validAccessToken.value
      Authorization(Credentials.Token(AuthScheme.Bearer, token))
    }
}

object CloudAuthTokenProvider {
  def apply[F[_]: Async](hostingModeConfig: AzureHostingModeConfig,
                         applicationConfig: ApplicationConfig
  ): CloudAuthTokenProvider[F] =
    if (hostingModeConfig.enabled) {
      new AzureCloudAuthTokenProvider[F](hostingModeConfig)
    } else {
      new GcpCloudAuthTokenProvider[F](applicationConfig)
    }
}

final case class CloudToken(value: String, expiration: java.time.Instant)
