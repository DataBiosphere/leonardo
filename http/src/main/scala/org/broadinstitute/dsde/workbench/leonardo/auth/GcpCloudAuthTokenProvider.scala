package org.broadinstitute.dsde.workbench.leonardo.auth

import cats.effect.Async
import cats.syntax.all._
import com.google.api.services.storage.StorageScopes
import org.broadinstitute.dsde.workbench.google2.credentialResource
import org.broadinstitute.dsde.workbench.leonardo.CloudProvider
import org.broadinstitute.dsde.workbench.leonardo.config.ApplicationConfig

import scala.jdk.CollectionConverters.SeqHasAsJava

/***
 * GcpCloudAuthTokenProvider is a CloudServiceAuthTokenProvider for GCP.
 * Gets a token using the service account json file and the scopes.

 */
class GcpCloudAuthTokenProvider[F[_]](config: ApplicationConfig)(implicit F: Async[F])
    extends CloudServiceAuthTokenProvider[F](CloudProvider.Gcp) {
  // TODO:  The scopes were hardcoded in the previous implementation as well
  // Perhaps we should consider adding them to the configuration.
  private val saScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    StorageScopes.DEVSTORAGE_READ_ONLY
  )
  override def getCloudProviderAuthToken: F[CloudToken] =
    credentialResource(
      config.leoServiceAccountJsonFile.toAbsolutePath.toString
    ).use { credentials =>
      val scopedCredential = credentials.createScoped(saScopes.asJava)

      F.blocking(scopedCredential.refresh())
        .map(_ =>
          CloudToken(scopedCredential.getAccessToken.getTokenValue,
                     scopedCredential.getAccessToken.getExpirationTime.toInstant
          )
        )
    }
}
