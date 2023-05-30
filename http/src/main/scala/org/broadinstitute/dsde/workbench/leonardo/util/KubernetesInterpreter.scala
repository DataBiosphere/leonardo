package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.auth.oauth2.GoogleCredentials
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.util.Config
import org.broadinstitute.dsde.workbench.azure.{AKSClusterName, AzureCloudContext, AzureContainerService}
import org.broadinstitute.dsde.workbench.google2.{
  autoClosableResourceF,
  GKEModels,
  GKEService,
  KubernetesClusterNotFoundException
}
import org.broadinstitute.dsde.workbench.leonardo.http._

import java.io.ByteArrayInputStream
import java.util.Base64

class KubernetesInterpreter[F[_]](azureContainerService: AzureContainerService[F],
                                  gkeService: GKEService[F],
                                  credentials: GoogleCredentials
)(implicit
  F: Async[F]
) extends KubernetesAlgebra[F] {

  override def createAzureClient(cloudContext: AzureCloudContext, clusterName: AKSClusterName)(implicit
    ev: Ask[F, AppContext]
  ): F[CoreV1Api] = for {
    credentials <- azureContainerService.getClusterCredentials(clusterName, cloudContext)
    client <- createClientInternal(credentials.token.value, credentials.certificate.value, credentials.server.value)
  } yield client

  override def createGcpClient(clusterId: GKEModels.KubernetesClusterId)(implicit
    ev: Ask[F, AppContext]
  ): F[CoreV1Api] = for {
    ctx <- ev.ask
    clusterOpt <- gkeService.getCluster(clusterId)
    cluster <- F.fromEither(
      clusterOpt.toRight(
        KubernetesClusterNotFoundException(
          s"Could not create client for cluster $clusterId because it does not exist in GCP. Trace ID: ${ctx.traceId.asString}"
        )
      )
    )
    _ <- F.delay(credentials.refreshIfExpired())
    token = credentials.getAccessToken.getTokenValue
    client <- createClientInternal(token, cluster.getMasterAuth.getClusterCaCertificate, cluster.getEndpoint)
  } yield client

  // The underlying http client for ApiClient claims that it releases idle threads and that shutdown is not necessary
  // Here is a guide on how to proactively release resource if this proves to be problematic https://square.github.io/okhttp/4.x/okhttp/okhttp3/-ok-http-client/#shutdown-isnt-necessary
  private def createClientInternal(token: String, cert: String, endpoint: String): F[CoreV1Api] = {
    val certResource = autoClosableResourceF(
      new ByteArrayInputStream(Base64.getDecoder.decode(cert))
    )
    for {
      apiClient <- certResource.use { certStream =>
        F.delay(
          Config
            .fromToken(
              endpoint,
              token
            )
            .setSslCaCert(certStream)
            // appending here a .setDebugging(true) prints out useful API request/response info for development
        )
      }
      _ <- F.blocking(apiClient.setApiKey(token))
    } yield new CoreV1Api(apiClient)
  }
}
