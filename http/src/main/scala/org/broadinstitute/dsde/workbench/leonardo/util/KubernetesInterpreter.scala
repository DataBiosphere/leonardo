package org.broadinstitute.dsde.workbench
package leonardo
package util

import cats.Show
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.{V1Namespace, V1NamespaceList, V1ObjectMeta}
import io.kubernetes.client.util.Config
import org.broadinstitute.dsde.workbench.azure.{AKSClusterName, AzureCloudContext, AzureContainerService}
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesNamespace, PodStatus}
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates.whenStatusCode
import org.broadinstitute.dsde.workbench.google2.{autoClosableResourceF, recoverF}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.util2.withLogging
import org.typelevel.log4cats.StructuredLogger

import java.io.ByteArrayInputStream
import java.util.Base64
import scala.jdk.CollectionConverters._

class KubernetesInterpreter[F[_]](azureContainerService: AzureContainerService[F])(implicit
  F: Async[F],
  logger: StructuredLogger[F]
) extends KubernetesAlgebra[F] {

  override def createAzureClient(cloudContext: AzureCloudContext, clusterName: AKSClusterName)(implicit
    ev: Ask[F, AppContext]
  ): F[CoreV1Api] = for {
    credentials <- azureContainerService.getClusterCredentials(clusterName, cloudContext)
    client <- createClientInternal(credentials.token.value, credentials.certificate.value, credentials.server.value)
  } yield client

// Leave the implementation here in case later we'd like to converge this class with org.broadinstitute.dsde.workbench.google2.KubernetesService
//  override def createGcpClient(clusterId: GKEModels.KubernetesClusterId)(implicit
//    ev: Ask[F, AppContext]
//  ): F[CoreV1Api] = for {
//    ctx <- ev.ask
//    clusterOpt <- gkeService.getCluster(clusterId)
//    cluster <- F.fromEither(
//      clusterOpt.toRight(
//        KubernetesClusterNotFoundException(
//          s"Could not create client for cluster $clusterId because it does not exist in GCP. Trace ID: ${ctx.traceId.asString}"
//        )
//      )
//    )
//    _ <- F.blocking(credentials.refreshIfExpired())
//    token = credentials.getAccessToken.getTokenValue
//    client <- createClientInternal(token, cluster.getMasterAuth.getClusterCaCertificate, cluster.getEndpoint)
//  } yield client

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

  override def listPodStatus(client: CoreV1Api, namespace: KubernetesNamespace)(implicit
    ev: Ask[F, AppContext]
  ): F[List[PodStatus]] =
    for {
      ctx <- ev.ask
      call =
        F.blocking(
          client.listNamespacedPod(namespace.name.value,
                                   "true",
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null
          )
        )

      response <- withLogging(
        call,
        Some(ctx.traceId),
        s"io.kubernetes.client.apis.CoreV1Api.listNamespacedPod(${namespace.name.value}).pretty(true).execute()"
      )

      listPodStatus: List[PodStatus] = response.getItems.asScala.toList.flatMap(v1Pod =>
        PodStatus.stringToPodStatus
          .get(v1Pod.getStatus.getPhase)
      )

    } yield listPodStatus

  override def createNamespace(client: CoreV1Api, namespace: KubernetesNamespace)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      call = F
        .blocking(
          client.createNamespace(new V1Namespace().metadata(new V1ObjectMeta().name(namespace.name.value)),
                                 "true",
                                 null,
                                 null,
                                 null
          )
        )
        .void
      _ <- withLogging(
        call,
        Some(ctx.traceId),
        s"io.kubernetes.client.openapi.apis.CoreV1Api.createNamespace(${namespace.name.value}).pretty(true).execute()"
      )
    } yield ()

  override def deleteNamespace(client: CoreV1Api, namespace: KubernetesNamespace)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = {
    val delete = for {
      ctx <- ev.ask
      call =
        recoverF(
          F.blocking(
            client.deleteNamespace(
              namespace.name.value,
              "true",
              null,
              null,
              null,
              null,
              null
            )
          ).void
            .recoverWith {
              case e: com.google.gson.JsonSyntaxException
                  if e.getMessage.contains("Expected a string but was BEGIN_OBJECT") =>
                logger.error(e)("Ignore response parsing error")
            } // see https://github.com/kubernetes-client/java/wiki/6.-Known-Issues#1-exception-on-deleting-resources-javalangillegalstateexception-expected-a-string-but-was-begin_object
          ,
          whenStatusCode(404)
        )
      _ <- withLogging(
        call,
        Some(ctx.traceId),
        s"io.kubernetes.client.openapi.apis.CoreV1Api.deleteNamespace(${namespace.name.value}).pretty(true).execute()"
      )
    } yield ()

    // There is a known bug with the client lib json decoding.  `com.google.gson.JsonSyntaxException` occurs every time.
    // See https://github.com/kubernetes-client/java/issues/86
    delete.handleErrorWith {
      case _: com.google.gson.JsonSyntaxException =>
        F.unit
      case e: Throwable => F.raiseError(e)
    }
  }

  override def namespaceExists(client: CoreV1Api, namespace: KubernetesNamespace)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      ctx <- ev.ask
      call =
        recoverF(
          F.blocking(
            client.listNamespace("true", false, null, null, null, null, null, null, null, null, false)
          ),
          whenStatusCode(409)
        )
      v1NamespaceList <- withLogging(
        call,
        Some(ctx.traceId),
        s"io.kubernetes.client.apis.CoreV1Api.listNamespace().pretty(true).allowWatchBookmarks(false).watch(false).execute()",
        Show.show[Option[V1NamespaceList]](
          _.fold("No namespace found")(x => x.getItems.asScala.toList.map(_.getMetadata.getName).mkString(","))
        )
      )
    } yield v1NamespaceList
      .map(ls => ls.getItems.asScala.toList)
      .getOrElse(List.empty)
      .exists(x => x.getMetadata.getName == namespace.name.value)
}
