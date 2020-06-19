package org.broadinstitute.dsde.workbench.leonardo.service

import java.io.{FileInputStream, InputStream}
import java.security.cert.{Certificate, CertificateFactory}
import java.security.{KeyStore, SecureRandom}
import java.time.Instant
import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.javadsl.model.headers.RawHeader
import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import akka.http.scaladsl.model.Uri.Host
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.{HttpHeader, HttpRequest, HttpResponse, StatusCodes}
import akka.stream.scaladsl.{Sink, Source}
import cats.effect.{Async, Blocker, ContextShift, IO, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.auth.oauth2.GoogleCredentials
import com.typesafe.scalalogging.LazyLogging
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.google2.GKEService
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.AppName
import org.broadinstitute.dsde.workbench.leonardo.config.CacheConfig
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.http.service.{AppNotFoundException, ServiceNotFoundException}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, LeoException}
import org.broadinstitute.dsde.workbench.leonardo.service.KubernetesDnsCache._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}

import scala.collection.immutable
import scala.concurrent.ExecutionContext

case class KubernetesProxyConfig(port: Int, remoteUser: String)

class KubernetesProxyService[F[_]](
                              kubernetesProxyConfig: KubernetesProxyConfig,
                              gkeService: GKEService[F],
                              credentials: GoogleCredentials,
                              kubernetesDnsCache: KubernetesDnsCache[F],
authProvider: LeoAuthProvider[F],
//dateAccessUpdaterQueue: InspectableQueue[IO, UpdateDateAccessMessage],
blocker: Blocker)
                            (implicit val system: ActorSystem,
                             F: Async[F],
                             executionContext: ExecutionContext,
                             timer: Timer[F],
                             cs: ContextShift[F],
                             cs2: ContextShift[IO],
                             dbRef: DbReference[F])
  extends LazyLogging {

  /*
  * Checks the user has the required notebook action, returning 401 or 404 depending on whether they can know the runtime exists
  */
  private[leonardo] def authCheck(
                                   userInfo: UserInfo,
                                   googleProject: GoogleProject,
                                   appName: AppName
//                                   ,action: _
                                 )(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      x <- F.unit
//      samResource <- getCachedRuntimeSamResource(googleProject, runtimeName)
//      hasViewPermission <- authProvider
//        .hasRuntimePermission(samResource, userInfo, GetRuntimeStatus, googleProject)
//      //TODO: combine the sam calls into one
//      hasRequiredPermission <- authProvider
//        .hasRuntimePermission(samResource, userInfo, notebookAction, googleProject)
//      _ <- if (!hasViewPermission) {
//        IO.raiseError(RuntimeNotFoundException(googleProject, runtimeName, s"${notebookAction} permission is required"))
//      } else if (!hasRequiredPermission) {
//        IO.raiseError(AuthorizationError(Some(userInfo.userEmail)))
//      } else IO.unit
    } yield ()

  def proxyRequest(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName, serviceName: ServiceName, request: HttpRequest)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[HttpResponse] =
    for {
      _ <- authCheck(userInfo, googleProject, appName)
      now <- timer.clock.realTime(TimeUnit.MILLISECONDS)
      r <- proxyInternal(userInfo, googleProject, appName, serviceName, request, Instant.ofEpochMilli(now))
    } yield r

  def proxyInternal(userInfo: UserInfo, googleProject: GoogleProject, appName: AppName, serviceName: ServiceName, request: HttpRequest, now: Instant)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[HttpResponse] =
    for {
      _ <- F.delay(logger.debug(
        s"Received proxy request for ${googleProject}/${appName}: ${kubernetesDnsCache.stats} / ${kubernetesDnsCache.size}"
      ))
      ctx <- ev.ask
      resp <- getTargetHost(googleProject, appName, serviceName) flatMap {
        case HostAppReady(targetHost, gkeClusterId) =>
          // If this is a WebSocket request (e.g. wss://leo:8080/...) then akka-http injects a
          // virtual UpgradeToWebSocket header which contains facilities to handle the WebSocket data.
          // The presence of this header distinguishes WebSocket from http requests.
          val res = for {
            //          _ <- dateAccessUpdaterQueue.enqueue1(UpdateDateAccessMessage(runtimeName, googleProject, now))
          //TODO: when checking cache,ensure we dont make google call for cert again
            response <- handleHttpRequest(userInfo, targetHost, request)
            r <- if (response.status.isFailure())
              F.delay(logger.info(s"Error response for proxied request ${request.uri}: ${response.status}")).as(response)
            else F.pure(response)
          } yield r

          res.recoverWith {
            case e =>
              F.delay(logger.error("Error occurred in proxy", e)) >> F.raiseError[HttpResponse](
                KubernetesProxyException(googleProject, appName)
              )
          }
        case HostAppNotReady =>
          F.delay(logger.warn(s"proxy host not ready for ${googleProject}/${appName}")) >> F.raiseError[HttpResponse](
            AppNotReadyException(googleProject, appName)
          )
        case HostServiceNotFound =>
          F.delay(logger.warn(s"service not found for ${googleProject}/${appName}/${serviceName}")) >> F.raiseError[HttpResponse](
            ServiceNotFoundException(googleProject, appName, serviceName, ctx)
          )
        case HostAppNotFound =>
          F.delay(logger.warn(s"proxy host not found for ${googleProject}/${appName}")) >> F.raiseError[HttpResponse](
            AppNotFoundException(googleProject, appName, ctx)
          )
      }
    } yield resp

  def getTargetHost(googleProject: GoogleProject, appName: AppName, serviceName: ServiceName): F[AppHostStatus] =
    kubernetesDnsCache.getHostStatus(KubernetesCacheKey(googleProject, appName, serviceName))

  import akka.http.scaladsl.Http
  private def handleHttpRequest(userInfo: UserInfo, targetHost: Host, gkeClusterId: KubernetesClusterId, request: HttpRequest): F[HttpResponse] =
    for {
     _ <- F.delay(logger.debug(s"Opening https connection to ${targetHost.address}:${kubernetesProxyConfig.port}"))
     httpsContext <- getSSLContext(null)
    flow <- F.delay(Http().outgoingConnectionHttps(targetHost.address, kubernetesProxyConfig.port, httpsContext))

     //TODO: add remote_user header
    newHeaders = filterHeaders(request.headers) ++ getKubernetesHeaders(userInfo)
    source <- F.liftIO(IO.fromFuture(IO(
      Source
      .single(request.copy(headers = newHeaders))
      .via(flow)
      .runWith(Sink.head)
    )))
    } yield source

  def getSSLContext(sslCaCert: InputStream): F[HttpsConnectionContext] = {
    //TODO ??
    val password = null
// see https://stackoverflow.com/questions/889406/using-multiple-ssl-client-certificates-in-java-with-the-same-host
    for {
     certificateFactory <- F.delay(CertificateFactory.getInstance("X.509"))
     cert <- F.delay(certificateFactory.generateCertificate(sslCaCert))

     keyStore <- F.delay(KeyStore.getInstance(KeyStore.getDefaultType())) //TODO: PKCS12?
     _ <- F.delay(keyStore.load(null, password))
     alias = "ca" + UUID.randomUUID()
     _ <- F.delay(keyStore.setCertificateEntry(alias, cert))

     trustManagerFactory <- F.delay(TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm))
     _ <- F.delay(trustManagerFactory.init(keyStore))

     keyManagerFactory <- F.delay(KeyManagerFactory.getInstance("SunX509"))
     _ <- F.delay(keyManagerFactory.init(keyStore, password))

    sslContext <- F.delay(SSLContext.getInstance("TLS"))
    _ <- F.delay(sslContext.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, null))
    } yield ConnectionContext.https(sslContext)
  }

  private def getKubernetesHeaders(userInfo: UserInfo): immutable.Seq[HttpHeader] =
    immutable.Seq(
      Authorization(OAuth2BearerToken(credentials.getAccessToken.getTokenValue)),
      RawHeader.create("HTTP_REMOTE_USER", userInfo.userEmail.toString)
    )

  private def filterHeaders(headers: immutable.Seq[HttpHeader]): immutable.Seq[HttpHeader] =
    headers.filterNot(header => HeadersToFilter(header.lowercaseName()))

  private val HeadersToFilter = Set(
    "Timeout-Access",
    "Connection"
  ).map(_.toLowerCase)

  final case class KubernetesProxyException(googleProject: GoogleProject, appName: AppName)
    extends LeoException(s"Unable to proxy connection to app on ${googleProject.value}/${appName.value}",
      StatusCodes.InternalServerError)
}

final case class AppNotReadyException(googleProject: GoogleProject, appName: AppName)
  extends LeoException(
    s"App ${googleProject.value}/${appName.value} is not ready yet. It may be creating, try again later",
    StatusCodes.Locked
  )

final case class AppPausedException(googleProject: GoogleProject, appName: AppName)
  extends LeoException(
    s"Runtime ${googleProject.value}/${appName.value} is stopped. Start your app before proceeding.",
    StatusCodes.UnprocessableEntity
  )

final case class ProxyException(googleProject: GoogleProject, appName: AppName)
  extends LeoException(s"Unable to proxy connection to app ${googleProject.value}/${appName.value}",
    StatusCodes.InternalServerError)
