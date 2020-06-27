package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.io.{ByteArrayInputStream, InputStream}
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import akka.http.scaladsl.model.Uri.{Host, Path}
import cats.effect.implicits._
import cats.effect.{Async, Blocker, ContextShift, Effect, Sync, Timer}
import com.google.common.cache.{CacheBuilder, CacheLoader, CacheStats}
import com.typesafe.scalalogging.LazyLogging
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesApiServerIp, KubernetesClusterCaCert}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.config.CacheConfig
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, GetAppResult, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import cats.implicits._
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google2.{autoClosableResourceF, GKEService}
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppHostStatus.{
  HostAppNotFound,
  HostAppNotReady,
  HostAppReady,
  HostServiceNotFound
}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.concurrent.ExecutionContext

class KubernetesProxyCache[F[_]: Effect: ContextShift: Sync: Timer](
  kubernetesCacheConfig: CacheConfig,
  blocker: Blocker,
  gkeService: GKEService[F]
)(implicit ec: ExecutionContext, dbRef: DbReference[F], F: Async[F])
    extends LazyLogging {

  def getHostStatus(key: AppStatusCacheKey): F[AppHostStatus] =
    blocker.blockOn(Effect[F].delay(projectAppToHostStatus.get(key)))
  def size: Long = projectAppToHostStatus.size
  def stats: CacheStats = projectAppToHostStatus.stats

  private val projectAppToHostStatus =
    CacheBuilder
      .newBuilder()
      .expireAfterWrite(kubernetesCacheConfig.cacheExpiryTime.toSeconds, TimeUnit.SECONDS)
      .maximumSize(kubernetesCacheConfig.cacheMaxSize)
      .recordStats
      .build(
        new CacheLoader[AppStatusCacheKey, AppHostStatus] {
          def load(key: AppStatusCacheKey): AppHostStatus = {
            logger.debug(
              s"DNS Cache miss for ${key.googleProject} / ${key.appName} / ${key.serviceName} ...loading from DB..."
            )
            val res = for {
              appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(key.googleProject, key.appName).transaction
              status = appOpt match {
                case Some(app) => hostStatusByAppResult(app, key)
                case None      => HostAppNotFound
              }
            } yield status
            res.toIO.unsafeRunSync
          }
        }
      )

  private val clusterSSLContextCache =
    CacheBuilder
      .newBuilder()
      .expireAfterWrite(kubernetesCacheConfig.cacheExpiryTime.toSeconds, TimeUnit.SECONDS)
      .maximumSize(kubernetesCacheConfig.cacheMaxSize)
      .recordStats
      .build(
        new CacheLoader[ClusterSSLContextCacheKey, HttpsConnectionContext] {
          def load(key: ClusterSSLContextCacheKey): HttpsConnectionContext = {
            logger.debug(
              s"DNS Cache miss for ${key.kubernetesClusterId.toString}. Loading SSLContext with a google call..."
            )
            implicit val traceId = ApplicativeAsk.const[F, TraceId](TraceId(UUID.randomUUID()))
            performCacheLoad(key).toIO.unsafeRunSync
          }
        }
      )

  def performCacheLoad(
    key: ClusterSSLContextCacheKey
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[HttpsConnectionContext] =
    for {
      clusterOpt <- gkeService.getCluster(key.kubernetesClusterId)
      cluster <- F.fromOption(clusterOpt, AppIsReadyButClusterNotFound(key.kubernetesClusterId, key.appKey))
      cert = KubernetesClusterCaCert(cluster.getMasterAuth.getClusterCaCertificate)
      cert <- F.fromEither(cert.base64Cert)
      certResource = autoClosableResourceF(new ByteArrayInputStream(cert))
      sslContext <- certResource.use(certStream => getSSLContext(certStream))
    } yield sslContext

  case class AppIsReadyButClusterNotFound(kubernetesClusterId: KubernetesClusterId, appKey: AppStatusCacheKey)
      extends LeoException(
        s"App ${appKey.googleProject}/${appKey.appName}/${appKey.serviceName} had a ready status in the database, but cluster ${kubernetesClusterId.toString} was not found in google.",
        StatusCodes.InternalServerError
      )

  def getSSLContext(sslCaCert: InputStream): F[HttpsConnectionContext] =
    // see https://stackoverflow.com/questions/889406/using-multiple-ssl-client-certificates-in-java-with-the-same-host
    for {
      certificateFactory <- F.delay(CertificateFactory.getInstance("X.509"))
      password = null
      cert <- F.delay(certificateFactory.generateCertificate(sslCaCert))

      keyStore <- F.delay(KeyStore.getInstance(KeyStore.getDefaultType())) //TODO: PKCS12?
      //passing null here created a new keyStore as per the docs. We want a keystore per cluster
      //https://docs.oracle.com/javase/6/docs/api/java/security/KeyStore.html#load%28java.io.InputStream,%20char%5B%5D%29
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

  private def readyAppToHost(appResult: GetAppResult,
                             ip: KubernetesApiServerIp,
                             key: AppStatusCacheKey): AppHostStatus = {

    val service = appResult.app.appResources.services.filter(_.config.name == key.serviceName).headOption
    service.fold[AppHostStatus](HostServiceNotFound) { s =>
      val internalHost = Host(ip.value)
      val internalPath =
        Path(
          s"/api/v1/namespaces/${appResult.app.appResources.namespace.name.value}/services/${s.config.name.value}:${s.config.port.num.value}/proxy/"
        )
      val sslContext = clusterSSLContextCache.get(ClusterSSLContextCacheKey(appResult.cluster.getGkeClusterId, key))
      HostAppReady(
        internalHost,
        internalPath,
        sslContext
      )
    }
  }

  private def hostStatusByAppResult(appResult: GetAppResult, key: AppStatusCacheKey): AppHostStatus =
    appResult.cluster.asyncFields
      .map(_.apiServerIp)
      .fold[AppHostStatus](HostAppNotReady)(ip => readyAppToHost(appResult, ip, key))

}

sealed trait AppHostStatus extends Product with Serializable
object AppHostStatus {
  final case object HostAppNotFound extends AppHostStatus
  final case object HostServiceNotFound extends AppHostStatus
  final case object HostAppNotReady extends AppHostStatus
  final case class HostAppReady(hostname: Host, path: Path, sslContext: HttpsConnectionContext) extends AppHostStatus
}

case class AppStatusCacheKey(googleProject: GoogleProject, appName: AppName, serviceName: ServiceName)
case class ClusterSSLContextCacheKey(kubernetesClusterId: KubernetesClusterId, appKey: AppStatusCacheKey)
