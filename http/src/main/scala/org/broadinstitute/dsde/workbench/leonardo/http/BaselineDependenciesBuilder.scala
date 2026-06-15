package org.broadinstitute.dsde.workbench.leonardo.http

import akka.actor.ActorSystem
import cats.effect.std.{Dispatcher, Queue, Semaphore}
import cats.effect.{Async, Ref, Resource}
import cats.{Monad, Parallel}
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.api.gax.longrunning.OperationFuture
import com.google.cloud.compute.v1.Operation
import fs2.Stream
import fs2.io.net.{Network, Socket => Fs2Socket, SocketGroup, SocketOption}
import com.comcast.ip4s.{
  Host => Ip4sHost,
  Hostname => Ip4sHostname,
  IpAddress => Ip4sIpAddress,
  Port => Ip4sPort,
  SocketAddress => Ip4sSocketAddress
}
import io.kubernetes.client.openapi.ApiClient
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.google2.{GooglePublisher, GoogleSubscriber}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.auth.{AuthCacheKey, CloudAuthTokenProvider, SamAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.Config.{
  applicationConfig,
  asyncTaskProcessorConfig,
  autoFreezeConfig,
  dataprocConfig,
  dateAccessUpdaterConfig,
  gceConfig,
  gkeClusterConfig,
  httpSamDaoConfig,
  imageConfig,
  kubernetesDnsCacheConfig,
  proxyConfig,
  publisherConfig,
  pubsubConfig,
  runtimeDnsCacheConfig,
  samAuthConfig,
  subscriberConfig
}
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.{HttpSamApiClientProvider, SamService, SamServiceInterp}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{RuntimeServiceConfig, SamResourceCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubCodec.leoPubsubMessageDecoder
import org.broadinstitute.dsde.workbench.leonardo.monitor.{LeoPubsubMessage, UpdateDateAccessedMessage}
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.leonardo.{AppAccessScope, KeyLock, LeoPublisher}
import org.broadinstitute.dsde.workbench.model.{IP, UserInfo}
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.messaging.{CloudPublisher, CloudSubscriber, ReceivedMessage}
import org.broadinstitute.dsp.HelmInterpreter
import org.http4s.Request
import org.http4s.client.middleware.{Logger => Http4sLogger, Metrics, Retry, RetryPolicy}
import org.typelevel.log4cats.{LoggerFactory, StructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jFactory
import scalacache.Cache
import scalacache.caffeine.CaffeineCache
import java.net.SocketException
import java.time.Instant
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import fs2.io.net.tls.TLSContext

/**
 * This class builds the baseline dependencies for the Leo App.
 * Baseline dependencies have the following characteristics:
 *  - Required regardless of where the App is hosted.
 *  - Includes clod-agnostic traits with interpreters for the cloud provider (e.g. CloudPublisher, CloudAuthTokenProvider).
 */
class BaselineDependenciesBuilder {

  def createBaselineDependencies[F[_]: Parallel](
  )(implicit
    logger: StructuredLogger[F],
    F: Async[F],
    network: Network[F],
    ec: ExecutionContext,
    as: ActorSystem,
    dbRef: DbReference[F],
    openTelemetry: OpenTelemetryMetrics[F]
  ): Resource[F, BaselineDependencies[F]] =
    for {

      // Set up DNS caches
      hostToIpMapping <- Resource.eval(Ref.of(Map.empty[String, IP]))
      proxyResolver <- Dispatcher.parallel[F].map(d => ProxyResolver(hostToIpMapping, d))

      underlyingRuntimeDnsCache = buildCache[RuntimeDnsCacheKey, scalacache.Entry[HostStatus]](
        runtimeDnsCacheConfig.cacheMaxSize,
        runtimeDnsCacheConfig.cacheExpiryTime
      )
      runtimeDnsCaffeineCache <- Resource.make(
        F.delay(CaffeineCache[F, RuntimeDnsCacheKey, HostStatus](underlyingRuntimeDnsCache))
      )(_.close)
      runtimeDnsCache = new RuntimeDnsCache(proxyConfig, dbRef, hostToIpMapping, runtimeDnsCaffeineCache)
      underlyingKubernetesDnsCache = buildCache[KubernetesDnsCacheKey, scalacache.Entry[HostStatus]](
        kubernetesDnsCacheConfig.cacheMaxSize,
        kubernetesDnsCacheConfig.cacheExpiryTime
      )

      kubernetesDnsCaffineCache <- Resource.make(
        F.delay(CaffeineCache[F, KubernetesDnsCacheKey, HostStatus](underlyingKubernetesDnsCache))
      )(_.close)
      kubernetesDnsCache = new KubernetesDnsCache(proxyConfig, dbRef, hostToIpMapping, kubernetesDnsCaffineCache)

      // Set up SSL context and http clients
      sslContext <- Resource.eval(SslContextReader.getSSLContext())
      underlyingPetKeyCache = buildCache[UserEmailAndProject, scalacache.Entry[Option[io.circe.Json]]](
        httpSamDaoConfig.petCacheMaxSize,
        httpSamDaoConfig.petCacheExpiryTime
      )
      petKeyCache <- Resource.make(
        F.delay(CaffeineCache[F, UserEmailAndProject, Option[io.circe.Json]](underlyingPetKeyCache))
      )(_.close)

      cloudAuthTokenProvider = CloudAuthTokenProvider[F](applicationConfig)
      implicit0(loggerFactory: LoggerFactory[F]) = Slf4jFactory.create[F]

      samClientProvider = new HttpSamApiClientProvider(httpSamDaoConfig.samUri.renderString,
                                                       httpSamDaoConfig.maxConcurrentRequests
      )
      samService = new SamServiceInterp(samClientProvider, cloudAuthTokenProvider)

      samDao <- buildHttpClient(sslContext, hostToIpMapping, Some("leo_sam_client"), true).map(client =>
        HttpSamDAO[F](
          client,
          httpSamDaoConfig,
          petKeyCache,
          cloudAuthTokenProvider
        )
      )
      jupyterDao <- buildHttpClient(sslContext, hostToIpMapping, Some("leo_jupyter_client"), false).map(client =>
        new HttpJupyterDAO[F](runtimeDnsCache, client, samDao)
      )
      welderDao <- buildHttpClient(sslContext, hostToIpMapping, Some("leo_welder_client"), false).map(client =>
        new HttpWelderDAO[F](runtimeDnsCache, client, samDao)
      )
      rstudioDAO <- buildHttpClient(sslContext, hostToIpMapping, Some("leo_rstudio_client"), false).map(client =>
        new HttpRStudioDAO(runtimeDnsCache, client)
      )
      appDAO <- buildHttpClient(sslContext, hostToIpMapping, Some("leo_app_client"), false).map(client =>
        new HttpAppDAO(kubernetesDnsCache, client)
      )
      appDescriptorDAO <- buildHttpClient(sslContext, hostToIpMapping, None, true).map(client =>
        new HttpAppDescriptorDAO(client)
      )
      dockerDao <- buildHttpClient(sslContext, hostToIpMapping, None, true).map(client => HttpDockerDAO[F](client))

      // Set up identity providers
      underlyingAuthCache = buildCache[AuthCacheKey, scalacache.Entry[Boolean]](samAuthConfig.authCacheMaxSize,
                                                                                samAuthConfig.authCacheExpiryTime
      )
      authCache <- Resource.make(F.delay(CaffeineCache[F, AuthCacheKey, Boolean](underlyingAuthCache)))(s => s.close)
      authProvider = new SamAuthProvider(samDao, samAuthConfig, authCache)

      cloudPublisher <- createCloudPublisher[F]

      underlyingNodepoolLockCache = buildCache[KubernetesClusterId, scalacache.Entry[Semaphore[F]]](
        gkeClusterConfig.nodepoolLockCacheMaxSize,
        gkeClusterConfig.nodepoolLockCacheExpiryTime
      )
      nodepoolLockCache <- Resource.make(
        F.delay(CaffeineCache[F, KubernetesClusterId, Semaphore[F]](underlyingNodepoolLockCache))
      )(_.close)
      nodepoolLock = KeyLock[F, KubernetesClusterId](nodepoolLockCache)

      // Set up PubSub queues
      publisherQueue <- Resource.eval(Queue.bounded[F, LeoPubsubMessage](pubsubConfig.queueSize))
      leoPublisher = new LeoPublisher(publisherQueue, cloudPublisher)
      dataAccessedUpdater <- Resource.eval(
        Queue.bounded[F, UpdateDateAccessedMessage](dateAccessUpdaterConfig.queueSize)
      )
      subscriberQueue <- Resource.eval(Queue.bounded[F, ReceivedMessage[LeoPubsubMessage]](pubsubConfig.queueSize))
      subscriber <- createCloudSubscriber(subscriberQueue)

      asyncTasksQueue <- Resource.eval(Queue.bounded[F, Task[F]](asyncTaskProcessorConfig.queueBound))

      // Set up k8s and helm clients
      underlyingKubeClientCache = buildCache[KubernetesClusterId, scalacache.Entry[ApiClient]](
        200,
        2 hours
      )

      underlyingGoogleTokenCache = buildCache[String, scalacache.Entry[(UserInfo, Instant)]](
        proxyConfig.tokenCacheMaxSize,
        proxyConfig.tokenCacheExpiryTime
      )
      googleTokenCache <- Resource.make(
        F.delay(CaffeineCache[F, String, (UserInfo, Instant)](underlyingGoogleTokenCache))
      )(_.close)

      underlyingSamResourceCache = buildCache[SamResourceCacheKey,
                                              scalacache.Entry[(Option[String], Option[AppAccessScope])]
      ](
        proxyConfig.internalIdCacheMaxSize,
        proxyConfig.internalIdCacheExpiryTime
      )
      samResourceCache <- Resource.make(
        F.delay(
          CaffeineCache[F, SamResourceCacheKey, (Option[String], Option[AppAccessScope])](underlyingSamResourceCache)
        )
      )(s => s.close)

      underlyingOperationFutureCache = buildCache[Long, scalacache.Entry[OperationFuture[Operation, Operation]]](
        500,
        5 minutes
      )
      operationFutureCache <- Resource.make(
        F.delay(CaffeineCache[F, Long, OperationFuture[Operation, Operation]](underlyingOperationFutureCache))
      )(_.close)

      oidcConfig <- Resource.eval(
        OpenIDConnectConfiguration[F](
          ConfigReader.appConfig.oidc.authorityEndpoint.renderString,
          ConfigReader.appConfig.oidc.clientId,
          extraAuthParams = Some("prompt=login")
        )
      )

      // Use a low concurrency for helm because it can generate very chatty network traffic
      // (especially for Galaxy) and cause issues at high concurrency.
      helmConcurrency <- Resource.eval(Semaphore[F](20L))
      helmClient = new HelmInterpreter[F](helmConcurrency)

      recordMetricsProcesses = List(
        CacheMetrics("authCache").processWithUnderlyingCache(underlyingAuthCache),
        CacheMetrics("petTokenCache")
          .processWithUnderlyingCache(underlyingPetKeyCache),
        CacheMetrics("googleTokenCache")
          .processWithUnderlyingCache(underlyingGoogleTokenCache),
        CacheMetrics("samResourceCache")
          .processWithUnderlyingCache(underlyingSamResourceCache),
        CacheMetrics("runtimeDnsCache")
          .processWithUnderlyingCache(underlyingRuntimeDnsCache),
        CacheMetrics("kubernetesDnsCache")
          .processWithUnderlyingCache(underlyingKubernetesDnsCache),
        CacheMetrics("kubernetesApiClient")
          .processWithUnderlyingCache(underlyingKubeClientCache)
      )

      runtimeServiceConfig = RuntimeServiceConfig(
        proxyConfig.proxyUrlBase,
        imageConfig,
        autoFreezeConfig,
        dataprocConfig,
        gceConfig
      )
    } yield BaselineDependencies[F](
      sslContext,
      runtimeDnsCache,
      samDao,
      dockerDao,
      jupyterDao,
      rstudioDAO,
      welderDao,
      authProvider,
      leoPublisher,
      publisherQueue,
      dataAccessedUpdater,
      subscriber,
      asyncTasksQueue,
      nodepoolLock,
      proxyResolver,
      recordMetricsProcesses,
      googleTokenCache,
      samResourceCache,
      oidcConfig,
      appDAO,
      runtimeServiceConfig,
      kubernetesDnsCache,
      appDescriptorDAO,
      helmClient,
      operationFutureCache,
      openTelemetry,
      samService
    )

  private def createCloudSubscriber[F[_]: Parallel](
    subscriberQueue: Queue[F, ReceivedMessage[LeoPubsubMessage]]
  )(implicit F: Async[F], logger: StructuredLogger[F]): Resource[F, CloudSubscriber[F, LeoPubsubMessage]] =
    GoogleSubscriber.resource[F, LeoPubsubMessage](subscriberConfig, subscriberQueue)

  private def createCloudPublisher[F[_]](implicit
    F: Async[F],
    logger: StructuredLogger[F]
  ): Resource[F, CloudPublisher[F]] = GooglePublisher.cloudPublisherResource[F](publisherConfig)

  private def buildCache[K, V](maxSize: Int,
                               expiresIn: FiniteDuration
  ): com.github.benmanes.caffeine.cache.Cache[K, V] =
    Caffeine
      .newBuilder()
      .maximumSize(maxSize)
      .expireAfterWrite(expiresIn.toSeconds, TimeUnit.SECONDS)
      .recordStats()
      .build[K, V]()

  private def buildHttpClient[F[_]: Async: StructuredLogger: Network: LoggerFactory](
    sslContext: SSLContext,
    hostToIpMapping: Ref[F, Map[String, IP]],
    metricsPrefix: Option[String],
    withRetry: Boolean
  ): Resource[F, org.http4s.client.Client[F]] = {
    val retryPolicy = RetryPolicy[F](
      RetryPolicy.exponentialBackoff(30 seconds, 5),
      (req, result) =>
        result match {
          case Left(e) if e.isInstanceOf[SocketException] => true
          case _                                          => RetryPolicy.defaultRetriable(req, result)
        }
    )

    val tlsContext = TLSContext.Builder.forAsync[F].fromSSLContext(sslContext)

    for {
      httpClient <- org.http4s.ember.client.EmberClientBuilder.default
        .withTLSContext(tlsContext)
        .withTimeout(60 seconds)
        .withMaxTotal(100)
        .withIdleConnectionTime(30 seconds)
        .withMaxResponseHeaderSize(16384)
        .withSocketGroup(new MappedDnsSocketGroup[F](Network[F], hostToIpMapping))
        .build

      httpClientWithLogging = Http4sLogger[F](logHeaders = true, logBody = false, logAction = Some(s => logAction(s)))(
        httpClient
      )

      clientWithRetry = if (withRetry) Retry(retryPolicy)(httpClientWithLogging) else httpClientWithLogging

      finalClient <- metricsPrefix match {
        case None => Resource.pure[F, org.http4s.client.Client[F]](clientWithRetry)
        case Some(prefix) =>
          val classifierFunc = (r: Request[F]) => Some(r.method.toString.toLowerCase)
          for {
            metricsOps <- org.http4s.metrics.prometheus.Prometheus
              .metricsOps(io.prometheus.client.CollectorRegistry.defaultRegistry, prefix)
            meteredClient = Metrics[F](
              metricsOps,
              classifierFunc
            )(clientWithRetry)
          } yield meteredClient
      }
    } yield finalClient
  }

  /** A SocketGroup[F] wrapper that intercepts TCP connections to mapped proxy hostnames and
    * redirects them to the corresponding VM IP, without modifying the request URI.
    *
    * This preserves the original hostname in the URI so Ember's TLS layer uses it for SNI,
    * allowing TLS certificate validation to succeed against the VM's hostname-based certificate.
    * Only the TCP connection itself is redirected to the resolved IP.
    *
    * This replicates the behavior of Blaze's withCustomDnsResolver at the socket layer.
    */
  private class MappedDnsSocketGroup[F[_]: Async](
    underlying: SocketGroup[F],
    hostToIpMapping: Ref[F, Map[String, IP]]
  ) extends SocketGroup[F] {

    override def client(
      to: Ip4sSocketAddress[Ip4sHost],
      options: List[SocketOption] = List.empty
    ): Resource[F, Fs2Socket[F]] =
      Resource.eval(resolveHost(to)).flatMap(underlying.client(_, options))

    override def server(
      address: Option[Ip4sHost] = None,
      port: Option[Ip4sPort] = None,
      options: List[SocketOption] = List.empty
    ): Stream[F, Fs2Socket[F]] =
      underlying.server(address, port, options)

    override def serverResource(
      address: Option[Ip4sHost] = None,
      port: Option[Ip4sPort] = None,
      options: List[SocketOption] = List.empty
    ): Resource[F, (Ip4sSocketAddress[Ip4sIpAddress], Stream[F, Fs2Socket[F]])] =
      underlying.serverResource(address, port, options)

    private def resolveHost(to: Ip4sSocketAddress[Ip4sHost]): F[Ip4sSocketAddress[Ip4sHost]] =
      to.host match {
        case hostname: Ip4sHostname =>
          Async[F].map(hostToIpMapping.get) { mapping =>
            mapping.get(hostname.toString).flatMap(ip => Ip4sIpAddress.fromString(ip.asString)) match {
              case Some(ipAddr) => Ip4sSocketAddress[Ip4sHost](ipAddr, to.port)
              case None         => to
            }
          }
        case _ => Async[F].pure(to)
      }
  }

  private def logAction[F[_]: Monad: StructuredLogger](s: String): F[Unit] =
    StructuredLogger[F].info(s)
}

object BaselineDependenciesBuilder {
  def apply(): BaselineDependenciesBuilder =
    new BaselineDependenciesBuilder()
}

final case class BaselineDependencies[F[_]](
  sslContext: SSLContext,
  runtimeDnsCache: RuntimeDnsCache[F],
  samDAO: HttpSamDAO[F],
  dockerDAO: HttpDockerDAO[F],
  jupyterDAO: HttpJupyterDAO[F],
  rstudioDAO: HttpRStudioDAO[F],
  welderDAO: HttpWelderDAO[F],
  authProvider: SamAuthProvider[F],
  leoPublisher: LeoPublisher[F],
  publisherQueue: Queue[F, LeoPubsubMessage],
  dateAccessedUpdaterQueue: Queue[F, UpdateDateAccessedMessage],
  subscriber: CloudSubscriber[F, LeoPubsubMessage],
  asyncTasksQueue: Queue[F, Task[F]],
  nodepoolLock: KeyLock[F, KubernetesClusterId],
  proxyResolver: ProxyResolver[F],
  recordMetricsProcesses: List[Stream[F, Unit]],
  googleTokenCache: scalacache.Cache[F, String, (UserInfo, Instant)],
  samResourceCache: scalacache.Cache[F, SamResourceCacheKey, (Option[String], Option[AppAccessScope])],
  openIDConnectConfiguration: OpenIDConnectConfiguration,
  appDAO: AppDAO[F],
  runtimeServicesConfig: RuntimeServiceConfig,
  kubernetesDnsCache: KubernetesDnsCache[F],
  appDescriptorDAO: HttpAppDescriptorDAO[F],
  helmClient: HelmInterpreter[F],
  operationFutureCache: Cache[F, Long, OperationFuture[Operation, Operation]],
  openTelemetryMetrics: OpenTelemetryMetrics[F],
  samService: SamService[F]
)
