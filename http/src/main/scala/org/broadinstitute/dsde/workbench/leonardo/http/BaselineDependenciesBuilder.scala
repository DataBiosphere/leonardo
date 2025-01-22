package org.broadinstitute.dsde.workbench.leonardo.http

import akka.actor.ActorSystem
import cats.effect.std.{Dispatcher, Queue, Semaphore}
import cats.effect.{Async, Ref, Resource}
import cats.{Monad, Parallel}
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.api.gax.longrunning.OperationFuture
import com.google.cloud.compute.v1.Operation
import fs2.Stream
import io.kubernetes.client.openapi.ApiClient
import org.broadinstitute.dsde.workbench.azure._
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
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  AzureServiceConfig,
  RuntimeServiceConfig,
  SamResourceCacheKey
}
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
import org.http4s.blaze.client
import org.http4s.client.RequestKey
import org.http4s.client.middleware.{Logger => Http4sLogger, Metrics, Retry, RetryPolicy}
import org.typelevel.log4cats.StructuredLogger
import scalacache.Cache
import scalacache.caffeine.CaffeineCache

import java.net.{InetSocketAddress, SocketException}
import java.time.Instant
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}

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

      cloudAuthTokenProvider = CloudAuthTokenProvider[F](ConfigReader.appConfig.azure.hostingModeConfig,
                                                         applicationConfig
      )

      samClientProvider = new HttpSamApiClientProvider(httpSamDaoConfig.samUri.renderString)
      samService = new SamServiceInterp(samClientProvider, cloudAuthTokenProvider)

      samDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_sam_client"), true).map(client =>
        HttpSamDAO[F](
          client,
          httpSamDaoConfig,
          petKeyCache,
          cloudAuthTokenProvider
        )
      )
      cromwellDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_cromwell_client"), false).map(
        client => new HttpCromwellDAO[F](client)
      )
      cbasDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_cbas_client"), false).map(client =>
        new HttpCbasDAO[F](client)
      )
      wdsDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_wds_client"), false).map(client =>
        new HttpWdsDAO[F](client)
      )
      hailBatchDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_hail_batch_client"), false)
        .map(client => new HttpHailBatchDAO[F](client))
      listenerDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_listener_client"), false).map(
        client => new HttpListenerDAO[F](client)
      )
      jupyterDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_jupyter_client"), false).map(
        client => new HttpJupyterDAO[F](runtimeDnsCache, client, samDao)
      )
      welderDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_welder_client"), false).map(
        client => new HttpWelderDAO[F](runtimeDnsCache, client, samDao)
      )
      rstudioDAO <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_rstudio_client"), false).map(
        client => new HttpRStudioDAO(runtimeDnsCache, client)
      )
      appDAO <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_app_client"), false).map(client =>
        new HttpAppDAO(kubernetesDnsCache, client)
      )
      appDescriptorDAO <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, None, true).map(client =>
        new HttpAppDescriptorDAO(client)
      )
      dockerDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, None, true).map(client =>
        HttpDockerDAO[F](client)
      )
      wsmDao <- buildHttpClient(sslContext, proxyResolver.resolveHttp4s, Some("leo_wsm_client"), true)
        .map(client => new HttpWsmDao[F](client, ConfigReader.appConfig.azure.wsm))

      wsmClientProvider = new HttpWsmClientProvider(ConfigReader.appConfig.azure.wsm.uri)

      bpmClientProvider = new HttpBpmClientProvider(ConfigReader.appConfig.azure.bpm.uri)

      azureRelay <- AzureRelayService.fromAzureAppRegistrationConfig(ConfigReader.appConfig.azure.appRegistration)

      azureVmService <- AzureVmService.fromAzureAppRegistrationConfig(ConfigReader.appConfig.azure.appRegistration)

      azureContainerService <- AzureContainerService.fromAzureAppRegistrationConfig(
        ConfigReader.appConfig.azure.appRegistration
      )

      azureBatchService <- AzureBatchService.fromAzureAppRegistrationConfig(
        ConfigReader.appConfig.azure.appRegistration
      )

      azureApplicationInsightsService <- AzureApplicationInsightsService.fromAzureAppRegistrationConfig(
        ConfigReader.appConfig.azure.appRegistration
      )
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
        gceConfig,
        AzureServiceConfig(
          // For now azure disks share same defaults as normal disks
          ConfigReader.appConfig.persistentDisk,
          ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.image,
          ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.listenerImage,
          ConfigReader.appConfig.azure.pubsubHandler.welderImage
        )
      )
    } yield BaselineDependencies[F](
      sslContext,
      runtimeDnsCache,
      samDao,
      dockerDao,
      jupyterDao,
      rstudioDAO,
      welderDao,
      wsmDao,
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
      wdsDao,
      cbasDao,
      cromwellDao,
      hailBatchDao,
      listenerDao,
      wsmClientProvider,
      bpmClientProvider,
      azureContainerService,
      runtimeServiceConfig,
      kubernetesDnsCache,
      appDescriptorDAO,
      helmClient,
      azureRelay,
      azureVmService,
      operationFutureCache,
      azureBatchService,
      azureApplicationInsightsService,
      openTelemetry,
      samService
    )

  private def createCloudSubscriber[F[_]: Parallel](
    subscriberQueue: Queue[F, ReceivedMessage[LeoPubsubMessage]]
  )(implicit F: Async[F], logger: StructuredLogger[F]): Resource[F, CloudSubscriber[F, LeoPubsubMessage]] =
    ConfigReader.appConfig.azure.hostingModeConfig.enabled match {
      case false =>
        GoogleSubscriber.resource[F, LeoPubsubMessage](subscriberConfig, subscriberQueue)
      case true =>
        AzureSubscriberInterpreter.subscriber[F, LeoPubsubMessage](
          ConfigReader.appConfig.azure.hostingModeConfig.subscriberConfig,
          subscriberQueue
        )
    }

  private def createCloudPublisher[F[_]](implicit
    F: Async[F],
    logger: StructuredLogger[F]
  ): Resource[F, CloudPublisher[F]] =
    ConfigReader.appConfig.azure.hostingModeConfig.enabled match {
      case false =>
        GooglePublisher.cloudPublisherResource[F](publisherConfig)
      case true =>
        AzurePublisherInterpreter.publisher[F](ConfigReader.appConfig.azure.hostingModeConfig.publisherConfig)
    }

  private def buildCache[K, V](maxSize: Int,
                               expiresIn: FiniteDuration
  ): com.github.benmanes.caffeine.cache.Cache[K, V] =
    Caffeine
      .newBuilder()
      .maximumSize(maxSize)
      .expireAfterWrite(expiresIn.toSeconds, TimeUnit.SECONDS)
      .recordStats()
      .build[K, V]()

  private def buildHttpClient[F[_]: Async: StructuredLogger](
    sslContext: SSLContext,
    dnsResolver: RequestKey => Either[Throwable, InetSocketAddress],
    metricsPrefix: Option[String],
    withRetry: Boolean
  ): Resource[F, org.http4s.client.Client[F]] = {
    // Retry all SocketExceptions to deal with pooled HTTP connections getting closed.
    // See https://broadworkbench.atlassian.net/browse/IA-4069.
    val retryPolicy = RetryPolicy[F](
      RetryPolicy.exponentialBackoff(30 seconds, 5),
      (req, result) =>
        result match {
          case Left(e) if e.isInstanceOf[SocketException] => true
          case _                                          => RetryPolicy.defaultRetriable(req, result)
        }
    )

    for {
      httpClient <- client
        .BlazeClientBuilder[F]
        .withSslContext(sslContext)
        // Note a custom resolver is needed for making requests through the Leo proxy
        // (for example HttpJupyterDAO). Otherwise the proxyResolver falls back to default
        // hostname resolution, so it's okay to use for all clients.
        .withCustomDnsResolver(dnsResolver)
        .withConnectTimeout(30 seconds)
        .withRequestTimeout(60 seconds)
        .withMaxTotalConnections(100)
        .withMaxWaitQueueLimit(1024)
        .withMaxIdleDuration(30 seconds)
        .resource
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
  wsmDAO: HttpWsmDao[F],
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
  wdsDAO: WdsDAO[F],
  cbasDAO: CbasDAO[F],
  cromwellDAO: CromwellDAO[F],
  hailBatchDAO: HailBatchDAO[F],
  listenerDAO: ListenerDAO[F],
  wsmClientProvider: HttpWsmClientProvider[F],
  bpmClientProvider: HttpBpmClientProvider[F],
  azureContainerService: AzureContainerService[F],
  runtimeServicesConfig: RuntimeServiceConfig,
  kubernetesDnsCache: KubernetesDnsCache[F],
  appDescriptorDAO: HttpAppDescriptorDAO[F],
  helmClient: HelmInterpreter[F],
  azureRelay: AzureRelayService[F],
  azureVmService: AzureVmService[F],
  operationFutureCache: Cache[F, Long, OperationFuture[Operation, Operation]],
  azureBatchService: AzureBatchService[F],
  azureApplicationInsightsService: AzureApplicationInsightsService[F],
  openTelemetryMetrics: OpenTelemetryMetrics[F],
  samService: SamService[F]
)
