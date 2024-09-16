package org.broadinstitute.dsde.workbench.leonardo.http

import akka.actor.ActorSystem
import cats.Parallel
import cats.effect._
import cats.effect.std.{Queue, Semaphore}
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.api.services.container.ContainerScopes
import com.google.auth.oauth2.GoogleCredentials
import fs2.Stream
import fs2.io.file.Files
import io.kubernetes.client.openapi.ApiClient
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Json
import org.broadinstitute.dsde.workbench.google.{
  GoogleProjectDAO,
  HttpGoogleDirectoryDAO,
  HttpGoogleIamDAO,
  HttpGoogleProjectDAO
}
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{
  credentialResource,
  GKEService,
  GoogleComputeService,
  GoogleDataprocService,
  GoogleDiskService,
  GooglePublisher,
  GoogleResourceService,
  GoogleStorageService,
  GoogleSubscriber,
  KubernetesService
}
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns._
import org.broadinstitute.dsde.workbench.leonardo.http.Boot.workbenchMetricsBaseName
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.monitor.NonLeoMessageSubscriber.nonLeoMessageDecoder
import org.broadinstitute.dsde.workbench.leonardo.monitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.leonardo.{googleDataprocRetryPolicy, CloudService}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.messaging.{CloudPublisher, CloudSubscriber, ReceivedMessage}
import org.typelevel.log4cats.StructuredLogger
import scalacache.caffeine.CaffeineCache

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

class GcpDependencyBuilder extends CloudDependenciesBuilder {

  override def registryOpenTelemetryTracing: Resource[IO, Unit] =
    OpenTelemetryMetrics.registerTracing[IO](applicationConfig.leoServiceAccountJsonFile)

  override def createCloudSpecificProcessesList(baselineDependencies: BaselineDependencies[IO],
                                                cloudSpecificDependencies: ServicesRegistry
  )(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    dbReference: DbReference[IO],
    openTelemetry: OpenTelemetryMetrics[IO]
  ): List[Stream[IO, Unit]] = {

    val gcpDependencies: GcpDependencies[IO] = cloudSpecificDependencies.lookup[GcpDependencies[IO]].get

    val gkeAlg = new GKEInterpreter[IO](
      gkeInterpConfig,
      gcpDependencies.bucketHelper,
      gcpDependencies.vpcInterp,
      gcpDependencies.gkeService,
      gcpDependencies.kubeService,
      baselineDependencies.helmClient,
      baselineDependencies.appDAO,
      gcpDependencies.credentials,
      gcpDependencies.googleIamDAO,
      gcpDependencies.googleDiskService,
      baselineDependencies.appDescriptorDAO,
      baselineDependencies.nodepoolLock,
      gcpDependencies.googleResourceService,
      gcpDependencies.googleComputeService
    )
    val monitorAtBoot =
      new MonitorAtBoot[IO](
        baselineDependencies.publisherQueue,
        Some(gcpDependencies.googleComputeService),
        baselineDependencies.samDAO,
        baselineDependencies.wsmClientProvider
      )

    val nonLeoMessageSubscriber =
      new NonLeoMessageSubscriber[IO](
        NonLeoMessageSubscriberConfig(gceConfig.userDiskDeviceName),
        gkeAlg,
        gcpDependencies.googleComputeService,
        baselineDependencies.samDAO,
        baselineDependencies.authProvider,
        gcpDependencies.nonLeoMessageSubscriber,
        gcpDependencies.cryptoMiningUserPublisher,
        baselineDependencies.asyncTasksQueue,
        baselineDependencies.samService
      )

    List(
      nonLeoMessageSubscriber.process,
      Stream.eval(gcpDependencies.nonLeoMessageSubscriber.start),
      monitorAtBoot.process // checks database to see if there's on-going runtime status transition
    )
  }

  override def createDependenciesRegistry(baselineDependencies: BaselineDependencies[IO])(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    as: ActorSystem,
    dbReference: DbReference[IO],
    openTelemetry: OpenTelemetryMetrics[IO]
  ): Resource[IO, ServicesRegistry] =
    for {
      googleDependencies <- createGcpDependencies(baselineDependencies)

      gcpOnlyServicesRegistry = createGcpOnlyServicesRegistry(baselineDependencies, googleDependencies)

    } yield gcpOnlyServicesRegistry

  private def buildCache[K, V](maxSize: Int,
                               expiresIn: FiniteDuration
  ): com.github.benmanes.caffeine.cache.Cache[K, V] =
    Caffeine
      .newBuilder()
      .maximumSize(maxSize)
      .expireAfterWrite(expiresIn.toSeconds, TimeUnit.SECONDS)
      .recordStats()
      .build[K, V]()

  /***
   * Create GCP dependencies that a require for GCP only functionality.
   * These are the first of instances (leafs) in the dependency graph.
   * Note: This method is public to allow overriding the results in tests.
   */
  def createGcpDependencies[F[_]: Parallel](
    baselineDependencies: BaselineDependencies[F]
  )(implicit
    F: Async[F],
    logger: StructuredLogger[F],
    ec: ExecutionContext,
    as: ActorSystem,
    openTelemetry: OpenTelemetryMetrics[F],
    files: Files[F]
  ): Resource[F, GcpDependencies[F]] =
    for {
      semaphore <- Resource.eval(Semaphore[F](applicationConfig.concurrency))
      credential <- credentialResource(applicationConfig.leoServiceAccountJsonFile.toString)
      scopedCredential = credential.createScoped(Seq("https://www.googleapis.com/auth/compute").asJava)
      kubernetesScopedCredential = credential.createScoped(Seq(ContainerScopes.CLOUD_PLATFORM).asJava)
      credentialJson <- Resource.eval(
        readFileToString(applicationConfig.leoServiceAccountJsonFile)
      )
      json = Json(credentialJson)
      jsonWithServiceAccountUser = Json(credentialJson, Option(googleGroupsConfig.googleAdminEmail))

      // Set up Google DAOs
      googleProjectDAO = new HttpGoogleProjectDAO(applicationConfig.applicationName, json, workbenchMetricsBaseName)
      googleIamDAO = new HttpGoogleIamDAO(applicationConfig.applicationName, json, workbenchMetricsBaseName)
      googleDirectoryDAO = new HttpGoogleDirectoryDAO(applicationConfig.applicationName,
                                                      jsonWithServiceAccountUser,
                                                      workbenchMetricsBaseName
      )

      googleResourceService <- GoogleResourceService.resource[F](applicationConfig.leoServiceAccountJsonFile, semaphore)
      googleStorage <- GoogleStorageService
        .resource[F](applicationConfig.leoServiceAccountJsonFile.toString, Some(semaphore))
      googlePublisher <- GooglePublisher.cloudPublisherResource[F](publisherConfig)
      cryptoMiningUserPublisher <- GooglePublisher.cloudPublisherResource[F](cryptominingTopicPublisherConfig)
      gkeService <- GKEService.resource(applicationConfig.leoServiceAccountJsonFile, semaphore)

      // Retry 400 responses from Google, as those can occur when resources aren't ready yet
      // (e.g. if the subnet isn't ready when creating an instance).
      googleComputeRetryPolicy = RetryPredicates.retryConfigWithPredicates(RetryPredicates.standardGoogleRetryPredicate,
                                                                           RetryPredicates.whenStatusCode(400)
      )

      googleComputeService <- GoogleComputeService.fromCredential(scopedCredential, semaphore, googleComputeRetryPolicy)
      dataprocService <- GoogleDataprocService.resource(googleComputeService,
                                                        applicationConfig.leoServiceAccountJsonFile.toString,
                                                        semaphore,
                                                        dataprocConfig.supportedRegions,
                                                        googleDataprocRetryPolicy
      )
      googleDiskService <- GoogleDiskService.resource(applicationConfig.leoServiceAccountJsonFile.toString, semaphore)
      googleOauth2DAO <- GoogleOAuth2Service.resource(semaphore)

      // _ <- OpenTelemetryMetrics.registerTracing[F](applicationConfig.leoServiceAccountJsonFile)

      bucketHelperConfig = BucketHelperConfig(
        imageConfig,
        welderConfig,
        proxyConfig,
        securityFilesConfig
      )

      bucketHelper = new BucketHelper[F](bucketHelperConfig, googleStorage, baselineDependencies.samService)

      vpcInterp = new VPCInterpreter(vpcInterpreterConfig, googleResourceService, googleComputeService)

      // Set up k8s and helm clients
      underlyingKubeClientCache = buildCache[KubernetesClusterId, scalacache.Entry[ApiClient]](
        200,
        2 hours
      )
      kubeCache <- Resource.make(F.delay(CaffeineCache[F, KubernetesClusterId, ApiClient](underlyingKubeClientCache)))(
        _.close
      )
      kubeService <- org.broadinstitute.dsde.workbench.google2.KubernetesService
        .resource(applicationConfig.leoServiceAccountJsonFile, gkeService, kubeCache)

      nonLeoMessageSubscriberQueue <- Resource.eval(
        Queue.bounded[F, ReceivedMessage[NonLeoMessage]](pubsubConfig.queueSize)
      )
      // TODO: Verify why the queue is not referenced anywhere else. Is the subscriber's functionality self-contained?
      nonLeoMessageSubscriber <- GoogleSubscriber.resource(nonLeoMessageSubscriberConfig, nonLeoMessageSubscriberQueue)

    } yield GcpDependencies(
      googleStorage,
      googleComputeService,
      googleResourceService,
      googleDirectoryDAO,
      cryptoMiningUserPublisher,
      googlePublisher,
      googleIamDAO,
      dataprocService,
      baselineDependencies.kubernetesDnsCache, // Assuming kubernetesDnsCache is provided as a parameter
      gkeService,
      openTelemetry, // Assuming openTelemetry is provided as a parameter
      kubernetesScopedCredential,
      googleOauth2DAO,
      googleDiskService,
      googleProjectDAO,
      bucketHelper,
      vpcInterp,
      kubeService,
      nonLeoMessageSubscriber
    )

  def createGcpOnlyServicesRegistry(baselineDependencies: BaselineDependencies[IO],
                                    gcpDependencies: GcpDependencies[IO]
  )(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    as: ActorSystem,
    dbReference: DbReference[IO],
    openTelemetry: OpenTelemetryMetrics[IO]
  ): ServicesRegistry = {

    // Services used by the HTTP routes (Front-End)
    val proxyService = new ProxyService(
      baselineDependencies.sslContext,
      proxyConfig,
      baselineDependencies.jupyterDAO,
      baselineDependencies.runtimeDnsCache,
      gcpDependencies.kubernetesDnsCache,
      baselineDependencies.authProvider,
      baselineDependencies.dateAccessedUpdaterQueue,
      gcpDependencies.googleOauth2DAO,
      baselineDependencies.proxyResolver,
      baselineDependencies.samDAO,
      baselineDependencies.googleTokenCache,
      baselineDependencies.samResourceCache
    )

    val diskService = new DiskServiceInterp[IO](
      ConfigReader.appConfig.persistentDisk,
      baselineDependencies.authProvider,
      baselineDependencies.publisherQueue,
      Some(gcpDependencies.googleDiskService),
      Some(gcpDependencies.googleProjectDAO),
      baselineDependencies.samService
    )

    val runtimeService = RuntimeService(
      baselineDependencies.runtimeServicesConfig,
      ConfigReader.appConfig.persistentDisk,
      baselineDependencies.authProvider,
      baselineDependencies.dockerDAO,
      Some(gcpDependencies.googleStorageService),
      Some(gcpDependencies.googleComputeService),
      baselineDependencies.publisherQueue,
      baselineDependencies.samService
    )

    val dataprocInterp = new DataprocInterpreter(
      dataprocInterpreterConfig,
      gcpDependencies.bucketHelper,
      gcpDependencies.vpcInterp,
      gcpDependencies.googleDataproc,
      gcpDependencies.googleComputeService,
      gcpDependencies.googleDiskService,
      gcpDependencies.googleDirectoryDAO,
      gcpDependencies.googleIamDAO,
      gcpDependencies.googleResourceService,
      baselineDependencies.welderDAO
    )

    val gceInterp = new GceInterpreter(
      gceInterpreterConfig,
      gcpDependencies.bucketHelper,
      gcpDependencies.vpcInterp,
      gcpDependencies.googleComputeService,
      gcpDependencies.googleDiskService,
      baselineDependencies.welderDAO
    )

    val leoKubernetesService: LeoAppServiceInterp[IO] =
      new LeoAppServiceInterp(
        appServiceConfig,
        baselineDependencies.authProvider,
        baselineDependencies.publisherQueue,
        Some(gcpDependencies.googleComputeService),
        Some(gcpDependencies.googleResourceService),
        gkeCustomAppConfig,
        baselineDependencies.wsmClientProvider,
        baselineDependencies.samService
      )

    val resourcesService = new ResourcesServiceInterp[IO](
      baselineDependencies.authProvider,
      runtimeService,
      leoKubernetesService,
      diskService
    )

    // Services used by Leo PubSub Message subscriber (Backend)
    val gkeAlg = new GKEInterpreter[IO](
      gkeInterpConfig,
      gcpDependencies.bucketHelper,
      gcpDependencies.vpcInterp,
      gcpDependencies.gkeService,
      gcpDependencies.kubeService,
      baselineDependencies.helmClient,
      baselineDependencies.appDAO,
      gcpDependencies.credentials,
      gcpDependencies.googleIamDAO,
      gcpDependencies.googleDiskService,
      baselineDependencies.appDescriptorDAO,
      baselineDependencies.nodepoolLock,
      gcpDependencies.googleResourceService,
      gcpDependencies.googleComputeService
    )

    implicit val clusterToolToToolDao = ToolDAO.clusterToolToToolDao(baselineDependencies.jupyterDAO,
                                                                     baselineDependencies.welderDAO,
                                                                     baselineDependencies.rstudioDAO
    )

    val gceRuntimeMonitor = new GceRuntimeMonitor[IO](
      gceMonitorConfig,
      gcpDependencies.googleComputeService,
      baselineDependencies.authProvider,
      gcpDependencies.googleStorageService,
      gcpDependencies.googleDiskService,
      baselineDependencies.publisherQueue,
      gceInterp,
      baselineDependencies.samService
    )

    val dataprocRuntimeMonitor = new DataprocRuntimeMonitor[IO](
      dataprocMonitorConfig,
      gcpDependencies.googleComputeService,
      baselineDependencies.authProvider,
      gcpDependencies.googleStorageService,
      gcpDependencies.googleDiskService,
      dataprocInterp,
      gcpDependencies.googleDataproc,
      baselineDependencies.samService
    )

    val cloudServiceRuntimeMonitor =
      new CloudServiceRuntimeMonitor(gceRuntimeMonitor, dataprocRuntimeMonitor)

    val runtimeInstances: RuntimeInstances[IO] = new RuntimeInstances(dataprocInterp, gceInterp)

    val servicesRegistry = ServicesRegistry()

    servicesRegistry.register[ProxyService](proxyService)
    servicesRegistry.register[RuntimeService[IO]](runtimeService)
    servicesRegistry.register[DiskService[IO]](diskService)
    servicesRegistry.register[DataprocInterpreter[IO]](dataprocInterp)
    servicesRegistry.register[GceInterpreter[IO]](gceInterp)
    servicesRegistry.register[GcpDependencies[IO]](gcpDependencies)
    servicesRegistry.register[GKEAlgebra[IO]](gkeAlg)
    servicesRegistry.register[RuntimeMonitor[IO, CloudService]](cloudServiceRuntimeMonitor)
    servicesRegistry.register[ResourcesService[IO]](resourcesService)
    servicesRegistry.register[LeoAppServiceInterp[IO]](leoKubernetesService)
    servicesRegistry.register[RuntimeInstances[IO]](runtimeInstances)

    servicesRegistry

  }

}

final case class GcpDependencies[F[_]](
  googleStorageService: GoogleStorageService[F],
  googleComputeService: GoogleComputeService[F],
  googleResourceService: GoogleResourceService[F],
  googleDirectoryDAO: HttpGoogleDirectoryDAO,
  cryptoMiningUserPublisher: CloudPublisher[F],
  publisher: CloudPublisher[F],
  googleIamDAO: HttpGoogleIamDAO,
  googleDataproc: GoogleDataprocService[F],
  kubernetesDnsCache: KubernetesDnsCache[F],
  gkeService: GKEService[F],
  openTelemetryMetrics: OpenTelemetryMetrics[F],
  credentials: GoogleCredentials,
  googleOauth2DAO: GoogleOAuth2Service[F],
  googleDiskService: GoogleDiskService[F],
  googleProjectDAO: GoogleProjectDAO,
  bucketHelper: BucketHelper[F],
  vpcInterp: VPCInterpreter[F],
  kubeService: KubernetesService[F],
  nonLeoMessageSubscriber: CloudSubscriber[F, NonLeoMessage]
)
