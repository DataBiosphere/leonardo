package org.broadinstitute.dsde.workbench.leonardo.http

import akka.actor.ActorSystem
import cats.Parallel
import cats.effect._
import cats.effect.std.Semaphore
import com.google.api.services.container.ContainerScopes
import com.google.auth.oauth2.GoogleCredentials
import fs2.io.file.Files
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Json
import org.broadinstitute.dsde.workbench.google.{GoogleProjectDAO, HttpGoogleDirectoryDAO, HttpGoogleIamDAO, HttpGoogleProjectDAO}
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{GKEService, GoogleComputeService, GoogleDataprocService, GoogleDiskService, GooglePublisher, GoogleResourceService, GoogleStorageService, credentialResource}
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns._
import org.broadinstitute.dsde.workbench.leonardo.googleDataprocRetryPolicy
import org.broadinstitute.dsde.workbench.leonardo.http.Boot.workbenchMetricsBaseName
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.messaging.CloudPublisher
import org.typelevel.log4cats.StructuredLogger

import java.nio.file.Paths
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

class GcpDependencyBuilder extends AppDependenciesBuilderImpl {

  def registryOpenTelemetryTracing[F[_]:Async](): Unit = {
    for{
        _ <- OpenTelemetryMetrics.registerTracing[F](applicationConfig.leoServiceAccountJsonFile)
        } yield ()
  }
  def createGcpDependencies[F[_]: Parallel](
    pathToCredentialJson: String,
    semaphore: Semaphore[F],
    kubernetesDnsCache: KubernetesDnsCache[F]
  )(implicit
    F: Async[F],
    logger: StructuredLogger[F],
    ec: ExecutionContext,
    as: ActorSystem,
    openTelemetry: OpenTelemetryMetrics[F],
    files: Files[F]
  ): Resource[F, GcpDependencies[F]] =
    for {

      credential <- credentialResource(pathToCredentialJson)
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

      googleResourceService <- GoogleResourceService.resource[F](Paths.get(pathToCredentialJson), semaphore)
      googleStorage <- GoogleStorageService.resource[F](pathToCredentialJson, Some(semaphore))
      googlePublisher <- GooglePublisher.cloudPublisherResource[F](publisherConfig)
      cryptoMiningUserPublisher <- GooglePublisher.cloudPublisherResource[F](cryptominingTopicPublisherConfig)
      gkeService <- GKEService.resource(Paths.get(pathToCredentialJson), semaphore)

      // Retry 400 responses from Google, as those can occur when resources aren't ready yet
      // (e.g. if the subnet isn't ready when creating an instance).
      googleComputeRetryPolicy = RetryPredicates.retryConfigWithPredicates(RetryPredicates.standardGoogleRetryPredicate,
                                                                           RetryPredicates.whenStatusCode(400)
      )

      googleComputeService <- GoogleComputeService.fromCredential(scopedCredential, semaphore, googleComputeRetryPolicy)
      dataprocService <- GoogleDataprocService.resource(googleComputeService,
                                                        pathToCredentialJson,
                                                        semaphore,
                                                        dataprocConfig.supportedRegions,
                                                        googleDataprocRetryPolicy
      )
      googleDiskService <- GoogleDiskService.resource(pathToCredentialJson, semaphore)
      googleOauth2DAO <- GoogleOAuth2Service.resource(semaphore)

      _ <- OpenTelemetryMetrics.registerTracing[F](Paths.get(pathToCredentialJson))

    } yield GcpDependencies(
      googleStorage,
      googleComputeService,
      googleResourceService,
      googleDirectoryDAO,
      cryptoMiningUserPublisher,
      googlePublisher,
      googleIamDAO,
      dataprocService,
      kubernetesDnsCache, // Assuming kubernetesDnsCache is provided as a parameter
      gkeService,
      openTelemetry, // Assuming openTelemetry is provided as a parameter
      kubernetesScopedCredential,
      googleOauth2DAO,
      googleDiskService,
      googleProjectDAO
    )

  def createGcpOnlyServicesRegistry(appDependencies: BaselineDependencies[IO], googleDependencies: GcpDependencies[IO])(implicit
                                                                                                                        logger: StructuredLogger[IO],
                                                                                                                        ec: ExecutionContext,
                                                                                                                        as: ActorSystem,
                                                                                                                        dbReference: DbReference[IO],
                                                                                                                        openTelemetry: OpenTelemetryMetrics[IO],
  ):ServicesRegistry = {

    val proxyService = new ProxyService(
      appDependencies.sslContext,
      proxyConfig,
      appDependencies.jupyterDAO,
      appDependencies.runtimeDnsCache,
      googleDependencies.kubernetesDnsCache,
      appDependencies.authProvider,
      appDependencies.dateAccessedUpdaterQueue,
      googleDependencies.googleOauth2DAO,
      appDependencies.proxyResolver,
      appDependencies.samDAO,
      appDependencies.googleTokenCache,
      appDependencies.samResourceCache
    )

    val diskService = new DiskServiceInterp[IO](
      ConfigReader.appConfig.persistentDisk,
      appDependencies.authProvider,
      appDependencies.serviceAccountProvider,
      appDependencies.publisherQueue,
      googleDependencies.googleDiskService,
      googleDependencies.googleProjectDAO
    )

    val runtimeService = RuntimeService(
      appDependencies.runtimeServicesConfig,
      ConfigReader.appConfig.persistentDisk,
      appDependencies.authProvider,
      appDependencies.serviceAccountProvider,
      appDependencies.dockerDAO,
      googleDependencies.googleStorageService,
      googleDependencies.googleComputeService,
      appDependencies.publisherQueue
    )

    val bucketHelperConfig = BucketHelperConfig(
      imageConfig,
      welderConfig,
      proxyConfig,
      securityFilesConfig
    )

    val bucketHelper = new BucketHelper[IO](bucketHelperConfig, googleDependencies.googleStorageService, appDependencies.serviceAccountProvider)
    val vpcInterp = new VPCInterpreter(vpcInterpreterConfig, googleDependencies.googleResourceService, googleDependencies.googleComputeService)

    val dataprocInterp = new DataprocInterpreter(
      dataprocInterpreterConfig,
      bucketHelper,
      vpcInterp,
      googleDependencies.googleDataproc,
      googleDependencies.googleComputeService,
      googleDependencies.googleDiskService,
      googleDependencies.googleDirectoryDAO,
      googleDependencies.googleIamDAO,
      googleDependencies.googleResourceService,
      appDependencies.welderDAO
    )

    val gceInterp = new GceInterpreter(
      gceInterpreterConfig,
      bucketHelper,
      vpcInterp,
      googleDependencies.googleComputeService,
      googleDependencies.googleDiskService,
      appDependencies.welderDAO
    )

    implicit val runtimeInstances: RuntimeInstances[IO] = new RuntimeInstances(dataprocInterp, gceInterp)

    val servicesRegistry = ServicesRegistry()

    servicesRegistry.register[ProxyService](proxyService)
    servicesRegistry.register[RuntimeService[IO]](runtimeService)
    servicesRegistry.register[DiskService[IO]](diskService)
    servicesRegistry.register[DataprocInterpreter[IO]](dataprocInterp)
    servicesRegistry.register[GceInterpreter[IO]](gceInterp)
    servicesRegistry

  }

//  def createLeoAppDependencies(appDependencies: BaselineDependencies[IO])(implicit
//                                                                          logger: StructuredLogger[IO],
//                                                                          ec: ExecutionContext,
//                                                                          as: ActorSystem,
//                                                                          dbReference: DbReference[IO],
//                                                                          openTelemetry: OpenTelemetryMetrics[IO],
//  ): HttpRoutesDependencies = {
//
//  }

//  def createHttpRoutesDependencies(appDependencies: BaselineDependencies[IO],
//                                   gcpServicesRegistry: ServicesRegistry
//  )(implicit
//    logger: StructuredLogger[IO],
//    ec: ExecutionContext,
//    as: ActorSystem,
//    dbReference: DbReference[IO],
//    openTelemetry: OpenTelemetryMetrics[IO],
//  ): HttpRoutesDependencies = {
//    val statusService = new StatusService(appDependencies.samDAO, dbReference)
//    val diskV2Service = new DiskV2ServiceInterp[IO](
//      ConfigReader.appConfig.persistentDisk,
//      appDependencies.authProvider,
//      appDependencies.wsmDAO,
//      appDependencies.samDAO,
//      appDependencies.publisherQueue,
//      appDependencies.wsmClientProvider
//    )
//    val leoKubernetesService =
//      new LeoAppServiceInterp(
//        appServiceConfig,
//        appDependencies.authProvider,
//        appDependencies.serviceAccountProvider,
//        appDependencies.publisherQueue,
//        gcpServicesRegistry,
//        gkeCustomAppConfig,
//        appDependencies.wsmDAO,
//        appDependencies.wsmClientProvider
//      )
//
//    val azureService = new RuntimeV2ServiceInterp[IO](
//      appDependencies.runtimeServicesConfig,
//      appDependencies.authProvider,
//      appDependencies.wsmDAO,
//      appDependencies.publisherQueue,
//      appDependencies.dateAccessedUpdaterQueue,
//      appDependencies.wsmClientProvider
//    )
//    val adminService = new AdminServiceInterp[IO](appDependencies.authProvider, appDependencies.publisherQueue)
//
//    HttpRoutesDependencies(
//      appDependencies.openIDConnectConfiguration,
//      statusService,
//      gcpServicesRegistry,
//      diskV2Service,
//      leoKubernetesService,
//      azureService,
//      adminService,
//      StandardUserInfoDirectives,
//      contentSecurityPolicy,
//      refererConfig
//    )
//  }

  override def createBackEndDependencies[F[_] : Parallel](baselineDependencies: BaselineDependencies[F])(implicit F: Async[F]): Resource[F, BackEndDependencies] = ???

  override def createDependenciesRegistry[F[_] : Parallel](baselineDependencies: BaselineDependencies[F])(implicit F: Async[F]): Resource[F, ServicesRegistry] = ???
}

object GcpDependencyBuilder {
    def apply[F[_]: Parallel](implicit F: Async[F],
                                logger: StructuredLogger[F],
                                ec: ExecutionContext,
                                as: ActorSystem,
                                openTelemetry: OpenTelemetryMetrics[F],
                                files: Files[F]
    ): GcpDependencyBuilder = new GcpDependencyBuilder()
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
  googleProjectDAO: GoogleProjectDAO
)






//BACK END:
///val gceInterp = new GceInterpreter(
//        gceInterpreterConfig,
//        bucketHelper,
//        vpcInterp,
//        googleDependencies.googleComputeService,
//        googleDiskService,
//        welderDao
//      )
//
//      implicit val runtimeInstances = new RuntimeInstances(dataprocInterp, gceInterp)
//
//      val kubeAlg = new KubernetesInterpreter[F](
//        azureContainerService,
//        googleDependencies.gkeService,
//        googleDependencies.credentials
//
//val gkeAlg = new GKEInterpreter[F](
//        gkeInterpConfig,
//        bucketHelper,
//        vpcInterp,
//        googleDependencies.gkeService,
//        kubeService,
//        helmClient,
//        appDAO,
//        googleDependencies.credentials,
//        googleDependencies.googleIamDAO,
//        googleDiskService,
//        appDescriptorDAO,
//        nodepoolLock,
//        googleDependencies.googleResourceService,
//        googleDependencies.googleComputeService
//      )
//
//      val aksAlg = new AKSInterpreter[F](
//        AKSInterpreterConfig(
//          samConfig,
//          appMonitorConfig,
//          ConfigReader.appConfig.azure.wsm,
//          applicationConfig.leoUrlBase,
//          ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.listenerImage,
//          ConfigReader.appConfig.azure.listenerChartConfig
//        ),
//        helmClient,
//        azureContainerService,
//        azureRelay,
//        samDao,
//        wsmDao,
//        kubeAlg,
//        wsmClientProvider,
//        wsmDao
//      )
//
//      val azureAlg = new AzurePubsubHandlerInterp[F](
//        ConfigReader.appConfig.azure.pubsubHandler,
//        applicationConfig,
//        contentSecurityPolicy,
//        asyncTasksQueue,
//        wsmDao,
//        samDao,
//        welderDao,
//        jupyterDao,
//        azureRelay,
//        azureVmService,
//        aksAlg,
//        refererConfig
//      )
//implicit val clusterToolToToolDao = ToolDAO.clusterToolToToolDao(jupyterDao, welderDao, rstudioDAO)
//val gceRuntimeMonitor = new GceRuntimeMonitor[F](
//  gceMonitorConfig,
//  googleDependencies.googleComputeService,
//  authProvider,
//  googleDependencies.googleStorageService,
//  googleDependencies.googleDiskService,
//  publisherQueue,
//  gceInterp
//)
//
//val dataprocRuntimeMonitor = new DataprocRuntimeMonitor[F](
//  dataprocMonitorConfig,
//  googleDependencies.googleComputeService,
//  authProvider,
//  googleDependencies.googleStorageService,
//  googleDependencies.googleDiskService,
//  dataprocInterp,
//  googleDependencies.googleDataproc
//)
//
//implicit val cloudServiceRuntimeMonitor =
//  new CloudServiceRuntimeMonitor(gceRuntimeMonitor, dataprocRuntimeMonitor)
//
//val pubsubSubscriber = new LeoPubsubMessageSubscriber[F](
//  leoPubsubMessageSubscriberConfig,
//  subscriber,
//  asyncTasksQueue,
//  googleDiskService,
//  authProvider,
//  gkeAlg,
//  azureAlg,
//  operationFutureCache
//)