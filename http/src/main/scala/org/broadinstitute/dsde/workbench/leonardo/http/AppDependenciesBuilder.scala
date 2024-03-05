package org.broadinstitute.dsde.workbench.leonardo.http

import akka.actor.ActorSystem
import cats.effect.std.Semaphore
import cats.effect.{IO, Resource}
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor
import org.broadinstitute.dsde.workbench.leonardo.app.{AppInstall, CromwellAppInstall, CromwellRunnerAppInstall, HailBatchAppInstall, WdsAppInstall, WorkflowsAppInstall}
import org.broadinstitute.dsde.workbench.leonardo.config.Config.{appMonitorConfig, appServiceConfig, applicationConfig, asyncTaskProcessorConfig, autoFreezeConfig, contentSecurityPolicy, dbConcurrency, gkeCustomAppConfig, leoPubsubMessageSubscriberConfig, liquibaseConfig, prometheusConfig, refererConfig, samConfig}
import org.broadinstitute.dsde.workbench.leonardo.config.{ContentSecurityPolicyConfig, RefererConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.http.api.{StandardUserInfoDirectives, UserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.monitor.{AutopauseMonitor, LeoMetricsMonitor, LeoPubsubMessageSubscriber}
import org.broadinstitute.dsde.workbench.leonardo.util.{AKSInterpreter, AKSInterpreterConfig, AzurePubsubHandlerInterp, KubernetesInterpreter, ServicesRegistry}
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

trait AppDependenciesBuilder {
    def createAppDependencies(implicit
                              logger: StructuredLogger[IO],
                              ec: ExecutionContext,
                              as: ActorSystem): Resource[IO,LeoAppDependencies]
}

abstract class AppDependenciesBuilderImpl extends AppDependenciesBuilder {
  override def createAppDependencies(implicit
                                  logger: StructuredLogger[IO],
                                  ec: ExecutionContext,
                                  as: ActorSystem): Resource[IO,LeoAppDependencies] = {
    for {
      concurrentDbAccessPermits <- Resource.eval(Semaphore[IO](dbConcurrency))

      implicit0(dbRef: DbReference[IO]) <- DbReference.init(liquibaseConfig, concurrentDbAccessPermits)
      implicit0(openTelemetry: OpenTelemetryMetrics[IO]) <- OpenTelemetryMetrics
        .resource[IO](applicationConfig.applicationName, prometheusConfig.endpointPort)

      baseDependencies <- BaselineDependenciesBuilder().createBaselineDependencies[IO]()

      dependenciesRegistry <- createDependenciesRegistry(baseDependencies)

      httpRoutesDependencies <- createFrontEndDependencies(baseDependencies, dependenciesRegistry)

      backEndDependencies <- createBackEndDependencies(baseDependencies, dependenciesRegistry)
    } yield {
      LeoAppDependencies(httpRoutesDependencies, backEndDependencies)
    }
  }

    private def createFrontEndDependencies(baselineDependencies: BaselineDependencies[IO],
                                           dependenciesRegistry: ServicesRegistry
    )(implicit
      logger: StructuredLogger[IO],
      ec: ExecutionContext,
      as: ActorSystem,
      dbReference: DbReference[IO],
      openTelemetry: OpenTelemetryMetrics[IO],
    ): Resource[IO,FrontEndDependencies] = {
      val statusService = new StatusService(baselineDependencies.samDAO, dbReference)
      val diskV2Service = new DiskV2ServiceInterp[IO](
        ConfigReader.appConfig.persistentDisk,
        baselineDependencies.authProvider,
        baselineDependencies.wsmDAO,
        baselineDependencies.samDAO,
        baselineDependencies.publisherQueue,
        baselineDependencies.wsmClientProvider
      )
      val leoKubernetesService =
        new LeoAppServiceInterp(
          appServiceConfig,
          baselineDependencies.authProvider,
          baselineDependencies.serviceAccountProvider,
          baselineDependencies.publisherQueue,
          dependenciesRegistry,
          gkeCustomAppConfig,
          baselineDependencies.wsmDAO,
          baselineDependencies.wsmClientProvider
        )

      val azureService = new RuntimeV2ServiceInterp[IO](
        baselineDependencies.runtimeServicesConfig,
        baselineDependencies.authProvider,
        baselineDependencies.wsmDAO,
        baselineDependencies.publisherQueue,
        baselineDependencies.dateAccessedUpdaterQueue,
        baselineDependencies.wsmClientProvider
      )
      val adminService = new AdminServiceInterp[IO](baselineDependencies.authProvider, baselineDependencies.publisherQueue)


      Resource.make(IO(FrontEndDependencies(
        baselineDependencies.openIDConnectConfiguration,
        statusService,
        dependenciesRegistry,
        diskV2Service,
        leoKubernetesService,
        azureService,
        adminService,
        StandardUserInfoDirectives,
        contentSecurityPolicy,
        refererConfig
      )))(_ => IO.unit)
    }

  private def createBackEndDependencies(baselineDependencies: BaselineDependencies[IO], cloudSpecificDependencies: ServicesRegistry)(implicit
                                                                                logger: StructuredLogger[IO],
                                                                                ec: ExecutionContext,
                                                                                as: ActorSystem,
                                                                                dbReference: DbReference[IO],
                                                                                openTelemetry: OpenTelemetryMetrics[IO],
  ): Resource[IO, BackEndDependencies] = {

    val processesList = createCloudSpecificBackEndProcessesList(baselineDependencies, cloudSpecificDependencies)

    val asyncTasks = AsyncTaskProcessor(asyncTaskProcessorConfig, baselineDependencies.asyncTasksQueue)

    val autopauseMonitorProcess = AutopauseMonitor.process(
      autoFreezeConfig,
      baselineDependencies.jupyterDAO,
      baselineDependencies.publisherQueue
    )

    // LeoMetricsMonitor collects metrics from both runtimes and apps.
    // - clusterToolToToolDao provides jupyter/rstudio/welder DAOs for runtime status checking.
    // - appDAO, wdsDAO, cbasDAO, cromwellDAO are for status checking apps.
    implicit val clusterToolToToolDao =
      ToolDAO.clusterToolToToolDao(baselineDependencies.jupyterDAO,
        baselineDependencies.welderDAO,
        baselineDependencies.rstudioDAO
      )
    val kubeAlg = new KubernetesInterpreter[IO](
      baselineDependencies.azureContainerService
    )

    val metricsMonitor = new LeoMetricsMonitor(
      ConfigReader.appConfig.metrics,
      baselineDependencies.appDAO,
      baselineDependencies.wdsDAO,
      baselineDependencies.cbasDAO,
      baselineDependencies.cromwellDAO,
      baselineDependencies.hailBatchDAO,
      baselineDependencies.listenerDAO,
      baselineDependencies.samDAO,
      kubeAlg,
      baselineDependencies.azureContainerService
    )


    val cromwellAppInstall = new CromwellAppInstall[IO](
      ConfigReader.appConfig.azure.coaAppConfig,
      ConfigReader.appConfig.drs,
      baselineDependencies.samDAO,
      baselineDependencies.cromwellDAO,
      baselineDependencies.cbasDAO,
      baselineDependencies.azureBatchService,
      baselineDependencies.azureApplicationInsightsService
    )

    val cromwellRunnerAppInstall =
      new CromwellRunnerAppInstall[IO](ConfigReader.appConfig.azure.cromwellRunnerAppConfig,
        ConfigReader.appConfig.drs,
        baselineDependencies.samDAO,
        baselineDependencies.cromwellDAO,
        baselineDependencies.azureBatchService,
        baselineDependencies.azureApplicationInsightsService
      )
    val hailBatchAppInstall =
      new HailBatchAppInstall[IO](ConfigReader.appConfig.azure.hailBatchAppConfig, baselineDependencies.hailBatchDAO)
    val wdsAppInstall = new WdsAppInstall[IO](ConfigReader.appConfig.azure.wdsAppConfig,
      ConfigReader.appConfig.azure.tdr,
      baselineDependencies.samDAO,
      baselineDependencies.wdsDAO,
      baselineDependencies.azureApplicationInsightsService
    )
    val workflowsAppInstall =
      new WorkflowsAppInstall[IO](
        ConfigReader.appConfig.azure.workflowsAppConfig,
        ConfigReader.appConfig.drs,
        baselineDependencies.samDAO,
        baselineDependencies.cromwellDAO,
        baselineDependencies.cbasDAO,
        baselineDependencies.azureBatchService,
        baselineDependencies.azureApplicationInsightsService
      )

    implicit val appTypeToAppInstall = AppInstall.appTypeToAppInstall(wdsAppInstall,
      cromwellAppInstall,
      workflowsAppInstall,
      hailBatchAppInstall,
      cromwellRunnerAppInstall)

    val aksAlg = new AKSInterpreter[IO](
      AKSInterpreterConfig(
        samConfig,
        appMonitorConfig,
        ConfigReader.appConfig.azure.wsm,
        applicationConfig.leoUrlBase,
        ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.listenerImage,
        ConfigReader.appConfig.azure.listenerChartConfig
      ),
      baselineDependencies.helmClient,
      baselineDependencies.azureContainerService,
      baselineDependencies.azureRelay,
      baselineDependencies.samDAO,
      baselineDependencies.wsmDAO,
      kubeAlg,
      baselineDependencies.wsmClientProvider,
      baselineDependencies.wsmDAO
    )

    val azureAlg = new AzurePubsubHandlerInterp[IO](
      ConfigReader.appConfig.azure.pubsubHandler,
      applicationConfig,
      contentSecurityPolicy,
      baselineDependencies.asyncTasksQueue,
      baselineDependencies.wsmDAO,
      baselineDependencies.samDAO,
      baselineDependencies.welderDAO,
      baselineDependencies.jupyterDAO,
      baselineDependencies.azureRelay,
      baselineDependencies.azureVmService,
      aksAlg,
      refererConfig
    )

    val pubsubSubscriber = new LeoPubsubMessageSubscriber[IO](
      leoPubsubMessageSubscriberConfig,
      baselineDependencies.asyncTasksQueue,
      baselineDependencies.authProvider,
      azureAlg,
      baselineDependencies.operationFutureCache,
      cloudSpecificDependencies
    )


    Resource.make(IO(BackEndDependencies(processesList)))(_ => IO.unit)

  }

  def createCloudSpecificBackEndProcessesList(baselineDependencies: BaselineDependencies[IO],
                                              cloudSpecificDependenciesRegistry: ServicesRegistry)(implicit
                                                                       logger: StructuredLogger[IO],
                                                                       ec: ExecutionContext,
                                                                       dbReference: DbReference[IO],
                                                                       openTelemetry: OpenTelemetryMetrics[IO]
                                ): List[Stream[IO,Unit]]

  def createDependenciesRegistry(baselineDependencies: BaselineDependencies[IO])(implicit
                                                                                 logger: StructuredLogger[IO],
                                                                                 ec: ExecutionContext,
                                                                                 as: ActorSystem,
                                                                                 dbReference: DbReference[IO],
                                                                                 openTelemetry: OpenTelemetryMetrics[IO],
  ): Resource[IO, ServicesRegistry]
}

final case class LeoAppDependencies(
                                     frontEndDependencies: FrontEndDependencies,
                                     backEndDependencies: BackEndDependencies,
                                   )
final case class FrontEndDependencies(
                                       oidcConfig: OpenIDConnectConfiguration,
                                       statusService: StatusService,
                                       cloudSpecificDependenciesRegistry: ServicesRegistry,
                                       diskV2Service: DiskV2Service[IO],
                                       kubernetesService: AppService[IO],
                                       azureService: RuntimeV2Service[IO],
                                       adminService: AdminService[IO],
                                       userInfoDirectives: UserInfoDirectives,
                                       contentSecurityPolicy: ContentSecurityPolicyConfig,
                                       refererConfig: RefererConfig
                                       )
final case class BackEndDependencies(processesList:List[Stream[IO,Unit]])