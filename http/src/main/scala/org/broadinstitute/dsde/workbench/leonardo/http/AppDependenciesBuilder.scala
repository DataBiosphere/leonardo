package org.broadinstitute.dsde.workbench.leonardo.http

import akka.actor.ActorSystem
import cats.effect.std.Semaphore
import cats.effect.{IO, Resource}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor
import org.broadinstitute.dsde.workbench.leonardo.app._
import org.broadinstitute.dsde.workbench.leonardo.config.Config.{
  appMonitorConfig,
  applicationConfig,
  asyncTaskProcessorConfig,
  autoFreezeConfig,
  contentSecurityPolicy,
  dateAccessUpdaterConfig,
  dbConcurrency,
  leoExecutionModeConfig,
  leoPubsubMessageSubscriberConfig,
  liquibaseConfig,
  prometheusConfig,
  refererConfig,
  samConfig
}
import org.broadinstitute.dsde.workbench.leonardo.config.LeoExecutionModeConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.http.api.StandardUserInfoDirectives
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  AutopauseMonitor,
  DateAccessedUpdater,
  LeoMetricsMonitor,
  LeoPubsubMessageSubscriber
}
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

/**
 * Builds the App dependencies
 * @param baselineDependenciesBuilder Baseline dependencies builder.
 * @param cloudHostDependenciesBuilder interpreter of the cloud hosting dependencies builder.
 */
class AppDependenciesBuilder(baselineDependenciesBuilder: BaselineDependenciesBuilder,
                             cloudHostDependenciesBuilder: CloudDependenciesBuilder
) {
  def createAppDependencies()(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    as: ActorSystem
  ): Resource[IO, LeoAppDependencies] =
    for {
      concurrentDbAccessPermits <- Resource.eval(Semaphore[IO](dbConcurrency))

      implicit0(dbRef: DbReference[IO]) <- DbReference.init(liquibaseConfig, concurrentDbAccessPermits)

      // This is for sending custom metrics to stackdriver. all custom metrics starts with `OpenCensus/leonardo/`.
      // Typing in `leonardo` in metrics explorer will show all leonardo custom metrics.
      // As best practice, we should have all related metrics under same prefix separated by `/`
      implicit0(openTelemetry: OpenTelemetryMetrics[IO]) <- OpenTelemetryMetrics
        .resource[IO](applicationConfig.applicationName, prometheusConfig.endpointPort)

      _ <- cloudHostDependenciesBuilder.registryOpenTelemetryTracing

      baseDependencies <- baselineDependenciesBuilder.createBaselineDependencies[IO]()

      dependenciesRegistry <- cloudHostDependenciesBuilder.createDependenciesRegistry(baseDependencies)

      httpRoutesDependencies <- createFrontEndDependencies(baseDependencies, dependenciesRegistry)

      backEndDependencies <- createBackEndDependencies(baseDependencies, dependenciesRegistry, leoExecutionModeConfig)
    } yield LeoAppDependencies(httpRoutesDependencies, backEndDependencies)

  /**
   *This method creates the services required to start HttpRoutes.
   *The list of services returned must be cloud-provider agnostic.
   */
  private def createFrontEndDependencies(baselineDependencies: BaselineDependencies[IO],
                                         dependenciesRegistry: ServicesRegistry
  )(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    as: ActorSystem,
    dbReference: DbReference[IO]
  ): Resource[IO, ServicesDependencies] = {
    val statusService = new StatusService(baselineDependencies.samDAO, dbReference)
    val diskV2Service = new DiskV2ServiceInterp[IO](
      ConfigReader.appConfig.persistentDisk,
      baselineDependencies.authProvider,
      baselineDependencies.wsmDAO,
      baselineDependencies.samDAO,
      baselineDependencies.publisherQueue,
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
    val adminService =
      new AdminServiceInterp[IO](baselineDependencies.authProvider, baselineDependencies.publisherQueue)

    // The instance must be present in both Azure and GCP modes.
    // However, when running on Azure, the service is created without GCP dependencies.
    // The LeoAppServiceInterp cannot be created in this method because it is a dependency of the Resources Services, which is GCP only.
    // This method only creates services that are agnostic of the cloud provider.
    val leoKubernetesService = dependenciesRegistry.lookup[LeoAppServiceInterp[IO]].get

    Resource.make[IO, ServicesDependencies](
      IO(
        ServicesDependencies(
          statusService,
          dependenciesRegistry,
          diskV2Service,
          leoKubernetesService,
          azureService,
          adminService,
          StandardUserInfoDirectives,
          contentSecurityPolicy,
          refererConfig,
          baselineDependencies
        )
      )
    )(_ => IO.unit)
  }

  def createBackEndDependencies(baselineDependencies: BaselineDependencies[IO],
                                cloudSpecificDependencies: ServicesRegistry,
                                leoExecutionModeConfig: LeoExecutionModeConfig
  )(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    dbReference: DbReference[IO],
    openTelemetry: OpenTelemetryMetrics[IO]
  ): Resource[IO, LeoAppProcesses] = {

    val cloudSpecificProcessList =
      cloudHostDependenciesBuilder.createCloudSpecificProcessesList(baselineDependencies, cloudSpecificDependencies)

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
      new CromwellRunnerAppInstall[IO](
        ConfigReader.appConfig.azure.cromwellRunnerAppConfig,
        ConfigReader.appConfig.drs,
        baselineDependencies.samDAO,
        baselineDependencies.cromwellDAO,
        baselineDependencies.azureBatchService,
        baselineDependencies.azureApplicationInsightsService
      )
    val hailBatchAppInstall =
      new HailBatchAppInstall[IO](ConfigReader.appConfig.azure.hailBatchAppConfig, baselineDependencies.hailBatchDAO)
    val wdsAppInstall = new WdsAppInstall[IO](
      ConfigReader.appConfig.azure.wdsAppConfig,
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
                                                                      cromwellRunnerAppInstall
    )

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
      baselineDependencies.wsmDAO,
      baselineDependencies.authProvider
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
      refererConfig,
      baselineDependencies.wsmClientProvider
    )

    val pubsubSubscriber = new LeoPubsubMessageSubscriber[IO](
      leoPubsubMessageSubscriberConfig,
      baselineDependencies.asyncTasksQueue,
      baselineDependencies.authProvider,
      azureAlg,
      baselineDependencies.operationFutureCache,
      cloudSpecificDependencies
    )

    val configuredProcesses = leoExecutionModeConfig match {
      case LeoExecutionModeConfig.BackLeoOnly =>
        List(
          pubsubSubscriber.process(baselineDependencies.subscriber),
          autopauseMonitorProcess,
          metricsMonitor.process
        ) ++ cloudSpecificProcessList
      case LeoExecutionModeConfig.FrontLeoOnly =>
        AsyncTaskProcessor(asyncTaskProcessorConfig,
                           baselineDependencies.asyncTasksQueue
        ).process :: createFrontEndLeoProcesses(baselineDependencies)
      case LeoExecutionModeConfig.Combined =>
        List(
          pubsubSubscriber.process(baselineDependencies.subscriber),
          autopauseMonitorProcess,
          metricsMonitor.process
        ) ++ cloudSpecificProcessList ++ createFrontEndLeoProcesses(baselineDependencies)
    }

    val allProcesses = List(baselineDependencies.leoPublisher.process) ++ configuredProcesses

    Resource.make(IO(LeoAppProcesses(allProcesses)))(_ => IO.unit)
  }

  private def createFrontEndLeoProcesses(baselineDependencies: BaselineDependencies[IO])(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    dbReference: DbReference[IO],
    openTelemetry: OpenTelemetryMetrics[IO]
  ) = {
    val dateAccessedUpdater =
      new DateAccessedUpdater(dateAccessUpdaterConfig, baselineDependencies.dateAccessedUpdaterQueue)

    val uniquefrontLeoOnlyProcesses = List(
      dateAccessedUpdater.process // We only need to update dateAccessed in front-end leo
    ) ++ baselineDependencies.recordMetricsProcesses
    uniquefrontLeoOnlyProcesses
  }
}

object AppDependenciesBuilder {
  def apply(): AppDependenciesBuilder =
    ConfigReader.appConfig.azure.hostingModeConfig.enabled match {
      case true =>
        new AppDependenciesBuilder(BaselineDependenciesBuilder(), new AzureDependenciesBuilder())
      case false =>
        new AppDependenciesBuilder(BaselineDependenciesBuilder(), new GcpDependencyBuilder())
    }
}
