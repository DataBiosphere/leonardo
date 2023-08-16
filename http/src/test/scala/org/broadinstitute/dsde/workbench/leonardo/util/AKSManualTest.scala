package org.broadinstitute.dsde.workbench.leonardo.util

import java.net.URL
import cats.effect.std.Semaphore
import cats.effect.{IO, Resource}
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.leonardo.CloudContext.Azure
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.landingZoneResources
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config.{appMonitorConfig, dbConcurrency, liquibaseConfig}
import org.broadinstitute.dsde.workbench.leonardo.config.SamConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{
  CbasDAO,
  CbasUiDAO,
  CromwellDAO,
  HailBatchDAO,
  SamDAO,
  WdsDAO,
  WsmApiClientProvider,
  WsmDao
}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, KubernetesServiceDbQueries, SaveKubernetesCluster, _}
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsde.workbench.leonardo.{
  App,
  AppName,
  AppResources,
  AppStatus,
  AppType,
  CloudContext,
  DefaultNodepool,
  KubernetesClusterStatus,
  ManagedIdentityName,
  Namespace,
  NamespaceId,
  NodepoolStatus,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsp.{ChartName, HelmInterpreter, Release}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID
import scala.concurrent.ExecutionContext

/**
 * Manual test for deploying CoA helm chart against an AKS cluster. Usage:

 *   sbt "project http" test:console
 *   import cats.effect.unsafe.implicits.global
 *   import org.broadinstitute.dsde.workbench.leonardo.util.AKSManualTest
 *   AKSManualTest.deployApp.unsafeRunSync
 */
object AKSManualTest {
  // Constants

  // vault read secret/dsde/terra/azure/dev/leonardo/managed-app-publisher
  val appRegConfig = AzureAppRegistrationConfig(
    ClientId("client-id"),
    ClientSecret("client-secret"),
    ManagedAppTenantId("tenant-id")
  )

  // This is your WSM workspace ID
  val workspaceId = WorkspaceId(UUID.randomUUID)

  // this is your MRG
  val cloudContext = AzureCloudContext(
    TenantId("tenant-id"),
    SubscriptionId("subscription-id"),
    ManagedResourceGroupName("mrg-name")
  )

  // This is your pet UAMI
  val uamiName = ManagedIdentityName("uami-name")

  val appName = AppName("coa-app")
  val appSamResourceId = AppSamResourceId("sam-id", None)

  // Implicit dependencies
  implicit val logger = Slf4jLogger.getLogger[IO]
  implicit val executionContext = ExecutionContext.global

  /** Initializes DbReference */
  def getDbRef: Resource[IO, DbReference[IO]] = for {
    concurrentDbAccessPermits <- Resource.eval(Semaphore[IO](dbConcurrency))
    dbRef <- DbReference.init(liquibaseConfig, concurrentDbAccessPermits)
  } yield dbRef

  /** Populates the DB with a cluster, nodepool, and app */
  def populateDb(implicit dbRef: DbReference[IO]): IO[App] =
    for {
      appOpt <- KubernetesServiceDbQueries.getActiveFullAppByName(Azure(cloudContext), appName).transaction
      app <- appOpt match {
        case Some(a) => IO.pure(a.app)
        case None =>
          val cluster = makeKubeCluster(1)
          val saveCluster = SaveKubernetesCluster(
            CloudContext.Azure(cloudContext),
            cluster.clusterName,
            cluster.location,
            cluster.region,
            KubernetesClusterStatus.Running,
            cluster.ingressChart,
            cluster.auditInfo,
            DefaultNodepool.fromNodepool(
              cluster.nodepools.headOption
                .getOrElse(throw new Exception("test clusters to be saved must have at least 1 nodepool"))
            )
          )
          for {
            saveClusterResult <- dbRef.inTransaction(KubernetesServiceDbQueries.saveOrGetClusterForApp(saveCluster))
            saveNodepool = makeNodepool(1, saveClusterResult.minimalCluster.id).copy(status = NodepoolStatus.Running)
            saveNodepoolResult <- dbRef.inTransaction(nodepoolQuery.saveForCluster(saveNodepool))
            saveApp = makeApp(1, saveNodepoolResult.id)
              .copy(
                appName = AppName("coa-app"),
                status = AppStatus.Running,
                appType = AppType.Cromwell,
                chart = ConfigReader.appConfig.azure.coaAppConfig.chart
                  .copy(name = ChartName("cromwell-helm/cromwell-on-azure")),
                release = Release(s"manual-${ConfigReader.appConfig.azure.coaAppConfig.releaseNameSuffix.value}"),
                samResourceId = appSamResourceId,
                googleServiceAccount = WorkbenchEmail(
                  s"/subscriptions/${cloudContext.subscriptionId.value}/resourcegroups/${cloudContext.managedResourceGroupName.value}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/${uamiName.value}"
                ),
                appResources = AppResources(
                  namespace = Namespace(
                    NamespaceId(-1),
                    NamespaceName(s"manual-${ConfigReader.appConfig.azure.coaAppConfig.namespaceNameSuffix.value}")
                  ),
                  disk = None,
                  services = List.empty,
                  kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
                )
              )
            saveAppResult <- dbRef.inTransaction(appQuery.save(SaveApp(saveApp), None))
          } yield saveAppResult
      }
    } yield app

  /** Creates an AKSInterpreter */
  def getAksInterp(implicit dbRef: DbReference[IO]): Resource[IO, AKSInterpreter[IO]] = for {
    containerService <- AzureContainerService.fromAzureAppRegistrationConfig[IO](appRegConfig)
    batchService <- AzureBatchService.fromAzureAppRegistrationConfig[IO](appRegConfig)
    azureApplicationInsightsService <- AzureApplicationInsightsService.fromAzureAppRegistrationConfig[IO](appRegConfig)
    relayService <- AzureRelayService.fromAzureAppRegistrationConfig[IO](appRegConfig)
    helmConcurrency <- Resource.eval(Semaphore[IO](20L))
    helmClient = new HelmInterpreter[IO](helmConcurrency)
    config = AKSInterpreterConfig(
      ConfigReader.appConfig.terraAppSetupChart.copy(chartName = ChartName("terra-app-setup-charts/terra-app-setup")),
      ConfigReader.appConfig.azure.coaAppConfig,
      ConfigReader.appConfig.azure.workflowsAppConfig,
      ConfigReader.appConfig.azure.cromwellRunnerAppConfig,
      ConfigReader.appConfig.azure.wdsAppConfig,
      ConfigReader.appConfig.azure.hailBatchAppConfig,
      ConfigReader.appConfig.azure.aadPodIdentityConfig,
      appRegConfig,
      SamConfig("https://sam.dsde-dev.broadinstitute.org/"),
      appMonitorConfig,
      ConfigReader.appConfig.azure.wsm,
      ConfigReader.appConfig.drs,
      new URL("https://leo-dummy-url.org"),
      ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.listenerImage,
      ConfigReader.appConfig.azure.tdr
    )
    // TODO Sam and Cromwell should not be using mocks
  } yield new AKSInterpreter(
    config,
    helmClient,
    batchService,
    containerService,
    azureApplicationInsightsService,
    relayService,
    mock[SamDAO[IO]],
    mock[CromwellDAO[IO]],
    mock[CbasDAO[IO]],
    mock[CbasUiDAO[IO]],
    mock[WdsDAO[IO]],
    mock[HailBatchDAO[IO]],
    mock[WsmDao[IO]],
    mock[KubernetesAlgebra[IO]],
    mock[WsmApiClientProvider]
  )

  /** Deploys a CoA app */
  def deployApp: IO[Unit] = {
    val deps = for {
      implicit0(dbRef: DbReference[IO]) <- getDbRef
      app <- Resource.eval(populateDb)
      interp <- getAksInterp
    } yield Dependencies(interp, app)

    deps.use { deps =>
      deps.interp.createAndPollApp(
        CreateAKSAppParams(
          deps.app.id,
          deps.app.appName,
          workspaceId,
          cloudContext,
          landingZoneResources,
          None
        )
      )
    }
  }
}

case class Dependencies(interp: AKSInterpreter[IO], app: App)
