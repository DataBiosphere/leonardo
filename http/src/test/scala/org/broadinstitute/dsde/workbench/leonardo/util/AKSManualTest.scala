package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.std.Semaphore
import cats.effect.{IO, Resource}
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.leonardo.CloudContext.Azure
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config.{dbConcurrency, liquibaseConfig}
import org.broadinstitute.dsde.workbench.leonardo.config.SamConfig
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, KubernetesServiceDbQueries, SaveKubernetesCluster, _}
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsde.workbench.leonardo.{
  App,
  AppName,
  AppResources,
  AppStatus,
  AppType,
  BatchAccountName,
  CloudContext,
  DefaultNodepool,
  KubernetesClusterStatus,
  LandingZoneResources,
  ManagedIdentityName,
  Namespace,
  NamespaceId,
  NodepoolStatus,
  StorageAccountName,
  SubnetName
}
import org.broadinstitute.dsp.{ChartName, HelmInterpreter, Release}
import org.typelevel.log4cats.slf4j.Slf4jLogger

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
  val appRegConfig = AzureAppRegistrationConfig(
    ClientId("f075c9b3-7df8-4645-9ce3-21a798f8b03b"),
    ClientSecret("2nR7Q~UoGsoa3l1BVd-lDxuCcF~AgVN.1e4D8"),
    ManagedAppTenantId("fad90753-2022-4456-9b0a-c7e5b934e408")
  )
  val cloudContext = AzureCloudContext(
    TenantId("0cb7a640-45a2-4ed6-be9f-63519f86e04b"),
    SubscriptionId("3efc5bdf-be0e-44e7-b1d7-c08931e3c16c"),
    ManagedResourceGroupName("mrg-rtcromaks-20221011092807")
  )
  val uamiName = ManagedIdentityName("pet-266437562972788b024f5")
  val appSamResourceId = AppSamResourceId("8ab5833c-b4b3-450c-8f86-b404292a3c4c")
  val landingZoneResources = LandingZoneResources(
    AKSClusterName("lz2e6dcd2d552ad623cd338a1"),
    BatchAccountName("lzcfda4a58c8aee1f20396d8"),
    RelayNamespace("lz90d831b04e4bc77a174535ec31929eb838b9e306404081a1"),
    StorageAccountName("lzd8f8824b75a8148fb67dff"),
    SubnetName("BATCH_SUBNET")
  )
  val appName = AppName("coa-app")

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
                chart = ConfigReader.appConfig.azure.coaAppConfig.chart,
                release = Release(s"manual-${ConfigReader.appConfig.azure.coaAppConfig.releaseNameSuffix.value}"),
                samResourceId = appSamResourceId,
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
    relayService <- AzureRelayService.fromAzureAppRegistrationConfig[IO](appRegConfig)
    helmConcurrency <- Resource.eval(Semaphore[IO](20L))
    helmClient = new HelmInterpreter[IO](helmConcurrency)
    config = AKSInterpreterConfig(
      ConfigReader.appConfig.terraAppSetupChart.copy(chartName = ChartName("terra-app-setup-charts/terra-app-setup")),
      ConfigReader.appConfig.azure.coaAppConfig,
      SamConfig("https://sam.dsde-dev.broadinstitute.org/")
    )
  } yield new AKSInterpreter(config, helmClient, containerService, relayService)

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
          cloudContext
        )
      )
    }
  }
}

case class Dependencies(interp: AKSInterpreter[IO], app: App)
