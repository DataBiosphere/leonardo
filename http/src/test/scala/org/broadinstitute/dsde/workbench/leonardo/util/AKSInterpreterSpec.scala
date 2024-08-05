package org.broadinstitute.dsde.workbench.leonardo
package util

import bio.terra.workspace.api.{ControlledAzureResourceApi, ResourceApi, WorkspaceApi}
import bio.terra.workspace.model.{DeleteControlledAzureResourceRequest, _}
import cats.data.Kleisli
import cats.effect.IO
import cats.mtl.Ask
import com.azure.resourcemanager.containerservice.models.KubernetesCluster
import io.kubernetes.client.openapi.apis.CoreV1Api
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.PodStatus
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.google2.{KubernetesModels, NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.app.Database.ControlledDatabase
import org.broadinstitute.dsde.workbench.leonardo.app.{AppInstall, WorkflowsAppInstall}
import org.broadinstitute.dsde.workbench.leonardo.auth.SamAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.Config.appMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.config.SamConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{dbioToIO, ConfigReader}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsp.mocks.MockHelm
import org.broadinstitute.dsp.{AuthContext, ChartName, ChartVersion, HelmException, Release, Values}
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.scalatest.Succeeded
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatestplus.mockito.MockitoSugar

import java.net.URL
import java.nio.file.Files
import java.util.{Base64, UUID}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._

class AKSInterpreterSpec extends AnyFlatSpecLike with TestComponent with LeonardoTestSuite with MockitoSugar {

  val config = AKSInterpreterConfig(
    SamConfig("https://sam.dsde-dev.broadinstitute.org/"),
    appMonitorConfig,
    ConfigReader.appConfig.azure.wsm,
    new URL("https://leo-dummy-url.org"),
    ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.listenerImage,
    ConfigReader.appConfig.azure.listenerChartConfig
  )

  val mockSamDAO = setUpMockSamDAO
  val mockWsmDAO = new MockWsmDAO
  val mockAzureContainerService = setUpMockAzureContainerService
  val mockAzureRelayService = setUpMockAzureRelayService
  val mockKube = setUpMockKube
  val (mockWsm, mockControlledResourceApi, mockResourceApi, mockWorkspaceApi) = setUpMockWsmApiClientProvider()
  val mockSamAuthProvider = setUpMockSamAuthProvider

  implicit val appTypeToAppInstall: AppType => AppInstall[IO] = {
    case AppType.WorkflowsApp => setUpMockWorkflowAppInstall()
    case _                    => setUpMockAppInstall()
  }
  def newAksInterp(configuration: AKSInterpreterConfig = config,
                   mockWsm: WsmApiClientProvider[IO] = mockWsm,
                   helmClient: MockHelm = new MockHelm
  ) =
    new AKSInterpreter[IO](
      configuration,
      helmClient,
      mockAzureContainerService,
      mockAzureRelayService,
      mockSamDAO,
      mockWsmDAO,
      mockKube,
      mockWsm,
      mockWsmDAO,
      mockSamAuthProvider
    )

  val aksInterp = newAksInterp(config)

  val cloudContext = AzureCloudContext(
    TenantId("tenant"),
    SubscriptionId("sub"),
    ManagedResourceGroupName("mrg")
  )

  val lzResources = LandingZoneResources(
    UUID.fromString("5c12f64b-f4ac-4be1-ae4a-4cace5de807d"),
    AKSCluster("cluster", Map.empty[String, Boolean]),
    BatchAccountName("batch"),
    RelayNamespace("relay"),
    StorageAccountName("storage"),
    NetworkName("network"),
    SubnetworkName("subnet1"),
    SubnetworkName("subnet2"),
    azureRegion,
    ApplicationInsightsName("lzappinsights"),
    Some(PostgresServer("postgres", false))
  )

  val storageContainer = StorageContainerResponse(
    ContainerName("sc-container"),
    WsmControlledResourceId(UUID.randomUUID)
  )

  "AKSInterpreter" should "get a helm auth context" in {
    val res = for {
      authContext <- aksInterp.getHelmAuthContext(lzResources.aksCluster.asClusterName,
                                                  cloudContext,
                                                  NamespaceName("ns")
      )
    } yield {
      authContext.namespace.asString shouldBe "ns"
      authContext.kubeApiServer.asString shouldBe "server"
      authContext.kubeToken.asString shouldBe "token"
      Files.exists(authContext.caCertFile.path) shouldBe true
      Files.readAllLines(authContext.caCertFile.path).asScala.mkString shouldBe "cert"
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // create and poll each app type
  for (
    appType <- List(AppType.Wds, AppType.Cromwell, AppType.HailBatch, AppType.WorkflowsApp, AppType.CromwellRunnerApp)
  )
    it should s"create and poll ${appType} app" in isolatedDbTest {
      val res = for {
        cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
        nodepool <- IO(makeNodepool(1, cluster.id).save())
        namespace = NamespaceName(s"$appType-ns-1")
        app = makeApp(1, nodepool.id).copy(
          appType = appType,
          appResources = AppResources(
            namespace = namespace,
            disk = None,
            services = List.empty,
            kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
          ),
          googleServiceAccount = WorkbenchEmail(
            "/subscriptions/sub/resourcegroups/mrg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/mi-1"
          )
        )
        saveApp <- IO(app.save())

        appId = saveApp.id
        appName = saveApp.appName

        params = CreateAKSAppParams(appId, appName, workspaceId, cloudContext, billingProfileId)
        _ <- aksInterp.createAndPollApp(params)

        app <- KubernetesServiceDbQueries
          .getActiveFullAppByName(CloudContext.Azure(params.cloudContext), appName)
          .transaction
      } yield {
        app shouldBe defined
        app.get.app.status shouldBe AppStatus.Running
        app.get.cluster.asyncFields shouldBe defined

        // verify that cloning instructions for Azure K8s namespace is COPY_NOTHING for all apps
        val createNamespaceCaptor =
          ArgumentCaptor.forClass(classOf[CreateControlledAzureKubernetesNamespaceRequestBody])
        verify(mockControlledResourceApi, atLeastOnce()).createAzureKubernetesNamespace(createNamespaceCaptor.capture(),
                                                                                        any()
        )
        assert(createNamespaceCaptor.getValue.isInstanceOf[CreateControlledAzureKubernetesNamespaceRequestBody])
        val createNamespaceCaptorValues =
          createNamespaceCaptor.getValue.asInstanceOf[CreateControlledAzureKubernetesNamespaceRequestBody]
        createNamespaceCaptorValues.getCommon.getName shouldBe namespace.value
        createNamespaceCaptorValues.getCommon.getCloningInstructions shouldBe CloningInstructionsEnum.NOTHING
      }

      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

  // update and poll each app type
  for (
    appType <- List(AppType.Wds, AppType.Cromwell, AppType.HailBatch, AppType.WorkflowsApp, AppType.CromwellRunnerApp)
  )
    it should s"update and poll ${appType} app" in isolatedDbTest {
      val res = for {
        cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
        nodepool <- IO(makeNodepool(1, cluster.id).save())
        app = makeApp(1, nodepool.id).copy(
          appType = appType,
          status = AppStatus.Updating,
          chart = Chart(ChartName("myapp"), ChartVersion("0.0.1")),
          appResources = AppResources(
            namespace = NamespaceName("ns-1"),
            disk = None,
            services = List.empty,
            kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
          )
        )
        saveApp <- IO(app.save())

        appName = saveApp.appName
        appId = saveApp.id

        namespaceId = UUID.randomUUID()
        _ <- appControlledResourceQuery
          .insert(appId.id,
                  WsmControlledResourceId(namespaceId),
                  WsmResourceType.AzureKubernetesNamespace,
                  AppControlledResourceStatus.Created
          )
          .transaction

        _ <- aksInterp.updateAndPollApp(
          UpdateAKSAppParams(appId, appName, ChartVersion("0.0.2"), Some(workspaceIdForUpdating), cloudContext)
        )
        app <- KubernetesServiceDbQueries
          .getActiveFullAppByName(CloudContext.Azure(cloudContext), appName)
          .transaction
      } yield {
        app shouldBe defined
        app.get.app.status shouldBe AppStatus.Running
        app.get.app.chart shouldBe Chart(ChartName("myapp"), ChartVersion("0.0.2"))
      }
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

  it should s"emit an exception if the app is not alive prior to upgrading" in isolatedDbTest {
    implicit val appTypeToAppInstall: AppType => AppInstall[IO] = {
      case AppType.WorkflowsApp => setUpMockWorkflowAppInstall(false)
      case _                    => setUpMockAppInstall(false)
    }
    val aksInterp = new AKSInterpreter[IO](
      config,
      new MockHelm,
      mockAzureContainerService,
      mockAzureRelayService,
      mockSamDAO,
      mockWsmDAO,
      mockKube,
      mockWsm,
      mockWsmDAO,
      mockSamAuthProvider
    )
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id).copy(
        appType = AppType.Cromwell,
        status = AppStatus.Running,
        chart = Chart(ChartName("myapp"), ChartVersion("0.0.1")),
        appResources = AppResources(
          namespace = NamespaceName("ns-1"),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())

      appName = saveApp.appName
      appId = saveApp.id

      namespaceId = UUID.randomUUID()
      _ <- appControlledResourceQuery
        .insert(appId.id,
                WsmControlledResourceId(namespaceId),
                WsmResourceType.AzureKubernetesNamespace,
                AppControlledResourceStatus.Created
        )
        .transaction

      _ <- aksInterp
        .updateAndPollApp(
          UpdateAKSAppParams(appId, appName, ChartVersion("0.0.2"), Some(workspaceIdForUpdating), cloudContext)
        )

    } yield ()
    val either = res.attempt.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    either.isLeft shouldBe true
    either match {
      case Left(value) =>
        value.getMessage should include("was not alive")
        value.getClass shouldBe AppUpdatePollingException("message", None).getClass
      case _ => fail()
    }
  }

  // Note that the app will eventually transition to error or running status, but `Running` occurs on success and `Error` occurs in `LeoPubsubMessageSubscriber` (the latter being applicable to this test).
  // This test ensures that the underlying `AKSInterpreter` correctly transitions app to `Upgrading` and doesn't transition it to `Running` if an error occurs
  it should s"exit with app in Updating status if error occurs in helm client" in isolatedDbTest {
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id).copy(
        appType = AppType.Cromwell,
        status = AppStatus.Running,
        chart = Chart(ChartName("myapp"), ChartVersion("0.0.1")),
        appResources = AppResources(
          namespace = NamespaceName("ns-1"),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())

      appName = saveApp.appName
      appId = saveApp.id

      namespaceId = UUID.randomUUID()
      _ <- appControlledResourceQuery
        .insert(appId.id,
                WsmControlledResourceId(namespaceId),
                WsmResourceType.AzureKubernetesNamespace,
                AppControlledResourceStatus.Created
        )
        .transaction

      mockHelm = new MockHelm {
        override def upgradeChart(release: Release,
                                  chartName: ChartName,
                                  chartVersion: ChartVersion,
                                  values: Values
        ): Kleisli[IO, AuthContext, Unit] = Kleisli.liftF(IO.raiseError(HelmException("test exception")))
      }

      aksInterp = newAksInterp(helmClient = mockHelm)

      // Throw away the error here, we aren't trying to verify that the exception we are mocking above is being thrown
      _ <- aksInterp
        .updateAndPollApp(
          UpdateAKSAppParams(appId, appName, ChartVersion("0.0.2"), Some(workspaceIdForUpdating), cloudContext)
        )
        .handleErrorWith(_ => IO.unit)

      app <- KubernetesServiceDbQueries
        .getActiveFullAppByName(CloudContext.Azure(cloudContext), appName)
        .transaction
    } yield {
      app shouldBe defined
      app.get.app.status shouldBe AppStatus.Updating
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // delete each app type
  for (
    appType <- List(AppType.Wds, AppType.Cromwell, AppType.HailBatch, AppType.WorkflowsApp, AppType.CromwellRunnerApp)
  )
    it should s"delete and poll $appType app" in isolatedDbTest {
      val res = for {
        cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
        nodepool <- IO(makeNodepool(1, cluster.id).save())
        app = makeApp(1, nodepool.id).copy(
          appType = appType,
          status = AppStatus.Running,
          appResources = AppResources(
            namespace = NamespaceName("ns-1"),
            disk = None,
            services = List.empty,
            kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
          )
        )
        saveApp <- IO(app.save())

        appName = saveApp.appName

        _ <- aksInterp.deleteApp(DeleteAKSAppParams(appName, workspaceId, cloudContext, billingProfileId))
        app <- KubernetesServiceDbQueries
          .getActiveFullAppByName(CloudContext.Azure(cloudContext), appName)
          .transaction
      } yield app shouldBe None

      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

  it should "retrieve a WSM controlled identity if it exists in workspace" in isolatedDbTest {
    val res = for {
      retrievedIdentity <- aksInterp.retrieveWsmManagedIdentity(mockResourceApi,
                                                                AppType.WorkflowsApp,
                                                                workspaceId.value
      )
    } yield {
      retrievedIdentity.get.wsmResourceName shouldBe "idworkflows_app"
      retrievedIdentity.get.managedIdentityName shouldBe "abcxyz"
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
  it should "not retrieve a WSM controlled identity if it doesn't exist in workspace" in isolatedDbTest {
    val res = for {
      retrievedIdentity <- aksInterp.retrieveWsmManagedIdentity(mockResourceApi,
                                                                AppType.CromwellRunnerApp,
                                                                workspaceId.value
      )
    } yield retrievedIdentity shouldBe None

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "retrieve a WSM database if it exists in workspace" in isolatedDbTest {
    val res = for {
      retrievedDatabases <- aksInterp.retrieveWsmDatabases(mockResourceApi, Set("cromwellmetadata"), workspaceId.value)
    } yield {
      retrievedDatabases.head.wsmDatabaseName shouldBe "cromwellmetadata"
      retrievedDatabases.head.azureDatabaseName shouldBe "cromwellmetadata_abcxyz"
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not retrieve a WSM database if it doesn't exist in workspace" in isolatedDbTest {
    val res = for {
      retrievedDatabases <- aksInterp.retrieveWsmDatabases(mockResourceApi, Set("cbas"), workspaceId.value)
    } yield retrievedDatabases shouldBe empty

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "retrieve a WSM namespace if it exists in workspace" in isolatedDbTest {
    val res = for {
      retrievedNamespaces <- aksInterp.retrieveWsmNamespace(mockResourceApi, "ns-name", workspaceId.value)
    } yield retrievedNamespaces.map(ns => ns.name.value) shouldBe Some("ns-name")

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not retrieve a WSM namespace if doesn't exist in workspace" in isolatedDbTest {
    val res = for {
      retrievedNamespaces <- aksInterp.retrieveWsmNamespace(mockResourceApi, "something-else", workspaceId.value)
    } yield retrievedNamespaces shouldBe None

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // create wsm identity for shared apps
  for (appType <- List(AppType.Wds, AppType.WorkflowsApp))
    it should s"create a WSM controlled identity for $appType app" in isolatedDbTest {
      val res = for {
        cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
        nodepool <- IO(makeNodepool(1, cluster.id).save())
        app = makeApp(1, nodepool.id, appAccessScope = AppAccessScope.WorkspaceShared).copy(
          appType = appType,
          status = AppStatus.Running,
          appResources = AppResources(
            namespace = NamespaceName("ns-1"),
            disk = None,
            services = List.empty,
            kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
          )
        )
        saveApp <- IO(app.save())

        appId = saveApp.id

        createdIdentity <- aksInterp.createWsmIdentityResource(saveApp, "ns", workspaceId)

        controlledResources <- appControlledResourceQuery
          .getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
          .transaction
      } yield {
        createdIdentity.getAzureManagedIdentity.getMetadata.getName shouldBe s"id${appType.toString.toLowerCase}"
        controlledResources.size shouldBe 1
        controlledResources.head.resourceId.value shouldBe createdIdentity.getResourceId
        controlledResources.head.resourceType shouldBe WsmResourceType.AzureManagedIdentity
        controlledResources.head.status shouldBe AppControlledResourceStatus.Created
        controlledResources.head.appId shouldBe appId.id

        // verify that cloning instructions for Azure managed identity is:
        //  - COPY_NOTHING for WDS app
        //  - COPY_RESOURCE for Workflows app
        val expectedCloningInstructions =
          if (appType == AppType.WorkflowsApp) CloningInstructionsEnum.RESOURCE else CloningInstructionsEnum.NOTHING
        createdIdentity.getAzureManagedIdentity.getMetadata.getCloningInstructions shouldBe expectedCloningInstructions
      }

      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

  // fetch wsm database or create if it doesn't exists
  it should "retrieve cbas database and create cromwellmetadata database for WORKFLOWS app" in isolatedDbTest {
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id, appAccessScope = AppAccessScope.WorkspaceShared).copy(
        appType = AppType.WorkflowsApp,
        status = AppStatus.Running,
        appResources = AppResources(
          namespace = NamespaceName("ns-1"),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())

      appId = saveApp.id

      controlledDatabases <- aksInterp.createOrFetchWsmDatabaseResources(saveApp,
                                                                         saveApp.appType,
                                                                         workspaceIdForCloning,
                                                                         app.appResources.namespace.value,
                                                                         Option("idworkflows_app"),
                                                                         lzResources,
                                                                         mockResourceApi
      )

      _ <- aksInterp.createMissingAppControlledResources(saveApp,
                                                         saveApp.appType,
                                                         workspaceIdForCloning,
                                                         lzResources,
                                                         mockResourceApi
      )

      controlledResources <- appControlledResourceQuery
        .getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
        .transaction
    } yield {
      controlledDatabases.size shouldBe 2
      controlledDatabases.head.wsmDatabaseName shouldBe "cbas"
      controlledDatabases.head.azureDatabaseName shouldBe "cbas_cloned_db_abcxyz"
      controlledDatabases(1).wsmDatabaseName shouldBe "cromwellmetadata"
      controlledDatabases(1).azureDatabaseName shouldBe "cromwellmetadata_ns"

      controlledResources.size shouldBe 2
      controlledResources.head.resourceType shouldBe WsmResourceType.AzureDatabase
      controlledResources.head.status shouldBe AppControlledResourceStatus.Created
      controlledResources.head.appId shouldBe appId.id
      controlledResources(1).resourceType shouldBe WsmResourceType.AzureDatabase
      controlledResources(1).status shouldBe AppControlledResourceStatus.Created
      controlledResources(1).appId shouldBe appId.id
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // create wsm database
  it should "create a WSM controlled database" in isolatedDbTest {
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id, appAccessScope = AppAccessScope.WorkspaceShared).copy(
        appType = AppType.Wds,
        status = AppStatus.Running,
        appResources = AppResources(
          namespace = NamespaceName("ns-1"),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())

      appId = saveApp.id
      owner = "idwds"

      controlledDatabases <- aksInterp.createOrFetchWsmDatabaseResources(saveApp,
                                                                         saveApp.appType,
                                                                         workspaceIdForAppCreation,
                                                                         app.appResources.namespace.value,
                                                                         Some(owner),
                                                                         lzResources,
                                                                         mockResourceApi
      )

      controlledResources <- appControlledResourceQuery
        .getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
        .transaction
    } yield {
      controlledDatabases.size shouldBe 1
      // for all apps (except Workflows app) "db1" is the returned controlled database in mock data
      controlledDatabases.head.wsmDatabaseName shouldBe "db1"
      controlledDatabases.head.azureDatabaseName shouldBe "db1_ns"

      controlledResources.size shouldBe 1
      controlledResources.head.resourceType shouldBe WsmResourceType.AzureDatabase
      controlledResources.head.status shouldBe AppControlledResourceStatus.Created
      controlledResources.head.appId shouldBe appId.id

      // verify that cloning instructions for Azure database is set to COPY_NOTHING
      val createDbCaptor = ArgumentCaptor.forClass(classOf[CreateControlledAzureDatabaseRequestBody])
      verify(mockControlledResourceApi, atLeastOnce()).createAzureDatabase(createDbCaptor.capture(), any())
      assert(createDbCaptor.getValue.isInstanceOf[CreateControlledAzureDatabaseRequestBody])
      val createDbCaptorValues = createDbCaptor.getValue.asInstanceOf[CreateControlledAzureDatabaseRequestBody]
      createDbCaptorValues.getCommon.getName shouldBe "db1"
      createDbCaptorValues.getCommon.getCloningInstructions shouldBe CloningInstructionsEnum.NOTHING
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // create wsm databases for Workflows app
  it should "create WSM controlled databases for Workflows app" in isolatedDbTest {
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id, appAccessScope = AppAccessScope.WorkspaceShared).copy(
        appType = AppType.WorkflowsApp,
        status = AppStatus.Running,
        appResources = AppResources(
          namespace = NamespaceName("ns-1"),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())

      appId = saveApp.id

      controlledDatabases <- aksInterp.createOrFetchWsmDatabaseResources(saveApp,
                                                                         saveApp.appType,
                                                                         workspaceIdForAppCreation,
                                                                         app.appResources.namespace.value,
                                                                         Option("idworkflows_app"),
                                                                         lzResources,
                                                                         mockResourceApi
      )

      controlledResources <- appControlledResourceQuery
        .getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
        .transaction
    } yield {
      controlledDatabases.size shouldBe 2
      controlledDatabases.head.wsmDatabaseName shouldBe "cbas"
      controlledDatabases.head.azureDatabaseName shouldBe "cbas_ns"
      controlledDatabases(1).wsmDatabaseName shouldBe "cromwellmetadata"
      controlledDatabases(1).azureDatabaseName shouldBe "cromwellmetadata_ns"

      controlledResources.size shouldBe 2
      controlledResources.head.resourceType shouldBe WsmResourceType.AzureDatabase
      controlledResources.head.status shouldBe AppControlledResourceStatus.Created
      controlledResources.head.appId shouldBe appId.id
      controlledResources(1).resourceType shouldBe WsmResourceType.AzureDatabase
      controlledResources(1).status shouldBe AppControlledResourceStatus.Created
      controlledResources(1).appId shouldBe appId.id

      val createDbCaptor = ArgumentCaptor.forClass(classOf[CreateControlledAzureDatabaseRequestBody])
      verify(mockControlledResourceApi, atLeastOnce()).createAzureDatabase(createDbCaptor.capture(), any())
      // there are multiple invocations of "createAzureDatabase" in the mock API before this test is run.
      // We extract the last 2 invocations which reflect the calls made as part of this test.
      // Call for "cbas" db creation will be second last in the list
      val cbasDbCreationCallIndex = createDbCaptor.getAllValues.size() - 2
      assert(
        createDbCaptor.getAllValues
          .get(cbasDbCreationCallIndex)
          .isInstanceOf[CreateControlledAzureDatabaseRequestBody]
      )
      // Call for "crowmellmetadata" db creation will be last in the list
      assert(createDbCaptor.getValue.isInstanceOf[CreateControlledAzureDatabaseRequestBody])

      // verify that cloning instructions for "cbas" Azure database is set to COPY_RESOURCE
      val cbasDbCaptorValues = createDbCaptor.getAllValues
        .get(cbasDbCreationCallIndex)
        .asInstanceOf[CreateControlledAzureDatabaseRequestBody]
      cbasDbCaptorValues.getCommon.getName shouldBe "cbas"
      cbasDbCaptorValues.getCommon.getCloningInstructions shouldBe CloningInstructionsEnum.RESOURCE

      // verify that cloning instructions for "cromwellmetadata" Azure database is set to COPY_NOTHING
      val cromwellMetadataDbCaptorValues =
        createDbCaptor.getValue.asInstanceOf[CreateControlledAzureDatabaseRequestBody]
      cromwellMetadataDbCaptorValues.getCommon.getName shouldBe "cromwellmetadata"
      cromwellMetadataDbCaptorValues.getCommon.getCloningInstructions shouldBe CloningInstructionsEnum.NOTHING
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // create wsm k8s namespace
  it should "create a WSM controlled namespace" in isolatedDbTest {
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id, appAccessScope = AppAccessScope.WorkspaceShared).copy(
        appType = AppType.Cromwell,
        status = AppStatus.Running,
        appResources = AppResources(
          NamespaceName("ns-1"),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())

      appId = saveApp.id
      databases = List("db1", "db2")
      identity = "id"

      createdNamespace <- aksInterp.createWsmKubernetesNamespaceResource(saveApp,
                                                                         workspaceId,
                                                                         "ns",
                                                                         databases,
                                                                         Some(identity)
      )

      controlledResources <- appControlledResourceQuery
        .getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
        .transaction
    } yield {
      createdNamespace.name.value should startWith("ns")
      controlledResources.size shouldBe 1
      controlledResources.head.resourceId shouldBe createdNamespace.wsmResourceId
      controlledResources.head.resourceType shouldBe WsmResourceType.AzureKubernetesNamespace
      controlledResources.head.status shouldBe AppControlledResourceStatus.Created
      controlledResources.head.appId shouldBe appId.id
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not create a WSM managed identity for a private app" in isolatedDbTest {
    val (mockWsm, mockControlledResourceApi, _, _) = setUpMockWsmApiClientProvider()
    val aksInterp = newAksInterp(config, mockWsm = mockWsm)
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id).copy(
        appType = AppType.Cromwell,
        status = AppStatus.Running,
        appResources = AppResources(
          NamespaceName("ns-1"),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())

      appId = saveApp.id
      appName = saveApp.appName

      params = CreateAKSAppParams(appId, appName, workspaceId, cloudContext, billingProfileId)
      _ <- aksInterp.createAndPollApp(params)

      controlledResources <- appControlledResourceQuery
        .getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
        .transaction
    } yield {
      controlledResources.size shouldBe 2
      verify(mockControlledResourceApi, never()).createAzureManagedIdentity(any(), any())
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not create a WSM controlled namespace if one already exists" in isolatedDbTest {
    val (mockWsm, mockControlledResourceApi, _, _) = setUpMockWsmApiClientProvider()
    val aksInterp = newAksInterp(config, mockWsm = mockWsm)
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id).copy(
        appType = AppType.Cromwell,
        status = AppStatus.Running,
        appResources = AppResources(
          NamespaceName("ns-name"),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())

      appId = saveApp.id
      databases = List("db1", "db2")
      identity = "id"

      createdNamespace <- aksInterp.createWsmKubernetesNamespaceResource(saveApp,
                                                                         workspaceId,
                                                                         "ns-name",
                                                                         databases,
                                                                         Some(identity)
      )

      params = CreateAKSAppParams(appId, saveApp.appName, workspaceId, cloudContext, billingProfileId)
      _ <- aksInterp.createAndPollApp(params)

      controlledResources <- appControlledResourceQuery
        .getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
        .transaction
      namespaceRecord = controlledResources.filter(r => r.resourceType == WsmResourceType.AzureKubernetesNamespace).head
    } yield {
      verify(mockControlledResourceApi, times(1))
        .createAzureKubernetesNamespace(any[CreateControlledAzureKubernetesNamespaceRequestBody], any[UUID])
      controlledResources.size shouldBe 2
      namespaceRecord.resourceId shouldBe createdNamespace.wsmResourceId
      namespaceRecord.status shouldBe AppControlledResourceStatus.Created
      namespaceRecord.appId shouldBe appId.id
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not create a WSM managed identity if one already exists" in isolatedDbTest {
    val (mockWsm, mockControlledResourceApi, _, _) = setUpMockWsmApiClientProvider()
    val aksInterp = newAksInterp(config, mockWsm = mockWsm)
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id).copy(
        appType = AppType.WorkflowsApp,
        status = AppStatus.Running,
        appResources = AppResources(
          NamespaceName("ns-1"),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())
      appId = saveApp.id

      _ <- aksInterp.createAzureManagedIdentity(saveApp, "ns-1", workspaceId)

      params = CreateAKSAppParams(appId, saveApp.appName, workspaceId, cloudContext, billingProfileId)
      _ <- aksInterp.createAndPollApp(params)

      controlledResources <- appControlledResourceQuery
        .getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
        .transaction
      idRecord = controlledResources.filter(r => r.resourceType == WsmResourceType.AzureManagedIdentity).head
    } yield {
      verify(mockControlledResourceApi, times(1))
        .createAzureManagedIdentity(any[CreateControlledAzureManagedIdentityRequestBody], any[UUID])
      controlledResources.size shouldBe 4
      idRecord.status shouldBe AppControlledResourceStatus.Created
      idRecord.appId shouldBe appId.id
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete app WSM resources" in isolatedDbTest {
    val (mockWsm, mockControlledResourceApi, _, _) = setUpMockWsmApiClientProvider()
    val aksInterp = newAksInterp(config, mockWsm = mockWsm)
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id).copy(
        appType = AppType.Cromwell,
        status = AppStatus.Running,
        appResources = AppResources(
          namespace = NamespaceName("ns-1"),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())

      appId = saveApp.id
      databaseId = UUID.randomUUID()
      _ <- appControlledResourceQuery
        .insert(appId.id,
                WsmControlledResourceId(databaseId),
                WsmResourceType.AzureDatabase,
                AppControlledResourceStatus.Created
        )
        .transaction
      namespaceId = UUID.randomUUID()
      _ <- appControlledResourceQuery
        .insert(appId.id,
                WsmControlledResourceId(namespaceId),
                WsmResourceType.AzureKubernetesNamespace,
                AppControlledResourceStatus.Created
        )
        .transaction
      identityId = UUID.randomUUID()
      _ <- appControlledResourceQuery
        .insert(appId.id,
                WsmControlledResourceId(identityId),
                WsmResourceType.AzureManagedIdentity,
                AppControlledResourceStatus.Created
        )
        .transaction

      params = DeleteAKSAppParams(saveApp.appName, workspaceId, cloudContext, billingProfileId)
      _ <- aksInterp.deleteApp(params)

      deletedControlledResources <- appControlledResourceQuery
        .getAllForApp(appId)
        .transaction
    } yield {
      deletedControlledResources.length shouldBe 3
      deletedControlledResources.map(_.status).distinct shouldBe List(AppControlledResourceStatus.Deleted)
      verify(mockControlledResourceApi, times(1)).deleteAzureDatabaseAsync(any,
                                                                           mockitoEq(workspaceId.value),
                                                                           mockitoEq(databaseId)
      )
      verify(mockControlledResourceApi, times(1)).getDeleteAzureDatabaseResult(mockitoEq(workspaceId.value), any)
      verify(mockControlledResourceApi, times(1)).deleteAzureKubernetesNamespace(any,
                                                                                 mockitoEq(workspaceId.value),
                                                                                 mockitoEq(namespaceId)
      )
      verify(mockControlledResourceApi, times(1)).getDeleteAzureKubernetesNamespaceResult(mockitoEq(workspaceId.value),
                                                                                          any
      )
      verify(mockControlledResourceApi, times(1)).deleteAzureManagedIdentity(mockitoEq(workspaceId.value),
                                                                             mockitoEq(identityId)
      )
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle deleting WSM resources that don't exist" in isolatedDbTest {
    val (mockWsm, mockControlledResourceApi, _, _) = setUpMockWsmApiClientProvider(false, false, false)
    val aksInterp = newAksInterp(config, mockWsm = mockWsm)
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id).copy(
        appType = AppType.Cromwell,
        status = AppStatus.Running,
        appResources = AppResources(
          namespace = NamespaceName("ns-1"),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())

      appId = saveApp.id
      databaseId = UUID.randomUUID()
      _ <- appControlledResourceQuery
        .insert(appId.id,
                WsmControlledResourceId(databaseId),
                WsmResourceType.AzureDatabase,
                AppControlledResourceStatus.Created
        )
        .transaction
      namespaceId = UUID.randomUUID()
      _ <- appControlledResourceQuery
        .insert(appId.id,
                WsmControlledResourceId(namespaceId),
                WsmResourceType.AzureKubernetesNamespace,
                AppControlledResourceStatus.Created
        )
        .transaction
      identityId = UUID.randomUUID()
      _ <- appControlledResourceQuery
        .insert(appId.id,
                WsmControlledResourceId(identityId),
                WsmResourceType.AzureManagedIdentity,
                AppControlledResourceStatus.Created
        )
        .transaction

      params = DeleteAKSAppParams(saveApp.appName, workspaceId, cloudContext, billingProfileId)
      _ <- aksInterp.deleteApp(params)

      deletedControlledResources <- appControlledResourceQuery
        .getAllForApp(appId)
        .transaction
    } yield {
      deletedControlledResources.length shouldBe 3
      deletedControlledResources.map(_.status).distinct shouldBe List(AppControlledResourceStatus.Deleted)
      verify(mockControlledResourceApi, times(1)).deleteAzureDatabaseAsync(any,
                                                                           mockitoEq(workspaceId.value),
                                                                           mockitoEq(databaseId)
      )
      verify(mockControlledResourceApi, times(0)).getDeleteAzureDatabaseResult(mockitoEq(workspaceId.value), any)
      verify(mockControlledResourceApi, times(1)).deleteAzureKubernetesNamespace(any,
                                                                                 mockitoEq(workspaceId.value),
                                                                                 mockitoEq(namespaceId)
      )
      verify(mockControlledResourceApi, times(0)).getDeleteAzureKubernetesNamespaceResult(mockitoEq(workspaceId.value),
                                                                                          any
      )
      verify(mockControlledResourceApi, times(1)).deleteAzureManagedIdentity(mockitoEq(workspaceId.value),
                                                                             mockitoEq(identityId)
      )
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  private def setUpMockAzureContainerService: AzureContainerService[IO] = {
    val container = mock[AzureContainerService[IO]]
    val cluster = mock[KubernetesCluster]
    when {
      cluster.nodeResourceGroup()
    } thenReturn "node-rg"
    when {
      container.getCluster(any[String].asInstanceOf[AKSClusterName], any)(any)
    } thenReturn IO.pure(cluster)
    when {
      container.getClusterCredentials(any[String].asInstanceOf[AKSClusterName], any)(any)
    } thenReturn IO.pure(
      AKSCredentials(AKSServer("server"),
                     AKSToken("token"),
                     AKSCertificate(Base64.getEncoder.encodeToString("cert".getBytes()))
      )
    )
    container
  }

  private def setUpMockAzureRelayService: AzureRelayService[IO] = {
    val mockAzureRelayService = mock[AzureRelayService[IO]]
    val primaryKey = PrimaryKey("testKey")

    when {
      mockAzureRelayService.createRelayHybridConnection(any[String].asInstanceOf[RelayNamespace],
                                                        any[String].asInstanceOf[RelayHybridConnectionName],
                                                        any[String].asInstanceOf[AzureCloudContext]
      )(any())
    } thenReturn IO.pure(primaryKey)
    when {
      mockAzureRelayService.getRelayHybridConnectionKey(any[String].asInstanceOf[RelayNamespace],
                                                        any[String].asInstanceOf[RelayHybridConnectionName],
                                                        any[String].asInstanceOf[AzureCloudContext]
      )(any)
    } thenReturn IO.pure(primaryKey)
    when {
      mockAzureRelayService.deleteRelayHybridConnection(any[String].asInstanceOf[RelayNamespace],
                                                        any[String].asInstanceOf[RelayHybridConnectionName],
                                                        any[String].asInstanceOf[AzureCloudContext]
      )(any())
    } thenReturn IO.unit
    mockAzureRelayService
  }

  private def setUpMockKube(): KubernetesAlgebra[IO] = {
    val coreV1Api = mock[CoreV1Api]
    new KubernetesAlgebra[IO] {
      override def createAzureClient(cloudContext: AzureCloudContext, clusterName: AKSClusterName)(implicit
        ev: Ask[IO, AppContext]
      ): IO[CoreV1Api] = IO.pure(coreV1Api)
      override def listPodStatus(clusterId: CoreV1Api, namespace: KubernetesModels.KubernetesNamespace)(implicit
        ev: Ask[IO, AppContext]
      ): IO[List[PodStatus]] = IO.pure(List(PodStatus.Failed))
      override def createNamespace(client: CoreV1Api, namespace: KubernetesModels.KubernetesNamespace)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Unit] = IO.unit
      override def deleteNamespace(client: CoreV1Api, namespace: KubernetesModels.KubernetesNamespace)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Unit] = IO.unit
      override def namespaceExists(client: CoreV1Api, namespace: KubernetesModels.KubernetesNamespace)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Boolean] = IO.pure(false)
    }
  }

  private def setUpMockSamDAO: SamDAO[IO] = {
    val sam = mock[SamDAO[IO]]
    when {
      sam.getCachedArbitraryPetAccessToken(any)(any)
    } thenReturn IO.pure(Some("token"))
    when {
      sam.deleteResourceInternal(any, any)(any, any)
    } thenReturn IO.unit
    when {
      sam.getLeoAuthToken
    } thenReturn IO.pure(Authorization(Credentials.Token(AuthScheme.Bearer, "leotoken")))
    sam
  }

  private def setUpMockWsmApiClientProvider(databaseExists: Boolean = true,
                                            namespaceExists: Boolean = true,
                                            identityExists: Boolean = true
  ): (WsmApiClientProvider[IO], ControlledAzureResourceApi, ResourceApi, WorkspaceApi) = {
    val wsm = mock[WsmApiClientProvider[IO]]
    val api = mock[ControlledAzureResourceApi]
    val resourceApi = mock[ResourceApi]
    val workspaceApi = mock[WorkspaceApi]
    val dbsByJob = mutable.Map.empty[String, CreateControlledAzureDatabaseRequestBody]
    val namespacesByJob = mutable.Map.empty[String, CreateControlledAzureKubernetesNamespaceRequestBody]

    when {
      wsm.getWorkspace(any, any, any)
    } thenReturn {
      new MockWsmClientProvider().getWorkspace("string", WorkspaceId(UUID.randomUUID()))
    }
    // Create managed identity
    when {
      api.createAzureManagedIdentity(any, any)
    } thenAnswer { invocation =>
      val requestBody = invocation.getArgument[CreateControlledAzureManagedIdentityRequestBody](0)
      new CreatedControlledAzureManagedIdentity()
        .resourceId(requestBody.getCommon.getResourceId)
        .azureManagedIdentity(
          new AzureManagedIdentityResource()
            .metadata(
              new ResourceMetadata()
                .name(requestBody.getCommon.getName)
                .cloningInstructions(requestBody.getCommon.getCloningInstructions)
            )
            .attributes(new AzureManagedIdentityAttributes().managedIdentityName(requestBody.getCommon.getName))
        )
    }

    // delete managed identity
    when {
      api.deleteAzureManagedIdentity(any, any)
    } thenAnswer { _ =>
      if (identityExists)
        Succeeded
      else throw new TestException()
    }

    // Create database
    when {
      api.createAzureDatabase(any, any)
    } thenAnswer { invocation =>
      val requestBody = invocation.getArgument[CreateControlledAzureDatabaseRequestBody](0)
      val uuid = requestBody.getCommon.getResourceId
      val jobId = requestBody.getJobControl.getId
      dbsByJob += (jobId -> requestBody)
      new CreatedControlledAzureDatabaseResult().resourceId(uuid)
    }

    // Get create database job result
    when {
      api.getCreateAzureDatabaseResult(any, any)
    } thenAnswer { invocation =>
      val jobId = invocation.getArgument[String](1)
      val request = dbsByJob(jobId)
      new CreatedControlledAzureDatabaseResult()
        .resourceId(request.getCommon.getResourceId)
        .azureDatabase(
          new AzureDatabaseResource()
            .metadata(
              new ResourceMetadata()
                .resourceId(request.getCommon.getResourceId)
                .name(request.getCommon.getName)
            )
            .attributes(
              new AzureDatabaseAttributes()
                .allowAccessForAllWorkspaceUsers(request.getAzureDatabase.isAllowAccessForAllWorkspaceUsers)
                .databaseName(request.getAzureDatabase.getName)
                .databaseOwner(request.getAzureDatabase.getOwner)
            )
        )
        .jobReport(new JobReport().status(JobReport.StatusEnum.SUCCEEDED))
    }

    // delete database
    when {
      api.deleteAzureDatabaseAsync(any, any, any)
    } thenAnswer { _ =>
      if (databaseExists)
        new DeleteControlledAzureResourceResult()
          .jobReport(new JobReport().status(JobReport.StatusEnum.SUCCEEDED))
      else throw new TestException()
    }

    // get delete database job result
    when {
      api.getDeleteAzureDatabaseResult(any, any)
    } thenAnswer { _ =>
      new DeleteControlledAzureResourceResult()
        .jobReport(new JobReport().status(JobReport.StatusEnum.SUCCEEDED))
    }

    // Create Kubernetes Namespace
    when {
      api.createAzureKubernetesNamespace(any, any)
    } thenAnswer { invocation =>
      val requestBody = invocation.getArgument[CreateControlledAzureKubernetesNamespaceRequestBody](0)
      val uuid = requestBody.getCommon.getResourceId
      val jobId = requestBody.getJobControl.getId
      namespacesByJob += (jobId -> requestBody)
      new CreatedControlledAzureKubernetesNamespaceResult().resourceId(uuid)
    }
    // Get create Kubernetes Namespace job result
    when {
      api.getCreateAzureKubernetesNamespaceResult(any, any)
    } thenAnswer { invocation =>
      val jobId = invocation.getArgument[String](1)
      val request = namespacesByJob(jobId)
      new CreatedControlledAzureKubernetesNamespaceResult()
        .resourceId(request.getCommon.getResourceId)
        .azureKubernetesNamespace(
          new AzureKubernetesNamespaceResource()
            .metadata(
              new ResourceMetadata().resourceId(request.getCommon.getResourceId).name(request.getCommon.getName)
            )
            .attributes(
              new AzureKubernetesNamespaceAttributes()
                .kubernetesNamespace(request.getAzureKubernetesNamespace.getNamespacePrefix)
                .databases(request.getAzureKubernetesNamespace.getDatabases)
                .managedIdentity(request.getAzureKubernetesNamespace.getManagedIdentity)
                .kubernetesServiceAccount(
                  Option(request.getAzureKubernetesNamespace.getManagedIdentity).map(_.toString).getOrElse("ksa-1")
                )
            )
        )
        .jobReport(
          new JobReport().status(JobReport.StatusEnum.SUCCEEDED)
        )
    }
    // Get Kubernetes Namespace
    when {
      api.getAzureKubernetesNamespace(any, any)
    } thenAnswer { invocation =>
      val resourceId = invocation.getArgument[UUID](1)
      new AzureKubernetesNamespaceResource()
        .metadata(
          new ResourceMetadata().resourceId(resourceId).name("namespace")
        )
        .attributes(
          new AzureKubernetesNamespaceAttributes()
            .kubernetesNamespace("ns")
            .databases(List("db1").asJava)
            .managedIdentity("id1")
            .kubernetesServiceAccount("ksa-1")
        )
    }
    // Delete Kubernetes Namespace
    when {
      api.deleteAzureKubernetesNamespace(any, any, any)
    } thenAnswer { invocation =>
      val request = invocation.getArgument[DeleteControlledAzureResourceRequest](0)
      if (namespaceExists)
        new DeleteControlledAzureResourceResult().jobReport(
          new JobReport().status(JobReport.StatusEnum.SUCCEEDED).id(request.getJobControl.getId)
        )
      else throw new TestException()
    }
    // Get delete Kubernetes Namespace job
    when {
      api.getDeleteAzureKubernetesNamespaceResult(any, any)
    } thenReturn {
      new DeleteControlledAzureResourceResult().jobReport(new JobReport().status(JobReport.StatusEnum.SUCCEEDED))
    }
    when {
      wsm.getControlledAzureResourceApi(any)(any)
    } thenReturn IO.pure(api)

    // "ns-name" workspace database resource

    when {
      resourceApi.getResourceByName(
        workspaceId.value,
        "cromwellmetadata"
      )
    }.thenReturn(
      new ResourceDescription()
        .metadata(new ResourceMetadata().name("cromwellmetadata").resourceId(java.util.UUID.randomUUID()))
        .resourceAttributes(
          new ResourceAttributesUnion().azureDatabase(
            new AzureDatabaseAttributes().databaseName("cromwellmetadata_abcxyz")
          )
        )
    )

    // workspace database resource for a workspace with cloned db
    when {
      resourceApi.getResourceByName(
        workspaceIdForCloning.value,
        "cbas"
      )
    }.thenReturn {
      new ResourceDescription()
        .metadata(new ResourceMetadata().name("cbas").resourceId(java.util.UUID.randomUUID()))
        .resourceAttributes(
          new ResourceAttributesUnion().azureDatabase(
            new AzureDatabaseAttributes().databaseName("cbas_cloned_db_abcxyz")
          )
        )
    }
    // workspace database resource for an updating app
    when {
      resourceApi.getResourceByName(
        workspaceIdForUpdating.value,
        "cbas"
      )
    }.thenReturn {
      new ResourceDescription()
        .metadata(new ResourceMetadata().name("cbas").resourceId(cbasUuidForUpdateApp))
        .resourceAttributes(
          new ResourceAttributesUnion().azureDatabase(
            new AzureDatabaseAttributes().databaseName("cbas_db_abcxyz")
          )
        )

    }

    when {
      resourceApi.getResourceByName(
        workspaceIdForUpdating.value,
        "cromwellmetadata"
      )
    }.thenReturn {
      new ResourceDescription()
        .metadata(
          new ResourceMetadata().name("cromwellmetadata").resourceId(cromwellmetadataUuidForUpdateApp)
        )
        .resourceAttributes(
          new ResourceAttributesUnion().azureDatabase(
            new AzureDatabaseAttributes().databaseName("cromwellmetadata_abcxyz")
          )
        )

    }

    // getAzureDatabase for an updating app
    when {
      api.getAzureDatabase(ArgumentMatchers.eq(workspaceIdForUpdating.value), ArgumentMatchers.eq(cbasUuidForUpdateApp))
    } thenReturn {
      new AzureDatabaseResource()
        .metadata(
          new ResourceMetadata()
            .resourceId(cbasUuidForUpdateApp)
            .name("cbas")
        )
        .attributes(
          new AzureDatabaseAttributes()
            .allowAccessForAllWorkspaceUsers(true)
            .databaseName("cbas_db_abcxyz")
        )
    }
    when {
      api.getAzureDatabase(ArgumentMatchers.eq(workspaceIdForUpdating.value),
                           ArgumentMatchers.eq(cromwellmetadataUuidForUpdateApp)
      )
    } thenReturn {
      new AzureDatabaseResource()
        .metadata(
          new ResourceMetadata()
            .resourceId(cromwellmetadataUuidForUpdateApp)
            .name("cromwellmetadata")
        )
        .attributes(
          new AzureDatabaseAttributes()
            .allowAccessForAllWorkspaceUsers(true)
            .databaseName("cromwellmetadata_db_abcxyz")
        )
    }
    // workspace managed identity resource
    when {
      resourceApi.getResourceByName(workspaceId.value, s"idworkflows_app")
    }.thenReturn {
      new ResourceDescription()
        .metadata(new ResourceMetadata().name(s"idworkflows_app").resourceId(java.util.UUID.randomUUID()))
        .resourceAttributes(
          new ResourceAttributesUnion().azureManagedIdentity(
            new AzureManagedIdentityAttributes().managedIdentityName("abcxyz")
          )
        )
    }

    //  workspace namespace resource
    when {
      resourceApi.getResourceByName(workspaceId.value, s"ns-name-${workspaceId.value.toString}")
    }.thenReturn {
      new ResourceDescription()
        .resourceAttributes(
          new ResourceAttributesUnion().azureKubernetesNamespace(
            new AzureKubernetesNamespaceAttributes().kubernetesNamespace("ns-name")
          )
        )
        .metadata(new ResourceMetadata().name(s"ns-name-${workspaceId.value.toString}"))
    }
    when {
      workspaceApi.getWorkspace(ArgumentMatchers.eq(workspaceId.value), any)
    } thenReturn {
      new bio.terra.workspace.model.WorkspaceDescription().createdDate(workspaceCreatedDate);
    }

    when {
      workspaceApi.getWorkspace(ArgumentMatchers.eq(workspaceIdForUpdating.value), any)
    } thenReturn {
      new bio.terra.workspace.model.WorkspaceDescription().createdDate(workspaceCreatedDate);
    }

    when {
      wsm.getResourceApi(any)(any)
    } thenReturn IO.pure(resourceApi)

    when {
      wsm.getWorkspaceApi(any)(any)
    } thenReturn IO.pure(workspaceApi)

    (wsm, api, resourceApi, workspaceApi)

  }

  private def setUpMockAppInstall(checkStatus: Boolean = true): AppInstall[IO] = {
    val appInstall = mock[AppInstall[IO]]
    when {
      appInstall.databases
    } thenReturn List(ControlledDatabase("db1"))
    when {
      appInstall.buildHelmOverrideValues(any)(any)
    } thenReturn IO.pure(Values("values"))
    when {
      appInstall.checkStatus(any, any)(any)
    } thenReturn IO.pure(checkStatus)
    appInstall
  }

  private def setUpMockWorkflowAppInstall(checkStatus: Boolean = true): AppInstall[IO] = {
    val mockWorkflowsAppInstall = mock[WorkflowsAppInstall[IO]]

    when(mockWorkflowsAppInstall.databases) thenReturn
      List(
        ControlledDatabase("cbas", cloningInstructions = CloningInstructionsEnum.RESOURCE),
        ControlledDatabase("cromwellmetadata", allowAccessForAllWorkspaceUsers = true)
      )
    when {
      mockWorkflowsAppInstall.buildHelmOverrideValues(any)(any)
    } thenReturn IO.pure(Values("values"))
    when {
      mockWorkflowsAppInstall.checkStatus(any, any)(any)
    } thenReturn IO.pure(checkStatus)

    mockWorkflowsAppInstall
  }

  private def setUpMockSamAuthProvider: SamAuthProvider[IO] = {
    val mockSamAuth = mock[SamAuthProvider[IO]]

    when {
      mockSamAuth.getLeoAuthToken
    } thenReturn IO.pure(tokenValue)
    mockSamAuth
  }
}
