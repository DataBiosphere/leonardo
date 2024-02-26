package org.broadinstitute.dsde.workbench.leonardo
package util

import bio.terra.workspace.api.{ControlledAzureResourceApi, ResourceApi}
import bio.terra.workspace.model.{DeleteControlledAzureResourceRequest, _}
import cats.effect.IO
import cats.mtl.Ask
import com.azure.resourcemanager.containerservice.models.KubernetesCluster
import io.kubernetes.client.openapi.apis.CoreV1Api
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.PodStatus
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.google2.{GKEModels, KubernetesModels, NetworkName, SubnetworkName}
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
import org.broadinstitute.dsde.workbench.leonardo.http.{ConfigReader, dbioToIO}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsp.mocks.MockHelm
import org.broadinstitute.dsp.{ChartName, ChartVersion, Values}
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{atLeastOnce, times, verify, when}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatestplus.mockito.MockitoSugar

import java.net.URL
import java.nio.file.Files
import java.util.{ArrayList, Base64, UUID}
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
  val (mockWsm, mockControlledResourceApi, mockResourceApi) = setUpMockWsmApiClientProvider
  val mockSamAuthProvider = setUpMockSamAuthProvider

  implicit val appTypeToAppInstall: AppType => AppInstall[IO] = {
    case AppType.WorkflowsApp => setUpMockWorkflowAppInstall
    case _                    => setUpMockAppInstall
  }
  def newAksInterp(configuration: AKSInterpreterConfig, mockWsm: WsmApiClientProvider[IO] = mockWsm) =
    new AKSInterpreter[IO](
      configuration,
      MockHelm,
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
          UpdateAKSAppParams(appId, appName, ChartVersion("0.0.2"), Some(workspaceId), cloudContext)
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
    } yield retrievedNamespaces.head.name.value shouldBe "ns-name"

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not retrieve a WSM namespace if doesn't exist in workspace" in isolatedDbTest {
    val res = for {
      retrievedNamespaces <- aksInterp.retrieveWsmNamespace(mockResourceApi, "something-else", workspaceId.value)
    } yield retrievedNamespaces.length shouldBe 0

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

      _ <- aksInterp.createMissingAppControlledResources(saveApp,
                                                         saveApp.appType,
                                                         workspaceIdForCloning,
                                                         lzResources,
                                                         mockResourceApi
      )

      controlledDatabases <- aksInterp.createOrFetchWsmDatabaseResources(saveApp,
                                                                         saveApp.appType,
                                                                         workspaceIdForCloning,
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

  it should "not create a WSM controlled namespace if one already exists" in isolatedDbTest {
    val (mockWsm, mockControlledResourceApi, _) = setUpMockWsmApiClientProvider
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
      controlledResources.size shouldBe 3
      namespaceRecord.resourceId shouldBe createdNamespace.wsmResourceId
      namespaceRecord.status shouldBe AppControlledResourceStatus.Created
      namespaceRecord.appId shouldBe appId.id
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not create a WSM managed identity if one already exists" in isolatedDbTest {
    val (mockWsm, mockControlledResourceApi, _) = setUpMockWsmApiClientProvider
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

  // delete wsm resources
  it should "delete app WSM resources" in isolatedDbTest {
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

      _ <- aksInterp.deleteWsmResource(
        workspaceId,
        saveApp,
        AppControlledResourceRecord(appId.id,
                                    WsmControlledResourceId(databaseId),
                                    WsmResourceType.AzureDatabase,
                                    AppControlledResourceStatus.Created
        )
      )

      _ <- aksInterp.deleteWsmNamespaceResource(
        workspaceId,
        saveApp,
        AppControlledResourceRecord(appId.id,
                                    WsmControlledResourceId(namespaceId),
                                    WsmResourceType.AzureKubernetesNamespace,
                                    AppControlledResourceStatus.Created
        )
      )

      _ <- aksInterp.deleteWsmResource(
        workspaceId,
        saveApp,
        AppControlledResourceRecord(appId.id,
                                    WsmControlledResourceId(identityId),
                                    WsmResourceType.AzureManagedIdentity,
                                    AppControlledResourceStatus.Created
        )
      )

      controlledResources <- appControlledResourceQuery
        .getAllForAppByStatus(app.id.id, AppControlledResourceStatus.Created)
        .transaction
    } yield controlledResources shouldBe empty

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
      override def createGcpClient(clusterId: GKEModels.KubernetesClusterId)(implicit
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

  private def setUpMockWsmApiClientProvider: (WsmApiClientProvider[IO], ControlledAzureResourceApi, ResourceApi) = {
    val wsm = mock[WsmApiClientProvider[IO]]
    val api = mock[ControlledAzureResourceApi]
    val resourceApi = mock[ResourceApi]
    val dbsByJob = mutable.Map.empty[String, CreateControlledAzureDatabaseRequestBody]
    val namespacesByJob = mutable.Map.empty[String, CreateControlledAzureKubernetesNamespaceRequestBody]
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
      new DeleteControlledAzureResourceResult().jobReport(
        new JobReport().status(JobReport.StatusEnum.SUCCEEDED).id(request.getJobControl.getId)
      )
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

    // enumerate workspace resources - return empty list when creating app for first time
    when {
      resourceApi.enumerateResources(ArgumentMatchers.eq(workspaceIdForAppCreation.value), any, any, any, any)
    } thenReturn {
      val resourceList = new ArrayList[ResourceDescription]
      new ResourceList().resources(resourceList)
    }

    // enumerate workspace database resources
    when {
      resourceApi.enumerateResources(ArgumentMatchers.eq(workspaceId.value),
                                     any,
                                     any,
                                     ArgumentMatchers.eq(ResourceType.AZURE_DATABASE),
                                     any
      )
    } thenReturn {
      val resourceList = new ArrayList[ResourceDescription]
      val resourceDesc = new ResourceDescription()
      resourceDesc.metadata(new ResourceMetadata().name("cromwellmetadata").resourceId(java.util.UUID.randomUUID()))
      resourceDesc.resourceAttributes(
        new ResourceAttributesUnion().azureDatabase(
          new AzureDatabaseAttributes().databaseName("cromwellmetadata_abcxyz")
        )
      )
      resourceList.add(resourceDesc)
      new ResourceList().resources(resourceList)
    }
    // enumerate workspace database resources for a workspace with cloned db
    when {
      resourceApi.enumerateResources(ArgumentMatchers.eq(workspaceIdForCloning.value),
                                     any,
                                     any,
                                     ArgumentMatchers.eq(ResourceType.AZURE_DATABASE),
                                     any
      )
    } thenReturn {
      val resourceList = new ArrayList[ResourceDescription]
      val resourceDesc = new ResourceDescription()
      resourceDesc.metadata(new ResourceMetadata().name("cbas").resourceId(java.util.UUID.randomUUID()))
      resourceDesc.resourceAttributes(
        new ResourceAttributesUnion().azureDatabase(
          new AzureDatabaseAttributes().databaseName("cbas_cloned_db_abcxyz")
        )
      )
      resourceList.add(resourceDesc)
      new ResourceList().resources(resourceList)
    }
    // enumerate workspace managed identity resources
    when {
      resourceApi.enumerateResources(ArgumentMatchers.eq(workspaceId.value),
                                     any,
                                     any,
                                     ArgumentMatchers.eq(ResourceType.AZURE_MANAGED_IDENTITY),
                                     any
      )
    } thenReturn {
      val resourceList = new ArrayList[ResourceDescription]
      val resourceDesc = new ResourceDescription()
      resourceDesc.metadata(new ResourceMetadata().name("idworkflows_app").resourceId(java.util.UUID.randomUUID()))
      resourceDesc.resourceAttributes(
        new ResourceAttributesUnion().azureManagedIdentity(
          new AzureManagedIdentityAttributes().managedIdentityName("abcxyz")
        )
      )
      resourceList.add(resourceDesc)
      new ResourceList().resources(resourceList)
    }
    // enumerate workspace namespace resources
    when {
      resourceApi.enumerateResources(ArgumentMatchers.eq(workspaceId.value),
                                     any,
                                     any,
                                     ArgumentMatchers.eq(ResourceType.AZURE_KUBERNETES_NAMESPACE),
                                     any
      )
    } thenReturn {
      val resourceList = new ArrayList[ResourceDescription]
      val resourceDesc = new ResourceDescription()
      resourceDesc.metadata(new ResourceMetadata().name("idworkflows_app"))
      resourceDesc.resourceAttributes(
        new ResourceAttributesUnion().azureKubernetesNamespace(
          new AzureKubernetesNamespaceAttributes().kubernetesNamespace("ns-name")
        )
      )
      resourceList.add(resourceDesc)
      new ResourceList().resources(resourceList)
    }
    when {
      wsm.getResourceApi(any)(any)
    } thenReturn IO.pure(resourceApi)
    (wsm, api, resourceApi)

  }

  private def setUpMockAppInstall(): AppInstall[IO] = {
    val appInstall = mock[AppInstall[IO]]
    when {
      appInstall.databases
    } thenReturn List(ControlledDatabase("db1"))
    when {
      appInstall.buildHelmOverrideValues(any)(any)
    } thenReturn IO.pure(Values("values"))
    when {
      appInstall.checkStatus(any, any)(any)
    } thenReturn IO.pure(true)
    appInstall
  }

  private def setUpMockWorkflowAppInstall(): AppInstall[IO] = {
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
    } thenReturn IO.pure(true)

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
