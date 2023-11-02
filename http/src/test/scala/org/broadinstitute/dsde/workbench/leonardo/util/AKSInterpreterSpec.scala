package org.broadinstitute.dsde.workbench.leonardo
package util

import bio.terra.workspace.api.{ControlledAzureResourceApi, ResourceApi}
import bio.terra.workspace.model.{DeleteControlledAzureResourceRequest, _}
import cats.effect.IO
import com.azure.resourcemanager.containerservice.models.KubernetesCluster
import io.kubernetes.client.openapi.apis.CoreV1Api
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.PodStatus
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{azureRegion, billingProfileId, workspaceId}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.app.AppInstall
import org.broadinstitute.dsde.workbench.leonardo.app.Database.ControlledDatabase
import org.broadinstitute.dsde.workbench.leonardo.config.Config.appMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.config.SamConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{dbioToIO, ConfigReader}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsp.mocks.MockHelm
import org.broadinstitute.dsp.{ChartName, ChartVersion, Values}
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
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

  implicit val appTypeToAppInstall: AppType => AppInstall[IO] = _ => setUpMockAppInstall
  def newAksInterp(configuration: AKSInterpreterConfig) = new AKSInterpreter[IO](
    configuration,
    MockHelm,
    mockAzureContainerService,
    mockAzureRelayService,
    mockSamDAO,
    mockWsmDAO,
    mockKube,
    mockWsm,
    mockWsmDAO
  )

  val aksInterp = newAksInterp(config)

  val cloudContext = AzureCloudContext(
    TenantId("tenant"),
    SubscriptionId("sub"),
    ManagedResourceGroupName("mrg")
  )

  val lzResources = LandingZoneResources(
    UUID.fromString("5c12f64b-f4ac-4be1-ae4a-4cace5de807d"),
    AKSClusterName("cluster"),
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
      authContext <- aksInterp.getHelmAuthContext(lzResources.clusterName, cloudContext, NamespaceName("ns"))
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
        app = makeApp(1, nodepool.id).copy(
          appType = AppType.Cromwell,
          appResources = AppResources(
            namespace = NamespaceName("ns-1"),
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
          appType = AppType.Cromwell,
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

        appName = saveApp.appName

        _ <- aksInterp.deleteApp(DeleteAKSAppParams(appName, workspaceId, cloudContext, billingProfileId))
        app <- KubernetesServiceDbQueries
          .getActiveFullAppByName(CloudContext.Azure(cloudContext), appName)
          .transaction
      } yield app shouldBe None

      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

  // create wsm identity
  it should "create a WSM controlled identity" in isolatedDbTest {
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

      createdIdentity <- aksInterp.createWsmIdentityResource(saveApp, "ns", workspaceId)

      controlledResources <- appControlledResourceQuery
        .getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
        .transaction
    } yield {
      createdIdentity.getAzureManagedIdentity.getMetadata.getName shouldBe "idns"
      controlledResources.size shouldBe 1
      controlledResources.head.resourceId.value shouldBe createdIdentity.getResourceId
      controlledResources.head.resourceType shouldBe WsmResourceType.AzureManagedIdentity
      controlledResources.head.status shouldBe AppControlledResourceStatus.Created
      controlledResources.head.appId shouldBe appId.id
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // create wsm database
  it should "create a WSM controlled database" in isolatedDbTest {
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
      owner = "id"

      createdDatabase <- aksInterp.createWsmDatabaseResource(saveApp,
                                                             workspaceId,
                                                             ControlledDatabase("test", false),
                                                             "wds",
                                                             Some(owner),
                                                             mockControlledResourceApi
      )

      controlledResources <- appControlledResourceQuery
        .getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
        .transaction
    } yield {
      createdDatabase.getAzureDatabase.getMetadata.getName shouldBe "test_wds"
      createdDatabase.getAzureDatabase.getAttributes.getDatabaseOwner shouldBe owner
      controlledResources.size shouldBe 1
      controlledResources.head.resourceId.value shouldBe createdDatabase.getResourceId
      controlledResources.head.resourceType shouldBe WsmResourceType.AzureDatabase
      controlledResources.head.status shouldBe AppControlledResourceStatus.Created
      controlledResources.head.appId shouldBe appId.id
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
                                                                         List.empty,
                                                                         Some(identity)
      )

      controlledResources <- appControlledResourceQuery
        .getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
        .transaction
    } yield {
      createdNamespace.getAzureKubernetesNamespace.getAttributes.getKubernetesNamespace should startWith("ns")
      createdNamespace.getAzureKubernetesNamespace.getAttributes.getDatabases.asScala.toSet shouldBe databases.toSet
      createdNamespace.getAzureKubernetesNamespace.getAttributes.getManagedIdentity shouldBe identity
      createdNamespace.getAzureKubernetesNamespace.getAttributes.getKubernetesServiceAccount shouldBe identity.toString
      controlledResources.size shouldBe 1
      controlledResources.head.resourceId.value shouldBe createdNamespace.getResourceId
      controlledResources.head.resourceType shouldBe WsmResourceType.AzureKubernetesNamespace
      controlledResources.head.status shouldBe AppControlledResourceStatus.Created
      controlledResources.head.appId shouldBe appId.id
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

  private def setUpMockKube: KubernetesAlgebra[IO] = {
    val kube = mock[KubernetesAlgebra[IO]]
    val coreV1Api = mock[CoreV1Api]
    when {
      kube.createAzureClient(any, any[String].asInstanceOf[AKSClusterName])(any)
    } thenReturn IO.pure(coreV1Api)
    when {
      kube.listPodStatus(any, any)(any)
    } thenReturn IO.pure(List(PodStatus.Failed))
    when {
      kube.deleteNamespace(any, any)(any)
    } thenReturn IO.unit
    when {
      kube.namespaceExists(any, any)(any)
    } thenReturn IO.pure(false)
    when {
      kube.createNamespace(any, any)(any)
    } thenReturn IO.unit
    kube
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
            .metadata(new ResourceMetadata().name(requestBody.getCommon.getName))
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

    // enumerate workspace resources
    when {
      resourceApi.enumerateResources(any, any, any, any, any)
    } thenReturn {
      val resourceList = new ArrayList[ResourceDescription]
      val resourceDesc = new ResourceDescription()
      resourceDesc.metadata(new ResourceMetadata().name("cromwellmetadata"))
      resourceDesc.resourceAttributes(
        new ResourceAttributesUnion().azureDatabase(
          new AzureDatabaseAttributes().databaseName("cromwellmetadata_abcxyz")
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

  private def setUpMockAppInstall: AppInstall[IO] = {
    val appInstall = mock[AppInstall[IO]]
    when {
      appInstall.databases
    } thenReturn List(ControlledDatabase("db1", false))
    when {
      appInstall.buildHelmOverrideValues(any)(any)
    } thenReturn IO.pure(Values("values"))
    when {
      appInstall.checkStatus(any, any)(any)
    } thenReturn IO.pure(true)
    appInstall
  }

}
