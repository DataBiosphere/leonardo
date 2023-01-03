package org.broadinstitute.dsde.workbench.leonardo
package util

import cats.effect.IO
import com.azure.core.http.rest.PagedIterable
import com.azure.resourcemanager.compute.ComputeManager
import com.azure.resourcemanager.compute.fluent.{ComputeManagementClient, VirtualMachineScaleSetsClient}
import com.azure.resourcemanager.compute.models.{VirtualMachineScaleSet, VirtualMachineScaleSets}
import com.azure.resourcemanager.containerservice.models.KubernetesCluster
import com.azure.resourcemanager.msi.MsiManager
import com.azure.resourcemanager.msi.models.{Identities, Identity}
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models._
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.azure.mock.FakeAzureRelayService
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{landingZoneResources, workspaceId}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config.appMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.config.SamConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.{KubernetesServiceDbQueries, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsp.Release
import org.broadinstitute.dsp.mocks.MockHelm
import org.http4s.Uri
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatestplus.mockito.MockitoSugar

import java.nio.file.Files
import java.util.{Base64, UUID}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._

class AKSInterpreterSpec extends AnyFlatSpecLike with TestComponent with LeonardoTestSuite with MockitoSugar {

  val config = AKSInterpreterConfig(
    ConfigReader.appConfig.terraAppSetupChart,
    ConfigReader.appConfig.azure.coaAppConfig,
    ConfigReader.appConfig.azure.aadPodIdentityConfig,
    ConfigReader.appConfig.azure.appRegistration,
    SamConfig("https://sam"),
    appMonitorConfig,
    ConfigReader.appConfig.azure.wsm,
    ConfigReader.appConfig.drs
  )

  val mockSamDAO = setUpMockSamDAO
  val mockCromwellDAO = setUpMockCromwellDAO
  val mockCbasDAO = setUpMockCbasDAO
  val mockWdsDAO = setUpMockWdsDAO

  val aksInterp = new AKSInterpreter[IO](
    config,
    MockHelm,
    setUpMockAzureContainerService,
    FakeAzureRelayService,
    mockSamDAO,
    mockCromwellDAO,
    mockCbasDAO,
    mockWdsDAO
  ) {
    override private[util] def buildMsiManager(cloudContext: AzureCloudContext) = IO.pure(setUpMockMsiManager)
    override private[util] def buildComputeManager(cloudContext: AzureCloudContext) = IO.pure(setUpMockComputeManager)
    override private[util] def buildCoreV1Client(cloudContext: AzureCloudContext,
                                                 clusterName: AKSClusterName
    ): IO[CoreV1Api] = IO.pure(setUpMockKubeAPI)
  }

  val cloudContext = AzureCloudContext(
    TenantId("tenant"),
    SubscriptionId("sub"),
    ManagedResourceGroupName("mrg")
  )

  val lzResources = LandingZoneResources(
    AKSClusterName("cluster"),
    BatchAccountName("batch"),
    RelayNamespace("relay"),
    StorageAccountName("storage"),
    NetworkName("network"),
    PostgresName("pg"),
    LogAnalyticsWorkspaceName("logs"),
    SubnetworkName("subnet1"),
    SubnetworkName("subnet2"),
    SubnetworkName("subnet3"),
    SubnetworkName("subnet4")
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

  it should "build coa override values" in {
    val workspaceId = WorkspaceId(UUID.randomUUID)
    val overrides = aksInterp.buildCromwellChartOverrideValues(
      Release("rel-1"),
      AppName("app"),
      cloudContext,
      workspaceId,
      lzResources,
      Uri.unsafeFromString("https://relay.com/app"),
      setUpMockIdentity,
      storageContainer
    )
    overrides.asString shouldBe
      "config.resourceGroup=mrg," +
      "config.batchAccountName=batch," +
      "config.batchNodesSubnetId=subnet1," +
      s"config.drsUrl=${ConfigReader.appConfig.drs.url}," +
      "config.workflowExecutionIdentity=identity-id" +
      "relay.path=https://relay.com/app," +
      "persistence.storageResourceGroup=mrg," +
      "persistence.storageAccount=storage," +
      "persistence.blobContainer=sc-container," +
      "persistence.leoAppInstanceName=app," +
      s"persistence.workspaceManager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      s"persistence.workspaceManager.workspaceId=${workspaceId.value}," +
      s"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}," +
      "identity.name=identity-name," +
      "identity.resourceId=identity-id," +
      "identity.clientId=identity-client-id," +
      "fullnameOverride=coa-rel-1"
  }

  it should "create and poll a coa app, then successfully delete it" in isolatedDbTest {
    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      app = makeApp(1, nodepool.id).copy(
        appType = AppType.Cromwell,
        appResources = AppResources(
          namespace = Namespace(
            NamespaceId(-1),
            NamespaceName("ns-1")
          ),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        )
      )
      saveApp <- IO(app.save())
      appId = saveApp.id
      appName = saveApp.appName

      params = CreateAKSAppParams(appId,
                                  appName,
                                  workspaceId,
                                  cloudContext,
                                  landingZoneResources,
                                  Some(storageContainer)
      )
      _ <- aksInterp.createAndPollApp(params)

      app <- KubernetesServiceDbQueries
        .getActiveFullAppByName(CloudContext.Azure(params.cloudContext), appName)
        .transaction
    } yield {
      app shouldBe defined
      app.get.app.status shouldBe AppStatus.Running
      app.get.cluster.asyncFields shouldBe defined
      app
    }

    val dbApp = res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    dbApp shouldBe defined
    val app = dbApp.get.app

    val deletion = for {
      _ <- aksInterp.deleteApp(DeleteAKSAppParams(app.appName, workspaceId, landingZoneResources, cloudContext))
      app <- KubernetesServiceDbQueries
        .getActiveFullAppByName(CloudContext.Azure(cloudContext), app.appName)
        .transaction
    } yield app shouldBe None

    deletion.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  private def setUpMockIdentity: Identity = {
    val identity = mock[Identity]
    when {
      identity.clientId()
    } thenReturn "identity-client-id"
    when {
      identity.id()
    } thenReturn "identity-id"
    when {
      identity.name()
    } thenReturn "identity-name"
    identity
  }

  private def setUpMockMsiManager: MsiManager = {
    val msi = mock[MsiManager]
    val identities = mock[Identities]
    val identity = setUpMockIdentity
    when {
      identities.getById(anyString)
    } thenReturn identity
    when {
      msi.identities()
    } thenReturn identities
    msi
  }

  private def setUpMockComputeManager: ComputeManager = {
    val compute = mock[ComputeManager]
    val vmss = mock[VirtualMachineScaleSets]
    val pagedIterable = mock[PagedIterable[VirtualMachineScaleSet]]
    val aVmss = mock[VirtualMachineScaleSet]
    val serviceClient = mock[ComputeManagementClient]
    val vmssServiceClient = mock[VirtualMachineScaleSetsClient]
    when {
      aVmss.userAssignedManagedServiceIdentityIds()
    } thenReturn Set("agent-pool").asJava
    when {
      pagedIterable.iterator()
    } thenReturn List(aVmss).iterator.asJava
    when {
      vmss.listByResourceGroup(anyString)
    } thenReturn pagedIterable
    when {
      serviceClient.getVirtualMachineScaleSets
    } thenReturn vmssServiceClient
    when {
      compute.virtualMachineScaleSets()
    } thenReturn vmss
    when {
      compute.serviceClient()
    } thenReturn serviceClient
    compute
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

  private def setUpMockKubeAPI: CoreV1Api = {
    val coreV1Api = mock[CoreV1Api]
    val podList = mock[V1PodList]
    val mockPod = mock[V1Pod]
    val mockNamespace = mock[V1NamespaceList]
    val mockV1Status = mock[V1Status]
    when {
      coreV1Api.listNamespacedPod(any, any, any, any, any, any, any, any, any, any, any)
    } thenReturn podList
    when {
      podList.getItems
    } thenReturn List(mockPod).asJava
    when {
      mockPod.getStatus
    } thenReturn new V1PodStatus().phase("Failed")
    when {
      coreV1Api.listNamespace(any, any, any, any, any, any, any, any, any, any)
    } thenReturn mockNamespace
    when {
      mockNamespace.getItems
    } thenReturn List.empty.asJava
    when {
      coreV1Api.deleteNamespace(any, any, any, any, any, any, any)
    } thenReturn mockV1Status
    coreV1Api
  }

  private def setUpMockSamDAO: SamDAO[IO] = {
    val sam = mock[SamDAO[IO]]
    when {
      sam.getCachedArbitraryPetAccessToken(any)(any)
    } thenReturn IO.pure(Some("token"))
    when {
      sam.deleteResourceInternal(any, any)(any, any)
    } thenReturn IO.unit
    sam
  }

  private def setUpMockCromwellDAO: CromwellDAO[IO] = {
    val cromwell = mock[CromwellDAO[IO]]
    when {
      cromwell.getStatus(any, any)(any)
    } thenReturn IO.pure(true)
    cromwell
  }

  private def setUpMockCbasDAO: CbasDAO[IO] = {
    val cbas = mock[CbasDAO[IO]]
    when {
      cbas.getStatus(any, any)(any)
    } thenReturn IO.pure(true)
    cbas
  }

  private def setUpMockWdsDAO: WdsDAO[IO] = {
    val wds = mock[WdsDAO[IO]]
    when {
      wds.getStatus(any, any)(any)
    } thenReturn IO.pure(true)
    wds
  }
}
