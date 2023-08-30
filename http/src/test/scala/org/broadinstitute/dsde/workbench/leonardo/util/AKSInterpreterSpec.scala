package org.broadinstitute.dsde.workbench.leonardo
package util

import bio.terra.workspace.api.ControlledAzureResourceApi
import bio.terra.workspace.model._
import cats.effect.IO
import com.azure.core.http.rest.PagedIterable
import com.azure.resourcemanager.applicationinsights.models.ApplicationInsightsComponent
import com.azure.resourcemanager.batch.models.{BatchAccount, BatchAccountKeys}
import com.azure.resourcemanager.compute.ComputeManager
import com.azure.resourcemanager.compute.fluent.{ComputeManagementClient, VirtualMachineScaleSetsClient}
import com.azure.resourcemanager.compute.models.{VirtualMachineScaleSet, VirtualMachineScaleSets}
import com.azure.resourcemanager.containerservice.models.KubernetesCluster
import com.azure.resourcemanager.msi.MsiManager
import com.azure.resourcemanager.msi.models.{Identities, Identity}
import io.kubernetes.client.openapi.apis.CoreV1Api
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.{KubernetesNamespace, PodStatus}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{NamespaceName, ServiceAccountName}
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{
  azureRegion,
  landingZoneResources,
  petUserInfo,
  workspaceId
}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config.appMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.config.SamConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.{
  appControlledResourceQuery,
  AppControlledResourceStatus,
  KubernetesServiceDbQueries,
  TestComponent,
  WsmResourceType
}
import org.broadinstitute.dsde.workbench.leonardo.http.{dbioToIO, ConfigReader}
import org.broadinstitute.dsp.Release
import org.broadinstitute.dsp.mocks.MockHelm
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials, Uri}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{verify, when}
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
    ConfigReader.appConfig.azure.coaAppConfig,
    ConfigReader.appConfig.azure.workflowsAppConfig,
    ConfigReader.appConfig.azure.cromwellRunnerAppConfig,
    ConfigReader.appConfig.azure.wdsAppConfig,
    ConfigReader.appConfig.azure.hailBatchAppConfig,
    ConfigReader.appConfig.azure.aadPodIdentityConfig,
    ConfigReader.appConfig.azure.appRegistration,
    SamConfig("https://sam.dsde-dev.broadinstitute.org/"),
    appMonitorConfig,
    ConfigReader.appConfig.azure.wsm,
    ConfigReader.appConfig.drs,
    new URL("https://leo-dummy-url.org"),
    ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.listenerImage,
    ConfigReader.appConfig.azure.tdr,
    ConfigReader.appConfig.azure.listenerChartConfig
  )

  val mockSamDAO = setUpMockSamDAO
  val mockCromwellDAO = setUpMockCromwellDAO
  val mockCbasDAO = setUpMockCbasDAO
  val mockCbasUiDAO = setUpMockCbasUiDAO
  val mockWdsDAO = setUpMockWdsDAO
  val mockWsmDAO = new MockWsmDAO
  val mockHailBatchDAO = setUpMockHailBatchDAO
  val mockAzureContainerService = setUpMockAzureContainerService
  val mockAzureApplicationInsightsService = setUpMockAzureApplicationInsightsService
  val mockAzureBatchService = setUpMockAzureBatchService
  val mockAzureRelayService = setUpMockAzureRelayService
  val mockKube = setUpMockKube
  val mockWsm = setUpMockWsmApiClientProvider

  def newAksInterp(configuration: AKSInterpreterConfig) = new AKSInterpreter[IO](
    configuration,
    MockHelm,
    mockAzureBatchService,
    mockAzureContainerService,
    mockAzureApplicationInsightsService,
    mockAzureRelayService,
    mockSamDAO,
    mockCromwellDAO,
    mockCbasDAO,
    mockCbasUiDAO,
    mockWdsDAO,
    mockHailBatchDAO,
    mockWsmDAO,
    mockKube,
    mockWsm
  ) {
    override private[util] def buildMsiManager(cloudContext: AzureCloudContext) = IO.pure(setUpMockMsiManager)
    override private[util] def buildComputeManager(cloudContext: AzureCloudContext) = IO.pure(setUpMockComputeManager)
  }

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

  it should "build coa override values" in {
    val workspaceId = WorkspaceId(UUID.randomUUID)
    val overrides = aksInterp.buildCromwellChartOverrideValues(
      Release("rel-1"),
      AppName("app"),
      cloudContext,
      workspaceId,
      lzResources,
      Uri.unsafeFromString("https://relay.com/app"),
      Some(setUpMockIdentity),
      storageContainer,
      BatchAccountKey("batchKey"),
      "applicationInsightsConnectionString",
      None,
      petUserInfo.accessToken.token,
      IdentityType.PodIdentity,
      None
    )
    overrides.asString shouldBe
      "config.resourceGroup=mrg," +
      "config.batchAccountKey=batchKey," +
      "config.batchAccountName=batch," +
      "config.batchNodesSubnetId=subnet1," +
      s"config.drsUrl=${ConfigReader.appConfig.drs.url}," +
      "config.landingZoneId=5c12f64b-f4ac-4be1-ae4a-4cace5de807d," +
      "config.subscriptionId=sub," +
      s"config.region=${azureRegion}," +
      "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
      "relay.path=https://relay.com/app," +
      "persistence.storageResourceGroup=mrg," +
      "persistence.storageAccount=storage," +
      "persistence.blobContainer=sc-container," +
      "persistence.leoAppInstanceName=app," +
      s"persistence.workspaceManager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      s"persistence.workspaceManager.workspaceId=${workspaceId.value}," +
      s"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}," +
      "identity.enabled=true," +
      "identity.name=identity-name," +
      "identity.resourceId=identity-id," +
      "identity.clientId=identity-client-id," +
      "workloadIdentity.enabled=false," +
      "workloadIdentity.serviceAccountName=identity-name," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      "cbas.enabled=true," +
      "cbasUI.enabled=true," +
      "cromwell.enabled=true," +
      "dockstore.baseUrl=https://staging.dockstore.org/," +
      "fullnameOverride=coa-rel-1," +
      "instrumentationEnabled=false," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}"
  }

  it should "build coa override values with databases" in {
    val workspaceId = WorkspaceId(UUID.randomUUID)
    val databaseNames = CromwellDatabaseNames("cromwell", "cbas", "tes")
    val overrides = aksInterp.buildCromwellChartOverrideValues(
      Release("rel-1"),
      AppName("app"),
      cloudContext,
      workspaceId,
      lzResources,
      Uri.unsafeFromString("https://relay.com/app"),
      Some(setUpMockIdentity),
      storageContainer,
      BatchAccountKey("batchKey"),
      "applicationInsightsConnectionString",
      None,
      petUserInfo.accessToken.token,
      IdentityType.WorkloadIdentity,
      Some(databaseNames)
    )
    overrides.asString shouldBe
      "config.resourceGroup=mrg," +
      "config.batchAccountKey=batchKey," +
      "config.batchAccountName=batch," +
      "config.batchNodesSubnetId=subnet1," +
      s"config.drsUrl=${ConfigReader.appConfig.drs.url}," +
      "config.landingZoneId=5c12f64b-f4ac-4be1-ae4a-4cace5de807d," +
      "config.subscriptionId=sub," +
      s"config.region=${azureRegion}," +
      "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
      "relay.path=https://relay.com/app," +
      "persistence.storageResourceGroup=mrg," +
      "persistence.storageAccount=storage," +
      "persistence.blobContainer=sc-container," +
      "persistence.leoAppInstanceName=app," +
      s"persistence.workspaceManager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      s"persistence.workspaceManager.workspaceId=${workspaceId.value}," +
      s"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}," +
      "identity.enabled=false," +
      "identity.name=identity-name," +
      "identity.resourceId=identity-id," +
      "identity.clientId=identity-client-id," +
      "workloadIdentity.enabled=true," +
      "workloadIdentity.serviceAccountName=identity-name," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      "cbas.enabled=true," +
      "cbasUI.enabled=true," +
      "cromwell.enabled=true," +
      "dockstore.baseUrl=https://staging.dockstore.org/," +
      "fullnameOverride=coa-rel-1," +
      "instrumentationEnabled=false," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      "postgres.podLocalDatabaseEnabled=false," +
      s"postgres.host=${lzResources.postgresServer.map(_.name).get}.postgres.database.azure.com," +
      "postgres.pgbouncer.enabled=false," +
      "postgres.user=identity-name," +
      s"postgres.dbnames.cromwell=${databaseNames.cromwell}," +
      s"postgres.dbnames.cbas=${databaseNames.cbas}," +
      s"postgres.dbnames.tes=${databaseNames.tes}"
  }

  it should "build coa override values with databases and pgbouncer" in {
    val workspaceId = WorkspaceId(UUID.randomUUID)
    val databaseNames = CromwellDatabaseNames("cromwell", "cbas", "tes")
    val overrides = aksInterp.buildCromwellChartOverrideValues(
      Release("rel-1"),
      AppName("app"),
      cloudContext,
      workspaceId,
      lzResources.copy(postgresServer = Option(PostgresServer("postgres", pgBouncerEnabled = true))),
      Uri.unsafeFromString("https://relay.com/app"),
      Some(setUpMockIdentity),
      storageContainer,
      BatchAccountKey("batchKey"),
      "applicationInsightsConnectionString",
      None,
      petUserInfo.accessToken.token,
      IdentityType.WorkloadIdentity,
      Some(databaseNames)
    )
    overrides.asString shouldBe
      "config.resourceGroup=mrg," +
      "config.batchAccountKey=batchKey," +
      "config.batchAccountName=batch," +
      "config.batchNodesSubnetId=subnet1," +
      s"config.drsUrl=${ConfigReader.appConfig.drs.url}," +
      "config.landingZoneId=5c12f64b-f4ac-4be1-ae4a-4cace5de807d," +
      "config.subscriptionId=sub," +
      s"config.region=${azureRegion}," +
      "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
      "relay.path=https://relay.com/app," +
      "persistence.storageResourceGroup=mrg," +
      "persistence.storageAccount=storage," +
      "persistence.blobContainer=sc-container," +
      "persistence.leoAppInstanceName=app," +
      s"persistence.workspaceManager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      s"persistence.workspaceManager.workspaceId=${workspaceId.value}," +
      s"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}," +
      "identity.enabled=false," +
      "identity.name=identity-name," +
      "identity.resourceId=identity-id," +
      "identity.clientId=identity-client-id," +
      "workloadIdentity.enabled=true," +
      "workloadIdentity.serviceAccountName=identity-name," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      "cbas.enabled=true," +
      "cbasUI.enabled=true," +
      "cromwell.enabled=true," +
      "dockstore.baseUrl=https://staging.dockstore.org/," +
      "fullnameOverride=coa-rel-1," +
      "instrumentationEnabled=false," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      "postgres.podLocalDatabaseEnabled=false," +
      s"postgres.host=${lzResources.postgresServer.map(_.name).get}.postgres.database.azure.com," +
      "postgres.pgbouncer.enabled=true," +
      "postgres.user=identity-name," +
      s"postgres.dbnames.cromwell=${databaseNames.cromwell}," +
      s"postgres.dbnames.cbas=${databaseNames.cbas}," +
      s"postgres.dbnames.tes=${databaseNames.tes}"
  }

  it should "build wds override values" in {
    val workspaceId = WorkspaceId(UUID.randomUUID)
    val overrides = aksInterp.buildWdsChartOverrideValues(
      Release("rel-1"),
      AppName("app"),
      cloudContext,
      workspaceId,
      lzResources,
      Some(setUpMockIdentity),
      "applicationInsightsConnectionString",
      None,
      petUserInfo.accessToken.token,
      IdentityType.PodIdentity,
      None,
      None
    )
    overrides.asString shouldBe
      "config.resourceGroup=mrg," +
      "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
      "config.subscriptionId=sub," +
      s"config.region=${azureRegion}," +
      "general.leoAppInstanceName=app," +
      s"general.workspaceManager.workspaceId=${workspaceId.value}," +
      "identity.enabled=true," +
      "identity.name=identity-name," +
      "identity.resourceId=identity-id," +
      "identity.clientId=identity-client-id," +
      "workloadIdentity.enabled=false," +
      "workloadIdentity.serviceAccountName=none," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      s"workspacemanager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      "fullnameOverride=wds-rel-1," +
      "instrumentationEnabled=false," +
      "import.dataRepoUrl=https://jade.datarepo-dev.broadinstitute.org," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      "provenance.sourceWorkspaceId="
  }

  it should "build wds override values with sourceWorkspaceId" in {
    val workspaceId = WorkspaceId(UUID.randomUUID)
    val sourceWorkspaceId = WorkspaceId(UUID.randomUUID)

    val overrides = aksInterp.buildWdsChartOverrideValues(
      Release("rel-1"),
      AppName("app"),
      cloudContext,
      workspaceId,
      lzResources,
      Some(setUpMockIdentity),
      "applicationInsightsConnectionString",
      Some(sourceWorkspaceId),
      petUserInfo.accessToken.token,
      IdentityType.PodIdentity,
      None,
      None
    )
    overrides.asString shouldBe
      "config.resourceGroup=mrg," +
      "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
      "config.subscriptionId=sub," +
      s"config.region=${azureRegion}," +
      "general.leoAppInstanceName=app," +
      s"general.workspaceManager.workspaceId=${workspaceId.value}," +
      "identity.enabled=true," +
      "identity.name=identity-name," +
      "identity.resourceId=identity-id," +
      "identity.clientId=identity-client-id," +
      "workloadIdentity.enabled=false," +
      "workloadIdentity.serviceAccountName=none," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      s"workspacemanager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      "fullnameOverride=wds-rel-1," +
      "instrumentationEnabled=false," +
      "import.dataRepoUrl=https://jade.datarepo-dev.broadinstitute.org," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      s"provenance.sourceWorkspaceId=${sourceWorkspaceId.value}"
  }

  it should "build wds override values with workload identity" in {
    val workspaceId = WorkspaceId(UUID.randomUUID)
    val overrides = aksInterp.buildWdsChartOverrideValues(
      Release("rel-1"),
      AppName("app"),
      cloudContext,
      workspaceId,
      lzResources,
      Some(setUpMockIdentity),
      "applicationInsightsConnectionString",
      None,
      petUserInfo.accessToken.token,
      IdentityType.WorkloadIdentity,
      Some(ServiceAccountName("ksa")),
      Some("dbname")
    )
    overrides.asString shouldBe
      "config.resourceGroup=mrg," +
      "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
      "config.subscriptionId=sub," +
      s"config.region=${azureRegion}," +
      "general.leoAppInstanceName=app," +
      s"general.workspaceManager.workspaceId=${workspaceId.value}," +
      "identity.enabled=false," +
      "identity.name=identity-name," +
      "identity.resourceId=identity-id," +
      "identity.clientId=identity-client-id," +
      "workloadIdentity.enabled=true," +
      "workloadIdentity.serviceAccountName=ksa," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      s"workspacemanager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      "fullnameOverride=wds-rel-1," +
      "instrumentationEnabled=false," +
      "import.dataRepoUrl=https://jade.datarepo-dev.broadinstitute.org," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      "provenance.sourceWorkspaceId=," +
      "postgres.podLocalDatabaseEnabled=false," +
      s"postgres.host=${lzResources.postgresServer.map(_.name).get}.postgres.database.azure.com," +
      "postgres.pgbouncer.enabled=false," +
      "postgres.dbname=dbname," +
      "postgres.user=ksa"
  }

  it should "build wds override values with workload identity and pgBouncer" in {
    val workspaceId = WorkspaceId(UUID.randomUUID)
    val overrides = aksInterp.buildWdsChartOverrideValues(
      Release("rel-1"),
      AppName("app"),
      cloudContext,
      workspaceId,
      lzResources.copy(postgresServer = Option(PostgresServer("postgres", pgBouncerEnabled = true))),
      Some(setUpMockIdentity),
      "applicationInsightsConnectionString",
      None,
      petUserInfo.accessToken.token,
      IdentityType.WorkloadIdentity,
      Some(ServiceAccountName("ksa")),
      Some("dbname")
    )
    overrides.asString shouldBe
      "config.resourceGroup=mrg," +
      "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
      "config.subscriptionId=sub," +
      s"config.region=${azureRegion}," +
      "general.leoAppInstanceName=app," +
      s"general.workspaceManager.workspaceId=${workspaceId.value}," +
      "identity.enabled=false," +
      "identity.name=identity-name," +
      "identity.resourceId=identity-id," +
      "identity.clientId=identity-client-id," +
      "workloadIdentity.enabled=true," +
      "workloadIdentity.serviceAccountName=ksa," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      s"workspacemanager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      "fullnameOverride=wds-rel-1," +
      "instrumentationEnabled=false," +
      "import.dataRepoUrl=https://jade.datarepo-dev.broadinstitute.org," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      "provenance.sourceWorkspaceId=," +
      "postgres.podLocalDatabaseEnabled=false," +
      s"postgres.host=${lzResources.postgresServer.map(_.name).get}.postgres.database.azure.com," +
      "postgres.pgbouncer.enabled=true," +
      "postgres.dbname=dbname," +
      "postgres.user=ksa"
  }

  it should "build hail batch override values" in {
    val workspaceId = WorkspaceId(UUID.randomUUID)
    val overrides = aksInterp.buildHailBatchChartOverrideValues(AppName("app"),
                                                                workspaceId,
                                                                lzResources,
                                                                Some(setUpMockIdentity),
                                                                storageContainer,
                                                                "relay.com",
                                                                RelayHybridConnectionName("app")
    )
    overrides.asString shouldBe
      "persistence.storageAccount=storage," +
      "persistence.blobContainer=sc-container," +
      s"persistence.workspaceManager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      s"persistence.workspaceManager.workspaceId=${workspaceId.value}," +
      s"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}," +
      s"persistence.workspaceManager.storageContainerUrl=https://${lzResources.storageAccountName.value}.blob.core.windows.net/${storageContainer.name.value}," +
      "persistence.leoAppName=app," +
      "identity.name=identity-name," +
      "identity.resourceId=identity-id," +
      "identity.clientId=identity-client-id," +
      s"relay.domain=relay.com," +
      "relay.subpath=/app"
  }

  it should "build workflows-app override values with workload identity and pgbouncer" in {
    val workspaceId = WorkspaceId(UUID.randomUUID)
    val overrides = aksInterp.buildWorkflowsAppChartOverrideValues(
      Release("rel-1"),
      AppName("app"),
      cloudContext,
      workspaceId,
      lzResources.copy(postgresServer = Option(PostgresServer("postgres", pgBouncerEnabled = true))),
      Uri.unsafeFromString("https://relay.com/app"),
      storageContainer,
      BatchAccountKey("batchKey"),
      "applicationInsightsConnectionString",
      None,
      petUserInfo.accessToken.token,
      IdentityType.WorkloadIdentity,
      Some(ServiceAccountName("ksa")),
      Some(WorkflowsAppDatabaseNames("cbasdbname", "cromwellmetadatadbname"))
    )
    overrides.asString shouldBe
      "config.resourceGroup=mrg," +
      "config.batchAccountKey=batchKey," +
      "config.batchAccountName=batch," +
      "config.batchNodesSubnetId=subnet1," +
      s"config.drsUrl=${ConfigReader.appConfig.drs.url}," +
      "config.landingZoneId=5c12f64b-f4ac-4be1-ae4a-4cace5de807d," +
      "config.subscriptionId=sub," +
      s"config.region=${azureRegion}," +
      "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
      "relay.path=https://relay.com/app," +
      "persistence.storageResourceGroup=mrg," +
      "persistence.storageAccount=storage," +
      "persistence.blobContainer=sc-container," +
      "persistence.leoAppInstanceName=app," +
      s"persistence.workspaceManager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      s"persistence.workspaceManager.workspaceId=${workspaceId.value}," +
      s"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}," +
      "workloadIdentity.serviceAccountName=ksa," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      "dockstore.baseUrl=https://staging.dockstore.org/," +
      "fullnameOverride=wfa-rel-1," +
      "instrumentationEnabled=false," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      s"postgres.host=${lzResources.postgresServer.map(_.name).get}.postgres.database.azure.com," +
      "postgres.pgbouncer.enabled=true," +
      "postgres.user=ksa," +
      s"postgres.dbnames.cromwellMetadata=cromwellmetadatadbname," +
      s"postgres.dbnames.cbas=cbasdbname"
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

  for (appType <- List(AppType.Wds, AppType.Cromwell, AppType.HailBatch, AppType.WorkflowsApp))
    it should s"create and poll a shared ${appType} app, then successfully delete it" in isolatedDbTest {
      val mockAzureRelayService = setUpMockAzureRelayService

      val aksInterp = new AKSInterpreter[IO](
        config.copy(workflowsAppConfig = config.workflowsAppConfig.copy(enabled = true)),
        MockHelm,
        mockAzureBatchService,
        mockAzureContainerService,
        mockAzureApplicationInsightsService,
        mockAzureRelayService,
        mockSamDAO,
        mockCromwellDAO,
        mockCbasDAO,
        mockCbasUiDAO,
        mockWdsDAO,
        mockHailBatchDAO,
        mockWsmDAO,
        mockKube,
        mockWsm
      ) {
        override private[util] def buildMsiManager(cloudContext: AzureCloudContext) = IO.pure(setUpMockMsiManager)
        override private[util] def buildComputeManager(cloudContext: AzureCloudContext) =
          IO.pure(setUpMockComputeManager)
      }
      val res = for {
        cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
        nodepool <- IO(makeNodepool(1, cluster.id).save())
        customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace",
                            "RELAY_HYBRID_CONNECTION_NAME" -> s"app1-${workspaceId.value}"
        )
        app = makeApp(1, nodepool.id, customEnvironmentVariables = customEnvVars).copy(
          appType = appType,
          appResources = AppResources(
            namespace = Namespace(
              NamespaceId(-1),
              NamespaceName("ns-1")
            ),
            disk = None,
            services = List.empty,
            kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
          ),
          appAccessScope = Some(AppAccessScope.WorkspaceShared)
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
        app.get.app.appAccessScope shouldBe Some(AppAccessScope.WorkspaceShared)
        app.get.app.samResourceId.resourceType shouldBe SamResourceType.SharedApp
        app.get.cluster.asyncFields shouldBe defined
        app
      }

      val dbApp = res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      dbApp shouldBe defined
      val app = dbApp.get.app

      val deletion = for {
        _ <- aksInterp.deleteApp(DeleteAKSAppParams(app.appName, workspaceId, landingZoneResources, cloudContext))
        deletedApp <- KubernetesServiceDbQueries
          .getActiveFullAppByName(CloudContext.Azure(cloudContext), app.appName)
          .transaction
      } yield {
        deletedApp shouldBe None
        verify(mockAzureRelayService).deleteRelayHybridConnection(
          RelayNamespace(ArgumentMatchers.eq(landingZoneResources.relayNamespace.value)),
          RelayHybridConnectionName(ArgumentMatchers.eq(s"${app.appName.value}-${workspaceId.value}")),
          ArgumentMatchers.eq(cloudContext)
        )(any())
      }

      deletion.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

  for (appType <- List(AppType.Wds, AppType.Cromwell, AppType.CromwellRunnerApp, AppType.WorkflowsApp))
    it should s"create ${appType} with wsm resources, then successfully delete them" in isolatedDbTest {
      val mockAzureRelayService = setUpMockAzureRelayService

      val aksInterp = new AKSInterpreter[IO](
        config.copy(
          wdsAppConfig = config.wdsAppConfig.copy(databaseEnabled = true),
          coaAppConfig = config.coaAppConfig.copy(databaseEnabled = true),
          cromwellRunnerAppConfig = config.cromwellRunnerAppConfig.copy(enabled = true),
          workflowsAppConfig = config.workflowsAppConfig.copy(enabled = true)
        ),
        MockHelm,
        mockAzureBatchService,
        mockAzureContainerService,
        mockAzureApplicationInsightsService,
        mockAzureRelayService,
        mockSamDAO,
        mockCromwellDAO,
        mockCbasDAO,
        mockCbasUiDAO,
        mockWdsDAO,
        mockHailBatchDAO,
        mockWsmDAO,
        mockKube,
        mockWsm
      ) {
        override private[util] def buildMsiManager(cloudContext: AzureCloudContext) = IO.pure(setUpMockMsiManager)

        override private[util] def buildComputeManager(cloudContext: AzureCloudContext) =
          IO.pure(setUpMockComputeManager)
      }
      val res = for {
        cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
        nodepool <- IO(makeNodepool(1, cluster.id).save())
        customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace",
                            "RELAY_HYBRID_CONNECTION_NAME" -> s"app1-${workspaceId.value}"
        )
        app = makeApp(1, nodepool.id, customEnvironmentVariables = customEnvVars).copy(
          appType = appType,
          appResources = AppResources(
            namespace = Namespace(
              NamespaceId(-1),
              NamespaceName("ns-1")
            ),
            disk = None,
            services = List.empty,
            kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
          ),
          appAccessScope = Some(AppAccessScope.WorkspaceShared)
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

        controlledResources <- dbioToIO(
          appControlledResourceQuery.getAllForAppByStatus(appId.id, AppControlledResourceStatus.Created)
        ).transaction
      } yield {
        app shouldBe defined
        app.get.app.status shouldBe AppStatus.Running
        app.get.app.appAccessScope shouldBe Some(AppAccessScope.WorkspaceShared)
        app.get.app.samResourceId.resourceType shouldBe SamResourceType.SharedApp
        app.get.cluster.asyncFields shouldBe defined

        val expectedControlledResourcesCount = appType match {
          case AppType.Wds               => 2
          case AppType.Cromwell          => 3
          case AppType.CromwellRunnerApp => 3
          case AppType.WorkflowsApp      => 3
          case _                         => 0
        }
        controlledResources.size shouldBe expectedControlledResourcesCount

        val expectedControlledResourcesTypes = appType match {
          case AppType.Wds => List(WsmResourceType.AzureManagedIdentity, WsmResourceType.AzureDatabase)
          case AppType.Cromwell =>
            List(WsmResourceType.AzureDatabase, WsmResourceType.AzureDatabase, WsmResourceType.AzureDatabase)
          case AppType.WorkflowsApp =>
            List(WsmResourceType.AzureManagedIdentity, WsmResourceType.AzureDatabase, WsmResourceType.AzureDatabase)
          case _ => List()
        }

        controlledResources.map(a => a.resourceType) should contain theSameElementsAs expectedControlledResourcesTypes

        app
      }

      val dbApp = res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      dbApp shouldBe defined
      val app = dbApp.get.app

      val deletion = for {
        _ <- aksInterp.deleteApp(DeleteAKSAppParams(app.appName, workspaceId, landingZoneResources, cloudContext))
        deletedApp <- KubernetesServiceDbQueries
          .getActiveFullAppByName(CloudContext.Azure(cloudContext), app.appName)
          .transaction
        controlledResources <- appControlledResourceQuery
          .getAllForAppByStatus(app.id.id, AppControlledResourceStatus.Created)
          .transaction
      } yield {
        controlledResources shouldBe empty
        deletedApp shouldBe None
        verify(mockAzureRelayService).deleteRelayHybridConnection(
          RelayNamespace(ArgumentMatchers.eq(landingZoneResources.relayNamespace.value)),
          RelayHybridConnectionName(ArgumentMatchers.eq(s"${app.appName.value}-${workspaceId.value}")),
          ArgumentMatchers.eq(cloudContext)
        )(any())
      }

      deletion.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

  it should "successfully delete an app with old relayHybridConnection naming convention" in isolatedDbTest {
    val mockAzureRelayService = setUpMockAzureRelayService

    val aksInterp = new AKSInterpreter[IO](
      config,
      MockHelm,
      mockAzureBatchService,
      mockAzureContainerService,
      mockAzureApplicationInsightsService,
      mockAzureRelayService,
      mockSamDAO,
      mockCromwellDAO,
      mockCbasDAO,
      mockCbasUiDAO,
      mockWdsDAO,
      mockHailBatchDAO,
      mockWsmDAO,
      mockKube,
      mockWsm
    ) {
      override private[util] def buildMsiManager(cloudContext: AzureCloudContext) = IO.pure(setUpMockMsiManager)
      override private[util] def buildComputeManager(cloudContext: AzureCloudContext) = IO.pure(setUpMockComputeManager)
    }

    val res = for {
      cluster <- IO(makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext)).save())
      nodepool <- IO(makeNodepool(1, cluster.id).save())
      customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
      app = makeApp(1, nodepool.id, customEnvironmentVariables = customEnvVars).copy(
        appType = AppType.Cromwell,
        appResources = AppResources(
          namespace = Namespace(
            NamespaceId(-1),
            NamespaceName("ns-1")
          ),
          disk = None,
          services = List.empty,
          kubernetesServiceAccountName = Some(ServiceAccountName("ksa-1"))
        ),
        appAccessScope = Some(AppAccessScope.WorkspaceShared)
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
      app.get.app.customEnvironmentVariables shouldBe customEnvVars
      app
    }

    val dbApp = res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    dbApp shouldBe defined
    val app = dbApp.get.app
    val deletion = for {
      _ <- aksInterp.deleteApp(DeleteAKSAppParams(app.appName, workspaceId, landingZoneResources, cloudContext))
      deletedApp <- KubernetesServiceDbQueries
        .getActiveFullAppByName(CloudContext.Azure(cloudContext), app.appName)
        .transaction
    } yield {
      deletedApp shouldBe None
      verify(mockAzureRelayService).deleteRelayHybridConnection(
        RelayNamespace(ArgumentMatchers.eq(landingZoneResources.relayNamespace.value)),
        RelayHybridConnectionName(ArgumentMatchers.eq(app.appName.value)),
        ArgumentMatchers.eq(cloudContext)
      )(any())
    }

    deletion.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not create a WSM database when the LZ does not support it" in {
    val cluster = makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext))
    val nodepool = makeNodepool(1, cluster.id)
    val app = makeApp(1, nodepool.id).copy(appType = AppType.Wds)
    val res = newAksInterp(config.copy(wdsAppConfig = config.wdsAppConfig.copy(databaseEnabled = true)))
      .maybeCreateWsmIdentityAndSharedDatabases(app,
                                                workspaceId,
                                                landingZoneResources.copy(postgresServer = None),
                                                KubernetesNamespace(NamespaceName("ns1"))
      )
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    res shouldBe (None, None)
  }

  it should "not create a WSM database when the app does not support it" in {
    val cluster = makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext))
    val nodepool = makeNodepool(1, cluster.id)
    val app = makeApp(1, nodepool.id).copy(appType = AppType.Wds)
    val res = newAksInterp(config.copy(wdsAppConfig = config.wdsAppConfig.copy(databaseEnabled = false)))
      .maybeCreateWsmIdentityAndSharedDatabases(app,
                                                workspaceId,
                                                landingZoneResources,
                                                KubernetesNamespace(NamespaceName("ns1"))
      )
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    res shouldBe (None, None)
  }

  it should "not create a Cromwell databases when the LZ does not support it" in {
    val cluster = makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext))
    val nodepool = makeNodepool(1, cluster.id)
    val app = makeApp(1, nodepool.id).copy(appType = AppType.Cromwell)
    val res = newAksInterp(config.copy(coaAppConfig = config.coaAppConfig.copy(databaseEnabled = true)))
      .maybeCreateCromwellDatabases(app,
                                    workspaceId,
                                    landingZoneResources.copy(postgresServer = None),
                                    KubernetesNamespace(NamespaceName("ns1"))
      )
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    res shouldBe None
  }

  it should "not create a Cromwell databases when the app does not support it" in {
    val cluster = makeKubeCluster(1).copy(cloudContext = CloudContext.Azure(cloudContext))
    val nodepool = makeNodepool(1, cluster.id)
    val app = makeApp(1, nodepool.id).copy(appType = AppType.Cromwell)
    val res = newAksInterp(config.copy(coaAppConfig = config.coaAppConfig.copy(databaseEnabled = false)))
      .maybeCreateCromwellDatabases(app, workspaceId, landingZoneResources, KubernetesNamespace(NamespaceName("ns1")))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    res shouldBe None
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

  private def setUpMockAzureBatchService: AzureBatchService[IO] = {
    val container = mock[AzureBatchService[IO]]
    val batchAccountKeys = mock[BatchAccountKeys]
    val batchAccount = mock[BatchAccount]
    when {
      container.getBatchAccount(any[String].asInstanceOf[BatchAccountName],
                                any[String].asInstanceOf[AzureCloudContext]
      )(any)
    } thenReturn IO.pure(batchAccount)
    when {
      batchAccount.getKeys()
    } thenReturn batchAccountKeys
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
      mockAzureRelayService.deleteRelayHybridConnection(any[String].asInstanceOf[RelayNamespace],
                                                        any[String].asInstanceOf[RelayHybridConnectionName],
                                                        any[String].asInstanceOf[AzureCloudContext]
      )(any())
    } thenReturn IO.unit
    mockAzureRelayService
  }

  private def setUpMockAzureApplicationInsightsService: AzureApplicationInsightsService[IO] = {
    val container = mock[AzureApplicationInsightsService[IO]]
    val applicationInsightsComponent = mock[ApplicationInsightsComponent]
    when {
      container.getApplicationInsights(any[String].asInstanceOf[ApplicationInsightsName], any)(any)
    } thenReturn IO.pure(applicationInsightsComponent)
    container
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

  private def setUpMockCbasUiDAO: CbasUiDAO[IO] = {
    val cbasUi = mock[CbasUiDAO[IO]]
    when {
      cbasUi.getStatus(any, any)(any)
    } thenReturn IO.pure(true)
    cbasUi
  }

  private def setUpMockWdsDAO: WdsDAO[IO] = {
    val wds = mock[WdsDAO[IO]]
    when {
      wds.getStatus(any, any)(any)
    } thenReturn IO.pure(true)
    wds
  }

  private def setUpMockHailBatchDAO: HailBatchDAO[IO] = {
    val batch = mock[HailBatchDAO[IO]]
    when {
      batch.getStatus(any, any)(any)
    } thenReturn IO.pure(true)
    when {
      batch.getDriverStatus(any, any)(any)
    } thenReturn IO.pure(true)
    batch
  }

  private def setUpMockWsmApiClientProvider: WsmApiClientProvider = {
    val wsm = mock[WsmApiClientProvider]
    val api = mock[ControlledAzureResourceApi]
    val dbUUIDsByName = mutable.Map.empty[String, UUID]
    when {
      api.createAzureManagedIdentity(any, any)
    } thenAnswer { invocation =>
      new CreatedControlledAzureManagedIdentity().resourceId(
        invocation.getArgument[CreateControlledAzureManagedIdentityRequestBody](0).getCommon.getResourceId
      )
    }
    when {
      api.createAzureDatabase(any, any)
    } thenAnswer { invocation =>
      val uuid = invocation.getArgument[CreateControlledAzureDatabaseRequestBody](0).getCommon.getResourceId
      val name = invocation.getArgument[CreateControlledAzureDatabaseRequestBody](0).getAzureDatabase.getName
      dbUUIDsByName += (name -> uuid)
      new CreatedControlledAzureDatabaseResult().resourceId(uuid)
    }
    when {
      api.getCreateAzureDatabaseResult(any, any)
    } thenAnswer { // thenAnswer is used so that the result of the call is different each time
      invocation =>
        new CreatedControlledAzureDatabaseResult()
          .azureDatabase(
            new AzureDatabaseResource()
              .metadata(new ResourceMetadata().resourceId(dbUUIDsByName(invocation.getArgument[String](1))))
          )
          .jobReport(
            new JobReport().status(JobReport.StatusEnum.SUCCEEDED)
          )
    }
    when {
      wsm.getControlledAzureResourceApi(any)
    } thenReturn api
    wsm
  }

}
