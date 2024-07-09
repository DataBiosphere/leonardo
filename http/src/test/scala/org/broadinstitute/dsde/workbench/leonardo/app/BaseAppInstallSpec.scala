package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.IO
import com.azure.resourcemanager.applicationinsights.models.ApplicationInsightsComponent
import com.azure.resourcemanager.batch.models.{BatchAccount, BatchAccountKeys}
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{azureRegion, billingProfileId, wsmResourceId}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.makeApp
import org.broadinstitute.dsde.workbench.leonardo.config.Config.appMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.config.SamConfig
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsde.workbench.leonardo.util.AKSInterpreterConfig
import org.broadinstitute.dsde.workbench.leonardo.{
  AKSCluster,
  LandingZoneResources,
  LeonardoTestSuite,
  ManagedIdentityName,
  NodepoolLeoId,
  PostgresServer,
  StorageAccountName,
  WorkspaceId,
  WsmControlledDatabaseResource,
  WsmControlledResourceId
}
import org.broadinstitute.dsp.Release
import org.http4s.Uri
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatestplus.mockito.MockitoSugar

import java.net.URL
import java.util.UUID

class BaseAppInstallSpec extends AnyFlatSpecLike with LeonardoTestSuite with MockitoSugar {

  val mockSamDAO = setUpMockSamDAO
  val mockCromwellDAO = setUpMockCromwellDAO
  val mockCbasDAO = setUpMockCbasDAO
  val mockAzureApplicationInsightsService = setUpMockAzureApplicationInsightsService
  val mockAzureBatchService = setUpMockAzureBatchService

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
    Some(PostgresServer("postgres", true))
  )

  val storageContainer = StorageContainerResponse(
    ContainerName("sc-container"),
    WsmControlledResourceId(UUID.randomUUID)
  )

  val app = makeApp(1, NodepoolLeoId(-1)).copy(
    release = Release("rel-1")
  )

  val workspaceId = WorkspaceId(UUID.randomUUID)
  val workspaceName = "workspaceName"
  val workspaceCreatedDate = java.time.OffsetDateTime.parse("1970-01-01T12:15:30-07:00")

  val aksInterpConfig = AKSInterpreterConfig(
    SamConfig("https://sam.dsde-dev.broadinstitute.org/"),
    appMonitorConfig,
    ConfigReader.appConfig.azure.wsm,
    new URL("https://leo-dummy-url.org"),
    ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.listenerImage,
    ConfigReader.appConfig.azure.listenerChartConfig
  )

  def buildHelmOverrideValuesParams(databases: List[WsmControlledDatabaseResource]): BuildHelmOverrideValuesParams =
    BuildHelmOverrideValuesParams(
      app,
      workspaceId,
      workspaceName,
      cloudContext,
      billingProfileId,
      lzResources,
      Some(storageContainer),
      Uri.unsafeFromString("https://relay.com/app"),
      ServiceAccountName("ksa-1"),
      ManagedIdentityName("mi-1"),
      databases,
      aksInterpConfig,
      None
    )

  private def setUpMockSamDAO: SamDAO[IO] = {
    val sam = mock[SamDAO[IO]]
    when {
      sam.getCachedArbitraryPetAccessToken(any)(any)
    } thenReturn IO.pure(Some("accessToken"))
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

  private def setUpMockAzureApplicationInsightsService: AzureApplicationInsightsService[IO] = {
    val service = mock[AzureApplicationInsightsService[IO]]
    val applicationInsightsComponent = mock[ApplicationInsightsComponent]
    when {
      service.getApplicationInsights(any[String].asInstanceOf[ApplicationInsightsName], any)(any)
    } thenReturn IO.pure(applicationInsightsComponent)
    when {
      applicationInsightsComponent.connectionString()
    } thenReturn "applicationInsightsConnectionString"
    service
  }

  private def setUpMockAzureBatchService: AzureBatchService[IO] = {
    val service = mock[AzureBatchService[IO]]
    val batchAccountKeys = mock[BatchAccountKeys]
    val batchAccount = mock[BatchAccount]
    when {
      service.getBatchAccount(any[String].asInstanceOf[BatchAccountName], any[String].asInstanceOf[AzureCloudContext])(
        any
      )
    } thenReturn IO.pure(batchAccount)
    when {
      batchAccount.getKeys()
    } thenReturn batchAccountKeys
    when {
      batchAccountKeys.primary()
    } thenReturn "batchKey"
    service
  }
}
