package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{azureRegion, landingZoneResources, petUserInfo}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException

class WorkflowsAppInstallSpec extends BaseAppInstallSpec {

  val workflowsAppInstall = new WorkflowsAppInstall[IO](
    ConfigReader.appConfig.azure.workflowsAppConfig,
    ConfigReader.appConfig.drs,
    mockSamDAO,
    mockCromwellDAO,
    mockCbasDAO,
    mockAzureBatchService,
    mockAzureApplicationInsightsService
  )

  it should "build workflows app override values" in {
    val params = buildHelmOverrideValuesParams(List("cromwellmetadata1", "cbas1"))

    val overrides = workflowsAppInstall.buildHelmOverrideValues(params)

    overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global).asString shouldBe
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
      s"postgres.dbnames.cromwellMetadata=cromwellmetadata1," +
      s"postgres.dbnames.cbas=cbas1"
  }

  it should "fail if there is no storage container" in {
    val params = buildHelmOverrideValuesParams(List("cromwellmetadata1", "cbas1")).copy(storageContainer = None)
    val overrides = workflowsAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail if there is no postgres server" in {
    val params = buildHelmOverrideValuesParams(List("cromwellmetadata1", "cbas1"))
      .copy(landingZoneResources = landingZoneResources.copy(postgresServer = None))
    val overrides = workflowsAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail if there are no databases" in {
    val params = buildHelmOverrideValuesParams(List.empty)
    val overrides = workflowsAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }
}
