package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{azureRegion, landingZoneResources, petUserInfo}
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException

class CromwellRunnerAppInstallSpec extends BaseAppInstallSpec {

  val cromwellRunnerAppInstall = new CromwellRunnerAppInstall[IO](
    ConfigReader.appConfig.azure.cromwellRunnerAppConfig,
    ConfigReader.appConfig.drs,
    mockSamDAO,
    mockCromwellDAO,
    mockAzureBatchService,
    mockAzureApplicationInsightsService
  )

  it should "build cromwell-runner override values" in {
    val params = buildHelmOverrideValuesParams(List("cromwell1", "tes1"))

    val overrides = cromwellRunnerAppInstall.buildHelmOverrideValues(params)

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
      "workloadIdentity.serviceAccountName=identity-name," +
      "cromwell.enabled=true," +
      "fullnameOverride=cra-rel-1," +
      "instrumentationEnabled=false," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      "postgres.podLocalDatabaseEnabled=false," +
      s"postgres.host=${lzResources.postgresServer.map(_.name).get}.postgres.database.azure.com," +
      "postgres.pgbouncer.enabled=true," +
      "postgres.user=identity-name," +
      s"postgres.dbnames.cromwell=cromwell1," +
      s"postgres.dbnames.tes=tes1"
  }

  it should "fail if there is no storage container" in {
    val params = buildHelmOverrideValuesParams(List("cromwell1", "tes1")).copy(storageContainer = None)
    val overrides = cromwellRunnerAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail if there is no postgres server" in {
    val params = buildHelmOverrideValuesParams(List("cromwell1", "tes1"))
      .copy(landingZoneResources = landingZoneResources.copy(postgresServer = None))
    val overrides = cromwellRunnerAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail if there are no databases" in {
    val params = buildHelmOverrideValuesParams(List.empty)
    val overrides = cromwellRunnerAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }
}
