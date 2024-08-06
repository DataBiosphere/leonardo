package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{azureRegion, landingZoneResources, petUserInfo}
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.{BillingProfileId, WsmControlledDatabaseResource}
import org.broadinstitute.dsde.workbench.leonardo.config.Config.samConfig
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException

import java.util.UUID

class CromwellRunnerAppInstallSpec extends BaseAppInstallSpec {

  val cromwellRunnerAppInstall = new CromwellRunnerAppInstall[IO](
    ConfigReader.appConfig.azure.cromwellRunnerAppConfig,
    ConfigReader.appConfig.drs,
    samConfig,
    mockSamDAO,
    mockCromwellDAO,
    mockAzureBatchService,
    mockAzureApplicationInsightsService,
    mockBpmClientProvider,
    mockSamAuthProvider
  )

  val cromwellAzureDbName = "cromwell_wgsdoi"
  val tesAzureDbName = "tes_oiwjnz"
  val cromwellMetadataAzureDbName = "cromwellmetadata_tyuiwk"
  val cromwellRunnerAzureDatabases: List[WsmControlledDatabaseResource] = List(
    WsmControlledDatabaseResource("cromwell", cromwellAzureDbName),
    WsmControlledDatabaseResource("tes", tesAzureDbName),
    WsmControlledDatabaseResource("cromwellmetadata", cromwellMetadataAzureDbName)
  )

  val expectedOverrides = "config.resourceGroup=mrg," +
    "config.batchAccountKey=batchKey," +
    "config.batchAccountName=batch," +
    "config.batchNodesSubnetId=subnet1," +
    s"config.drsUrl=${ConfigReader.appConfig.drs.url}," +
    "config.landingZoneId=5c12f64b-f4ac-4be1-ae4a-4cace5de807d," +
    "config.subscriptionId=sub," +
    s"config.region=${azureRegion}," +
    "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
    "relay.path=https://relay.com/app," +
    "persistence.storageAccount=storage," +
    "persistence.blobContainer=sc-container," +
    "persistence.leoAppInstanceName=app1," +
    s"persistence.workspaceManager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
    s"persistence.workspaceManager.workspaceId=${workspaceId.value}," +
    s"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}," +
    "workloadIdentity.serviceAccountName=ksa-1," +
    "identity.name=mi-1," +
    "cromwell.enabled=true," +
    "fullnameOverride=cra-rel-1," +
    "instrumentationEnabled=false," +
    s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
    "postgres.podLocalDatabaseEnabled=false," +
    s"postgres.host=${lzResources.postgresServer.map(_.name).get}.postgres.database.azure.com," +
    "postgres.pgbouncer.enabled=true," +
    "postgres.user=ksa-1," +
    s"postgres.dbnames.cromwell=$cromwellAzureDbName," +
    s"postgres.dbnames.tes=$tesAzureDbName," +
    s"postgres.dbnames.cromwellMetadata=$cromwellMetadataAzureDbName," +
    s"ecm.baseUri=https://externalcreds.dsde-dev.broadinstitute.org," +
    s"sam.baseUri=https://sam.test.org:443," +
    s"sam.acrPullActionIdentityResourceId=spend-profile," +
    "bard.bardUrl=https://terra-bard-dev.appspot.com," +
    "bard.enabled=false"

  it should "build cromwell-runner override values" in {
    val params = buildHelmOverrideValuesParams(cromwellRunnerAzureDatabases)

    val overrides = cromwellRunnerAppInstall.buildHelmOverrideValues(params)

    overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global).asString shouldBe expectedOverrides
  }

  it should "build cromwell-runner override values for a limited bp" in {
    val params = buildHelmOverrideValuesParams(cromwellRunnerAzureDatabases)
    val bpid = UUID.randomUUID().toString
    val updatedParams = params.copy(billingProfileId = BillingProfileId(bpid))

    val overrides = cromwellRunnerAppInstall.buildHelmOverrideValues(updatedParams)

    overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global).asString shouldBe expectedOverrides.replace(
      "spend-profile",
      bpid
    ) + s",config.concurrentJobLimit=100"
  }

  it should "fail if there is no storage container" in {
    val params = buildHelmOverrideValuesParams(cromwellRunnerAzureDatabases).copy(storageContainer = None)
    val overrides = cromwellRunnerAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail if there is no postgres server" in {
    val params = buildHelmOverrideValuesParams(cromwellRunnerAzureDatabases)
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

  it should "find the first instance of each database type" in {
    cromwellRunnerAppInstall.toCromwellRunnerAppDatabaseNames(
      List(
        WsmControlledDatabaseResource("cromwellmetadata", cromwellMetadataAzureDbName),
        WsmControlledDatabaseResource("cromwell", cromwellAzureDbName),
        WsmControlledDatabaseResource("tes", tesAzureDbName)
      ) // put cromwellmetadata first to ensure it doesn't get confused with cromwell
    ) should be(Some(CromwellRunnerAppDatabaseNames(cromwellAzureDbName, tesAzureDbName, cromwellMetadataAzureDbName)))
  }
}
