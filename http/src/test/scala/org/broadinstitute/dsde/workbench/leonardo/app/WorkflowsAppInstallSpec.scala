package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{landingZoneResources, petUserInfo}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.WsmControlledDatabaseResource
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

  val cbasAzureDbName = "cbas_wgsdoi"
  val cromwellMetadataAzureDbName = "cromwellmetadata_tyuiwk"
  val workflowsAzureDatabases: List[WsmControlledDatabaseResource] = List(
    WsmControlledDatabaseResource("cbas", cbasAzureDbName),
    WsmControlledDatabaseResource("cromwellmetadata", cromwellMetadataAzureDbName)
  )

  it should "build workflows app override values" in {
    val params = buildHelmOverrideValuesParams(workflowsAzureDatabases)

    val overrides = workflowsAppInstall.buildHelmOverrideValues(params)

    overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global).asString shouldBe
      s"config.drsUrl=${ConfigReader.appConfig.drs.url}," +
      "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
      "relay.path=https://relay.com/app," +
      "persistence.storageAccount=storage," +
      "persistence.blobContainer=sc-container," +
      "persistence.leoAppInstanceName=app1," +
      s"persistence.workspaceManager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      s"persistence.workspaceManager.workspaceId=${workspaceId.value}," +
      "workloadIdentity.serviceAccountName=ksa-1," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      "dockstore.baseUrl=https://staging.dockstore.org/," +
      "fullnameOverride=wfa-rel-1," +
      "instrumentationEnabled=false," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      "postgres.podLocalDatabaseEnabled=false," +
      s"postgres.host=${lzResources.postgresServer.map(_.name).get}.postgres.database.azure.com," +
      "postgres.pgbouncer.enabled=true," +
      "postgres.user=ksa-1," +
      s"postgres.dbnames.cromwellMetadata=$cromwellMetadataAzureDbName," +
      s"postgres.dbnames.cbas=$cbasAzureDbName," +
      s"ecm.baseUri=https://externalcreds.dsde-dev.broadinstitute.org," +
      s"bard.baseUri=https://terra-bard-dev.appspot.com," +
      s"bard.enabled=false"
  }

  it should "fail if there is no storage container" in {
    val params = buildHelmOverrideValuesParams(workflowsAzureDatabases).copy(storageContainer = None)
    val overrides = workflowsAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail if there is no postgres server" in {
    val params = buildHelmOverrideValuesParams(workflowsAzureDatabases)
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
