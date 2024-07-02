package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{
  azureRegion,
  billingProfileId,
  landingZoneResources,
  petUserInfo
}
import org.broadinstitute.dsde.workbench.leonardo.{ManagedIdentityName, PostgresServer, WsmControlledDatabaseResource}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.http4s.Uri

class CromwellAppInstallSpec extends BaseAppInstallSpec {

  val cromwellAppInstall = new CromwellAppInstall[IO](
    ConfigReader.appConfig.azure.coaAppConfig,
    ConfigReader.appConfig.drs,
    mockSamDAO,
    mockCromwellDAO,
    mockCbasDAO,
    mockAzureBatchService,
    mockAzureApplicationInsightsService
  )

  val cromwellAzureDbName = "cromwell_tghfgi"
  val cbasAzureDbName = "cbas_edfgvb"
  val tesAzureDbName = "tes_pasgjf"
  val cromwellOnAzureDatabases: List[WsmControlledDatabaseResource] = List(
    WsmControlledDatabaseResource("cromwell", cromwellAzureDbName),
    WsmControlledDatabaseResource("cbas", cbasAzureDbName),
    WsmControlledDatabaseResource("tes", tesAzureDbName)
  )

  it should "build coa override values" in {
    val params = buildHelmOverrideValuesParams(cromwellOnAzureDatabases)
    val overrides = cromwellAppInstall.buildHelmOverrideValues(params)

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
      "persistence.leoAppInstanceName=app1," +
      s"persistence.workspaceManager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      s"persistence.workspaceManager.workspaceId=${workspaceId.value}," +
      s"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}," +
      "identity.enabled=false," +
      "workloadIdentity.enabled=true," +
      "workloadIdentity.serviceAccountName=ksa-1," +
      "identity.name=mi-1," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      "cbas.enabled=true," +
      "cromwell.enabled=true," +
      "dockstore.baseUrl=https://staging.dockstore.org/," +
      "fullnameOverride=coa-rel-1," +
      "instrumentationEnabled=false," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      "postgres.podLocalDatabaseEnabled=false," +
      s"postgres.host=${lzResources.postgresServer.map(_.name).get}.postgres.database.azure.com," +
      "postgres.pgbouncer.enabled=true," +
      "postgres.user=ksa-1," +
      s"postgres.dbnames.cromwell=$cromwellAzureDbName," +
      s"postgres.dbnames.cbas=$cbasAzureDbName," +
      s"postgres.dbnames.tes=$tesAzureDbName"
  }

  it should "build coa override values when pgbouncer is not enabled" in {
    val params = BuildHelmOverrideValuesParams(
      app,
      workspaceId,
      workspaceName,
      cloudContext,
      billingProfileId,
      lzResources.copy(postgresServer = Some(PostgresServer("postgres", false))),
      Some(storageContainer),
      Uri.unsafeFromString("https://relay.com/app"),
      ServiceAccountName("ksa-1"),
      ManagedIdentityName("mi-1"),
      cromwellOnAzureDatabases,
      aksInterpConfig
    )

    val overrides = cromwellAppInstall.buildHelmOverrideValues(params)

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
      "persistence.leoAppInstanceName=app1," +
      s"persistence.workspaceManager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      s"persistence.workspaceManager.workspaceId=${workspaceId.value}," +
      s"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}," +
      "identity.enabled=false," +
      "workloadIdentity.enabled=true," +
      "workloadIdentity.serviceAccountName=ksa-1," +
      "identity.name=mi-1," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      "cbas.enabled=true," +
      "cromwell.enabled=true," +
      "dockstore.baseUrl=https://staging.dockstore.org/," +
      "fullnameOverride=coa-rel-1," +
      "instrumentationEnabled=false," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      "postgres.podLocalDatabaseEnabled=false," +
      s"postgres.host=${lzResources.postgresServer.map(_.name).get}.postgres.database.azure.com," +
      "postgres.pgbouncer.enabled=false," +
      "postgres.user=ksa-1," +
      s"postgres.dbnames.cromwell=$cromwellAzureDbName," +
      s"postgres.dbnames.cbas=$cbasAzureDbName," +
      s"postgres.dbnames.tes=$tesAzureDbName"
  }

  it should "fail if there is no storage container" in {
    val params = buildHelmOverrideValuesParams(cromwellOnAzureDatabases).copy(storageContainer = None)
    val overrides = cromwellAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail if there is no postgres server" in {
    val params = buildHelmOverrideValuesParams(cromwellOnAzureDatabases)
      .copy(landingZoneResources = landingZoneResources.copy(postgresServer = None))
    val overrides = cromwellAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail if there are no databases" in {
    val params = buildHelmOverrideValuesParams(List.empty)
    val overrides = cromwellAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }
}
