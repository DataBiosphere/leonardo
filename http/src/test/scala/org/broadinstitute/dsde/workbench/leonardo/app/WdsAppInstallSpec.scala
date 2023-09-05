package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{azureRegion, landingZoneResources, petUserInfo}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.WorkspaceId
import org.broadinstitute.dsde.workbench.leonardo.dao.WdsDAO
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when

import java.util.UUID

class WdsAppInstallSpec extends BaseAppInstallSpec {
  val mockWdsDAO = setUpMockWdsDAO

  val wdsAppInstall = new WdsAppInstall[IO](
    ConfigReader.appConfig.azure.wdsAppConfig,
    ConfigReader.appConfig.azure.tdr,
    mockSamDAO,
    mockWdsDAO,
    mockAzureApplicationInsightsService
  )

  it should "build wds override values" in {
    val params = buildHelmOverrideValuesParams(List("wds1"))

    val overrides = wdsAppInstall.buildHelmOverrideValues(params)

    overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global).asString shouldBe
      "config.resourceGroup=mrg," +
      "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
      "config.subscriptionId=sub," +
      s"config.region=${azureRegion}," +
      "general.leoAppInstanceName=app1," +
      s"general.workspaceManager.workspaceId=${workspaceId.value}," +
      "identity.enabled=false," +
      "workloadIdentity.enabled=true," +
      "workloadIdentity.serviceAccountName=ksa-1," +
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
      "postgres.dbname=wds1," +
      "postgres.user=ksa-1"
  }

  it should "build wds override values with a sourceWorkspaceId" in {
    val sourceWorkspaceId = WorkspaceId(UUID.randomUUID())
    val params = buildHelmOverrideValuesParams(List("wds1")).copy(
      app = app.copy(sourceWorkspaceId = Some(sourceWorkspaceId))
    )

    val overrides = wdsAppInstall.buildHelmOverrideValues(params)

    overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global).asString shouldBe
      "config.resourceGroup=mrg," +
      "config.applicationInsightsConnectionString=applicationInsightsConnectionString," +
      "config.subscriptionId=sub," +
      s"config.region=${azureRegion}," +
      "general.leoAppInstanceName=app1," +
      s"general.workspaceManager.workspaceId=${workspaceId.value}," +
      "identity.enabled=false," +
      "workloadIdentity.enabled=true," +
      "workloadIdentity.serviceAccountName=ksa-1," +
      "sam.url=https://sam.dsde-dev.broadinstitute.org/," +
      "leonardo.url=https://leo-dummy-url.org," +
      s"workspacemanager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      "fullnameOverride=wds-rel-1," +
      "instrumentationEnabled=false," +
      "import.dataRepoUrl=https://jade.datarepo-dev.broadinstitute.org," +
      s"provenance.userAccessToken=${petUserInfo.accessToken.token}," +
      s"provenance.sourceWorkspaceId=${sourceWorkspaceId.value}," +
      "postgres.podLocalDatabaseEnabled=false," +
      s"postgres.host=${lzResources.postgresServer.map(_.name).get}.postgres.database.azure.com," +
      "postgres.pgbouncer.enabled=true," +
      "postgres.dbname=wds1," +
      "postgres.user=ksa-1"
  }

  it should "fail if there is no postgres server" in {
    val params = buildHelmOverrideValuesParams(List("wds1"))
      .copy(landingZoneResources = landingZoneResources.copy(postgresServer = None))
    val overrides = wdsAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail if there are no databases" in {
    val params = buildHelmOverrideValuesParams(List.empty)
    val overrides = wdsAppInstall.buildHelmOverrideValues(params)
    assertThrows[AppCreationException] {
      overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  private def setUpMockWdsDAO: WdsDAO[IO] = {
    val wds = mock[WdsDAO[IO]]
    when {
      wds.getStatus(any, any)(any)
    } thenReturn IO.pure(true)
    wds
  }
}
