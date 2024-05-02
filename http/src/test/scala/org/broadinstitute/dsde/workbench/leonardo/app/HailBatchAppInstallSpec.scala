package org.broadinstitute.dsde.workbench.leonardo.app

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao.HailBatchDAO
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when

class HailBatchAppInstallSpec extends BaseAppInstallSpec {

  val mockHailBatchDAO = setUpMockHailBatchDAO
  val hailBatchAppInstall = new HailBatchAppInstall[IO](
    ConfigReader.appConfig.azure.hailBatchAppConfig,
    mockHailBatchDAO
  )

  it should "build hail batch override values" in {
    val params = buildHelmOverrideValuesParams(List.empty)

    val overrides = hailBatchAppInstall.buildHelmOverrideValues(params)

    overrides.unsafeRunSync()(cats.effect.unsafe.IORuntime.global).asString shouldBe
      "persistence.storageAccount=storage," +
      "persistence.blobContainer=sc-container," +
      s"persistence.workspaceManager.url=${ConfigReader.appConfig.azure.wsm.uri.renderString}," +
      s"persistence.workspaceManager.workspaceId=${workspaceId.value}," +
      s"persistence.workspaceManager.containerResourceId=${storageContainer.resourceId.value.toString}," +
      s"persistence.workspaceManager.storageContainerUrl=https://${lzResources.storageAccountName.value}.blob.core.windows.net/${storageContainer.name.value}," +
      "persistence.leoAppName=app1," +
      "workloadIdentity.serviceAccountName=ksa-1," +
      s"relay.domain=relay.com," +
      "relay.subpath=/app"
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
}
