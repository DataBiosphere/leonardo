package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.IO
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ManagedResourceGroupName, SubscriptionId, TenantId}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{
  makeApp,
  makeAzureCluster,
  makeKubeCluster,
  makeNodepool
}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoMetric.{AppStatusMetric, RuntimeStatusMetric}
import org.broadinstitute.dsde.workbench.leonardo.{
  AppName,
  AppStatus,
  AppType,
  CloudContext,
  CloudProvider,
  KubernetesCluster,
  LeonardoTestSuite,
  RuntimeContainerServiceType,
  RuntimeImageType,
  RuntimeMetrics,
  RuntimeName,
  RuntimeStatus,
  RuntimeUI,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.duration._

class LeoMetricsMonitorSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent with MockitoSugar {

  val azureContext = AzureCloudContext(
    TenantId("tenant"),
    SubscriptionId("sub"),
    ManagedResourceGroupName("mrg")
  )
  val googleProject = GoogleProject("goog")

  val config = LeoMetricsMonitorConfig(1 minute, true)
  val appDAO = setUpMockAppDAO
  val wdsDAO = setUpMockWdsDAO
  val cbasDAO = setUpMockCbasDAO
  val cbasUiDAO = setUpMockCbasUiDAO
  val cromwellDAO = setUpMockCromwellDAO
  val samDAO = setUpMockSamDAO
  val jupyterDAO = setUpMockJupyterDAO
  val rstudioDAO = setUpMockRStudioDAO
  val welderDAO = setUpMockWelderDAO
  implicit val clusterToolToToolDao =
    ToolDAO.clusterToolToToolDao(jupyterDAO, welderDAO, rstudioDAO)
  implicit val ec = cats.effect.unsafe.IORuntime.global.compute

  val leoMetricsMonitor = new LeoMetricsMonitor[IO](
    config,
    appDAO,
    wdsDAO,
    cbasDAO,
    cbasUiDAO,
    cromwellDAO,
    samDAO
  )

  private def genApp(isAzure: Boolean, appType: AppType, isAou: Boolean): KubernetesCluster = {
    val cluster = if (isAzure) makeAzureCluster(1) else makeKubeCluster(1)
    val nodepool = makeNodepool(1, cluster.id)
    val app = makeApp(1, nodepool.id).copy(
      appType = appType,
      status = AppStatus.Running,
      labels = if (isAou) Map(Config.uiConfig.allOfUsLabel -> "true") else Map(Config.uiConfig.terraLabel -> "true")
    )
    cluster.copy(nodepools = List(nodepool.copy(apps = List(app))))
  }

  private def cromwellAppAzure: KubernetesCluster =
    genApp(true, AppType.Cromwell, false).copy(cloudContext = CloudContext.Azure(azureContext))
  private def cromwellAppGcp: KubernetesCluster = genApp(false, AppType.Cromwell, false)
  private def galaxyAppGcp: KubernetesCluster = genApp(false, AppType.Galaxy, false)
  private def customAppGcp: KubernetesCluster = genApp(false, AppType.Custom, false)
  private def cromwellAppGcpAou: KubernetesCluster = genApp(false, AppType.Cromwell, true)

  private def allApps = List(cromwellAppAzure, cromwellAppGcp, galaxyAppGcp, customAppGcp, cromwellAppGcpAou)

  private def genRuntime(isJupyter: Boolean, isAou: Boolean, isGcp: Boolean): RuntimeMetrics =
    RuntimeMetrics(
      if (isGcp) CloudContext.Gcp(GoogleProject("project"))
      else
        CloudContext.Azure(
          AzureCloudContext(
            TenantId("tenant"),
            SubscriptionId("sub"),
            ManagedResourceGroupName("mrg")
          )
        ),
      RuntimeName("runtime"),
      RuntimeStatus.Running,
      Some(WorkspaceId(UUID.randomUUID())),
      if (isJupyter) Set(RuntimeContainerServiceType.JupyterService, RuntimeContainerServiceType.WelderService)
      else Set(RuntimeContainerServiceType.RStudioService, RuntimeContainerServiceType.WelderService),
      if (isAou) Map(Config.uiConfig.allOfUsLabel -> "true") else Map(Config.uiConfig.terraLabel -> "true")
    )

  private def jupyterGcp: RuntimeMetrics = genRuntime(true, false, true)
  private def rstudioGcp: RuntimeMetrics = genRuntime(false, false, true)
  private def jupyterAzure: RuntimeMetrics = genRuntime(true, false, false)
  private def jupyterGcpAou: RuntimeMetrics = genRuntime(true, true, true)

  val allRuntimes = List(jupyterGcp, rstudioGcp, jupyterAzure, jupyterGcpAou)

  "LeoMetricsMonitor" should "count apps by status" in {
    val test = leoMetricsMonitor.countAppsByDbStatus(allApps)
    test shouldBe Map(
      AppStatusMetric(CloudProvider.Azure,
                      AppType.Cromwell,
                      AppStatus.Running,
                      RuntimeUI.Terra,
                      Some(azureContext)
      ) -> 1,
      AppStatusMetric(CloudProvider.Gcp, AppType.Cromwell, AppStatus.Running, RuntimeUI.Terra, None) -> 1,
      AppStatusMetric(CloudProvider.Gcp, AppType.Galaxy, AppStatus.Running, RuntimeUI.Terra, None) -> 1,
      AppStatusMetric(CloudProvider.Gcp, AppType.Custom, AppStatus.Running, RuntimeUI.Terra, None) -> 1,
      AppStatusMetric(CloudProvider.Gcp, AppType.Cromwell, AppStatus.Running, RuntimeUI.AoU, None) -> 1
    )
  }

  it should "count runtimes by status" in {
    val test = leoMetricsMonitor.countRuntimesByDbStatus(allRuntimes)
    test shouldBe Map(
      RuntimeStatusMetric(CloudProvider.Gcp,
                          RuntimeImageType.Jupyter,
                          RuntimeStatus.Running,
                          RuntimeUI.Terra,
                          None
      ) -> 1,
      RuntimeStatusMetric(CloudProvider.Gcp,
                          RuntimeImageType.RStudio,
                          RuntimeStatus.Running,
                          RuntimeUI.Terra,
                          None
      ) -> 1,
      RuntimeStatusMetric(CloudProvider.Azure,
                          RuntimeImageType.Jupyter,
                          RuntimeStatus.Running,
                          RuntimeUI.Terra,
                          Some(azureContext)
      ) -> 1,
      RuntimeStatusMetric(CloudProvider.Gcp, RuntimeImageType.Jupyter, RuntimeStatus.Running, RuntimeUI.AoU, None) -> 1
    )
  }

  // TODO test health check

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

  private def setUpMockAppDAO: AppDAO[IO] = {
    val app = mock[AppDAO[IO]]
    when {
      app.isProxyAvailable(any, any[String].asInstanceOf[AppName], any)
    } thenReturn IO.pure(true)
    app
  }

  private def setUpMockJupyterDAO: JupyterDAO[IO] = {
    val jupyter = mock[JupyterDAO[IO]]
    when {
      jupyter.isProxyAvailable(any, any[String].asInstanceOf[RuntimeName])
    } thenReturn IO.pure(true)
    jupyter
  }

  private def setUpMockRStudioDAO: RStudioDAO[IO] = {
    val rstudio = mock[RStudioDAO[IO]]
    when {
      rstudio.isProxyAvailable(any, any[String].asInstanceOf[RuntimeName])
    } thenReturn IO.pure(true)
    rstudio
  }

  private def setUpMockWelderDAO: WelderDAO[IO] = {
    val welder = mock[WelderDAO[IO]]
    when {
      welder.isProxyAvailable(any, any[String].asInstanceOf[RuntimeName])
    } thenReturn IO.pure(true)
    welder
  }
}
