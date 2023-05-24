package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ManagedResourceGroupName, SubscriptionId, TenantId}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{
  makeApp,
  makeAzureCluster,
  makeKubeCluster,
  makeNodepool
}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoMetric.{
  AppHealthMetric,
  AppStatusMetric,
  RuntimeHealthMetric,
  RuntimeStatusMetric
}
import org.broadinstitute.dsde.workbench.leonardo.{
  AppName,
  AppStatus,
  AppType,
  Chart,
  CloudContext,
  CloudProvider,
  IpRange,
  KubernetesCluster,
  KubernetesClusterAsyncFields,
  KubernetesService,
  KubernetesServiceKindName,
  LeonardoTestSuite,
  NetworkFields,
  RuntimeImage,
  RuntimeImageType,
  RuntimeMetrics,
  RuntimeName,
  RuntimeStatus,
  RuntimeUI,
  ServiceConfig,
  ServiceId,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

class LeoMetricsMonitorSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent with MockitoSugar {
  val azureContext = AzureCloudContext(
    TenantId("tenant"),
    SubscriptionId("sub"),
    ManagedResourceGroupName("mrg")
  )

  // Mocks
  val appDAO = setUpMockAppDAO
  val wdsDAO = setUpMockWdsDAO
  val cbasDAO = setUpMockCbasDAO
  val cbasUiDAO = setUpMockCbasUiDAO
  val cromwellDAO = setUpMockCromwellDAO
  val samDAO = setUpMockSamDAO
  val jupyterDAO = setUpMockJupyterDAO
  val rstudioDAO = setUpMockRStudioDAO
  val welderDAO = setUpMockWelderDAO

  // Test object
  implicit val clusterToolToToolDao =
    ToolDAO.clusterToolToToolDao(jupyterDAO, welderDAO, rstudioDAO)
  implicit val ec = cats.effect.unsafe.IORuntime.global.compute
  val config = LeoMetricsMonitorConfig(true, 1 minute, true)
  val leoMetricsMonitor = new LeoMetricsMonitor[IO](
    config,
    appDAO,
    wdsDAO,
    cbasDAO,
    cbasUiDAO,
    cromwellDAO,
    samDAO
  )

  "LeoMetricsMonitor" should "count apps by status" in {
    val test = leoMetricsMonitor.countAppsByDbStatus(allApps)
    // 5 apps
    test.size shouldBe 6
    // Cromwell on Azure
    test.get(
      AppStatusMetric(CloudProvider.Azure,
                      AppType.Cromwell,
                      AppStatus.Running,
                      RuntimeUI.Terra,
                      Some(azureContext),
                      cromwellOnAzureChart
      )
    ) shouldBe Some(1)
    // Cromwell on GCP on Terra
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Cromwell, AppStatus.Running, RuntimeUI.Terra, None, cromwellChart)
    ) shouldBe Some(1)
    // Galaxy on GCP
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Galaxy, AppStatus.Running, RuntimeUI.Terra, None, galaxyChart)
    ) shouldBe Some(1)
    // Custom app on GCP
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Custom, AppStatus.Running, RuntimeUI.Terra, None, customChart)
    ) shouldBe Some(1)
    // Cromwell on GCP on AoU
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Cromwell, AppStatus.Running, RuntimeUI.AoU, None, cromwellChart)
    ) shouldBe Some(1)
    // RStudio on GCP on AoU
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.RStudio, AppStatus.Running, RuntimeUI.AoU, None, rstudioChart)
    ) shouldBe Some(1)
  }

  it should "count runtimes by status" in {
    val test = leoMetricsMonitor.countRuntimesByDbStatus(allRuntimes)
    // 4 runtimes
    test.size shouldBe 4
    // Jupyter on GCP on Terra
    test.get(
      RuntimeStatusMetric(CloudProvider.Gcp,
                          jupyterImage.imageType,
                          jupyterImage.imageUrl,
                          RuntimeStatus.Running,
                          RuntimeUI.Terra,
                          None
      )
    ) shouldBe Some(1)
    // RStudio on GCP on Terra
    test.get(
      RuntimeStatusMetric(CloudProvider.Gcp,
                          rstudioImage.imageType,
                          rstudioImage.imageUrl,
                          RuntimeStatus.Running,
                          RuntimeUI.Terra,
                          None
      )
    ) shouldBe Some(1)
    // Jupyter on Azure
    test.get(
      RuntimeStatusMetric(CloudProvider.Azure,
                          azureImage.imageType,
                          azureImage.imageUrl,
                          RuntimeStatus.Running,
                          RuntimeUI.Terra,
                          Some(azureContext)
      )
    ) shouldBe Some(1)
    // Jupyter on GCP on AoU
    test.get(
      RuntimeStatusMetric(CloudProvider.Gcp,
                          jupyterImage.imageType,
                          jupyterImage.imageUrl,
                          RuntimeStatus.Running,
                          RuntimeUI.AoU,
                          None
      )
    ) shouldBe Some(1)
  }

  it should "health check apps" in {
    val test =
      leoMetricsMonitor.countAppsByHealth(List(cromwellAppAzure, galaxyAppGcp)).unsafeRunSync()(IORuntime.global)
    // An up and a down metric for 5 services: wds, cbas, cbas-ui, cromwell galaxy
    test.size shouldBe 8
    List("cromwell", "cbas", "cbas-ui").foreach { s =>
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.Cromwell,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        Some(azureContext),
                        s != "cbas",
                        cromwellOnAzureChart
        )
      ) shouldBe Some(1)
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.Cromwell,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        Some(azureContext),
                        s == "cbas",
                        cromwellOnAzureChart
        )
      ) shouldBe Some(0)
    }
    test.get(
      AppHealthMetric(CloudProvider.Gcp,
                      AppType.Galaxy,
                      ServiceName("galaxy"),
                      RuntimeUI.Terra,
                      None,
                      true,
                      galaxyChart
      )
    ) shouldBe Some(1)
    test.get(
      AppHealthMetric(CloudProvider.Gcp,
                      AppType.Galaxy,
                      ServiceName("galaxy"),
                      RuntimeUI.Terra,
                      None,
                      false,
                      galaxyChart
      )
    ) shouldBe Some(0)
  }

  it should "health check runtimes" in {
    val test = leoMetricsMonitor.countRuntimesByHealth(List(jupyterAzure, rstudioGcp)).unsafeRunSync()(IORuntime.global)
    // An up and a down for jupyter, rstudio, welder * 2
    test.size shouldBe 8
    // Jupyter Azure
    List(azureImage, welderImage).foreach { i =>
      test.get(
        RuntimeHealthMetric(CloudProvider.Azure, i.imageType, i.imageUrl, RuntimeUI.Terra, Some(azureContext), true)
      ) shouldBe Some(1)
      test.get(
        RuntimeHealthMetric(CloudProvider.Azure, i.imageType, i.imageUrl, RuntimeUI.Terra, Some(azureContext), false)
      ) shouldBe Some(0)
    }
    // RStudio GCP
    List(rstudioImage, welderImage).foreach { i =>
      test.get(
        RuntimeHealthMetric(CloudProvider.Gcp, i.imageType, i.imageUrl, RuntimeUI.Terra, None, i != rstudioImage)
      ) shouldBe Some(1)
      test.get(
        RuntimeHealthMetric(CloudProvider.Gcp, i.imageType, i.imageUrl, RuntimeUI.Terra, None, i == rstudioImage)
      ) shouldBe Some(0)
    }
  }

  it should "not include AzureCloudContext if disabled" in {
    val config = LeoMetricsMonitorConfig(true, 1 minute, false)
    val azureDisabledMetricsMonitor = new LeoMetricsMonitor[IO](
      config,
      appDAO,
      wdsDAO,
      cbasDAO,
      cbasUiDAO,
      cromwellDAO,
      samDAO
    )
    val test =
      azureDisabledMetricsMonitor
        .countAppsByHealth(List(cromwellAppAzure, galaxyAppGcp))
        .unsafeRunSync()(IORuntime.global)
    // An up and a down metric for 5 services: wds, cbas, cbas-ui, cromwell galaxy
    test.size shouldBe 8
    List("cromwell", "cbas", "cbas-ui").foreach { s =>
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.Cromwell,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        None,
                        s != "cbas",
                        cromwellOnAzureChart
        )
      ) shouldBe Some(1)
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.Cromwell,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        None,
                        s == "cbas",
                        cromwellOnAzureChart
        )
      ) shouldBe Some(0)
    }
    test.get(
      AppHealthMetric(CloudProvider.Gcp,
                      AppType.Galaxy,
                      ServiceName("galaxy"),
                      RuntimeUI.Terra,
                      None,
                      true,
                      galaxyChart
      )
    ) shouldBe Some(1)
    test.get(
      AppHealthMetric(CloudProvider.Gcp,
                      AppType.Galaxy,
                      ServiceName("galaxy"),
                      RuntimeUI.Terra,
                      None,
                      false,
                      galaxyChart
      )
    ) shouldBe Some(0)
  }

  // Data generators

  private def genApp(isAzure: Boolean,
                     appType: AppType,
                     chart: Chart,
                     isAou: Boolean,
                     isCromwell: Boolean,
                     isGalaxy: Boolean,
                     isRstudio: Boolean
  ): KubernetesCluster = {
    val cluster = if (isAzure) makeAzureCluster(1) else makeKubeCluster(1)
    val clusterWithAsyncFields = cluster.copy(asyncFields =
      Some(
        KubernetesClusterAsyncFields(IP("1.2.3.4"),
                                     IP("2.4.5.6"),
                                     NetworkFields(NetworkName("network"), SubnetworkName("subnet"), IpRange("ipRange"))
        )
      )
    )
    val nodepool = makeNodepool(1, clusterWithAsyncFields.id)
    val app = makeApp(1, nodepool.id).copy(
      appType = appType,
      chart = chart,
      status = AppStatus.Running,
      labels = if (isAou) Map(Config.uiConfig.allOfUsLabel -> "true") else Map(Config.uiConfig.terraLabel -> "true")
    )
    val services =
      if (isCromwell) List("cbas", "cbas-ui", "cromwell")
      else if (isGalaxy) List("galaxy")
      else if (isRstudio) List("rstudio")
      else List("custom")
    val appWithServices = app.copy(appResources = app.appResources.copy(services = services.map(genService)))
    clusterWithAsyncFields.copy(nodepools = List(nodepool.copy(apps = List(appWithServices))))
  }

  def genService(name: String): KubernetesService =
    KubernetesService(ServiceId(-1), ServiceConfig(ServiceName(name), KubernetesServiceKindName("ClusterIP")))

  private def cromwellAppAzure: KubernetesCluster =
    genApp(true, AppType.Cromwell, cromwellOnAzureChart, false, true, false, false)
      .copy(cloudContext = CloudContext.Azure(azureContext))
  private def cromwellAppGcp: KubernetesCluster =
    genApp(false, AppType.Cromwell, cromwellChart, false, true, false, false)
  private def galaxyAppGcp: KubernetesCluster =
    genApp(false, AppType.Galaxy, galaxyChart, false, false, true, false)
  private def customAppGcp: KubernetesCluster =
    genApp(false, AppType.Custom, customChart, false, false, false, false)
  private def cromwellAppGcpAou: KubernetesCluster =
    genApp(false, AppType.Cromwell, cromwellChart, true, true, false, false)

  private def rstudioAppGcpAou: KubernetesCluster =
    genApp(false, AppType.RStudio, rstudioChart, true, false, false, true)

  private def cromwellChart = Chart.fromString("cromwell-0.0.1").get
  private def cromwellOnAzureChart = Chart.fromString("cromwell-on-azure-0.0.1").get
  private def galaxyChart = Chart.fromString("galaxy-0.0.1").get
  private def customChart = Chart.fromString("custom-0.0.1").get
  private def rstudioChart = Chart.fromString("rstudio-0.0.1").get

  private def allApps =
    List(cromwellAppAzure, cromwellAppGcp, galaxyAppGcp, customAppGcp, cromwellAppGcpAou, rstudioAppGcpAou)

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
      Set(if (isJupyter) if (isGcp) jupyterImage else azureImage else rstudioImage, welderImage),
      if (isAou) Map(Config.uiConfig.allOfUsLabel -> "true") else Map(Config.uiConfig.terraLabel -> "true")
    )

  private def jupyterGcp: RuntimeMetrics = genRuntime(true, false, true)
  private def rstudioGcp: RuntimeMetrics = genRuntime(false, false, true)
  private def jupyterAzure: RuntimeMetrics = genRuntime(true, false, false)
  private def jupyterGcpAou: RuntimeMetrics = genRuntime(true, true, true)

  private val jupyterImage = RuntimeImage(RuntimeImageType.Jupyter, "jupyter:0.0.1", None, Instant.now)
  private val rstudioImage = RuntimeImage(RuntimeImageType.RStudio, "rstudio:0.0.1", None, Instant.now)
  private val welderImage = RuntimeImage(RuntimeImageType.Welder, "welder:0.0.1", None, Instant.now)
  private val azureImage = RuntimeImage(RuntimeImageType.Azure, "azure:0.0.1", None, Instant.now)

  private def allRuntimes = List(jupyterGcp, rstudioGcp, jupyterAzure, jupyterGcpAou)

  // Mocks

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

  // CBAS is down
  private def setUpMockCbasDAO: CbasDAO[IO] = {
    val cbas = mock[CbasDAO[IO]]
    when {
      cbas.getStatus(any, any)(any)
    } thenReturn IO.pure(false)
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
      wds.getStatus(any, any, any)(any)
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

  // RStudio is down
  private def setUpMockRStudioDAO: RStudioDAO[IO] = {
    val rstudio = mock[RStudioDAO[IO]]
    when {
      rstudio.isProxyAvailable(any, any[String].asInstanceOf[RuntimeName])
    } thenReturn IO.pure(false)
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
