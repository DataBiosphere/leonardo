package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import io.kubernetes.client.openapi.models._
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.google2.{NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoMetric._
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
  RuntimeContainerServiceType,
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
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{IP, TraceId}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class LeoMetricsMonitorSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent with MockitoSugar {

  // Mocks
  val appDAO = setUpMockAppDAO
  val jupyterDAO = setUpMockJupyterDAO
  val rstudioDAO = setUpMockRStudioDAO
  val welderDAO = setUpMockWelderDAO

  // Test object
  implicit val clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType] =
    ToolDAO.clusterToolToToolDao(jupyterDAO, welderDAO, rstudioDAO)
  implicit val ec: ExecutionContext = cats.effect.unsafe.IORuntime.global.compute
  val config = LeoMetricsMonitorConfig(true, 1 minute)
  val leoMetricsMonitor = new LeoMetricsMonitor[IO](
    config,
    appDAO
  )

  "LeoMetricsMonitor" should "count apps by status" in {
    val test = leoMetricsMonitor.countAppsByDbStatus(allApps)
    // 10 apps
    test.size shouldBe 5
    // Cromwell on GCP on Terra
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Cromwell, AppStatus.Running, RuntimeUI.Terra, cromwellChart, true)
    ) shouldBe Some(1)
    // Galaxy on GCP
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Galaxy, AppStatus.Running, RuntimeUI.Terra, galaxyChart, true)
    ) shouldBe Some(1)
    // Custom app on GCP
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Custom, AppStatus.Running, RuntimeUI.Terra, customChart, true)
    ) shouldBe Some(1)
    // Cromwell on GCP on AoU
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Cromwell, AppStatus.Running, RuntimeUI.AoU, cromwellChart, true)
    ) shouldBe Some(1)
    // RStudio on GCP on AoU
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Allowed, AppStatus.Running, RuntimeUI.AoU, rstudioChart, true)
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
                          RuntimeUI.Terra
      )
    ) shouldBe Some(1)
    // RStudio on GCP on Terra
    test.get(
      RuntimeStatusMetric(CloudProvider.Gcp,
                          rstudioImage.imageType,
                          rstudioImage.imageUrl,
                          RuntimeStatus.Running,
                          RuntimeUI.Terra
      )
    ) shouldBe Some(1)
    // Jupyter on GCP on AoU
    test.get(
      RuntimeStatusMetric(CloudProvider.Gcp,
                          jupyterImage.imageType,
                          jupyterImage.imageUrl,
                          RuntimeStatus.Running,
                          RuntimeUI.AoU
      )
    ) shouldBe Some(1)
  }

  it should "health check apps" in {
    val test =
      leoMetricsMonitor
        .countAppsByHealth(List(cromwellAppGcp, galaxyAppGcp, cromwellAppGcpAou))
        .unsafeRunSync()(IORuntime.global)
    // An up and a down metric for 3 services
    test.size shouldBe 3
    test.get(
      AppHealthMetric(CloudProvider.Gcp,
                      AppType.Cromwell,
                      ServiceName("cromwell"),
                      RuntimeUI.Terra,
                      true,
                      cromwellChart,
                      true
      )
    ) shouldBe Some(1)
    test.get(
      AppHealthMetric(CloudProvider.Gcp,
                      AppType.Galaxy,
                      ServiceName("galaxy"),
                      RuntimeUI.Terra,
                      true,
                      galaxyChart,
                      true
      )
    ) shouldBe Some(1)
    test.get(
      AppHealthMetric(CloudProvider.Gcp,
                      AppType.Cromwell,
                      ServiceName("cromwell"),
                      RuntimeUI.AoU,
                      true,
                      cromwellChart,
                      true
      )
    ) shouldBe Some(1)

  }

  it should "health check runtimes" in {
    val test = leoMetricsMonitor.countRuntimesByHealth(List(jupyterGcp, rstudioGcp)).unsafeRunSync()(IORuntime.global)
    // An up and a down for jupyter, rstudio, welder * 2
    test.size shouldBe 8
    // Jupyter Azure
    List(jupyterImage, welderImage).foreach { i =>
      test.get(
        RuntimeHealthMetric(CloudProvider.Gcp, i.imageType, i.imageUrl, RuntimeUI.Terra, true)
      ) shouldBe Some(1)
      test.get(
        RuntimeHealthMetric(CloudProvider.Gcp, i.imageType, i.imageUrl, RuntimeUI.Terra, false)
      ) shouldBe Some(0)
    }
    // RStudio GCP
    List(rstudioImage, welderImage).foreach { i =>
      test.get(
        RuntimeHealthMetric(CloudProvider.Gcp, i.imageType, i.imageUrl, RuntimeUI.Terra, i != rstudioImage)
      ) shouldBe Some(1)
      test.get(
        RuntimeHealthMetric(CloudProvider.Gcp, i.imageType, i.imageUrl, RuntimeUI.Terra, i == rstudioImage)
      ) shouldBe Some(0)
    }
  }

  // Data generators

  private def genApp(appType: AppType, chart: Chart, isAou: Boolean, isCromwell: Boolean): KubernetesCluster = {
    val cluster = makeKubeCluster(1)
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
    val services = List(appType.toString.toLowerCase)
    val appWithServices = app.copy(appResources = app.appResources.copy(services = services.map(genService)))
    clusterWithAsyncFields.copy(nodepools = List(nodepool.copy(apps = List(appWithServices))))
  }

  def genService(name: String): KubernetesService =
    KubernetesService(ServiceId(-1), ServiceConfig(ServiceName(name), KubernetesServiceKindName("ClusterIP")))

  private def cromwellAppGcp: KubernetesCluster =
    genApp(AppType.Cromwell, cromwellChart, false, true)
  private def galaxyAppGcp: KubernetesCluster =
    genApp(AppType.Galaxy, galaxyChart, false, false)
  private def customAppGcp: KubernetesCluster =
    genApp(AppType.Custom, customChart, false, false)
  private def cromwellAppGcpAou: KubernetesCluster =
    genApp(AppType.Cromwell, cromwellChart, true, true)
  private def rstudioAppGcpAou: KubernetesCluster =
    genApp(AppType.Allowed, rstudioChart, true, false)

  private def cromwellChart = Chart.fromString("cromwell-0.0.1").get
  private def galaxyChart = Chart.fromString("galaxy-0.0.1").get
  private def customChart = Chart.fromString("custom-0.0.1").get
  private def rstudioChart = Chart.fromString("rstudio-0.0.1").get

  private def allApps =
    List(
      cromwellAppGcp,
      galaxyAppGcp,
      customAppGcp,
      cromwellAppGcpAou,
      rstudioAppGcpAou
    )

  private def genRuntime(isJupyter: Boolean, isAou: Boolean, isGcp: Boolean): RuntimeMetrics =
    RuntimeMetrics(
      CloudContext.Gcp(GoogleProject("project")),
      RuntimeName("runtime"),
      RuntimeStatus.Running,
      Some(WorkspaceId(UUID.randomUUID())),
      Set(if (isJupyter) jupyterImage else rstudioImage, welderImage),
      if (isAou) Map(Config.uiConfig.allOfUsLabel -> "true") else Map(Config.uiConfig.terraLabel -> "true")
    )

  private def jupyterGcp: RuntimeMetrics = genRuntime(true, false, true)
  private def rstudioGcp: RuntimeMetrics = genRuntime(false, false, true)
  private def jupyterGcpAou: RuntimeMetrics = genRuntime(true, true, true)

  private val jupyterImage = RuntimeImage(RuntimeImageType.Jupyter, "jupyter:0.0.1", None, Instant.now)
  private val rstudioImage = RuntimeImage(RuntimeImageType.RStudio, "rstudio:0.0.1", None, Instant.now)
  private val welderImage = RuntimeImage(RuntimeImageType.Welder, "welder:0.0.1", None, Instant.now)

  private def allRuntimes = List(jupyterGcp, rstudioGcp, jupyterGcpAou)

  // Mocks

  private def setUpMockAppDAO: AppDAO[IO] = {
    val app = mock[AppDAO[IO]]
    when {
      app.isProxyAvailable(any, any[String].asInstanceOf[AppName], any, TraceId(anyString()))
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
