package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.azure.resourcemanager.containerservice.models.KubernetesClusterAgentPool
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models._
import org.broadinstitute.dsde.workbench.azure._
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
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoMetric._
import org.broadinstitute.dsde.workbench.leonardo.util.KubernetesAlgebra
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
import org.broadinstitute.dsde.workbench.model.{IP, TraceId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class LeoMetricsMonitorSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent with MockitoSugar {
  val azureContext = AzureCloudContext(
    TenantId("tenant"),
    SubscriptionId("sub"),
    ManagedResourceGroupName("mrg")
  )
  val azureContext2 = AzureCloudContext(
    TenantId("tenant2"),
    SubscriptionId("sub2"),
    ManagedResourceGroupName("mrg2")
  )

  // Mocks
  val appDAO = setUpMockAppDAO
  val wdsDAO = setUpMockWdsDAO
  val cbasDAO = setUpMockCbasDAO
  val cromwellDAO = setUpMockCromwellDAO
  val samDAO = setUpMockSamDAO
  val jupyterDAO = setUpMockJupyterDAO
  val rstudioDAO = setUpMockRStudioDAO
  val welderDAO = setUpMockWelderDAO
  val hailBatchDAO = setUpMockHailBatchDAO
  val relayListenerDAO = setUpMockRelayListenerDAO
  val kube = setUpMockKubeDAO
  val containerService = setUpMockAzureContainerService

  // Test object
  implicit val clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType] =
    ToolDAO.clusterToolToToolDao(jupyterDAO, welderDAO, rstudioDAO)
  implicit val ec: ExecutionContext = cats.effect.unsafe.IORuntime.global.compute
  val config = LeoMetricsMonitorConfig(true, 1 minute, true)
  val leoMetricsMonitor = new LeoMetricsMonitor[IO](
    config,
    appDAO,
    wdsDAO,
    cbasDAO,
    cromwellDAO,
    hailBatchDAO,
    relayListenerDAO,
    samDAO,
    kube,
    containerService
  )

  "LeoMetricsMonitor" should "count apps by status" in {
    val test = leoMetricsMonitor.countAppsByDbStatus(allApps)
    // 10 apps
    test.size shouldBe 10
    // Cromwell on Azure
    test.get(
      AppStatusMetric(CloudProvider.Azure,
                      AppType.Cromwell,
                      AppStatus.Running,
                      RuntimeUI.Terra,
                      Some(azureContext),
                      cromwellOnAzureChart,
                      true
      )
    ) shouldBe Some(1)
    // Cromwell on GCP on Terra
    test.get(
      AppStatusMetric(CloudProvider.Gcp,
                      AppType.Cromwell,
                      AppStatus.Running,
                      RuntimeUI.Terra,
                      None,
                      cromwellChart,
                      true
      )
    ) shouldBe Some(1)
    // Galaxy on GCP
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Galaxy, AppStatus.Running, RuntimeUI.Terra, None, galaxyChart, true)
    ) shouldBe Some(1)
    // Custom app on GCP
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Custom, AppStatus.Running, RuntimeUI.Terra, None, customChart, true)
    ) shouldBe Some(1)
    // Cromwell on GCP on AoU
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Cromwell, AppStatus.Running, RuntimeUI.AoU, None, cromwellChart, true)
    ) shouldBe Some(1)
    // RStudio on GCP on AoU
    test.get(
      AppStatusMetric(CloudProvider.Gcp, AppType.Allowed, AppStatus.Running, RuntimeUI.AoU, None, rstudioChart, true)
    ) shouldBe Some(1)
    // Hail Batch on Azure
    test.get(
      AppStatusMetric(CloudProvider.Azure,
                      AppType.HailBatch,
                      AppStatus.Running,
                      RuntimeUI.Terra,
                      Some(azureContext),
                      hailBatchChart,
                      true
      )
    ) shouldBe Some(1)
    // WDS on Azure
    test.get(
      AppStatusMetric(CloudProvider.Azure,
                      AppType.Wds,
                      AppStatus.Running,
                      RuntimeUI.Terra,
                      Some(azureContext2),
                      wdsChart,
                      true
      )
    ) shouldBe Some(1)
    // Workflows App on Azure
    test.get(
      AppStatusMetric(CloudProvider.Azure,
                      AppType.WorkflowsApp,
                      AppStatus.Running,
                      RuntimeUI.Terra,
                      Some(azureContext2),
                      workflowsAppChart,
                      true
      )
    ) shouldBe Some(1)
    // Cromwell Runner App on Azure
    test.get(
      AppStatusMetric(CloudProvider.Azure,
                      AppType.CromwellRunnerApp,
                      AppStatus.Running,
                      RuntimeUI.Terra,
                      Some(azureContext2),
                      cromwellRunnerAppChart,
                      true
      )
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
      leoMetricsMonitor
        .countAppsByHealth(List(cromwellAppAzure, galaxyAppGcp, workflowsApp, cromwellRunnerApp))
        .unsafeRunSync()(IORuntime.global)
    // An up and a down metric for 7 services: 2 cbases, cromwell, cromwell-reader, cromwell-runner, galaxy
    test.size shouldBe 12
    List("cromwell", "cbas").foreach { s =>
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.Cromwell,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        Some(azureContext),
                        s != "cbas",
                        cromwellOnAzureChart,
                        true
        )
      ) shouldBe Some(1)
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.Cromwell,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        Some(azureContext),
                        s == "cbas",
                        cromwellOnAzureChart,
                        true
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
                      galaxyChart,
                      true
      )
    ) shouldBe Some(1)
    test.get(
      AppHealthMetric(CloudProvider.Gcp,
                      AppType.Galaxy,
                      ServiceName("galaxy"),
                      RuntimeUI.Terra,
                      None,
                      false,
                      galaxyChart,
                      true
      )
    ) shouldBe Some(0)
    List("cromwell-reader", "cbas").foreach { s =>
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.WorkflowsApp,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        Some(azureContext2),
                        s != "cbas",
                        workflowsAppChart,
                        true
        )
      ) shouldBe Some(1)
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.WorkflowsApp,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        Some(azureContext2),
                        s == "cbas",
                        workflowsAppChart,
                        true
        )
      ) shouldBe Some(0)
    }
    test.get(
      AppHealthMetric(CloudProvider.Azure,
                      AppType.CromwellRunnerApp,
                      ServiceName("cromwell-runner"),
                      RuntimeUI.Terra,
                      Some(azureContext2),
                      true,
                      cromwellRunnerAppChart,
                      true
      )
    ) shouldBe Some(1)
    test.get(
      AppHealthMetric(CloudProvider.Azure,
                      AppType.CromwellRunnerApp,
                      ServiceName("cromwell-runner"),
                      RuntimeUI.Terra,
                      Some(azureContext2),
                      false,
                      cromwellRunnerAppChart,
                      true
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
      cromwellDAO,
      hailBatchDAO,
      relayListenerDAO,
      samDAO,
      kube,
      containerService
    )
    val test =
      azureDisabledMetricsMonitor
        .countAppsByHealth(List(cromwellAppAzure, galaxyAppGcp, workflowsApp, cromwellRunnerApp))
        .unsafeRunSync()(IORuntime.global)
    // An up and a down metric for 7 services: 2 cbases, cromwell, cromwell-reader, cromwell-runner, galaxy
    test.size shouldBe 12
    List("cromwell", "cbas").foreach { s =>
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.Cromwell,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        None,
                        s != "cbas",
                        cromwellOnAzureChart,
                        true
        )
      ) shouldBe Some(1)
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.Cromwell,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        None,
                        s == "cbas",
                        cromwellOnAzureChart,
                        true
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
                      galaxyChart,
                      true
      )
    ) shouldBe Some(1)
    test.get(
      AppHealthMetric(CloudProvider.Gcp,
                      AppType.Galaxy,
                      ServiceName("galaxy"),
                      RuntimeUI.Terra,
                      None,
                      false,
                      galaxyChart,
                      true
      )
    ) shouldBe Some(0)
    List("cromwell-reader", "cbas").foreach { s =>
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.WorkflowsApp,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        None,
                        s != "cbas",
                        workflowsAppChart,
                        true
        )
      ) shouldBe Some(1)
      test.get(
        AppHealthMetric(CloudProvider.Azure,
                        AppType.WorkflowsApp,
                        ServiceName(s),
                        RuntimeUI.Terra,
                        None,
                        s == "cbas",
                        workflowsAppChart,
                        true
        )
      ) shouldBe Some(0)
    }
    test.get(
      AppHealthMetric(CloudProvider.Azure,
                      AppType.CromwellRunnerApp,
                      ServiceName("cromwell-runner"),
                      RuntimeUI.Terra,
                      None,
                      true,
                      cromwellRunnerAppChart,
                      true
      )
    ) shouldBe Some(1)
    test.get(
      AppHealthMetric(CloudProvider.Azure,
                      AppType.CromwellRunnerApp,
                      ServiceName("cromwell-runner"),
                      RuntimeUI.Terra,
                      None,
                      false,
                      cromwellRunnerAppChart,
                      true
      )
    ) shouldBe Some(0)
  }

  it should "record nodepool size" in {
    val test = leoMetricsMonitor.getNodepoolSize(List(wdsAppAzure, hailBatchAppAzure)).unsafeRunSync()(IORuntime.global)
    test.size shouldBe 4
    test.get(NodepoolSizeMetric(azureContext, "pool1")) shouldBe Some(10)
    test.get(NodepoolSizeMetric(azureContext, "pool2")) shouldBe Some(1)
    test.get(NodepoolSizeMetric(azureContext2, "pool1")) shouldBe Some(10)
    test.get(NodepoolSizeMetric(azureContext2, "pool2")) shouldBe Some(1)
  }

  it should "record app k8s metrics" in {
    val chart = Chart.fromString("wds-0.0.1").get
    val test = leoMetricsMonitor.getAppK8sResources(List(wdsAppAzure)).unsafeRunSync()(IORuntime.global)
    test.size shouldBe 4
    test.get(
      AppResourcesMetric(CloudProvider.Azure,
                         AppType.Wds,
                         ServiceName("wds"),
                         RuntimeUI.Terra,
                         Some(azureContext2),
                         "request",
                         "cpu",
                         chart
      )
    ) shouldBe Some(1)
    test.get(
      AppResourcesMetric(CloudProvider.Azure,
                         AppType.Wds,
                         ServiceName("wds"),
                         RuntimeUI.Terra,
                         Some(azureContext2),
                         "request",
                         "memory",
                         chart
      )
    ) shouldBe Some(1073741824d)
    test.get(
      AppResourcesMetric(CloudProvider.Azure,
                         AppType.Wds,
                         ServiceName("wds"),
                         RuntimeUI.Terra,
                         Some(azureContext2),
                         "limit",
                         "cpu",
                         chart
      )
    ) shouldBe Some(2)
    test.get(
      AppResourcesMetric(CloudProvider.Azure,
                         AppType.Wds,
                         ServiceName("wds"),
                         RuntimeUI.Terra,
                         Some(azureContext2),
                         "limit",
                         "memory",
                         chart
      )
    ) shouldBe Some(2147483648d)
  }

  // Data generators

  private def genApp(isAzure: Boolean,
                     appType: AppType,
                     chart: Chart,
                     isAou: Boolean,
                     isCromwell: Boolean,
                     isWorkflowsApp: Boolean,
                     isCromwellRunnerApp: Boolean = false
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
      if (isCromwell) List("cbas", "cromwell")
      else if (isCromwellRunnerApp) List("cromwell-runner")
      else if (isWorkflowsApp) List("cbas", "cromwell-reader")
      else List(appType.toString.toLowerCase)
    val appWithServices = app.copy(appResources = app.appResources.copy(services = services.map(genService)))
    clusterWithAsyncFields.copy(nodepools = List(nodepool.copy(apps = List(appWithServices))))
  }

  def genService(name: String): KubernetesService =
    KubernetesService(ServiceId(-1), ServiceConfig(ServiceName(name), KubernetesServiceKindName("ClusterIP")))

  private def cromwellAppAzure: KubernetesCluster =
    genApp(true, AppType.Cromwell, cromwellOnAzureChart, false, true, false)
      .copy(cloudContext = CloudContext.Azure(azureContext))
  private def cromwellAppGcp: KubernetesCluster =
    genApp(false, AppType.Cromwell, cromwellChart, false, true, false)
  private def galaxyAppGcp: KubernetesCluster =
    genApp(false, AppType.Galaxy, galaxyChart, false, false, false)
  private def customAppGcp: KubernetesCluster =
    genApp(false, AppType.Custom, customChart, false, false, false)
  private def cromwellAppGcpAou: KubernetesCluster =
    genApp(false, AppType.Cromwell, cromwellChart, true, true, false)
  private def rstudioAppGcpAou: KubernetesCluster =
    genApp(false, AppType.Allowed, rstudioChart, true, false, false)
  private def hailBatchAppAzure: KubernetesCluster =
    genApp(true, AppType.HailBatch, hailBatchChart, false, false, false)
      .copy(cloudContext = CloudContext.Azure(azureContext))
  private def wdsAppAzure: KubernetesCluster =
    genApp(true, AppType.Wds, wdsChart, false, false, false)
      .copy(cloudContext = CloudContext.Azure(azureContext2))
  private def workflowsApp: KubernetesCluster =
    genApp(true, AppType.WorkflowsApp, workflowsAppChart, false, false, true)
      .copy(cloudContext = CloudContext.Azure(azureContext2))
  private def cromwellRunnerApp: KubernetesCluster =
    genApp(true, AppType.CromwellRunnerApp, cromwellRunnerAppChart, false, false, false, true)
      .copy(cloudContext = CloudContext.Azure(azureContext2))

  private def cromwellChart = Chart.fromString("cromwell-0.0.1").get
  private def cromwellOnAzureChart = Chart.fromString("cromwell-on-azure-0.0.1").get
  private def galaxyChart = Chart.fromString("galaxy-0.0.1").get
  private def customChart = Chart.fromString("custom-0.0.1").get
  private def rstudioChart = Chart.fromString("rstudio-0.0.1").get
  private def hailBatchChart = Chart.fromString("hail-batch-0.1.0").get
  private def wdsChart = Chart.fromString("wds-0.0.1").get
  private def workflowsAppChart = Chart.fromString("workflows-app-0.0.1").get
  private def cromwellRunnerAppChart = Chart.fromString("cromwell-runner-app-0.0.1").get

  private def allApps =
    List(
      cromwellAppAzure,
      cromwellAppGcp,
      galaxyAppGcp,
      customAppGcp,
      cromwellAppGcpAou,
      rstudioAppGcpAou,
      hailBatchAppAzure,
      wdsAppAzure,
      workflowsApp,
      cromwellRunnerApp
    )

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

  private def setUpMockRelayListenerDAO: ListenerDAO[IO] = {
    val listener = mock[ListenerDAO[IO]]
    when {
      listener.getStatus(any)(any)
    } thenReturn IO.pure(true)
    listener
  }

  private def setUpMockKubeDAO: KubernetesAlgebra[IO] = {
    val client = mock[CoreV1Api]
    val podList = mock[V1PodList]
    val pod = mock[V1Pod]
    val spec = mock[V1PodSpec]
    val container = mock[V1Container]
    val kube = mock[KubernetesAlgebra[IO]]

    val mockRequest = mock[client.APIlistNamespacedPodRequest]
    when {
      container.getResources
    } thenReturn new V1ResourceRequirements()
      .requests(
        Map("cpu" -> Quantity.fromString("1"), "memory" -> Quantity.fromString("1073741824")).asJava
      )
      .limits(
        Map("cpu" -> Quantity.fromString("2"), "memory" -> Quantity.fromString("2147483648")).asJava
      )
    when {
      spec.getContainers
    } thenReturn List(container).asJava
    when {
      pod.getSpec
    } thenReturn spec
    when {
      pod.getMetadata
    } thenReturn new V1ObjectMeta().labels(Map("leoServiceName" -> "wds").asJava)
    when {
      podList.getItems
    } thenReturn List(pod).asJava
    when {
      mockRequest.execute()
    } thenReturn podList
    when {
      mockRequest.labelSelector(any[String])
    } thenReturn mockRequest
    when {
      client.listNamespacedPod(any[String])
    } thenReturn mockRequest
    when {
      kube.createAzureClient(any, any[String].asInstanceOf[AKSClusterName])(any)
    } thenReturn IO.pure(client)
    kube
  }

  private def setUpMockAzureContainerService: AzureContainerService[IO] = {
    val container = mock[AzureContainerService[IO]]
    val cluster = mock[com.azure.resourcemanager.containerservice.models.KubernetesCluster]
    val pool1 = mock[KubernetesClusterAgentPool]
    when {
      pool1.count()
    } thenReturn 10
    val pool2 = mock[KubernetesClusterAgentPool]
    when {
      pool2.count()
    } thenReturn 1
    when {
      cluster.agentPools()
    } thenReturn Map("pool1" -> pool1, "pool2" -> pool2).asJava
    when {
      container.getCluster(any[String].asInstanceOf[AKSClusterName], any)(any)
    } thenReturn IO.pure(cluster)
    container
  }
}
