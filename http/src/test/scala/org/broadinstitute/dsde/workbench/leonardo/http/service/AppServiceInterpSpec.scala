package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import bio.terra.workspace.api.WorkspaceApi
import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import com.google.cloud.compute.v1.MachineType
import fs2.Pipe
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.google2.mock.{FakeGoogleComputeService, FakeGoogleResourceService}
import org.broadinstitute.dsde.workbench.google2.{DiskName, GoogleResourceService, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.AppRestore.{GalaxyRestore, Other}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.{appContext, defaultMockitoAnswer}
import org.broadinstitute.dsde.workbench.leonardo.auth.AllowlistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.Config.leoKubernetesConfig
import org.broadinstitute.dsde.workbench.leonardo.config.{Config, CustomAppConfig, CustomApplicationAllowListConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.SamResource.AppSamResource
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAppMessage,
  CreateAppV2Message,
  DeleteAppMessage,
  DeleteAppV2Message
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  ClusterNodepoolAction,
  LeoPubsubMessage,
  LeoPubsubMessageType
}
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2.messaging.CloudPublisher
import org.broadinstitute.dsp.{ChartName, ChartVersion}
import org.http4s.Uri
import org.http4s.headers.Authorization
import org.mockito.{ArgumentMatchers, Mock}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatestplus.mockito.MockitoSugar
import org.typelevel.log4cats.StructuredLogger
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global

trait AppServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent with MockitoSugar {
  val appServiceConfig = Config.appServiceConfig
  val gkeCustomAppConfig = Config.gkeCustomAppConfig

  val wsmDao = new MockWsmDAO
  val workspaceApi = mock[WorkspaceApi]
  when {
    workspaceApi.getWorkspace(ArgumentMatchers.eq(workspaceId.value), any())
  } thenReturn {
    wsmWorkspaceDesc
  }
  when {
    workspaceApi.getWorkspace(ArgumentMatchers.eq(workspaceId2.value), any())
  } thenAnswer (_ => throw new Exception("workspace not found"))

  val wsmClientProvider = mock[HttpWsmClientProvider[IO]]
  when {
    wsmClientProvider.getWorkspaceApi(any)(any)
  } thenReturn {
    IO.pure(workspaceApi)
  }

  val gcpWsmDao = new MockWsmDAO {
    override def getWorkspace(workspaceId: WorkspaceId, authorization: Authorization)(implicit
      ev: Ask[IO, AppContext]
    ): IO[Option[WorkspaceDescription]] =
      IO.pure(
        Some(
          WorkspaceDescription(
            workspaceId,
            "someWorkspaceName" + workspaceId,
            "9f3434cb-8f18-4595-95a9-d9b1ec9731d4",
            None,
            Some(GoogleProject(workspaceId.toString))
          )
        )
      )
  }

  val appServiceInterp = makeInterp(QueueFactory.makePublisherQueue())
  val appServiceInterp2 = makeInterp(QueueFactory.makePublisherQueue(), authProvider = allowListAuthProvider2)
  val gcpWorkspaceAppServiceInterp = makeInterp(QueueFactory.makePublisherQueue(), wsmDao = gcpWsmDao)

  def withLeoPublisher(
    publisherQueue: Queue[IO, LeoPubsubMessage]
  )(validations: IO[Assertion]): IO[Assertion] = {

    val mockCloudPublisher = mock[CloudPublisher[IO]]
    def noOpPipe[A]: Pipe[IO, A, Unit] = _.evalMap(_ => IO.unit)
    when(mockCloudPublisher.publish[LeoPubsubMessage](any)).thenReturn(noOpPipe)
    when(mockCloudPublisher.publishOne[LeoPubsubMessage](any, any)(any, any)).thenReturn(IO.unit)

    val leoPublisher = new LeoPublisher[IO](publisherQueue, mockCloudPublisher)
    withInfiniteStream(leoPublisher.process, validations)
  }

  // used when we care about queue state
  def makeInterp(queue: Queue[IO, LeoPubsubMessage],
                 authProvider: LeoAuthProvider[IO] = allowListAuthProvider,
                 wsmDao: WsmDao[IO] = wsmDao,
                 enableCustomAppCheckFlag: Boolean = true,
                 enableSasApp: Boolean = true,
                 googleResourceService: GoogleResourceService[IO] = FakeGoogleResourceService,
                 customAppConfig: CustomAppConfig = gkeCustomAppConfig,
                 wsmClientProvider: WsmApiClientProvider[IO] = wsmClientProvider
  ) = {
    val appConfig = appServiceConfig.copy(enableCustomAppCheck = enableCustomAppCheckFlag, enableSasApp = enableSasApp)

    new LeoAppServiceInterp[IO](
      appConfig,
      authProvider,
      serviceAccountProvider,
      queue,
      Some(FakeGoogleComputeService),
      Some(googleResourceService),
      customAppConfig,
      wsmDao,
      wsmClientProvider
    )
  }
}

class AppServiceInterpTest extends AnyFlatSpec with AppServiceInterpSpec with LeonardoTestSuite with TestComponent {
  it should "validate galaxy runtime requirements correctly" in ioAssertion {
    val project = GoogleProject("project1")
    val passComputeService = new FakeGoogleComputeService {
      override def getMachineType(project: GoogleProject, zone: ZoneName, machineTypeName: MachineTypeName)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[MachineType]] =
        IO.pure(Some(MachineType.newBuilder().setName("pass").setMemoryMb(6 * 1024).setGuestCpus(4).build()))
    }
    val notEnoughMemoryComputeService = new FakeGoogleComputeService {
      override def getMachineType(project: GoogleProject, zone: ZoneName, machineTypeName: MachineTypeName)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[MachineType]] =
        IO.pure(Some(MachineType.newBuilder().setName("notEnoughMemory").setMemoryMb(3 * 1024).setGuestCpus(4).build()))
    }
    val notEnoughCpuComputeService = new FakeGoogleComputeService {
      override def getMachineType(project: GoogleProject, zone: ZoneName, machineTypeName: MachineTypeName)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[MachineType]] =
        IO.pure(Some(MachineType.newBuilder().setName("notEnoughMemory").setMemoryMb(6 * 1024).setGuestCpus(2).build()))
    }

    val highSecurityGoogleResourceService = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO(Some(Map("security-group" -> "high")))
    }

    val anySecurityGoogleResourceService = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO(Some(Map("security-group" -> "low")))
    }

    val noSecurityGoogleResourceService = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO(Some(Map("unused-label" -> "unused-label-value")))
    }

    val passAppService = new LeoAppServiceInterp[IO](
      appServiceConfig,
      allowListAuthProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(passComputeService),
      Some(FakeGoogleResourceService),
      gkeCustomAppConfig,
      wsmDao,
      wsmClientProvider
    )
    val notEnoughMemoryAppService = new LeoAppServiceInterp[IO](
      appServiceConfig,
      allowListAuthProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(notEnoughMemoryComputeService),
      Some(FakeGoogleResourceService),
      gkeCustomAppConfig,
      wsmDao,
      wsmClientProvider
    )
    val notEnoughCpuAppService = new LeoAppServiceInterp[IO](
      appServiceConfig,
      allowListAuthProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(notEnoughCpuComputeService),
      Some(FakeGoogleResourceService),
      gkeCustomAppConfig,
      wsmDao,
      wsmClientProvider
    )

    for {
      ctx <- appContext.ask[AppContext]
      _ <- passAppService.validateGalaxy(project, None, MachineTypeName("fake"))
      error1 <- notEnoughMemoryAppService.validateGalaxy(project, None, MachineTypeName("fake")).attempt
      error2 <- notEnoughCpuAppService.validateGalaxy(project, None, MachineTypeName("fake")).attempt
    } yield {
      error1 shouldBe (Left(BadRequestException("Galaxy needs more memory configuration", Some(ctx.traceId))))
      error2 shouldBe (Left(BadRequestException("Galaxy needs more CPU configuration", Some(ctx.traceId))))
    }
  }

  it should "fail request if user is not in custom_app_users group" in {
    val authProvider = new BaseMockAuthProvider {
      override def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
        IO.pure(false)
    }
    val noLabelsGoogleResourceService = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO(None)
    }
    val interp = new LeoAppServiceInterp[IO](
      AppServiceConfig(enableCustomAppCheck = true, enableSasApp = true, leoKubernetesConfig),
      authProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(FakeGoogleComputeService),
      Some(noLabelsGoogleResourceService),
      gkeCustomAppConfig,
      wsmDao,
      wsmClientProvider
    )

    an[ForbiddenError] should be thrownBy {
      interp
        .createApp(
          userInfo,
          cloudContextGcp,
          AppName("foo"),
          createAppRequest.copy(appType = AppType.Custom,
                                descriptorPath = Some(Uri.unsafeFromString("https://www.myappdescriptor.com/finaldesc"))
          )
        )
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "not fail customApp request if group check is not enabled" in {
    val authProvider = new BaseMockAuthProvider {
      override def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
        IO.pure(false)
    }
    val interp = new LeoAppServiceInterp[IO](
      AppServiceConfig(false, false, leoKubernetesConfig),
      authProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(FakeGoogleComputeService),
      Some(FakeGoogleResourceService),
      gkeCustomAppConfig,
      wsmDao,
      wsmClientProvider
    )
    val res = interp
      .createApp(userInfo, cloudContextGcp, AppName("foo"), createAppRequest.copy(appType = AppType.Custom))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    res.swap.toOption.get.isInstanceOf[ForbiddenError] shouldBe false
  }

  it should "determine patch version bump correctly" in isolatedDbTest {
    val first = ChartVersion("0.8.0")
    val second = ChartVersion("0.8.2")
    LeoAppServiceInterp.isPatchVersionDifference(first, second) shouldBe true
  }

  it should "create an app and a new disk" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig), customEnvironmentVariables = customEnvVars)

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusters = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(cloudContextGcp))
    }
    clusters.length shouldEqual 1
    clusters.flatMap(_.nodepools).length shouldEqual 1
    val cluster = clusters.head
    cluster.auditInfo.creator shouldEqual userInfo.userEmail

    val nodepool = clusters.flatMap(_.nodepools).head
    nodepool.machineType shouldEqual appReq.kubernetesRuntimeConfig.get.machineType
    nodepool.numNodes shouldEqual appReq.kubernetesRuntimeConfig.get.numNodes
    nodepool.autoscalingEnabled shouldEqual appReq.kubernetesRuntimeConfig.get.autoscalingEnabled
    nodepool.auditInfo.creator shouldEqual userInfo.userEmail

    clusters.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 1
    val app = clusters.flatMap(_.nodepools).flatMap(_.apps).head
    app.appName shouldEqual appName
    app.chart shouldEqual galaxyChart
    app.auditInfo.creator shouldEqual userInfo.userEmail
    app.customEnvironmentVariables shouldEqual customEnvVars

    val savedDisk = dbFutureValue {
      persistentDiskQuery.getById(app.appResources.disk.get.id)
    }
    savedDisk.map(_.name) shouldEqual Some(diskName)
  }

  it should "rollback Sam resource creation if app creation fails" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._

    // Fake an error in getLastUsedAppForDisk
    val disk = makePersistentDisk(None)
      .copy(cloudContext = cloudContextGcp)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig), customEnvironmentVariables = customEnvVars)

    val mockAuthProvider = mock[LeoAuthProvider[IO]]
    when(mockAuthProvider.hasPermission(any, any, any)(any, any)).thenReturn(IO.pure(true))
    when(mockAuthProvider.lookupOriginatingUserEmail(any)(any)).thenReturn(IO.pure(userInfo.userEmail))
    when(mockAuthProvider.notifyResourceCreated(any[AppSamResourceId], any, any)(any, any, any)).thenReturn(IO.unit)
    when(mockAuthProvider.notifyResourceDeleted(any[AppSamResourceId], any, any)(any, any)).thenReturn(IO.unit)
    val publisherQueue = QueueFactory.makePublisherQueue()
    val appService = makeInterp(publisherQueue, authProvider = mockAuthProvider)
    val res = appService
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    res.swap.toOption.get.getMessage shouldBe "Disk is not formatted yet. Only disks previously used by galaxy/cromwell/rstudio app can be re-used to create a new galaxy/cromwell/rstudio app"

    verify(mockAuthProvider).notifyResourceCreated(any[AppSamResourceId], any, any)(any, any, any)
    verify(mockAuthProvider).notifyResourceDeleted(any[AppSamResourceId], any, any)(any, any)
  }

  it should "create an app in a user's existing nodepool" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig), customEnvironmentVariables = customEnvVars)

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    val appName2 = AppName("app2")
    val createDiskConfig2 = PersistentDiskRequest(DiskName("disk2"), None, None, Map.empty)
    val appReq2 =
      createAppRequest.copy(diskConfig = Some(createDiskConfig2), customEnvironmentVariables = customEnvVars)
    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusters = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(cloudContextGcp))
    }

    clusters.flatMap(_.nodepools).length shouldBe 1
    clusters.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 2

    clusters.flatMap(_.nodepools).flatMap(_.apps).map(_.appName).sortBy(_.value) shouldBe List(appName, appName2)
      .sortBy(_.value)
    val app1 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }.get

    val app2 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName2)
    }.get

    app1.nodepool.id shouldBe app2.nodepool.id
  }

  it should "result in new nodepool creations when apps request distinct nodepool configurations" in isolatedDbTest {
    val defaultNodepoolConfig = KubernetesRuntimeConfig(
      NumNodes(1),
      MachineTypeName("n1-standard-8"),
      autoscalingEnabled = true
    )
    val nodepoolConfigWithMoreNodes = defaultNodepoolConfig.copy(numNodes = NumNodes(2))
    val nodepoolConfigWithMoreCpuAndMem = defaultNodepoolConfig.copy(machineType = MachineTypeName("n1-highmem-32"))
    val nodepoolConfigWithAutoscalingDisabled = defaultNodepoolConfig.copy(autoscalingEnabled = false)

    val appName1 = AppName("app-default-config")
    val appName2 = AppName("app-more-nodes")
    val appName3 = AppName("app-more-cpu-mem")
    val appName4 = AppName("app-autoscaling-disabled")

    val diskConfig1 = PersistentDiskRequest(DiskName("disk1"), None, None, Map.empty)
    val diskConfig2 = PersistentDiskRequest(DiskName("disk2"), None, None, Map.empty)
    val diskConfig3 = PersistentDiskRequest(DiskName("disk3"), None, None, Map.empty)
    val diskConfig4 = PersistentDiskRequest(DiskName("disk4"), None, None, Map.empty)

    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")

    val defaultAppReq =
      createAppRequest.copy(kubernetesRuntimeConfig = Some(defaultNodepoolConfig),
                            diskConfig = Some(diskConfig1),
                            customEnvironmentVariables = customEnvVars
      )
    val appReqWithMoreNodes =
      defaultAppReq.copy(kubernetesRuntimeConfig = Some(nodepoolConfigWithMoreNodes), diskConfig = Some(diskConfig2))
    val appReqWithMoreCpuAndMem = defaultAppReq.copy(kubernetesRuntimeConfig = Some(nodepoolConfigWithMoreCpuAndMem),
                                                     diskConfig = Some(diskConfig3)
    )
    val appReqWithAutoscalingDisabled =
      defaultAppReq.copy(kubernetesRuntimeConfig = Some(nodepoolConfigWithAutoscalingDisabled),
                         diskConfig = Some(diskConfig4)
      )

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName1, defaultAppReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName2, appReqWithMoreNodes)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName3, appReqWithMoreCpuAndMem)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName4, appReqWithAutoscalingDisabled)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusters = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(cloudContextGcp))
    }

    clusters.flatMap(_.nodepools).length shouldBe 4
    clusters.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 4

    val nodepoolId1 =
      dbFutureValue(KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName1)).get.nodepool.id
    val nodepoolId2 =
      dbFutureValue(KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName2)).get.nodepool.id
    val nodepoolId3 =
      dbFutureValue(KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName3)).get.nodepool.id
    val nodepoolId4 =
      dbFutureValue(KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName4)).get.nodepool.id

    Set(nodepoolId1, nodepoolId2, nodepoolId3, nodepoolId4).size shouldBe 4 // each app has a distinct nodepool
  }

  it should "queue the proper message when creating an app and a new disk" in isolatedDbTest {

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig), customEnvironmentVariables = customEnvVars)

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)

    kubeServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }.get

    val getMinimalCluster = dbFutureValue {
      kubernetesClusterQuery.getMinimalClusterById(getApp.cluster.id)
    }.get

    val defaultNodepools = getMinimalCluster.nodepools.filter(_.isDefault)
    defaultNodepools.length shouldBe 1
    val defaultNodepool = defaultNodepools.head

    val message = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message.messageType shouldBe LeoPubsubMessageType.CreateApp
    val createAppMessage = message.asInstanceOf[CreateAppMessage]
    createAppMessage.appId shouldBe getApp.app.id
    createAppMessage.project shouldBe project
    createAppMessage.createDisk shouldBe getApp.app.appResources.disk.map(_.id)
    createAppMessage.customEnvironmentVariables shouldBe customEnvVars
    createAppMessage.clusterNodepoolAction shouldBe Some(
      ClusterNodepoolAction.CreateClusterAndNodepool(getMinimalCluster.id, defaultNodepool.id, getApp.nodepool.id)
    )
  }

  it should "not able to create an app with an existing non-used disk" in isolatedDbTest {
    val disk = makePersistentDisk(None)
      .copy(cloudContext = cloudContextGcp)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val res = kubeServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.swap.toOption.get.getMessage shouldBe "Disk is not formatted yet. Only disks previously used by galaxy/cromwell/rstudio app can be re-used to create a new galaxy/cromwell/rstudio app"
  }

  it should "error creating an app with an existing used disk when WORKSPACE_NAME is not specified" in isolatedDbTest {
    val customEnvVariables = Map(WORKSPACE_NAME_KEY -> "fake_ws")
    val cluster = makeKubeCluster(0).save()
    val nodepool = makeNodepool(1, cluster.id).save()
    val app = makeApp(1, nodepool.id, customEnvVariables).save()
    val disk = makePersistentDisk(None,
                                  formattedBy = Some(FormattedBy.Galaxy),
                                  appRestore = Some(GalaxyRestore(PvcId("pv-id"), app.id))
    )
      .copy(cloudContext = cloudContextGcp)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig),
                                       customEnvironmentVariables = Map(WORKSPACE_NAME_KEY -> "fake_ws2")
    )

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val res = kubeServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    res.swap.toOption.get.getMessage shouldBe "workspace name has to be the same as last used app in order to restore data from existing disk"
  }

  it should "create an app with an existing used disk" in isolatedDbTest {
    val customEnvVariables = Map(WORKSPACE_NAME_KEY -> "fake_ws")
    val cluster = makeKubeCluster(0).save()
    val nodepool = makeNodepool(1, cluster.id).save()
    val app = makeApp(1, nodepool.id, customEnvVariables).save()
    val disk = makePersistentDisk(None,
                                  appRestore = Some(GalaxyRestore(PvcId("pv-id"), app.id)),
                                  formattedBy = Some(FormattedBy.Galaxy)
    )
      .copy(cloudContext = cloudContextGcp)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq =
      createAppRequest.copy(diskConfig = Some(createDiskConfig), customEnvironmentVariables = customEnvVariables)

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    kubeServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val message = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message.messageType shouldBe LeoPubsubMessageType.CreateApp
    message.asInstanceOf[CreateAppMessage].createDisk shouldBe None

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }

    appResult.flatMap(_.app.appResources.disk.map(_.name)) shouldEqual Some(disk.name)
    appResult.map(_.app.appName) shouldEqual Some(appName)
  }

  it should "allow pet SA to create an app" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = Some(PersistentDiskRequest(diskName, None, None, Map.empty))
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = cromwellAppCreateRequest(createDiskConfig, customEnvVars)

    appServiceInterp
      .createApp(petUserInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusters = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(cloudContextGcp))
    }
    clusters.length shouldEqual 1
    clusters.flatMap(_.nodepools).length shouldEqual 1
    val cluster = clusters.head
    cluster.auditInfo.creator shouldEqual userEmail

    val nodepool = clusters.flatMap(_.nodepools).head
    nodepool.auditInfo.creator shouldEqual userEmail

    clusters.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 1
    val app = clusters.flatMap(_.nodepools).flatMap(_.apps).head
    app.appName shouldEqual appName
    app.appType shouldEqual AppType.Cromwell
    app.auditInfo.creator shouldEqual userEmail
    app.customEnvironmentVariables shouldEqual customEnvVars

    val savedDisk = dbFutureValue {
      persistentDiskQuery.getById(app.appResources.disk.get.id)
    }.get
    savedDisk.name shouldEqual diskName
    savedDisk.auditInfo.creator shouldEqual userEmail
  }

  it should "allow pet SA to get app details" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = Some(PersistentDiskRequest(diskName, None, None, Map.empty))
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = cromwellAppCreateRequest(createDiskConfig, customEnvVars)

    appServiceInterp
      .createApp(petUserInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp: GetAppResponse =
      appServiceInterp
        .getApp(petUserInfo, cloudContextGcp, appName)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    getApp.appType shouldBe AppType.Cromwell
    getApp.diskName shouldBe Some(diskName)
    getApp.auditInfo.creator shouldBe userEmail
    getApp.customEnvironmentVariables shouldBe customEnvVars
  }

  it should "allow pet SA to delete an app" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val appName = AppName("app1")
    val createDiskConfig = Some(PersistentDiskRequest(diskName, None, None, Map.empty))
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = cromwellAppCreateRequest(createDiskConfig, customEnvVars)

    kubeServiceInterp
      .createApp(petUserInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }

    // Set the app status and nodepool status to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))

    // Call deleteApp
    kubeServiceInterp
      .deleteApp(petUserInfo, cloudContextGcp, appName, true)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // Verify that request using pet SA was successful and app is marked to be deleted
    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Predeleting

    // Verify database state
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(cloudContextGcp), includeDeleted = true)
    }
    clusterPostDelete.length shouldEqual 1
    val nodepool = clusterPostDelete.head.nodepools.head
    nodepool.status shouldEqual NodepoolStatus.Running
    nodepool.auditInfo.destroyedDate shouldBe None

    // throw away create message
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe a[CreateAppMessage]

    // Verify DeleteAppMessage message was generated
    publisherQueue.tryTake.unsafeRunSync()(cats.effect.unsafe.IORuntime.global).get shouldBe a[DeleteAppMessage]
  }

  it should "error creating an app with an existing disk if no restore info found" in isolatedDbTest {
    val disk = makePersistentDisk(None, formattedBy = Some(FormattedBy.Galaxy))
      .copy(cloudContext = cloudContextGcp)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val res = kubeServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.swap.toOption.get.getMessage shouldBe "Existing disk found, but no restore info found in DB"
  }

  it should "error on creation of a galaxy app without a disk" in isolatedDbTest {
    val appName = AppName("app1")
    val appReq = createAppRequest.copy(diskConfig = None, appType = AppType.Galaxy)

    an[AppRequiresDiskException] should be thrownBy {
      appServiceInterp
        .createApp(userInfo, cloudContextGcp, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "error creating Cromwell app with an existing disk if no restore info found" in isolatedDbTest {
    val disk = makePersistentDisk(None, formattedBy = Some(FormattedBy.Cromwell))
      .copy(cloudContext = cloudContextGcp)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig), appType = AppType.Cromwell)

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val res = kubeServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.swap.toOption.get.getMessage shouldBe "Existing disk found, but no restore info found in DB"
  }

  it should "error creating Galaxy app with an existing disk that was formatted by Cromwell" in isolatedDbTest {
    val cluster = makeKubeCluster(0).save()
    val nodepool = makeNodepool(1, cluster.id).save()
    val cromwellApp = makeApp(1, nodepool.id).save()
    val disk =
      makePersistentDisk(None, formattedBy = Some(FormattedBy.Cromwell), appRestore = Some(Other(cromwellApp.id)))
        .copy(cloudContext = cloudContextGcp)
        .save()
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val galaxyAppName = AppName("galaxy-app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val galaxyAppReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val res = kubeServiceInterp
      .createApp(userInfo, cloudContextGcp, galaxyAppName, galaxyAppReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.swap.toOption.get.getMessage shouldBe "Persistent disk Gcp/dsp-leo-test/disk is already formatted by CROMWELL"
  }

  it should "error on creation of Cromwell app without a disk" in isolatedDbTest {
    val appName = AppName("cromwell-app1")
    val appReq = createAppRequest.copy(diskConfig = None, appType = AppType.Cromwell)

    an[AppRequiresDiskException] should be thrownBy {
      appServiceInterp
        .createApp(userInfo, cloudContextGcp, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "error on creation if a disk is attached to another app" in isolatedDbTest {
    val customEnvVariables = Map(WORKSPACE_NAME_KEY -> "fake_ws")
    val cluster = makeKubeCluster(0).save()
    val nodepool = makeNodepool(1, cluster.id).save()
    val app = makeApp(1, nodepool.id, customEnvVariables).save()
    val disk = makePersistentDisk(None,
                                  formattedBy = Some(FormattedBy.Galaxy),
                                  appRestore = Some(GalaxyRestore(PvcId("pv-id"), app.id))
    )
      .copy(cloudContext = cloudContextGcp)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val appName1 = AppName("app1")
    val appName2 = AppName("app2")

    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName1, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName1)
    }
    appResult.flatMap(_.app.appResources.disk.map(_.name)) shouldEqual Some(disk.name)
    appResult.map(_.app.appName) shouldEqual Some(appName1)

    // we need to update status from creating because we don't allow creation of apps while cluster is creating
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    a[DiskAlreadyAttachedException] should be thrownBy {
      appServiceInterp
        .createApp(userInfo, cloudContextGcp, appName2, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "error on creation if an app with that name exists" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    an[AppAlreadyExistsException] should be thrownBy {
      appServiceInterp
        .createApp(userInfo, cloudContextGcp, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "error on creation if the disk is too small" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(DiskName("new-disk"), Some(DiskSize(50)), None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    a[BadRequestException] should be thrownBy {
      appServiceInterp
        .createApp(userInfo, cloudContextGcp, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "delete an app and update status appropriately" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    kubeServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }

    // we can't delete while its creating, so set it to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    kubeServiceInterp
      .deleteApp(userInfo, cloudContextGcp, appName, false)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(cloudContextGcp), includeDeleted = true)
    }

    clusterPostDelete.length shouldEqual 1
    val nodepool = clusterPostDelete.head.nodepools.head
    nodepool.status shouldEqual NodepoolStatus.Running
    val app = nodepool.apps.head
    app.status shouldEqual AppStatus.Predeleting

    // throw away create message
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val message = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message.messageType shouldBe LeoPubsubMessageType.DeleteApp
    val deleteAppMessage = message.asInstanceOf[DeleteAppMessage]
    deleteAppMessage.appId shouldBe app.id
    deleteAppMessage.project shouldBe project
    deleteAppMessage.diskId shouldBe None
  }

  it should "determine if an app is deletable properly" in {
    val deletableCombos = Table(
      ("appType", "status"),
      (AppType.Galaxy, AppStatus.Running),
      (AppType.Galaxy, AppStatus.Error),
      (AppType.Galaxy, AppStatus.Unspecified),
      (AppType.Allowed, AppStatus.Running)
    )

    forAll(deletableCombos) { (appType, status) =>
      LeoAppServiceInterp.checkIfCanBeDeleted(appType, status) shouldBe (Right(()))
    }

    val nonDeletableCombos = Table(
      ("appType", "status"),
      (AppType.Galaxy, AppStatus.Precreating),
      (AppType.Galaxy, AppStatus.Starting),
      (AppType.Galaxy, AppStatus.Predeleting),
      (AppType.Galaxy, AppStatus.Deleting),
      (AppType.Galaxy, AppStatus.Stopping),
      (AppType.Galaxy, AppStatus.Stopped),
      (AppType.Galaxy, AppStatus.Updating),
      (AppType.Allowed, AppStatus.Provisioning),
      (AppType.Allowed, AppStatus.Stopping),
      (AppType.Allowed, AppStatus.Stopped),
      (AppType.Allowed, AppStatus.Starting)
    )

    forAll(nonDeletableCombos) { (appType, status) =>
      LeoAppServiceInterp.checkIfCanBeDeleted(appType, status) shouldBe (Left(
        s"${appType} can not be deleted in ${status} status."
      ))
    }
  }

  it should "delete an app and record AppUsage stopTime" in isolatedDbTest {
    val res = for {
      publisherQueue <- Queue.bounded[IO, LeoPubsubMessage](10)
      kubeServiceInterp = makeInterp(publisherQueue)

      savedCluster <- IO(makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      savedNodepool <- IO(makeNodepool(1, savedCluster.id).copy(status = NodepoolStatus.Running).save())
      chart = Chart.fromString("/leonardo/sas-0.1.0")
      savedApp <- IO(
        makeApp(1, savedNodepool.id, appType = AppType.Allowed, chart = chart.get)
          .copy(status = AppStatus.Running)
          .save()
      )

      _ <- appUsageQuery.recordStart(savedApp.id, Instant.now())
      gcpContext = savedCluster.cloudContext match {
        case g @ CloudContext.Gcp(_) => g
        case _                       => fail("expected GCP context")
      }
      _ <- kubeServiceInterp.deleteApp(userInfo, gcpContext, savedApp.appName, false)
      _ <- withLeoPublisher(publisherQueue) {
        for {
          appUsage <- getAllAppUsage.transaction
        } yield appUsage.headOption.map(_.stopTime).isDefined shouldBe true
      }
    } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "error on delete if app is in a status that cannot be deleted" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }

    // TODO: update this once create publishes pubsub message
    appResultPreDelete.get.app.status shouldEqual AppStatus.Precreating
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    an[AppCannotBeDeletedException] should be thrownBy {
      appServiceInterp
        .deleteApp(userInfo, cloudContextGcp, appName, false)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "error on delete if app creator is removed from app's project" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }

    appResultPreDelete.get.app.status shouldEqual AppStatus.Precreating
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    an[ForbiddenError] should be thrownBy {
      appServiceInterp2
        .deleteApp(userInfo, cloudContextGcp, appName, false)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "delete an app in Error status" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    kubeServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }

    // Set the app status to Error, nodepool status to Running, and the disk status to Deleted to
    // simulate an error during app creation.
    // Note: if an app with a newly-created disk errors out, Leo will delete the disk along with the app.
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Error))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(persistentDiskQuery.delete(appResultPreStatusUpdate.get.app.appResources.disk.get.id, Instant.now))

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Error
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None
    appResultPreDelete.get.nodepool.status shouldBe NodepoolStatus.Running
    appResultPreDelete.get.nodepool.auditInfo.destroyedDate shouldBe None
    appResultPreDelete.get.app.appResources.disk.get.status shouldBe DiskStatus.Deleted
    appResultPreDelete.get.app.appResources.disk.get.auditInfo.destroyedDate shouldBe defined

    // Call deleteApp
    kubeServiceInterp
      .deleteApp(userInfo, cloudContextGcp, appName, true)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // Verify database state
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(cloudContextGcp), includeDeleted = true)
    }
    clusterPostDelete.length shouldEqual 1
    val nodepool = clusterPostDelete.head.nodepools.head
    nodepool.status shouldEqual NodepoolStatus.Running
    nodepool.auditInfo.destroyedDate shouldBe None
    val app = nodepool.apps.head
    app.status shouldEqual AppStatus.Deleted
    app.auditInfo.destroyedDate shouldBe defined
    val disk = app.appResources.disk
    disk shouldBe None

    // throw away create message
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe a[CreateAppMessage]

    // Verify no DeleteAppMessage message generated
    publisherQueue.tryTake.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe None
  }

  it should "list apps" in isolatedDbTest {
    val appName1 = AppName("app1")
    val appName2 = AppName("app2")
    val appName3 = AppName("app3")
    val createDiskConfig1 = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq1 = createAppRequest.copy(labels = Map("key1" -> "val1", "key2" -> "val2", "key3" -> "val3"),
                                        diskConfig = Some(createDiskConfig1)
    )
    val diskName2 = DiskName("newDiskName")
    val createDiskConfig2 = PersistentDiskRequest(diskName2, None, None, Map.empty)
    val appReq2 = createAppRequest.copy(diskConfig = Some(createDiskConfig2))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    appServiceInterp
      .createApp(userInfo, cloudContext2Gcp, appName3, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val listAllApps =
      appServiceInterp
        .listApp(userInfo, None, Map("includeLabels" -> "key1,key2,key4"))
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listAllApps.length shouldEqual 3
    listAllApps.map(_.appName) should contain(appName1)
    listAllApps.map(_.labels) should contain(Map("key1" -> "val1", "key2" -> "val2"))
    listAllApps.map(_.appName) should contain(appName2)
    listAllApps.map(_.appName) should contain(appName3)
    listAllApps.map(_.diskName).sortBy(_.get.value) shouldBe Vector(Some(diskName), Some(diskName), Some(diskName2))
      .sortBy(_.get.value)

    val listProject1Apps =
      appServiceInterp
        .listApp(userInfo, Some(cloudContextGcp), Map())
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listProject1Apps.length shouldBe 2
    listProject1Apps.map(_.appName) should contain(appName1)
    listProject1Apps.map(_.appName) should contain(appName2)

    val listProject2Apps =
      appServiceInterp
        .listApp(userInfo, Some(cloudContext2Gcp), Map())
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listProject2Apps.length shouldBe 1
    listProject2Apps.map(_.appName) should contain(appName3)

    val listProject3Apps =
      appServiceInterp
        .listApp(userInfo, Some(CloudContext.Gcp(GoogleProject("fakeProject"))), Map())
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listProject3Apps.length shouldBe 0
  }

  it should "list only apps for projects user has access to" in isolatedDbTest {
    val appName1 = AppName("app1")
    val createDiskConfig1 = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq1 = createAppRequest.copy(diskConfig = Some(createDiskConfig1))

    appServiceInterp
      .createApp(userInfo, cloudContext2Gcp, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val listAllApps =
      appServiceInterp2 // has a different allowlist
        .listApp(userInfo, None, Map())
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    listAllApps.length shouldEqual 0
  }

  it should "error if listing apps in a project user doesn't have access to" in isolatedDbTest {
    val appName1 = AppName("app1")
    val createDiskConfig1 = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq1 = createAppRequest.copy(labels = Map("key1" -> "val1", "key2" -> "val2", "key3" -> "val3"),
                                        diskConfig = Some(createDiskConfig1)
    )

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    a[ForbiddenError] should be thrownBy
      appServiceInterp
        .listApp(userInfo4, Some(cloudContextGcp), Map("includeLabels" -> "key1,key2,key4"))
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list apps with labels" in isolatedDbTest {
    val appName1 = AppName("app1")
    val appName2 = AppName("app2")
    val appName3 = AppName("app3")
    val createDiskConfig1 = PersistentDiskRequest(diskName, None, None, Map.empty)
    val label1 = "a" -> "b"
    val label2 = "c" -> "d"
    val labels: LabelMap = Map(label1, label2)
    val appReq1 = createAppRequest.copy(diskConfig = Some(createDiskConfig1))
    val diskName2 = DiskName("newDiskName")
    val createDiskConfig2 = PersistentDiskRequest(diskName2, None, None, Map.empty)
    val appReq2 = createAppRequest.copy(diskConfig = Some(createDiskConfig2), labels = labels)

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val app1Result = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName1)
    }

    dbFutureValue(kubernetesClusterQuery.updateStatus(app1Result.get.cluster.id, KubernetesClusterStatus.Running))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val app2Result = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName2)
    }
    app2Result.map(_.app.labels).get.toList should contain(label1)
    app2Result.map(_.app.labels).get.toList should contain(label2)

    appServiceInterp
      .createApp(userInfo, cloudContext2Gcp, appName3, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val listLabelApp =
      appServiceInterp.listApp(userInfo, None, labels).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listLabelApp.length shouldEqual 1
    listLabelApp.map(_.appName) should contain(appName2)

    val listPartialLabelApp1 =
      appServiceInterp.listApp(userInfo, None, Map(label1)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listPartialLabelApp1.length shouldEqual 1
    listPartialLabelApp1.map(_.appName) should contain(appName2)

    val listPartialLabelApp2 =
      appServiceInterp.listApp(userInfo, None, Map(label2)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listPartialLabelApp2.length shouldEqual 1
    listPartialLabelApp2.map(_.appName) should contain(appName2)
  }

  it should "list apps belonging to different users" in isolatedDbTest {
    // Make apps belonging to different users than the calling user and with different access scopes
    val res = for {
      savedCluster <- IO(makeKubeCluster(1).save())
      savedNodepool1 <- IO(makeNodepool(1, savedCluster.id).save())
      app1 = LeoLenses.appToCreator.set(WorkbenchEmail("a_different_user1@example.com"))(makeApp(1, savedNodepool1.id))
      _ <- IO(app1.save())

      savedNodepool2 <- IO(makeNodepool(2, savedCluster.id).save())
      app2 = LeoLenses.appToCreator.set(WorkbenchEmail("a_different_user2@example.com"))(makeApp(2, savedNodepool2.id))
      _ <- IO(app2.save())

      savedNodepool3 <- IO(makeNodepool(3, savedCluster.id).save())
      app3 = LeoLenses.appToCreator.set(WorkbenchEmail("a_different_user3@example.com"))(
        makeApp(3, savedNodepool3.id, appAccessScope = AppAccessScope.WorkspaceShared)
      )
      _ <- IO(app3.save())

      savedNodepool4 <- IO(makeNodepool(4, savedCluster.id).save())
      app4 = LeoLenses.appToCreator.set(WorkbenchEmail("a_different_user4@example.com"))(
        makeApp(4, savedNodepool4.id, appAccessScope = AppAccessScope.UserPrivate)
      )
      _ <- IO(app4.save())

      listResponse <- appServiceInterp.listApp(userInfo, None, Map.empty)
    } yield
    // Since the calling user is allowlisted in the auth provider, it should return
    // the apps belonging to other users.
    listResponse.map(_.appName).toSet shouldBe Set(app1.appName, app2.appName, app3.appName, app4.appName)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "get app" in isolatedDbTest {
    val appName1 = AppName("app1")
    val appName2 = AppName("app2")
    val appName3 = AppName("app3")
    val createDiskConfig1 = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq1 = createAppRequest.copy(diskConfig = Some(createDiskConfig1))
    val diskName2 = DiskName("newDiskName")
    val createDiskConfig2 = PersistentDiskRequest(diskName2, None, None, Map.empty)
    val appReq2 = createAppRequest.copy(diskConfig = Some(createDiskConfig2))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val diskName3 = DiskName("newDiskName2")
    val createDiskConfig3 = PersistentDiskRequest(diskName3, None, None, Map.empty)
    appServiceInterp
      .createApp(userInfo, cloudContext2Gcp, appName3, appReq1.copy(diskConfig = Some(createDiskConfig3)))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp1 =
      appServiceInterp.getApp(userInfo, cloudContextGcp, appName1).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    getApp1.diskName shouldBe Some(diskName)

    val getApp2 =
      appServiceInterp.getApp(userInfo, cloudContextGcp, appName2).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    getApp2.diskName shouldBe Some(diskName2)

    val getApp3 =
      appServiceInterp.getApp(userInfo, cloudContext2Gcp, appName3).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    getApp3.diskName shouldBe Some(diskName3)
  }

  it should "error on get app if an app does not exist" in isolatedDbTest {
    an[AppNotFoundException] should be thrownBy {
      appServiceInterp
        .getApp(userInfo, cloudContextGcp, AppName("schrodingersApp"))
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "error on get app if user doesn't have project permission" in isolatedDbTest {
    an[ForbiddenError] should be thrownBy {
      appServiceInterp
        .getApp(userInfo4, cloudContextGcp, AppName("schrodingersApp"))
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "stop an app" in isolatedDbTest {
    val res = for {
      publisherQueue <- Queue.bounded[IO, LeoPubsubMessage](10)
      kubeServiceInterp = makeInterp(publisherQueue)

      savedCluster <- IO(makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      savedNodepool <- IO(makeNodepool(1, savedCluster.id).copy(status = NodepoolStatus.Running).save())
      chart = Chart.fromString("/leonardo/sas-0.1.0")
      savedApp <- IO(
        makeApp(1, savedNodepool.id, appType = AppType.Allowed, chart = chart.get)
          .copy(status = AppStatus.Running)
          .save()
      )

      _ <- appUsageQuery.recordStart(savedApp.id, Instant.now())
      gcpContext = savedCluster.cloudContext match {
        case g @ CloudContext.Gcp(_) => g
        case _                       => fail("expected GCP context")
      }
      _ <- kubeServiceInterp.stopApp(userInfo, gcpContext, savedApp.appName)
      _ <- withLeoPublisher(publisherQueue) {
        for {
          dbAppOpt <- KubernetesServiceDbQueries
            .getActiveFullAppByName(savedCluster.cloudContext, savedApp.appName)
            .transaction
          msg <- publisherQueue.tryTake
          appUsage <- getAllAppUsage.transaction
        } yield {
          dbAppOpt.isDefined shouldBe true
          dbAppOpt.get.app.status shouldBe AppStatus.Stopping
          dbAppOpt.get.nodepool.status shouldBe NodepoolStatus.Running
          dbAppOpt.get.nodepool.numNodes shouldBe NumNodes(2)
          dbAppOpt.get.nodepool.autoscalingEnabled shouldBe true
          dbAppOpt.get.cluster.status shouldBe KubernetesClusterStatus.Running

          msg shouldBe None

          appUsage.headOption.map(_.stopTime).isDefined shouldBe true
        }
      }
    } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "start an app" in isolatedDbTest {
    val res = for {
      publisherQueue <- Queue.bounded[IO, LeoPubsubMessage](10)
      kubeServiceInterp = makeInterp(publisherQueue)

      savedCluster <- IO(makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      savedNodepool <- IO(makeNodepool(1, savedCluster.id).copy(status = NodepoolStatus.Running).save())
      savedApp <- IO(makeApp(1, savedNodepool.id).copy(status = AppStatus.Stopped).save())

      gcpContext = savedCluster.cloudContext match {
        case g @ CloudContext.Gcp(_) => g
        case _                       => fail("expected GCP context")
      }
      _ <- kubeServiceInterp.startApp(userInfo, gcpContext, savedApp.appName)
      _ <- withLeoPublisher(publisherQueue) {
        for {
          dbAppOpt <- KubernetesServiceDbQueries
            .getActiveFullAppByName(savedCluster.cloudContext, savedApp.appName)
            .transaction
          msg <- publisherQueue.tryTake
        } yield {
          dbAppOpt.isDefined shouldBe true
          dbAppOpt.get.app.status shouldBe AppStatus.Starting
          dbAppOpt.get.nodepool.status shouldBe NodepoolStatus.Running
          dbAppOpt.get.nodepool.numNodes shouldBe NumNodes(2)
          dbAppOpt.get.nodepool.autoscalingEnabled shouldBe true
          dbAppOpt.get.cluster.status shouldBe KubernetesClusterStatus.Running

          msg shouldBe None
        }
      }
    } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "throw a leo exception if AppType is custom and descriptorPath is undefined." in isolatedDbTest {
    val appName = AppName("my_custom_app")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customApplicationAllowList =
      CustomApplicationAllowListConfig(List(), List())
    val testInterp = new LeoAppServiceInterp[IO](
      AppServiceConfig(enableCustomAppCheck = true, enableSasApp = true, leoKubernetesConfig),
      allowListAuthProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(FakeGoogleComputeService),
      Some(FakeGoogleResourceService),
      CustomAppConfig(
        ChartName(""),
        ChartVersion(""),
        ReleaseNameSuffix(""),
        NamespaceNameSuffix(""),
        ServiceAccountName(""),
        customApplicationAllowList,
        true,
        List()
      ),
      wsmDao,
      wsmClientProvider
    )
    val appReq = createAppRequest.copy(
      diskConfig = Some(createDiskConfig),
      appType = AppType.Custom,
      descriptorPath = None
    )

    an[LeoException] should be thrownBy {
      testInterp
        .createApp(userInfo, cloudContextGcp, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "reject create SAS app request if it's not from AoU UI and user is not in the sas_app_users group" in {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val authProvider = new AllowlistAuthProvider(allowlistAuthConfig, serviceAccountProvider) {
      override def isSasAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
        IO.pure(false)
    }
    val appReq =
      createAppRequest.copy(
        diskConfig = Some(createDiskConfig),
        appType = AppType.Allowed,
        allowedChartName = Some(AllowedChartName.Sas)
      )

    val appServiceInterp = makeInterp(QueueFactory.makePublisherQueue(), authProvider = authProvider)
    val res = appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    res shouldBe (Left(
      AuthenticationError(Some(userInfo.userEmail), "You need to obtain a license in order to create a SAS App")
    ))
  }

  it should "reject create SAS app request if it's not for an AoU project" in {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val grs = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO.pure(Some(Map.from(List("security-group" -> "low"))))
    }
    val appServiceInterp = makeInterp(QueueFactory.makePublisherQueue(), googleResourceService = grs)
    val appReq =
      createAppRequest.copy(
        diskConfig = Some(createDiskConfig),
        appType = AppType.Allowed,
        allowedChartName = Some(AllowedChartName.Sas),
        labels = Map.from(List("all-of-us" -> "true"))
      )

    val res = for {
      ctx <- appContext.ask[AppContext]
      res <- appServiceInterp
        .createApp(userInfo, cloudContextGcp, appName, appReq)
        .attempt
    } yield res shouldBe (Left(ForbiddenError(userInfo.userEmail, Some(ctx.traceId))))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "reject create SAS app request if SAS is disabled" in {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val grs = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO.pure(Some(Map.from(List("security-group" -> "low"))))
    }
    val appServiceInterp =
      makeInterp(QueueFactory.makePublisherQueue(), googleResourceService = grs, enableSasApp = false)
    val appReq =
      createAppRequest.copy(
        diskConfig = Some(createDiskConfig),
        appType = AppType.Allowed,
        allowedChartName = Some(AllowedChartName.Sas),
        labels = Map.from(List("all-of-us" -> "true"))
      )

    val res = for {
      ctx <- appContext.ask[AppContext]
      res <- appServiceInterp
        .createApp(userInfo, cloudContextGcp, appName, appReq)
        .attempt
    } yield res shouldBe (Left(
      AuthenticationError(Some(userInfo.userEmail), "SAS is not enabled. Please contact your administrator.")
    ))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "create SAS app successfully for AoU even if user is not in sas_app_users group" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val grs = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO.pure(Some(Map.from(List("security-group" -> "high"))))
    }

    val authProvider = new AllowlistAuthProvider(allowlistAuthConfig, serviceAccountProvider) {
      override def isSasAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
        IO.pure(false)
    }
    val queue = QueueFactory.makePublisherQueue()
    val appServiceInterp = makeInterp(queue, googleResourceService = grs, authProvider = authProvider)
    val appReq =
      createAppRequest.copy(
        diskConfig = Some(createDiskConfig),
        appType = AppType.Allowed,
        allowedChartName = Some(AllowedChartName.Sas),
        labels = Map.from(List("all-of-us" -> "true"))
      )

    val res = for {
      ctx <- appContext.ask[AppContext]
      res <- appServiceInterp
        .createApp(userInfo, cloudContextGcp, appName, appReq)
        .attempt
      msg <- queue.take
    } yield msg.isInstanceOf[CreateAppMessage] shouldBe true
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "create a custom app with default security" in isolatedDbTest {
    val appName = AppName("my_custom_app")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customApplicationAllowList =
      CustomApplicationAllowListConfig(List("https://www.myappdescriptor.com/finaldesc"), List())
    val authProvider = new AllowlistAuthProvider(allowlistAuthConfig, serviceAccountProvider) {
      override def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
        IO.pure(true)
    }
    val testInterp = new LeoAppServiceInterp[IO](
      AppServiceConfig(enableCustomAppCheck = true, enableSasApp = true, leoKubernetesConfig),
      authProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(FakeGoogleComputeService),
      Some(FakeGoogleResourceService),
      CustomAppConfig(
        ChartName(""),
        ChartVersion(""),
        ReleaseNameSuffix(""),
        NamespaceNameSuffix(""),
        ServiceAccountName(""),
        customApplicationAllowList,
        true,
        List()
      ),
      wsmDao,
      wsmClientProvider
    )
    val appReq = createAppRequest.copy(
      diskConfig = Some(createDiskConfig),
      appType = AppType.Custom,
      descriptorPath = Some(Uri.unsafeFromString("https://www.myappdescriptor.com/finaldesc"))
    )

    testInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "create a custom app with no security-group project labels" in isolatedDbTest {
    val appName = AppName("my_custom_app")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customApplicationAllowList =
      CustomApplicationAllowListConfig(List(), List())
    val authProvider = new AllowlistAuthProvider(allowlistAuthConfig, serviceAccountProvider) {
      override def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
        IO.pure(true)
    }
    val noSecurityGroupGoogleResourceService = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO(Some(Map("not-security-group" -> "any-val")))
    }
    val testInterp = new LeoAppServiceInterp[IO](
      AppServiceConfig(enableCustomAppCheck = true, enableSasApp = true, leoKubernetesConfig),
      authProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(FakeGoogleComputeService),
      Some(noSecurityGroupGoogleResourceService),
      CustomAppConfig(
        ChartName(""),
        ChartVersion(""),
        ReleaseNameSuffix(""),
        NamespaceNameSuffix(""),
        ServiceAccountName(""),
        customApplicationAllowList,
        true,
        List()
      ),
      wsmDao,
      wsmClientProvider
    )
    val appReq = createAppRequest.copy(
      diskConfig = Some(createDiskConfig),
      appType = AppType.Custom,
      descriptorPath = Some(Uri.unsafeFromString("https://www.myappdescriptor.com/defaultSec"))
    )
    testInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "create a custom app with high security" in isolatedDbTest {
    val appName = AppName("my_custom_app")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val authProvider = new AllowlistAuthProvider(allowlistAuthConfig, serviceAccountProvider) {
      override def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
        IO.pure(true)
    }
    val customApplicationAllowList =
      CustomApplicationAllowListConfig(List(), List("https://www.myappdescriptor.com/finaldesc"))
    val highSecurityGoogleResourceService = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO(Some(Map("security-group" -> "high")))
    }
    val testInterp = new LeoAppServiceInterp[IO](
      AppServiceConfig(enableCustomAppCheck = true, enableSasApp = true, leoKubernetesConfig),
      authProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(FakeGoogleComputeService),
      Some(highSecurityGoogleResourceService),
      CustomAppConfig(
        ChartName(""),
        ChartVersion(""),
        ReleaseNameSuffix(""),
        NamespaceNameSuffix(""),
        ServiceAccountName(""),
        customApplicationAllowList,
        true,
        List()
      ),
      wsmDao,
      wsmClientProvider
    )
    val appReq = createAppRequest.copy(
      diskConfig = Some(createDiskConfig),
      appType = AppType.Custom,
      descriptorPath = Some(Uri.unsafeFromString("https://www.myappdescriptor.com/finaldesc"))
    )

    testInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "throw a ForbiddenError when trying to create a custom app with high security" in isolatedDbTest {
    val appName = AppName("my_custom_app")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customApplicationAllowList =
      CustomApplicationAllowListConfig(List(), List())
    val highSecurityGoogleResourceService = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO(Some(Map("security-group" -> "high")))
    }
    val testInterp = new LeoAppServiceInterp[IO](
      AppServiceConfig(enableCustomAppCheck = true, enableSasApp = true, leoKubernetesConfig),
      allowListAuthProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(FakeGoogleComputeService),
      Some(highSecurityGoogleResourceService),
      CustomAppConfig(
        ChartName(""),
        ChartVersion(""),
        ReleaseNameSuffix(""),
        NamespaceNameSuffix(""),
        ServiceAccountName(""),
        customApplicationAllowList,
        true,
        List()
      ),
      wsmDao,
      wsmClientProvider
    )
    val appReq = createAppRequest.copy(
      diskConfig = Some(createDiskConfig),
      appType = AppType.Custom,
      descriptorPath = Some(Uri.unsafeFromString("https://www.myappdescriptor.com/finaldesc"))
    )

    an[ForbiddenError] should be thrownBy {
      testInterp
        .createApp(userInfo, cloudContextGcp, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "throw a ForbiddenError when trying to create a custom app with project labels but no security group label" in isolatedDbTest {
    val appName = AppName("my_custom_app")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customApplicationAllowList =
      CustomApplicationAllowListConfig(List(), List())
    val authProvider = new AllowlistAuthProvider(allowlistAuthConfig, serviceAccountProvider) {
      override def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
        IO.pure(false)
    }
    val noSecurityGroupGoogleResourceService = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO(Some(Map("not-security-group" -> "any-val")))
    }
    val testInterp = new LeoAppServiceInterp[IO](
      AppServiceConfig(enableCustomAppCheck = true, enableSasApp = true, leoKubernetesConfig),
      authProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(FakeGoogleComputeService),
      Some(noSecurityGroupGoogleResourceService),
      CustomAppConfig(
        ChartName(""),
        ChartVersion(""),
        ReleaseNameSuffix(""),
        NamespaceNameSuffix(""),
        ServiceAccountName(""),
        customApplicationAllowList,
        true,
        List()
      ),
      wsmDao,
      wsmClientProvider
    )
    val appReq = createAppRequest.copy(
      diskConfig = Some(createDiskConfig),
      appType = AppType.Custom,
      descriptorPath = Some(Uri.unsafeFromString("https://www.myappdescriptor.com/finaldesc"))
    )

    an[ForbiddenError] should be thrownBy {
      testInterp
        .createApp(userInfo, cloudContextGcp, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "create a custom app with project labels but no security group label" in isolatedDbTest {
    val appName = AppName("my_custom_app")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customApplicationAllowList =
      CustomApplicationAllowListConfig(List(), List())
    val authProvider = new AllowlistAuthProvider(allowlistAuthConfig, serviceAccountProvider) {
      override def isCustomAppAllowed(userEmail: WorkbenchEmail)(implicit ev: Ask[IO, TraceId]): IO[Boolean] =
        IO.pure(true)
    }
    val noSecurityGroupGoogleResourceService = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO(Some(Map("not-security-group" -> "any-val")))
    }
    val testInterp = new LeoAppServiceInterp[IO](
      AppServiceConfig(enableCustomAppCheck = true, enableSasApp = true, leoKubernetesConfig),
      authProvider,
      serviceAccountProvider,
      QueueFactory.makePublisherQueue(),
      Some(FakeGoogleComputeService),
      Some(noSecurityGroupGoogleResourceService),
      CustomAppConfig(
        ChartName(""),
        ChartVersion(""),
        ReleaseNameSuffix(""),
        NamespaceNameSuffix(""),
        ServiceAccountName(""),
        customApplicationAllowList,
        true,
        List()
      ),
      wsmDao,
      wsmClientProvider
    )
    val appReq = createAppRequest.copy(
      diskConfig = Some(createDiskConfig),
      appType = AppType.Custom,
      descriptorPath = Some(Uri.unsafeFromString("https://www.myappdescriptor.com/finaldesc"))
    )
    testInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "V1 GCP - deleteAppRecords delete an app, nodepool and cluster in DB and records AppUsage stopTime" in isolatedDbTest {

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue, wsmDao = gcpWsmDao)

    val appName = AppName("app1")
    val diskConfig = PersistentDiskRequest(DiskName("disk1"), None, None, Map.empty)

    val appReq =
      createAppRequest.copy(kubernetesRuntimeConfig = None,
                            appType = AppType.Allowed,
                            allowedChartName = Some(AllowedChartName.Sas),
                            diskConfig = Some(diskConfig)
      )

    kubeServiceInterp
      .createApp(userInfo, CloudContext.Gcp(project), appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(CloudContext.Gcp(project), appName)
    }
    // Set the cluster, app, and nodepool to running and start tracking the usage
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(
      kubernetesClusterQuery.updateStatus(appResultPreStatusUpdate.get.cluster.id, KubernetesClusterStatus.Running)
    )

    val res = for {
      appUsageId <- appUsageQuery.recordStart(appResultPreStatusUpdate.get.app.id, Instant.now())
    } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    kubeServiceInterp
      .deleteAllAppsRecords(userInfo, CloudContext.Gcp(project))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appUsage = dbFutureValue(getAllAppUsage)
    appUsage.headOption.map(_.stopTime).isDefined shouldBe true

    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(CloudContext.Gcp(project)), includeDeleted = true)
    }

    clusterPostDelete.length shouldEqual 1
    clusterPostDelete.map(_.status) shouldEqual List(KubernetesClusterStatus.Deleted)
    val nodepools = clusterPostDelete.flatMap(_.nodepools)
    val apps = clusterPostDelete.flatMap(_.nodepools).flatMap(_.apps)
    nodepools.map(_.status) shouldEqual List(NodepoolStatus.Deleted)
    apps.map(_.status) shouldEqual List(AppStatus.Deleted)

    // throw away create messages
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val messages = publisherQueue.tryTakeN(Some(1)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    messages.map(_.messageType) shouldBe List.empty

  }

  it should "Azure - only delete App records in deleteAllAppsV2 if a workspace has been deleted" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)

    val appName = AppName("app1")

    val appReq =
      createAppRequest.copy(
        kubernetesRuntimeConfig = None,
        appType = AppType.Cromwell,
        allowedChartName = Some(AllowedChartName.Sas),
        diskConfig = None,
        workspaceId = Some(workspaceId2)
      )

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId2, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId2, appName)
    }
    // Set the cluster, app, and nodepool to running and start tracking the usage
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(
      kubernetesClusterQuery.updateStatus(appResultPreStatusUpdate.get.cluster.id, KubernetesClusterStatus.Running)
    )

    kubeServiceInterp
      .deleteAllAppsV2(userInfo, workspaceId2, true)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspaceId2), Map.empty, true)
    }

    clusterPostDelete.length shouldEqual 1
    clusterPostDelete.map(_.status) shouldEqual List(KubernetesClusterStatus.Deleted)
    val nodepools = clusterPostDelete.flatMap(_.nodepools)
    val apps = clusterPostDelete.flatMap(_.nodepools).flatMap(_.apps)
    nodepools.map(_.status) shouldEqual List(NodepoolStatus.Deleted)
    apps.map(_.status) shouldEqual List(AppStatus.Deleted)

    // throw away create messages
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val messages = publisherQueue.tryTakeN(Some(1)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    messages.map(_.messageType) shouldBe List.empty

  }

  it should "Azure - only delete App records in deleteAppV2 if a workspace has been deleted" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)

    val appName = AppName("app1")

    val appReq =
      createAppRequest.copy(
        kubernetesRuntimeConfig = None,
        appType = AppType.Cromwell,
        allowedChartName = Some(AllowedChartName.Sas),
        diskConfig = None,
        workspaceId = Some(workspaceId2)
      )

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId2, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId2, appName)
    }
    // Set the cluster, app, and nodepool to running and start tracking the usage
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(
      kubernetesClusterQuery.updateStatus(appResultPreStatusUpdate.get.cluster.id, KubernetesClusterStatus.Running)
    )

    kubeServiceInterp
      .deleteAppV2(userInfo, workspaceId2, appName, true)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspaceId2), Map.empty, true)
    }

    clusterPostDelete.length shouldEqual 1
    clusterPostDelete.map(_.status) shouldEqual List(KubernetesClusterStatus.Deleted)
    val nodepools = clusterPostDelete.flatMap(_.nodepools)
    val apps = clusterPostDelete.flatMap(_.nodepools).flatMap(_.apps)
    nodepools.map(_.status) shouldEqual List(NodepoolStatus.Deleted)
    apps.map(_.status) shouldEqual List(AppStatus.Deleted)

    // throw away create messages
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val messages = publisherQueue.tryTakeN(Some(1)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    messages.map(_.messageType) shouldBe List.empty

  }

  it should "error on deleteAllApps if the user doesn't have permission to access the workspace" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue, wsmDao = wsmDao)
    when {
      workspaceApi.getWorkspace(ArgumentMatchers.eq(workspaceId2.value), any())
    } thenAnswer (_ => throw new Exception("User does not have access to this workspace"))

    val appName = AppName("app1")

    val appReq =
      createAppRequest.copy(
        kubernetesRuntimeConfig = None,
        appType = AppType.Cromwell,
        allowedChartName = Some(AllowedChartName.Sas),
        diskConfig = None,
        workspaceId = Some(workspaceId2)
      )

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId2, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId2, appName)
    }
    // Set the cluster, app, and nodepool to running and start tracking the usage
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(
      kubernetesClusterQuery.updateStatus(appResultPreStatusUpdate.get.cluster.id, KubernetesClusterStatus.Running)
    )

    an[WorkspaceNotFoundException] should be thrownBy {
      kubeServiceInterp
        .deleteAllAppsV2(userInfo, workspaceId2, false)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "error on deleteAppV2 if the user doesn't have permission to access the workspace" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue, wsmDao = wsmDao)
    when {
      workspaceApi.getWorkspace(ArgumentMatchers.eq(workspaceId2.value), any())
    } thenAnswer (_ => throw new Exception("User does not have access to this workspace"))

    val appName = AppName("app1")

    val appReq =
      createAppRequest.copy(
        kubernetesRuntimeConfig = None,
        appType = AppType.Cromwell,
        allowedChartName = Some(AllowedChartName.Sas),
        diskConfig = None,
        workspaceId = Some(workspaceId2)
      )

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId2, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId2, appName)
    }
    // Set the cluster, app, and nodepool to running and start tracking the usage
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(
      kubernetesClusterQuery.updateStatus(appResultPreStatusUpdate.get.cluster.id, KubernetesClusterStatus.Running)
    )

    an[WorkspaceNotFoundException] should be thrownBy {
      kubeServiceInterp
        .deleteAppV2(userInfo, workspaceId2, appName, false)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "V1 GCP - deleteAppRecords error on delete if app creator is removed from app's project" in isolatedDbTest {

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    an[AppNotFoundException] should be thrownBy {
      appServiceInterp2
        .deleteAppRecords(userInfo, cloudContextGcp, appName)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "V1 GCP - deleteAllAppsRecords, update all status appropriately, and not queue messages" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue, wsmDao = gcpWsmDao)

    val appName = AppName("app1")
    val appName2 = AppName("app2")
    val diskConfig1 = PersistentDiskRequest(DiskName("disk1"), None, None, Map.empty)
    val diskConfig2 = PersistentDiskRequest(DiskName("disk2"), None, None, Map.empty)

    val appReq =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Galaxy, diskConfig = Some(diskConfig1))
    val appReq2 =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Galaxy, diskConfig = Some(diskConfig2))

    kubeServiceInterp
      .createApp(userInfo, CloudContext.Gcp(project), appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(CloudContext.Gcp(project), appName)
    }
    // we can't delete/create another while its creating, so set it to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(
      kubernetesClusterQuery.updateStatus(appResultPreStatusUpdate.get.cluster.id, KubernetesClusterStatus.Running)
    )

    kubeServiceInterp
      .createApp(userInfo, CloudContext.Gcp(project), appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    kubeServiceInterp
      .deleteAllAppsRecords(userInfo, CloudContext.Gcp(project))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(CloudContext.Gcp(project)), includeDeleted = true)
    }

    clusterPostDelete.length shouldEqual 1
    clusterPostDelete.map(_.status) shouldEqual List(KubernetesClusterStatus.Deleted)
    val nodepools = clusterPostDelete.flatMap(_.nodepools)
    val apps = clusterPostDelete.flatMap(_.nodepools).flatMap(_.apps)
    nodepools.map(_.status) shouldEqual List(NodepoolStatus.Deleted)
    apps.map(_.status) shouldEqual List(AppStatus.Deleted, AppStatus.Deleted)

    // throw away create messages
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val messages = publisherQueue.tryTakeN(Some(2)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    messages.map(_.messageType) shouldBe List.empty
  }

  it should "V1 GCP - deleteAllApp, update all status appropriately, and queue multiple messages" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue, wsmDao = gcpWsmDao)

    val appName = AppName("app1")
    val appName2 = AppName("app2")
    val diskConfig1 = PersistentDiskRequest(DiskName("disk1"), None, None, Map.empty)
    val diskConfig2 = PersistentDiskRequest(DiskName("disk2"), None, None, Map.empty)

    val appReq =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Galaxy, diskConfig = Some(diskConfig1))
    val appReq2 =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Galaxy, diskConfig = Some(diskConfig2))

    kubeServiceInterp
      .createApp(userInfo, CloudContext.Gcp(project), appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(CloudContext.Gcp(project), appName)
    }
    // we can't delete/create another while its creating, so set it to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(
      kubernetesClusterQuery.updateStatus(appResultPreStatusUpdate.get.cluster.id, KubernetesClusterStatus.Running)
    )

    kubeServiceInterp
      .createApp(userInfo, CloudContext.Gcp(project), appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate2 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(CloudContext.Gcp(project), appName2)
    }

    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate2.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate2.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(
      kubernetesClusterQuery.updateStatus(appResultPreStatusUpdate2.get.cluster.id, KubernetesClusterStatus.Running)
    )

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(CloudContext.Gcp(project), appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    val appResultPreDelete2 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(CloudContext.Gcp(project), appName2)
    }
    appResultPreDelete2.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete2.get.app.auditInfo.destroyedDate shouldBe None

    val attachedDisksNames = kubeServiceInterp
      .deleteAllApps(userInfo, CloudContext.Gcp(project), false)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    attachedDisksNames.length shouldEqual 2
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(CloudContext.Gcp(project)), includeDeleted = true)
    }

    clusterPostDelete.length shouldEqual 1
    val nodepools = clusterPostDelete.flatMap(_.nodepools)
    val apps = clusterPostDelete.flatMap(_.nodepools).flatMap(_.apps)
    nodepools.map(_.status) shouldEqual List(NodepoolStatus.Running)
    apps.map(_.status) shouldEqual List(AppStatus.Predeleting, AppStatus.Predeleting)

    // throw away create messages
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val messages = publisherQueue.tryTakeN(Some(2)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    messages.map(_.messageType) shouldBe List(LeoPubsubMessageType.DeleteApp, LeoPubsubMessageType.DeleteApp)
    val deleteAppMessages = messages.map(_.asInstanceOf[DeleteAppMessage])
    deleteAppMessages.map(_.appId) shouldBe apps.map(_.id)
    deleteAppMessages.map(_.project) shouldBe List(project, project)
    deleteAppMessages.map(_.diskId) shouldBe List(None, None)
  }

  it should "V1 GCP - deleteAllApp, should fail and not update anything if there is a non-deletable app status" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue, wsmDao = gcpWsmDao)
    val appName = AppName("app1")
    val appName2 = AppName("app2")
    val diskConfig1 = PersistentDiskRequest(DiskName("disk1"), None, None, Map.empty)
    val diskConfig2 = PersistentDiskRequest(DiskName("disk2"), None, None, Map.empty)

    val appReq =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Galaxy, diskConfig = Some(diskConfig1))
    val appReq2 =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Galaxy, diskConfig = Some(diskConfig2))

    kubeServiceInterp
      .createApp(userInfo, CloudContext.Gcp(project), appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val app1ResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cloudContextGcp, appName)
    }
    // we can't delete/create another while its creating, so set it to Running
    dbFutureValue(nodepoolQuery.updateStatus(app1ResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(
      kubernetesClusterQuery.updateStatus(app1ResultPreStatusUpdate.get.cluster.id, KubernetesClusterStatus.Running)
    )

    kubeServiceInterp
      .createApp(userInfo, CloudContext.Gcp(project), appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(CloudContext.Gcp(project), appName)
    }
    val appResultPreStatusUpdate2 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(CloudContext.Gcp(project), appName2)
    }

    // set appName to a non-deletable status
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Provisioning))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))

    // set appName2 to a deletable status
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate2.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate2.get.nodepool.id, NodepoolStatus.Running))

    val thrown = the[DeleteAllAppsV1CannotBePerformed] thrownBy {
      kubeServiceInterp
        .deleteAllApps(userInfo, CloudContext.Gcp(project), false)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(CloudContext.Gcp(project)))
    }

    clusterPostDelete.length shouldEqual 1
    val nodepools = clusterPostDelete.flatMap(_.nodepools)
    val apps = clusterPostDelete.flatMap(_.nodepools).flatMap(_.apps)
    nodepools.map(_.status) shouldEqual List(NodepoolStatus.Running)
    apps.map(_.status).toSet shouldEqual Set(AppStatus.Running, AppStatus.Provisioning)

    // throw away create messages
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val messages = publisherQueue.tryTakeN(Some(2)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    messages shouldBe List.empty
  }

  // ----- App V2 tests -----

  it should "V2 GCP - create an app V2 and a new disk" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig), customEnvironmentVariables = customEnvVars)

    gcpWorkspaceAppServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusters = dbFutureValue {
      KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspaceId))
    }
    clusters.length shouldEqual 1
    clusters.flatMap(_.nodepools).length shouldEqual 1
    val cluster = clusters.head
    cluster.auditInfo.creator shouldEqual userInfo.userEmail

    clusters.flatMap(_.nodepools).size shouldBe 1
    val nodepool = clusters.flatMap(_.nodepools).head
    nodepool.isDefault shouldBe true

    clusters.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 1
    val app = clusters.flatMap(_.nodepools).flatMap(_.apps).head
    app.appName shouldEqual appName
    app.chart shouldEqual galaxyChart
    app.auditInfo.creator shouldEqual userInfo.userEmail
    app.customEnvironmentVariables shouldEqual customEnvVars
    app.workspaceId shouldEqual Some(workspaceId)

    val savedDisk = dbFutureValue {
      persistentDiskQuery.getById(app.appResources.disk.get.id)
    }
    savedDisk.map(_.name) shouldEqual Some(diskName)
  }

  it should "V2 Azure - create an app V2" in isolatedDbTest {
    val appName = AppName("app1")
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace",
                            "RELAY_HYBRID_CONNECTION_NAME" -> s"${appName.value}-${workspaceId.value}"
    )
    val appReq = createAppRequest.copy(kubernetesRuntimeConfig = None,
                                       appType = AppType.Cromwell,
                                       diskConfig = None,
                                       customEnvironmentVariables = customEnvVars
    )

    appServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusters = dbFutureValue {
      KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspaceId))
    }
    clusters.length shouldEqual 1
    clusters.flatMap(_.nodepools).length shouldEqual 1
    val cluster = clusters.head
    cluster.auditInfo.creator shouldEqual userInfo.userEmail
    cluster.status shouldEqual KubernetesClusterStatus.Running

    clusters.flatMap(_.nodepools).size shouldBe 1
    val nodepool = clusters.flatMap(_.nodepools).head
    nodepool.isDefault shouldBe true

    clusters.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 1
    val app = clusters.flatMap(_.nodepools).flatMap(_.apps).head
    app.appName shouldEqual appName
    app.chart shouldEqual coaChart
    app.auditInfo.creator shouldEqual userInfo.userEmail
    app.customEnvironmentVariables shouldEqual customEnvVars
    app.status shouldEqual AppStatus.Precreating
    app.appResources.disk shouldEqual None
    app.workspaceId shouldEqual Some(workspaceId)
  }

  it should "V2 Azure - throw an error when providing a machine config" in isolatedDbTest {
    val appName = AppName("app1")
    val appReq = createAppRequest.copy(
      appType = AppType.Cromwell,
      diskConfig = None
    )

    a[AppMachineConfigNotSupportedException] should be thrownBy
      appServiceInterp
        .createAppV2(userInfo, workspaceId, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "V2 Azure - throw an error when providing a disk config" in isolatedDbTest {
    val appName = AppName("app1")
    val appReq = createAppRequest.copy(
      kubernetesRuntimeConfig = None,
      appType = AppType.Cromwell,
      diskConfig = Some(PersistentDiskRequest(diskName, None, None, Map.empty))
    )

    a[AppDiskNotSupportedException] should be thrownBy
      appServiceInterp
        .createAppV2(userInfo, workspaceId, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "V2 GCP - queue the proper v2 message when creating an app and a new disk" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig), customEnvironmentVariables = customEnvVars)

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue, wsmDao = gcpWsmDao)

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }.get

    val getMinimalCluster = dbFutureValue {
      kubernetesClusterQuery.getMinimalClusterById(getApp.cluster.id)
    }.get

    val defaultNodepools = getMinimalCluster.nodepools.filter(_.isDefault)
    defaultNodepools.length shouldBe 1

    val message = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message.messageType shouldBe LeoPubsubMessageType.CreateAppV2
    val createAppMessage = message.asInstanceOf[CreateAppV2Message]
    createAppMessage.appId shouldBe getApp.app.id
    createAppMessage.appName shouldBe getApp.app.appName
    createAppMessage.workspaceId shouldBe workspaceId
  }

  it should "V2 Azure - queue the proper v2 message when creating an app" in isolatedDbTest {
    val appName = AppName("app1")
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = createAppRequest.copy(kubernetesRuntimeConfig = None,
                                       appType = AppType.Cromwell,
                                       diskConfig = None,
                                       customEnvironmentVariables = customEnvVars
    )

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }.get

    val getMinimalCluster = dbFutureValue {
      kubernetesClusterQuery.getMinimalClusterById(getApp.cluster.id)
    }.get

    val defaultNodepools = getMinimalCluster.nodepools.filter(_.isDefault)
    defaultNodepools.length shouldBe 1

    val message = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message.messageType shouldBe LeoPubsubMessageType.CreateAppV2
    val createAppMessage = message.asInstanceOf[CreateAppV2Message]
    createAppMessage.appId shouldBe getApp.app.id
    createAppMessage.appName shouldBe getApp.app.appName
    createAppMessage.workspaceId shouldBe workspaceId
  }

  it should "V2 GCP - delete a gcp app V2, update status appropriately, and queue a message" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue, wsmDao = gcpWsmDao)
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }

    // we can't delete while its creating, so set it to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    kubeServiceInterp
      .deleteAppV2(userInfo, workspaceId, appName, false)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspaceId), includeDeleted = true)
    }

    clusterPostDelete.length shouldEqual 1
    val nodepool = clusterPostDelete.head.nodepools.head
    nodepool.status shouldEqual NodepoolStatus.Running
    val app = nodepool.apps.head
    app.status shouldEqual AppStatus.Predeleting

    // throw away create message
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val message = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message.messageType shouldBe LeoPubsubMessageType.DeleteAppV2
    val deleteAppMessage = message.asInstanceOf[DeleteAppV2Message]
    deleteAppMessage.appId shouldBe app.id
    deleteAppMessage.workspaceId shouldBe workspaceId
    deleteAppMessage.diskId shouldBe None
  }

  it should "V2 GCP - fail to delete an app when creator has lost workspace permission" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    gcpWorkspaceAppServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))
    val azureService2 = makeInterp(QueueFactory.makePublisherQueue(), allowListAuthProvider2, gcpWsmDao)
    a[ForbiddenError] should be thrownBy azureService2
      .deleteAppV2(userInfo, workspaceId, appName, true)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "V2 Azure - delete an Azure app V2, update status appropriately, and queue a message" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val appName = AppName("app1")
    val appReq =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Cromwell, diskConfig = None)

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }

    // we can't delete while its creating, so set it to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    kubeServiceInterp
      .deleteAppV2(userInfo, workspaceId, appName, false)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspaceId), includeDeleted = true)
    }

    clusterPostDelete.length shouldEqual 1
    val nodepool = clusterPostDelete.head.nodepools.head
    nodepool.status shouldEqual NodepoolStatus.Running
    val app = nodepool.apps.head
    app.status shouldEqual AppStatus.Predeleting

    // throw away create message
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val message = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message.messageType shouldBe LeoPubsubMessageType.DeleteAppV2
    val deleteAppMessage = message.asInstanceOf[DeleteAppV2Message]
    deleteAppMessage.appId shouldBe app.id
    deleteAppMessage.workspaceId shouldBe workspaceId
    deleteAppMessage.diskId shouldBe None
  }

  it should "V2 Azure - fail to delete an app when creator has lost workspace permission" in isolatedDbTest {
    val appName = AppName("app1")
    val appReq =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Cromwell, diskConfig = None)

    appServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))
    val azureService2 = makeInterp(QueueFactory.makePublisherQueue(), allowListAuthProvider2)
    a[ForbiddenError] should be thrownBy azureService2
      .deleteAppV2(userInfo, workspaceId, appName, true)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "V2 GCP - list apps V2 should return apps for workspace" in isolatedDbTest {
    val appName1 = AppName("app1")
    val diskName1 = DiskName("newDiskName1")
    val createDiskConfig1 = PersistentDiskRequest(diskName1, None, None, Map.empty)
    val appReq1 = createAppRequest.copy(labels = Map("key1" -> "val1", "key2" -> "val2", "key3" -> "val3"),
                                        diskConfig = Some(createDiskConfig1)
    )
    gcpWorkspaceAppServiceInterp
      .createAppV2(userInfo, workspaceId, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    val appName2 = AppName("app2")
    val diskName2 = DiskName("newDiskName2")
    val createDiskConfig2 = PersistentDiskRequest(diskName2, None, None, Map.empty)
    val appReq2 = createAppRequest.copy(diskConfig = Some(createDiskConfig2))

    gcpWorkspaceAppServiceInterp
      .createAppV2(userInfo, workspaceId, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName3 = AppName("app3")
    val diskName3 = DiskName("newDiskName3")
    val createDiskConfig3 = PersistentDiskRequest(diskName3, None, None, Map.empty)
    val appReq3 = createAppRequest.copy(diskConfig = Some(createDiskConfig3))

    gcpWorkspaceAppServiceInterp
      .createAppV2(userInfo, workspaceId3, appName3, appReq3)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val listProject1Apps =
      gcpWorkspaceAppServiceInterp
        .listAppV2(userInfo, workspaceId, Map())
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listProject1Apps.length shouldBe 2
    listProject1Apps.map(_.appName) should contain(appName1)
    listProject1Apps.map(_.appName) should contain(appName2)

    val listProject3Apps =
      gcpWorkspaceAppServiceInterp
        .listAppV2(userInfo, workspaceId3, Map())
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listProject3Apps.length shouldBe 1
  }

  it should "V2 GCP - list apps V2 should fail to return apps for workspace if creator lost workspace access" in isolatedDbTest {
    val appName1 = AppName("app1")
    val diskName1 = DiskName("newDiskName1")
    val createDiskConfig1 = PersistentDiskRequest(diskName1, None, None, Map.empty)
    val appReq1 = createAppRequest.copy(labels = Map("key1" -> "val1", "key2" -> "val2", "key3" -> "val3"),
                                        diskConfig = Some(createDiskConfig1)
    )
    gcpWorkspaceAppServiceInterp
      .createAppV2(userInfo, workspaceId, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    val listProject1Apps =
      gcpWorkspaceAppServiceInterp
        .listAppV2(userInfo, workspaceId, Map())
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listProject1Apps.length shouldBe 1
    listProject1Apps.map(_.appName) should contain(appName1)

    val azureService2 = makeInterp(QueueFactory.makePublisherQueue(), allowListAuthProvider2, gcpWsmDao)
    a[ForbiddenError] should be thrownBy azureService2
      .listAppV2(userInfo, workspaceId, Map())
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "V2 Azure - list apps V2 should return apps for workspace" in isolatedDbTest {
    val appName1 = AppName("app1")
    val appReq1 = createAppRequest.copy(labels = Map("key1" -> "val1", "key2" -> "val2", "key3" -> "val3"),
                                        kubernetesRuntimeConfig = None,
                                        appType = AppType.Cromwell,
                                        diskConfig = None
    )
    appServiceInterp
      .createAppV2(userInfo, workspaceId, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    val appName2 = AppName("app2")
    val appReq2 =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Cromwell, diskConfig = None)

    appServiceInterp
      .createAppV2(userInfo, workspaceId, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName3 = AppName("app3")
    val appReq3 =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Cromwell, diskConfig = None)

    appServiceInterp
      .createAppV2(userInfo, workspaceId3, appName3, appReq3)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val listProject1Apps =
      appServiceInterp
        .listAppV2(userInfo, workspaceId, Map())
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    listProject1Apps.map(_.appName) should contain(appName1)
    listProject1Apps.map(_.appName) should contain(appName2)
    listProject1Apps.length shouldBe 2

    val listProject3Apps =
      appServiceInterp
        .listAppV2(userInfo, workspaceId3, Map())
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listProject3Apps.length shouldBe 1
  }

  it should "V2 Azure - list apps V2 should fail to return apps for workspace if creator lost workspace access" in isolatedDbTest {
    val appName1 = AppName("app1")
    val appReq1 = createAppRequest.copy(labels = Map("key1" -> "val1", "key2" -> "val2", "key3" -> "val3"),
                                        kubernetesRuntimeConfig = None,
                                        appType = AppType.Cromwell,
                                        diskConfig = None
    )
    appServiceInterp
      .createAppV2(userInfo, workspaceId, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    val listProject1Apps =
      appServiceInterp
        .listAppV2(userInfo, workspaceId, Map())
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listProject1Apps.length shouldBe 1
    listProject1Apps.map(_.appName) should contain(appName1)

    val azureService2 = makeInterp(QueueFactory.makePublisherQueue(), allowListAuthProvider2)
    a[ForbiddenError] should be thrownBy azureService2
      .listAppV2(userInfo, workspaceId, Map())
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "V2 GCP - get app" in isolatedDbTest {
    val appName1 = AppName("app1")
    val diskName1 = DiskName("newDiskName1")
    val createDiskConfig1 = PersistentDiskRequest(diskName1, None, None, Map.empty)
    val appReq1 = createAppRequest.copy(labels = Map("key1" -> "val1", "key2" -> "val2", "key3" -> "val3"),
                                        diskConfig = Some(createDiskConfig1)
    )
    gcpWorkspaceAppServiceInterp
      .createAppV2(userInfo, workspaceId, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    val appName2 = AppName("app2")
    val diskName2 = DiskName("newDiskName2")
    val createDiskConfig2 = PersistentDiskRequest(diskName2, None, None, Map.empty)
    val appReq2 = createAppRequest.copy(diskConfig = Some(createDiskConfig2))

    gcpWorkspaceAppServiceInterp
      .createAppV2(userInfo, workspaceId, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName3 = AppName("app3")
    val diskName3 = DiskName("newDiskName3")
    val createDiskConfig3 = PersistentDiskRequest(diskName3, None, None, Map.empty)
    val appReq3 = createAppRequest.copy(diskConfig = Some(createDiskConfig3))

    gcpWorkspaceAppServiceInterp
      .createAppV2(userInfo, workspaceId3, appName3, appReq3)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // New service where the userInfo is not part of the allowlist and therefore not a workspace reader
    val azureService2 = makeInterp(QueueFactory.makePublisherQueue(), allowListAuthProvider2, gcpWsmDao)

    val getApp1 =
      appServiceInterp.getAppV2(userInfo, workspaceId, appName1).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    getApp1.diskName shouldBe Some(diskName1)
    a[ForbiddenError] should be thrownBy azureService2
      .getAppV2(userInfo, workspaceId, appName1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp2 =
      appServiceInterp.getAppV2(userInfo, workspaceId, appName2).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    getApp2.diskName shouldBe Some(diskName2)
    a[ForbiddenError] should be thrownBy azureService2
      .getAppV2(userInfo, workspaceId, appName2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp3 =
      appServiceInterp.getAppV2(userInfo, workspaceId3, appName3).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    getApp3.diskName shouldBe Some(diskName3)
    a[ForbiddenError] should be thrownBy azureService2
      .getAppV2(userInfo, workspaceId3, appName3)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "V2 GCP - error creating Galaxy app with an existing disk that was formatted by Cromwell" in isolatedDbTest {
    val cluster = makeKubeCluster(0).save()
    val nodepool = makeNodepool(1, cluster.id).save()
    val cromwellApp = makeApp(1, nodepool.id).save()
    val disk =
      makePersistentDisk(None, formattedBy = Some(FormattedBy.Cromwell), appRestore = Some(Other(cromwellApp.id)))
        .copy(cloudContext = CloudContext.Gcp(GoogleProject(workspaceId.toString)))
        .save()
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val galaxyAppName = AppName("galaxy-app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val galaxyAppReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue, wsmDao = gcpWsmDao)
    val res = kubeServiceInterp
      .createAppV2(userInfo, workspaceId, galaxyAppName, galaxyAppReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.swap.toOption.get.getMessage shouldBe s"Persistent disk Gcp/${workspaceId}/disk is already formatted by CROMWELL"
  }

  it should "V2 Azure - deleteAllApp, update all status appropriately, and queue multiple messages" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val appName = AppName("app1")
    val appReq =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Cromwell, diskConfig = None)
    val appName2 = AppName("app2")
    val appReq2 =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Cromwell, diskConfig = None)

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }
    val appResultPreStatusUpdate2 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName2)
    }

    // we can't delete while its creating, so set it to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))

    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate2.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate2.get.nodepool.id, NodepoolStatus.Running))

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    val appResultPreDelete2 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName2)
    }
    appResultPreDelete2.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete2.get.app.auditInfo.destroyedDate shouldBe None

    kubeServiceInterp
      .deleteAllAppsV2(userInfo, workspaceId, false)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspaceId), includeDeleted = true)
    }

    clusterPostDelete.length shouldEqual 1
    val nodepools = clusterPostDelete.flatMap(_.nodepools)
    val apps = clusterPostDelete.flatMap(_.nodepools).flatMap(_.apps)
    nodepools.map(_.status) shouldEqual List(NodepoolStatus.Running)
    apps.map(_.status) shouldEqual List(AppStatus.Predeleting, AppStatus.Predeleting)

    // throw away create messages
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val messages = publisherQueue.tryTakeN(Some(2)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    messages.map(_.messageType) shouldBe List(LeoPubsubMessageType.DeleteAppV2, LeoPubsubMessageType.DeleteAppV2)
    val deleteAppMessages = messages.map(_.asInstanceOf[DeleteAppV2Message])
    deleteAppMessages.map(_.appId) should contain theSameElementsAs apps.map(_.id)
    deleteAppMessages.map(_.workspaceId) shouldBe List(workspaceId, workspaceId)
    deleteAppMessages.map(_.diskId) shouldBe List(None, None)
  }

  it should "V2 GCP - deleteAllApp, update all status appropriately, and queue multiple messages" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue, wsmDao = gcpWsmDao)

    val appName = AppName("app1")
    val appName2 = AppName("app2")
    val diskConfig1 = PersistentDiskRequest(DiskName("disk1"), None, None, Map.empty)
    val diskConfig2 = PersistentDiskRequest(DiskName("disk2"), None, None, Map.empty)

    val appReq =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Galaxy, diskConfig = Some(diskConfig1))
    val appReq2 =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Galaxy, diskConfig = Some(diskConfig2))

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }
    // we can't delete/create another while its creating, so set it to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(
      kubernetesClusterQuery.updateStatus(appResultPreStatusUpdate.get.cluster.id, KubernetesClusterStatus.Running)
    )

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate2 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName2)
    }

    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate2.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate2.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(
      kubernetesClusterQuery.updateStatus(appResultPreStatusUpdate2.get.cluster.id, KubernetesClusterStatus.Running)
    )

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    val appResultPreDelete2 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName2)
    }
    appResultPreDelete2.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete2.get.app.auditInfo.destroyedDate shouldBe None

    kubeServiceInterp
      .deleteAllAppsV2(userInfo, workspaceId, false)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspaceId), includeDeleted = true)
    }

    clusterPostDelete.length shouldEqual 1
    val nodepools = clusterPostDelete.flatMap(_.nodepools)
    val apps = clusterPostDelete.flatMap(_.nodepools).flatMap(_.apps)
    nodepools.map(_.status) shouldEqual List(NodepoolStatus.Running)
    apps.map(_.status) shouldEqual List(AppStatus.Predeleting, AppStatus.Predeleting)

    // throw away create messages
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val messages = publisherQueue.tryTakeN(Some(2)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    messages.map(_.messageType) shouldBe List(LeoPubsubMessageType.DeleteAppV2, LeoPubsubMessageType.DeleteAppV2)
    val deleteAppMessages = messages.map(_.asInstanceOf[DeleteAppV2Message])
    deleteAppMessages.map(_.appId) shouldBe apps.map(_.id)
    deleteAppMessages.map(_.workspaceId) shouldBe List(workspaceId, workspaceId)
    deleteAppMessages.map(_.diskId) shouldBe List(None, None)
  }

  it should "V2 - deleteAllApp, should fail and not update anything if there is a non-deletable app status appropriately" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val appName = AppName("app1")
    val appReq =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Cromwell, diskConfig = None)
    val appName2 = AppName("app2")
    val appReq2 =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Cromwell, diskConfig = None)

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    kubeServiceInterp
      .createAppV2(userInfo, workspaceId, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }
    val appResultPreStatusUpdate2 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName2)
    }

    // set this one to a non-deletable status
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Provisioning))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))

    // set this one to a deletable status
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate2.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate2.get.nodepool.id, NodepoolStatus.Running))

    val thrown = the[DeleteAllAppsCannotBePerformed] thrownBy {
      kubeServiceInterp
        .deleteAllAppsV2(userInfo, workspaceId, false)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspaceId), includeDeleted = true)
    }

    clusterPostDelete.length shouldEqual 1
    val nodepools = clusterPostDelete.flatMap(_.nodepools)
    val apps = clusterPostDelete.flatMap(_.nodepools).flatMap(_.apps)
    nodepools.map(_.status) shouldEqual List(NodepoolStatus.Running)
    apps.map(_.status).toSet shouldEqual Set(AppStatus.Running, AppStatus.Provisioning)

    // throw away create messages
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val messages = publisherQueue.tryTakeN(Some(2)).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    messages shouldBe List.empty
  }

  /** TODO: Once disks are supported on Azure Apps
   it should "error on delete if disk is in a status that cannot be deleted" in isolatedDbTest {
   val publisherQueue = QueueFactory.makePublisherQueue()
   val wsmClientProvider = new MockWsmClientProvider() {
   override def getDiskState(token: String, workspaceId: WorkspaceId, wsmResourceId: WsmControlledResourceId)(implicit
   ev: Ask[IO, AppContext]
   ): IO[WsmState] =
        IO.pure(WsmState(Some("CREATING")))
   }
   val appServiceInterp = makeInterp(publisherQueue, wsmClientProvider = wsmClientProvider)

   val appName = AppName("app1")
   val diskConfig = PersistentDiskRequest(DiskName("disk1"), None, None, Map.empty)
   val appReq =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Cromwell, diskConfig = Some(diskConfig))

   appServiceInterp
   .createAppV2(userInfo, workspaceId, appName, appReq)
   .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

   val appResultPreStatusUpdate = dbFutureValue {
   KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
   }

   // we can't delete while its creating, so set it to Running
   dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
   dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))

   val appResultPreDelete = dbFutureValue {
   KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
   }
   appResultPreDelete.get.app.status shouldEqual AppStatus.Running
   appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

   an[DiskCannotBeDeletedWsmException] should be thrownBy {
   appServiceInterp
   .deleteAppV2(userInfo, workspaceId, appName, true)
   .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
   }
   }
   */

  it should "error on delete if app subresource is in a status that cannot be deleted" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val wsmClientProvider = new MockWsmClientProvider() {
      override def getWsmState(token: String,
                               workspaceId: WorkspaceId,
                               wsmResourceId: WsmControlledResourceId,
                               wsmResourceType: WsmResourceType
      )(implicit ev: Ask[IO, AppContext], log: StructuredLogger[IO]): IO[WsmState] =
        IO.pure(WsmState(Some("CREATING")))
    }
    val appServiceInterp = makeInterp(publisherQueue, wsmClientProvider = wsmClientProvider)

    val appName = AppName("app1")
    val appReq =
      createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.Cromwell, diskConfig = None)

    appServiceInterp
      .createAppV2(userInfo, workspaceId, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }

    // we can't delete while its creating, so set it to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))

    // add database record
    dbFutureValue(
      appControlledResourceQuery
        .insert(
          appResultPreStatusUpdate.get.app.id.id,
          wsmResourceId,
          WsmResourceType.AzureDatabase,
          AppControlledResourceStatus.Creating
        )
    )

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    an[AppResourceCannotBeDeletedException] should be thrownBy {
      appServiceInterp
        .deleteAppV2(userInfo, workspaceId, appName, true)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail to create a V2 app if it is disabled" in {
    val appName = AppName("app1")
    val appReq = createAppRequest.copy(kubernetesRuntimeConfig = None, appType = AppType.HailBatch, diskConfig = None)

    val thrown = the[AppTypeNotEnabledException] thrownBy {
      appServiceInterp
        .createAppV2(userInfo, workspaceId, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    thrown.appType shouldBe AppType.HailBatch
  }

  "checkIfAppCreationIsAllowed" should "enable IntraNodeVisibility if customApp check is disabled" in {
    val interp = makeInterp(QueueFactory.makePublisherQueue(), enableCustomAppCheckFlag = false)
    val res =
      interp.checkIfAppCreationIsAllowed(userEmail, project, Uri.unsafeFromString("https://dummy"))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe ()
  }

  it should "should fail if app is not in allowed list for non high security projects" in {
    val resourceService = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO.pure(Some(Map(SECURITY_GROUP -> "other")))
    }
    val userEmail = WorkbenchEmail("allowlist")
    val interp = makeInterp(
      QueueFactory.makePublisherQueue(),
      enableCustomAppCheckFlag = true,
      googleResourceService = resourceService,
      customAppConfig = gkeCustomAppConfig.copy(customApplicationAllowList =
        CustomApplicationAllowListConfig(List(), List("https://dummy"))
      )
    )
    val res =
      interp.checkIfAppCreationIsAllowed(userEmail, project, Uri.unsafeFromString("https://dummy"))
    res.attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ForbiddenError] shouldBe true
  }

  it should "should fail if app is not in allowed list for high security projects" in {
    val resourceService = new FakeGoogleResourceService {
      override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
        IO.pure(Some(Map(SECURITY_GROUP -> "high")))
    }
    val userEmail = WorkbenchEmail("allowlist")
    val interp = makeInterp(
      QueueFactory.makePublisherQueue(),
      enableCustomAppCheckFlag = true,
      googleResourceService = resourceService,
      customAppConfig = gkeCustomAppConfig.copy(customApplicationAllowList =
        CustomApplicationAllowListConfig(List("https://dummy"), List())
      )
    )
    val res =
      interp.checkIfAppCreationIsAllowed(userEmail, project, Uri.unsafeFromString("https://dummy"))
    res.attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ForbiddenError] shouldBe true
  }

  def setupAppWithConfig(autodeleteEnabled: Boolean, autodeleteThreshold: Option[Int]): AppName = {
    val appName = AppName("pong")
    val createReq = createAppRequest.copy(
      diskConfig = Some(PersistentDiskRequest(diskName, None, None, Map.empty)),
      autodeleteEnabled = Some(autodeleteEnabled),
      autodeleteThreshold = autodeleteThreshold
    )

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, createReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val createdApp = appServiceInterp
      .getApp(userInfo, cloudContextGcp, appName)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // confirm assumptions

    createdApp.autodeleteEnabled shouldBe autodeleteEnabled
    createdApp.autodeleteThreshold shouldBe autodeleteThreshold

    appName
  }

  "updateAppConfig" should "allow enabling autodeletion by specifying both fields" in isolatedDbTest {
    val initialAutodeleteEnabled = false
    val appName = setupAppWithConfig(initialAutodeleteEnabled, None)

    val autodeleteThreshold = 1000
    val updateReq = UpdateAppConfigRequest(Some(true), Some(autodeleteThreshold))
    appServiceInterp
      .updateAppConfig(userInfo, cloudContextGcp, appName, updateReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // confirm the update

    val updatedApp = appServiceInterp
      .getApp(userInfo, cloudContextGcp, appName)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    updatedApp.autodeleteEnabled shouldBe true
    updatedApp.autodeleteThreshold shouldBe Some(autodeleteThreshold)
  }

  it should "allow enabling autodeletion by setting autodeleteEnabled=true if the existing config has a valid autodeleteThreshold" in isolatedDbTest {
    val initialAutodeleteEnabled = false
    val autodeleteThreshold = 1000
    val appName = setupAppWithConfig(initialAutodeleteEnabled, Some(autodeleteThreshold))

    val updateReq = UpdateAppConfigRequest(Some(true), None)
    appServiceInterp
      .updateAppConfig(userInfo, cloudContextGcp, appName, updateReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // confirm the update

    val updatedApp = appServiceInterp
      .getApp(userInfo, cloudContextGcp, appName)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    updatedApp.autodeleteEnabled shouldBe true
    updatedApp.autodeleteThreshold shouldBe Some(autodeleteThreshold)
  }

  it should "reject enabling autodeletion by setting autodeleteEnabled=true if the existing config has an invalid autodeleteThreshold" in isolatedDbTest {
    val initialAutodeleteEnabled = false
    val badAutodeleteThreshold = 0
    val appName = setupAppWithConfig(initialAutodeleteEnabled, Some(badAutodeleteThreshold))

    val updateReq = UpdateAppConfigRequest(Some(true), None)
    a[BadRequestException] should be thrownBy {
      appServiceInterp
        .updateAppConfig(userInfo, cloudContextGcp, appName, updateReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "reject enabling autodeletion if the threshold in the request is invalid" in isolatedDbTest {
    val initialAutodeleteEnabled = false
    val appName = setupAppWithConfig(initialAutodeleteEnabled, None)

    val badAutodeleteThreshold = 0
    val updateReq = UpdateAppConfigRequest(Some(true), Some(badAutodeleteThreshold))
    a[BadRequestException] should be thrownBy {
      appServiceInterp
        .updateAppConfig(userInfo, cloudContextGcp, appName, updateReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "reject disabling autodeletion if the threshold in the request is invalid" in isolatedDbTest {
    val initialAutodeleteEnabled = true
    val autodeleteThreshold = 1000
    val appName = setupAppWithConfig(initialAutodeleteEnabled, Some(autodeleteThreshold))

    val badAutodeleteThreshold = 0
    val updateReq = UpdateAppConfigRequest(Some(false), Some(badAutodeleteThreshold))
    a[BadRequestException] should be thrownBy {
      appServiceInterp
        .updateAppConfig(userInfo, cloudContextGcp, appName, updateReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "reject autodeletionEnabled=None if the threshold in the request is invalid" in isolatedDbTest {
    val initialAutodeleteEnabled = true
    val autodeleteThreshold = 1000
    val appName = setupAppWithConfig(initialAutodeleteEnabled, Some(autodeleteThreshold))

    val badAutodeleteThreshold = 0
    val updateReq = UpdateAppConfigRequest(None, Some(badAutodeleteThreshold))
    a[BadRequestException] should be thrownBy {
      appServiceInterp
        .updateAppConfig(userInfo, cloudContextGcp, appName, updateReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "allow disabling autodeletion by setting enabled = false" in isolatedDbTest {
    val initialAutodeleteEnabled = true
    val autodeleteThreshold = 1000
    val appName = setupAppWithConfig(initialAutodeleteEnabled, Some(autodeleteThreshold))

    val updateReq = UpdateAppConfigRequest(Some(false), None)
    appServiceInterp
      .updateAppConfig(userInfo, cloudContextGcp, appName, updateReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // confirm the update

    val updatedApp = appServiceInterp
      .getApp(userInfo, cloudContextGcp, appName)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    updatedApp.autodeleteEnabled shouldBe false
    updatedApp.autodeleteThreshold shouldBe Some(autodeleteThreshold)
  }

  it should "allow disabling autodeletion by setting enabled = false and specifying a new autodeleteThreshold" in isolatedDbTest {
    val initialAutodeleteEnabled = true
    val autodeleteThreshold = 1000
    val appName = setupAppWithConfig(initialAutodeleteEnabled, Some(autodeleteThreshold))

    val newThreshold = 2000
    val updateReq = UpdateAppConfigRequest(Some(false), Some(newThreshold))
    appServiceInterp
      .updateAppConfig(userInfo, cloudContextGcp, appName, updateReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // confirm the update

    val updatedApp = appServiceInterp
      .getApp(userInfo, cloudContextGcp, appName)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    updatedApp.autodeleteEnabled shouldBe false
    updatedApp.autodeleteThreshold shouldBe Some(newThreshold)
  }

  it should "allow changing autodeletion threshold when enableAutodelete=true" in isolatedDbTest {
    val initialAutodeleteEnabled = true
    val autodeleteThreshold = 1000
    val appName = setupAppWithConfig(initialAutodeleteEnabled, Some(autodeleteThreshold))

    val newAutodeleteThreshold = 2000
    val updateReq = UpdateAppConfigRequest(Some(true), Some(newAutodeleteThreshold))
    appServiceInterp
      .updateAppConfig(userInfo, cloudContextGcp, appName, updateReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // confirm the update

    val updatedApp = appServiceInterp
      .getApp(userInfo, cloudContextGcp, appName)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    updatedApp.autodeleteEnabled shouldBe true
    updatedApp.autodeleteThreshold shouldBe Some(newAutodeleteThreshold)
  }

  it should "allow changing autodeletion threshold when enableAutodelete=None" in isolatedDbTest {
    val initialAutodeleteEnabled = true
    val autodeleteThreshold = 1000
    val appName = setupAppWithConfig(initialAutodeleteEnabled, Some(autodeleteThreshold))

    val newAutodeleteThreshold = 2000
    val updateReq = UpdateAppConfigRequest(None, Some(newAutodeleteThreshold))
    appServiceInterp
      .updateAppConfig(userInfo, cloudContextGcp, appName, updateReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // confirm the update

    val updatedApp = appServiceInterp
      .getApp(userInfo, cloudContextGcp, appName)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    updatedApp.autodeleteEnabled shouldBe true
    updatedApp.autodeleteThreshold shouldBe Some(newAutodeleteThreshold)
  }

  it should "error if app not found" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    appServiceInterp
      .createApp(userInfo, cloudContextGcp, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val wrongApp = AppName("wrongApp")
    an[AppNotFoundException] should be thrownBy {
      appServiceInterp
        .updateAppConfig(userInfo, cloudContextGcp, wrongApp, UpdateAppConfigRequest(None, None))
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }
}
