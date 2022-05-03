package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import com.google.cloud.compute.v1.MachineType
import org.broadinstitute.dsde.workbench.google2.mock.{FakeGoogleComputeService, FakeGooglePublisher}
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.db.{
  KubernetesServiceDbQueries,
  TestComponent,
  appQuery,
  kubernetesClusterQuery,
  persistentDiskQuery,
  _
}
import org.broadinstitute.dsde.workbench.leonardo.model.BadRequestException
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{CreateAppMessage, DeleteAppMessage}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  ClusterNodepoolAction,
  LeoPubsubMessage,
  LeoPubsubMessageType
}
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsp.ChartVersion
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.AppRestore.{CromwellRestore, GalaxyRestore}

import scala.concurrent.ExecutionContext.Implicits.global

final class AppServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {

  //used when we care about queue state
  def makeInterp(queue: Queue[IO, LeoPubsubMessage]) =
    new LeoAppServiceInterp[IO](whitelistAuthProvider,
                                serviceAccountProvider,
                                leoKubernetesConfig,
                                queue,
                                FakeGoogleComputeService)
  val appServiceInterp = new LeoAppServiceInterp[IO](whitelistAuthProvider,
                                                     serviceAccountProvider,
                                                     leoKubernetesConfig,
                                                     QueueFactory.makePublisherQueue(),
                                                     FakeGoogleComputeService)

  it should "validate galaxy runtime requirements correctly" in ioAssertion {
    val project = GoogleProject("project1")
    val passComputeService = new FakeGoogleComputeService {
      override def getMachineType(project: GoogleProject, zone: ZoneName, machineTypeName: MachineTypeName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[MachineType]] =
        IO.pure(Some(MachineType.newBuilder().setName("pass").setMemoryMb(6 * 1024).setGuestCpus(4).build()))
    }
    val notEnoughMemoryComputeService = new FakeGoogleComputeService {
      override def getMachineType(project: GoogleProject, zone: ZoneName, machineTypeName: MachineTypeName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[MachineType]] =
        IO.pure(Some(MachineType.newBuilder().setName("notEnoughMemory").setMemoryMb(3 * 1024).setGuestCpus(4).build()))
    }
    val notEnoughCpuComputeService = new FakeGoogleComputeService {
      override def getMachineType(project: GoogleProject, zone: ZoneName, machineTypeName: MachineTypeName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[MachineType]] =
        IO.pure(Some(MachineType.newBuilder().setName("notEnoughMemory").setMemoryMb(6 * 1024).setGuestCpus(2).build()))
    }

    val passAppService = new LeoAppServiceInterp[IO](whitelistAuthProvider,
                                                     serviceAccountProvider,
                                                     leoKubernetesConfig,
                                                     QueueFactory.makePublisherQueue(),
                                                     passComputeService)
    val notEnoughMemoryAppService = new LeoAppServiceInterp[IO](whitelistAuthProvider,
                                                                serviceAccountProvider,
                                                                leoKubernetesConfig,
                                                                QueueFactory.makePublisherQueue(),
                                                                notEnoughMemoryComputeService)
    val notEnoughCpuAppService = new LeoAppServiceInterp[IO](whitelistAuthProvider,
                                                             serviceAccountProvider,
                                                             leoKubernetesConfig,
                                                             QueueFactory.makePublisherQueue(),
                                                             notEnoughCpuComputeService)

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

  it should "determine patch version bump correctly" in isolatedDbTest {
    val first = ChartVersion("0.8.0")
    val second = ChartVersion("0.8.2")
    LeoAppServiceInterp.isPatchVersionDifference(first, second) shouldBe (true)
  }

  it should "create an app and a new disk" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig), customEnvironmentVariables = customEnvVars)

    appServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusters = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(project))
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

  it should "create an app in a user's existing nodepool" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig), customEnvironmentVariables = customEnvVars)

    appServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    val appName2 = AppName("app2")
    val createDiskConfig2 = PersistentDiskRequest(DiskName("disk2"), None, None, Map.empty)
    val appReq2 =
      createAppRequest.copy(diskConfig = Some(createDiskConfig2), customEnvironmentVariables = customEnvVars)
    appServiceInterp
      .createApp(userInfo, project, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusters = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(project))
    }

    clusters.flatMap(_.nodepools).length shouldBe 1
    clusters.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 2

    clusters.flatMap(_.nodepools).flatMap(_.apps).map(_.appName).sortBy(_.value) shouldBe List(appName, appName2)
      .sortBy(_.value)
    val app1 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }.get

    val app2 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName2)
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
                            customEnvironmentVariables = customEnvVars)
    val appReqWithMoreNodes =
      defaultAppReq.copy(kubernetesRuntimeConfig = Some(nodepoolConfigWithMoreNodes), diskConfig = Some(diskConfig2))
    val appReqWithMoreCpuAndMem = defaultAppReq.copy(kubernetesRuntimeConfig = Some(nodepoolConfigWithMoreCpuAndMem),
                                                     diskConfig = Some(diskConfig3))
    val appReqWithAutoscalingDisabled =
      defaultAppReq.copy(kubernetesRuntimeConfig = Some(nodepoolConfigWithAutoscalingDisabled),
                         diskConfig = Some(diskConfig4))

    appServiceInterp
      .createApp(userInfo, project, appName1, defaultAppReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    appServiceInterp
      .createApp(userInfo, project, appName2, appReqWithMoreNodes)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    appServiceInterp
      .createApp(userInfo, project, appName3, appReqWithMoreCpuAndMem)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    appServiceInterp
      .createApp(userInfo, project, appName4, appReqWithAutoscalingDisabled)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusters = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(project))
    }

    clusters.flatMap(_.nodepools).length shouldBe 4
    clusters.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 4

    val nodepoolId1 =
      dbFutureValue(KubernetesServiceDbQueries.getActiveFullAppByName(project, appName1)).get.nodepool.id
    val nodepoolId2 =
      dbFutureValue(KubernetesServiceDbQueries.getActiveFullAppByName(project, appName2)).get.nodepool.id
    val nodepoolId3 =
      dbFutureValue(KubernetesServiceDbQueries.getActiveFullAppByName(project, appName3)).get.nodepool.id
    val nodepoolId4 =
      dbFutureValue(KubernetesServiceDbQueries.getActiveFullAppByName(project, appName4)).get.nodepool.id

    Set(nodepoolId1, nodepoolId2, nodepoolId3, nodepoolId4).size shouldBe 4 // each app has a distinct nodepool
  }

  it should "queue the proper message when creating an app and a new disk" in isolatedDbTest {

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig), customEnvironmentVariables = customEnvVars)

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)

    kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
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
      .copy(cloudContext = cloudContext)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val res = kubeServiceInterp
      .createApp(userInfo, project, appName, appReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.swap.toOption.get.getMessage shouldBe ("Disk is not formatted yet. Only disks previously used by galaxy app can be re-used to create a new galaxy app")
  }

  it should "error creating an app with an existing used disk when WORKSPACE_NAME is not specified" in isolatedDbTest {
    val customEnvVariables = Map(WORKSPACE_NAME_KEY -> "fake_ws")
    val cluster = makeKubeCluster(0).save()
    val nodepool = makeNodepool(1, cluster.id).save()
    val app = makeApp(1, nodepool.id, customEnvVariables).save()
    val disk = makePersistentDisk(None,
                                  formattedBy = Some(FormattedBy.Galaxy),
                                  appRestore = Some(GalaxyRestore(PvcId("pv-id"), PvcId("pv-id2"), app.id)))
      .copy(cloudContext = cloudContext)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig),
                                       customEnvironmentVariables = Map(WORKSPACE_NAME_KEY -> "fake_ws2"))

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val res = kubeServiceInterp
      .createApp(userInfo, project, appName, appReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    res.swap.toOption.get.getMessage shouldBe ("workspace name has to be the same as last used app in order to restore data from existing disk")
  }

  it should "create an app with an existing used disk" in isolatedDbTest {
    val customEnvVariables = Map(WORKSPACE_NAME_KEY -> "fake_ws")
    val cluster = makeKubeCluster(0).save()
    val nodepool = makeNodepool(1, cluster.id).save()
    val app = makeApp(1, nodepool.id, customEnvVariables).save()
    val disk = makePersistentDisk(None,
                                  appRestore = Some(GalaxyRestore(PvcId("pv-id"), PvcId("pv-id2"), app.id)),
                                  formattedBy = Some(FormattedBy.Galaxy))
      .copy(cloudContext = cloudContext)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq =
      createAppRequest.copy(diskConfig = Some(createDiskConfig), customEnvironmentVariables = customEnvVariables)

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val message = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message.messageType shouldBe LeoPubsubMessageType.CreateApp
    message.asInstanceOf[CreateAppMessage].createDisk shouldBe None

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }

    appResult.flatMap(_.app.appResources.disk.map(_.name)) shouldEqual Some(disk.name)
    appResult.map(_.app.appName) shouldEqual Some(appName)
  }

  it should "allow pet SA to create an app" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = CreateAppRequest(
      kubernetesRuntimeConfig = None,
      appType = AppType.Cromwell,
      diskConfig = Some(createDiskConfig),
      labels = Map.empty,
      customEnvironmentVariables = customEnvVars,
      descriptorPath = None,
      extraArgs = List.empty
    )

    appServiceInterp
      .createApp(petUserInfo, project, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val clusters = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(project))
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
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = CreateAppRequest(
      kubernetesRuntimeConfig = None,
      appType = AppType.Cromwell,
      diskConfig = Some(createDiskConfig),
      labels = Map.empty,
      customEnvironmentVariables = customEnvVars,
      descriptorPath = None,
      extraArgs = List.empty
    )

    appServiceInterp
      .createApp(petUserInfo, project, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp: GetAppResponse =
      appServiceInterp.getApp(petUserInfo, project, appName).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    getApp.appType shouldBe AppType.Cromwell
    getApp.diskName shouldBe Some(diskName)
    getApp.auditInfo.creator shouldBe userEmail
    getApp.customEnvironmentVariables shouldBe customEnvVars
  }

  it should "allow pet SA to delete an app" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val customEnvVars = Map("WORKSPACE_NAME" -> "testWorkspace")
    val appReq = CreateAppRequest(
      kubernetesRuntimeConfig = None,
      appType = AppType.Cromwell,
      diskConfig = Some(createDiskConfig),
      labels = Map.empty,
      customEnvironmentVariables = customEnvVars,
      descriptorPath = None,
      extraArgs = List.empty
    )

    kubeServiceInterp
      .createApp(petUserInfo, project, appName, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }

    // Set the app status and nodepool status to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))

    // Call deleteApp
    val params = DeleteAppRequest(petUserInfo, project, appName, true)
    kubeServiceInterp.deleteApp(params).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // Verify that request using pet SA was successful and app is marked to be deleted
    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Predeleting

    // Verify database state
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(project), includeDeleted = true)
    }
    clusterPostDelete.length shouldEqual 1
    val nodepool = clusterPostDelete.head.nodepools.head
    nodepool.status shouldEqual NodepoolStatus.Running
    nodepool.auditInfo.destroyedDate shouldBe None

    // throw away create message
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe a[CreateAppMessage]

    // Verify DeleteAppMessage message was generated
    publisherQueue.tryTake.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe a[Some[DeleteAppMessage]]
  }

  it should "error creating an app with an existing disk if no restore info found" in isolatedDbTest {
    val disk = makePersistentDisk(None, formattedBy = Some(FormattedBy.Galaxy))
      .copy(cloudContext = cloudContext)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val res = kubeServiceInterp
      .createApp(userInfo, project, appName, appReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.swap.toOption.get.getMessage shouldBe ("Existing disk found, but no restore info found in DB")
  }

  it should "error on creation of a galaxy app without a disk" in isolatedDbTest {
    val appName = AppName("app1")
    val appReq = createAppRequest.copy(diskConfig = None, appType = AppType.Galaxy)

    an[AppRequiresDiskException] should be thrownBy {
      appServiceInterp
        .createApp(userInfo, project, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "error creating Cromwell app with an existing disk if no restore info found" in isolatedDbTest {
    val disk = makePersistentDisk(None, formattedBy = Some(FormattedBy.Cromwell))
      .copy(cloudContext = cloudContext)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig), appType = AppType.Cromwell)

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val res = kubeServiceInterp
      .createApp(userInfo, project, appName, appReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.swap.toOption.get.getMessage shouldBe ("Existing disk found, but no restore info found in DB")
  }

  it should "error creating Galaxy app with an existing disk that was formatted by Cromwell" in isolatedDbTest {
    val cluster = makeKubeCluster(0).save()
    val nodepool = makeNodepool(1, cluster.id).save()
    val cromwellApp = makeApp(1, nodepool.id).save()
    val disk = makePersistentDisk(None,
                                  formattedBy = Some(FormattedBy.Cromwell),
                                  appRestore = Some(CromwellRestore(cromwellApp.id)))
      .copy(cloudContext = cloudContext)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val galaxyAppName = AppName("galaxy-app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val galaxyAppReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val res = kubeServiceInterp
      .createApp(userInfo, project, galaxyAppName, galaxyAppReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.swap.toOption.get.getMessage shouldBe ("Persistent disk dsp-leo-test/disk is already formatted by CROMWELL")
  }

  it should "error on creation of Cromwell app without a disk" in isolatedDbTest {
    val appName = AppName("cromwell-app1")
    val appReq = createAppRequest.copy(diskConfig = None, appType = AppType.Cromwell)

    an[AppRequiresDiskException] should be thrownBy {
      appServiceInterp
        .createApp(userInfo, project, appName, appReq)
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
                                  appRestore = Some(GalaxyRestore(PvcId("pv-id"), PvcId("pv-id2"), app.id)))
      .copy(cloudContext = cloudContext)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val appName1 = AppName("app1")
    val appName2 = AppName("app2")

    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    appServiceInterp
      .createApp(userInfo, project, appName1, appReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName1)
    }
    appResult.flatMap(_.app.appResources.disk.map(_.name)) shouldEqual Some(disk.name)
    appResult.map(_.app.appName) shouldEqual Some(appName1)

    //we need to update status from creating because we don't allow creation of apps while cluster is creating
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    a[DiskAlreadyAttachedException] should be thrownBy {
      appServiceInterp
        .createApp(userInfo, project, appName2, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "error on creation if an app with that name exists" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    appServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    an[AppAlreadyExistsException] should be thrownBy {
      appServiceInterp
        .createApp(userInfo, project, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "error on creation if the disk is too small" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(DiskName("new-disk"), Some(DiskSize(50)), None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    a[BadRequestException] should be thrownBy {
      appServiceInterp
        .createApp(userInfo, project, appName, appReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "delete an app and update status appropriately" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }

    //we can't delete while its creating, so set it to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    val params = DeleteAppRequest(userInfo, project, appName, false)
    kubeServiceInterp.deleteApp(params).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(project), includeDeleted = true)
    }

    clusterPostDelete.length shouldEqual 1
    val nodepool = clusterPostDelete.head.nodepools.head
    nodepool.status shouldEqual NodepoolStatus.Running
    val app = nodepool.apps.head
    app.status shouldEqual AppStatus.Predeleting

    //throw away create message
    publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val message = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message.messageType shouldBe LeoPubsubMessageType.DeleteApp
    val deleteAppMessage = message.asInstanceOf[DeleteAppMessage]
    deleteAppMessage.appId shouldBe app.id
    deleteAppMessage.project shouldBe project
    deleteAppMessage.diskId shouldBe None
  }

  it should "error on delete if app is in a status that cannot be deleted" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    appServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }

    //TODO: update this once create publishes pubsub message
    appResultPreDelete.get.app.status shouldEqual AppStatus.Precreating
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    val params = DeleteAppRequest(userInfo, project, appName, false)
    an[AppCannotBeDeletedException] should be thrownBy {
      appServiceInterp.deleteApp(params).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "delete an app in Error status" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }

    // Set the app status to Error, nodepool status to Running, and the disk status to Deleted to
    // simulate an error during app creation.
    // Note: if an app with a newly-created disk errors out, Leo will delete the disk along with the app.
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Error))
    dbFutureValue(nodepoolQuery.updateStatus(appResultPreStatusUpdate.get.nodepool.id, NodepoolStatus.Running))
    dbFutureValue(persistentDiskQuery.delete(appResultPreStatusUpdate.get.app.appResources.disk.get.id, Instant.now))

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Error
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None
    appResultPreDelete.get.nodepool.status shouldBe NodepoolStatus.Running
    appResultPreDelete.get.nodepool.auditInfo.destroyedDate shouldBe None
    appResultPreDelete.get.app.appResources.disk.get.status shouldBe DiskStatus.Deleted
    appResultPreDelete.get.app.appResources.disk.get.auditInfo.destroyedDate shouldBe defined

    // Call deleteApp
    val params = DeleteAppRequest(userInfo, project, appName, true)
    kubeServiceInterp.deleteApp(params).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // Verify database state
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(project), includeDeleted = true)
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
                                        diskConfig = Some(createDiskConfig1))
    val diskName2 = DiskName("newDiskName")
    val createDiskConfig2 = PersistentDiskRequest(diskName2, None, None, Map.empty)
    val appReq2 = createAppRequest.copy(diskConfig = Some(createDiskConfig2))

    appServiceInterp
      .createApp(userInfo, project, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    appServiceInterp
      .createApp(userInfo, project, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    appServiceInterp
      .createApp(userInfo, project2, appName3, appReq1)
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
      appServiceInterp.listApp(userInfo, Some(project), Map()).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listProject1Apps.length shouldBe 2
    listProject1Apps.map(_.appName) should contain(appName1)
    listProject1Apps.map(_.appName) should contain(appName2)

    val listProject2Apps =
      appServiceInterp.listApp(userInfo, Some(project2), Map()).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listProject2Apps.length shouldBe 1
    listProject2Apps.map(_.appName) should contain(appName3)

    val listProject3Apps =
      appServiceInterp
        .listApp(userInfo, Some(GoogleProject("fakeProject")), Map())
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    listProject3Apps.length shouldBe 0
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
      .createApp(userInfo, project, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val app1Result = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName1)
    }

    dbFutureValue(kubernetesClusterQuery.updateStatus(app1Result.get.cluster.id, KubernetesClusterStatus.Running))

    appServiceInterp
      .createApp(userInfo, project, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val app2Result = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName2)
    }
    app2Result.map(_.app.labels).get.toList should contain(label1)
    app2Result.map(_.app.labels).get.toList should contain(label2)

    appServiceInterp
      .createApp(userInfo, project2, appName3, appReq1)
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
    // Make apps belonging to different users than the calling user
    val res = for {
      savedCluster <- IO(makeKubeCluster(1).save())
      savedNodepool1 <- IO(makeNodepool(1, savedCluster.id).save())
      app1 = LeoLenses.appToCreator.set(WorkbenchEmail("a_different_user1@example.com"))(makeApp(1, savedNodepool1.id))
      _ <- IO(app1.save())

      savedNodepool2 <- IO(makeNodepool(2, savedCluster.id).save())
      app2 = LeoLenses.appToCreator.set(WorkbenchEmail("a_different_user2@example.com"))(makeApp(2, savedNodepool2.id))
      _ <- IO(app2.save())

      listResponse <- appServiceInterp.listApp(userInfo, None, Map.empty)
    } yield {
      // Since the calling user is whitelisted in the auth provider, it should return
      // the apps belonging to other users.
      listResponse.map(_.appName).toSet shouldBe Set(app1.appName, app2.appName)
    }

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
      .createApp(userInfo, project, appName1, appReq1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    appServiceInterp
      .createApp(userInfo, project, appName2, appReq2)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val diskName3 = DiskName("newDiskName2")
    val createDiskConfig3 = PersistentDiskRequest(diskName3, None, None, Map.empty)
    appServiceInterp
      .createApp(userInfo, project2, appName3, appReq1.copy(diskConfig = Some(createDiskConfig3)))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp1 =
      appServiceInterp.getApp(userInfo, project, appName1).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    getApp1.diskName shouldBe Some(diskName)

    val getApp2 =
      appServiceInterp.getApp(userInfo, project, appName2).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    getApp2.diskName shouldBe Some(diskName2)

    val getApp3 =
      appServiceInterp.getApp(userInfo, project2, appName3).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    getApp3.diskName shouldBe Some(diskName3)
  }

  it should "error on get app if an app does not exist" in isolatedDbTest {
    an[AppNotFoundException] should be thrownBy {
      appServiceInterp
        .getApp(userInfo, project, AppName("schrodingersApp"))
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "stop an app" in isolatedDbTest {
    val res = for {
      publisherQueue <- Queue.bounded[IO, LeoPubsubMessage](10)
      kubeServiceInterp = makeInterp(publisherQueue)

      savedCluster <- IO(makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      savedNodepool <- IO(makeNodepool(1, savedCluster.id).copy(status = NodepoolStatus.Running).save())
      savedApp <- IO(makeApp(1, savedNodepool.id).copy(status = AppStatus.Running).save())

      _ <- kubeServiceInterp.stopApp(userInfo, savedCluster.googleProject, savedApp.appName)
      _ <- withLeoPublisher(publisherQueue) {
        for {
          dbAppOpt <- KubernetesServiceDbQueries
            .getActiveFullAppByName(savedCluster.googleProject, savedApp.appName)
            .transaction
          msg <- publisherQueue.tryTake
        } yield {
          dbAppOpt.isDefined shouldBe true
          dbAppOpt.get.app.status shouldBe AppStatus.Stopping
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

  it should "start an app" in isolatedDbTest {
    val res = for {
      publisherQueue <- Queue.bounded[IO, LeoPubsubMessage](10)
      kubeServiceInterp = makeInterp(publisherQueue)

      savedCluster <- IO(makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save())
      savedNodepool <- IO(makeNodepool(1, savedCluster.id).copy(status = NodepoolStatus.Running).save())
      savedApp <- IO(makeApp(1, savedNodepool.id).copy(status = AppStatus.Stopped).save())

      _ <- kubeServiceInterp.startApp(userInfo, savedCluster.googleProject, savedApp.appName)
      _ <- withLeoPublisher(publisherQueue) {
        for {
          dbAppOpt <- KubernetesServiceDbQueries
            .getActiveFullAppByName(savedCluster.googleProject, savedApp.appName)
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

  private def withLeoPublisher(
    publisherQueue: Queue[IO, LeoPubsubMessage]
  )(validations: IO[Assertion]): IO[Assertion] = {
    val leoPublisher = new LeoPublisher[IO](publisherQueue, new FakeGooglePublisher)
    withInfiniteStream(leoPublisher.process, validations)
  }
}
