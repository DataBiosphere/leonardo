package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.implicits._
import cats.mtl.Ask
import com.google.cloud.compute.v1.Operation
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.container.v1
import com.google.protobuf.Timestamp
import fs2.Stream
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.PodStatus
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.google2.mock.{
  MockGKEService,
  FakeGoogleComputeService,
  MockComputePollOperation,
  MockKubernetesService => WbLibsMockKubernetesService
}
import org.broadinstitute.dsde.workbench.google2.{
  ComputePollOperation,
  DiskName,
  Event,
  GKEModels,
  KubernetesModels,
  MachineTypeName,
  MockGoogleDiskService,
  OperationName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{
  makeApp,
  makeKubeCluster,
  makeNodepool,
  makeService
}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.VM
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockGalaxyDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.ClusterInvalidState
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{IP, TraceId, WorkbenchEmail}
import org.broadinstitute.dsp.mocks.MockHelm
import org.mockito.Mockito.{verify, _}
import org.scalatest.concurrent._
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.http.service.AppNotFoundException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Left

class LeoPubsubMessageSubscriberSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with TestComponent
    with Matchers
    with MockitoSugar
    with Eventually
    with LeonardoTestSuite {

  val mockWelderDAO = mock[WelderDAO[IO]]
  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO() {
    override def isGroupMember(groupEmail: WorkbenchEmail,
                               memberEmail: WorkbenchEmail): scala.concurrent.Future[Boolean] =
      scala.concurrent.Future.successful(true)
  }
  val gdDAO = new MockGoogleDataprocDAO
  val storageDAO = new MockGoogleStorageDAO
  // Kubernetes doesn't actually create a new Service Account when calling googleIamDAO
  val iamDAOKubernetes = new MockGoogleIamDAO {
    override def addIamPolicyBindingOnServiceAccount(serviceAccountProject: GoogleProject,
                                                     serviceAccountEmail: WorkbenchEmail,
                                                     memberEmail: WorkbenchEmail,
                                                     rolesToAdd: Set[String]): Future[Unit] = Future.successful(())
  }
  val iamDAO = new MockGoogleIamDAO
  val projectDAO = new MockGoogleProjectDAO
  val authProvider = mock[LeoAuthProvider[IO]]
  val currentTime = Instant.now
  val timestamp = Timestamp.newBuilder().setSeconds(now.toSeconds).build()

  val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
    new MockGoogleStorageDAO
  }

  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig, clusterResourcesConfig)
  val bucketHelper =
    new BucketHelper[IO](bucketHelperConfig, FakeGoogleStorageService, serviceAccountProvider, blocker)

  val vpcInterp =
    new VPCInterpreter[IO](Config.vpcInterpreterConfig,
                           projectDAO,
                           FakeGoogleComputeService,
                           new MockComputePollOperation)

  val dataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                   bucketHelper,
                                                   vpcInterp,
                                                   gdDAO,
                                                   FakeGoogleComputeService,
                                                   MockGoogleDiskService,
                                                   mockGoogleDirectoryDAO,
                                                   iamDAO,
                                                   projectDAO,
                                                   mockWelderDAO,
                                                   blocker)
  val gceInterp = new GceInterpreter[IO](Config.gceInterpreterConfig,
                                         bucketHelper,
                                         vpcInterp,
                                         FakeGoogleComputeService,
                                         MockGoogleDiskService,
                                         mockWelderDAO,
                                         blocker)

  implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)

  val runningCluster = makeCluster(1).copy(
    serviceAccount = serviceAccount,
    asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(hostIp = None)),
    status = RuntimeStatus.Running,
    dataprocInstances = Set(masterInstance, workerInstance1, workerInstance2)
  )

  val stoppedCluster = makeCluster(2).copy(
    serviceAccount = serviceAccount,
    asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(hostIp = None)),
    status = RuntimeStatus.Stopped
  )

  it should "handle CreateRuntimeMessage and create cluster" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(
        makeCluster(1)
          .copy(asyncRuntimeFields = None, status = RuntimeStatus.Creating, serviceAccount = serviceAccount)
          .save()
      )
      tr <- traceId.ask[TraceId]
      gceRuntimeConfigRequest = LeoLenses.runtimeConfigPrism.getOption(gceRuntimeConfig).get
      _ <- leoSubscriber.messageResponder(CreateRuntimeMessage.fromRuntime(runtime, gceRuntimeConfigRequest, Some(tr)))
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.asyncRuntimeFields shouldBe 'defined
      updatedRuntime.get.asyncRuntimeFields.get.stagingBucket.value should startWith("leostaging")
      updatedRuntime.get.asyncRuntimeFields.get.hostIp shouldBe None
      updatedRuntime.get.asyncRuntimeFields.get.operationName.value shouldBe "opName"
      updatedRuntime.get.asyncRuntimeFields.get.googleId.value shouldBe "target"
      updatedRuntime.get.runtimeImages.map(_.imageType) should contain(VM)
    }

    res.unsafeRunSync()
  }

  it should "handle DeleteRuntimeMessage and delete cluster" in isolatedDbTest {
    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask[TraceId]
      monitor = new MockRuntimeMonitor {
        override def pollCheck(a: CloudService)(
          googleProject: GoogleProject,
          runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
          operation: com.google.cloud.compute.v1.Operation,
          action: RuntimeStatus
        )(implicit ev: Ask[IO, TraceId]): IO[Unit] =
          clusterQuery.completeDeletion(runtime.id, Instant.now()).transaction
      }
      queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
      leoSubscriber = makeLeoSubscriber(runtimeMonitor = monitor, asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

      _ <- leoSubscriber.messageResponder(DeleteRuntimeMessage(runtime.id, None, Some(tr)))
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        clusterQuery
          .getClusterStatus(runtime.id)
          .transaction
          .map(status => status shouldBe (Some(RuntimeStatus.Deleted)))
      )
    } yield ()

    res.unsafeRunSync()
  }

  it should "delete disk when handling DeleteRuntimeMessage when autoDeleteDisks is set" in isolatedDbTest {
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      disk <- makePersistentDisk(None, Some(FormattedBy.GCE)).save()
      runtimeConfig = RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                                    bootDiskSize = DiskSize(50),
                                                    persistentDiskId = Some(disk.id))

      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(runtimeConfig))
      tr <- traceId.ask[TraceId]
      _ <- leoSubscriber.messageResponder(
        DeleteRuntimeMessage(runtime.id, Some(disk.id), Some(tr))
      )
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        persistentDiskQuery.getStatus(disk.id).transaction.map(status => status shouldBe (Some(DiskStatus.Deleted)))
      )
    } yield ()

    res.unsafeRunSync()
  }

  it should "persist delete disk error when if fail to delete disk" in isolatedDbTest {
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val pollOperation = new MockComputePollOperation {
      override def getZoneOperation(project: GoogleProject, zoneName: ZoneName, operationName: OperationName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Operation] = IO.pure(
        Operation.newBuilder().setId("op").setName("opName").setTargetId("target").setStatus("PENDING").build()
      )
    }
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, computePollOperation = pollOperation)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      disk <- makePersistentDisk(None, Some(FormattedBy.GCE)).save()
      runtimeConfig = RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                                    bootDiskSize = DiskSize(50),
                                                    persistentDiskId = Some(disk.id))

      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(runtimeConfig))
      tr <- traceId.ask[TraceId]
      _ <- leoSubscriber.messageResponder(DeleteRuntimeMessage(runtime.id, Some(disk.id), Some(tr)))
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        clusterErrorQuery.get(runtime.id).transaction.map { error =>
          val dummyNow = Instant.now()
          error.head.copy(timestamp = dummyNow) shouldBe RuntimeError(s"Fail to delete ${disk.name} in a timely manner",
                                                                      None,
                                                                      dummyNow)
        }
      )
    } yield ()

    res.unsafeRunSync()
  }

  it should "not handle DeleteRuntimeMessage when cluster is not in Deleting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask[TraceId]
      message = DeleteRuntimeMessage(runtime.id, None, Some(tr))
      attempt <- leoSubscriber.messageResponder(message).attempt
    } yield {
      attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))
    }

    res.unsafeRunSync()
  }

  it should "handle StopRuntimeMessage and stop cluster" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Stopping).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask[TraceId]

      _ <- leoSubscriber.messageResponder(StopRuntimeMessage(runtime.id, Some(tr)))
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopping
    }

    res.unsafeRunSync()
  }

  it should "not handle StopRuntimeMessage when cluster is not in Stopping status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask[TraceId]
      message = StopRuntimeMessage(runtime.id, Some(tr))
      attempt <- leoSubscriber.messageResponder(message).attempt
    } yield {
      attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))
    }

    res.unsafeRunSync()
  }

  it should "handle StartRuntimeMessage and start cluster" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Starting).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask[TraceId]

      _ <- leoSubscriber.messageResponder(StartRuntimeMessage(runtime.id, Some(tr)))
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Starting
    }

    res.unsafeRunSync()
  }

  it should "not handle StartRuntimeMessage when cluster is not in Starting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask[TraceId]
      message = StartRuntimeMessage(runtime.id, Some(tr))
      attempt <- leoSubscriber.messageResponder(message).attempt
    } yield {
      attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))
    }

    res.unsafeRunSync()
  }

  it should "handle UpdateRuntimeMessage, resize dataproc cluster and setting DB status properly" in isolatedDbTest {
    val monitor = new MockRuntimeMonitor {
      override def pollCheck(a: CloudService)(
        googleProject: GoogleProject,
        runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
        operation: com.google.cloud.compute.v1.Operation,
        action: RuntimeStatus
      )(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.never

      override def process(
        a: CloudService
      )(runtimeId: Long, action: RuntimeStatus)(implicit ev: Ask[IO, TraceId]): Stream[IO, Unit] =
        Stream.eval(clusterQuery.setToRunning(runtimeId, IP("0.0.0.0"), Instant.now).transaction.void)
    }
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val leoSubscriber = makeLeoSubscriber(runtimeMonitor = monitor, asyncTaskQueue = queue)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      runtime <- IO(
        makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(defaultDataprocRuntimeConfig)
      )
      tr <- traceId.ask[TraceId]
      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id, None, false, None, Some(100), None, Some(tr))
      )
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      updatedRuntimeConfig <- updatedRuntime.traverse(r =>
        RuntimeConfigQueries.getRuntimeConfig(r.runtimeConfigId).transaction
      )
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        clusterQuery.getClusterStatus(runtime.id).transaction.map(s => s shouldBe Some(RuntimeStatus.Running))
      )
    } yield {
      updatedRuntimeConfig.get.asInstanceOf[RuntimeConfig.DataprocConfig].numberOfWorkers shouldBe 100
    }

    res.unsafeRunSync()
  }

  it should "handle UpdateRuntimeMessage and stop the cluster when there's a machine type change" in isolatedDbTest {
    val monitor = new MockRuntimeMonitor {
      override def pollCheck(a: CloudService)(
        googleProject: GoogleProject,
        runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
        operation: com.google.cloud.compute.v1.Operation,
        action: RuntimeStatus
      )(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.never
    }
    val leoSubscriber = makeLeoSubscriber(monitor)

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask[TraceId]

      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id, Some(MachineTypeName("n1-highmem-64")), true, None, None, None, Some(tr))
      )
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      updatedRuntimeConfig <- updatedRuntime.traverse(r =>
        RuntimeConfigQueries.getRuntimeConfig(r.runtimeConfigId).transaction
      )
    } yield {
      // runtime should be Stopping
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopping
      // machine type should not be updated yet
      updatedRuntimeConfig shouldBe 'defined
      updatedRuntimeConfig.get.machineType shouldBe MachineTypeName("n1-standard-4")
    }

    res.unsafeRunSync()
  }

  it should "handle UpdateRuntimeMessage and go through a stop-start transition for machine type" in isolatedDbTest {
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask[TraceId]

      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id, Some(MachineTypeName("n1-highmem-64")), true, None, None, None, Some(tr))
      )
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        for {
          updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
          updatedRuntimeConfig <- updatedRuntime.traverse(r =>
            RuntimeConfigQueries.getRuntimeConfig(r.runtimeConfigId).transaction
          )
          patchInProgress <- patchQuery.isInprogress(runtime.id).transaction
        } yield {
          // runtime should be Starting after having gone through a stop -> update -> start
          updatedRuntime shouldBe 'defined
          updatedRuntime.get.status shouldBe RuntimeStatus.Starting
          // machine type should be updated
          updatedRuntimeConfig shouldBe 'defined
          updatedRuntimeConfig.get.machineType shouldBe MachineTypeName("n1-highmem-64")
          patchInProgress shouldBe false
        }
      )
    } yield ()

    res.unsafeRunSync()
  }

  it should "handle UpdateRuntimeMessage and restart runtime for persistent disk size update" in isolatedDbTest {
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      disk <- makePersistentDisk(None).copy(size = DiskSize(100)).save()
      runtime <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Running)
          .saveWithRuntimeConfig(gceWithPdRuntimeConfig.copy(persistentDiskId = Some(disk.id)))
      )
      tr <- traceId.ask[TraceId]

      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id,
                             None,
                             true,
                             Some(DiskUpdate.PdSizeUpdate(disk.id, disk.name, DiskSize(200))),
                             None,
                             None,
                             Some(tr))
      )

      assert = for {
        updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
        updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
      } yield {
        // runtime should be Starting after having gone through a stop -> start
        updatedRuntime shouldBe 'defined
        updatedRuntime.get.status shouldBe RuntimeStatus.Starting
        // machine type should be updated
        updatedDisk shouldBe 'defined
        updatedDisk.get.size shouldBe DiskSize(200)
      }

      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        assert
      )
    } yield ()

    res.unsafeRunSync()
  }

  it should "update diskSize should trigger a stop-start transition" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Stopped).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask[TraceId]

      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id,
                             Some(MachineTypeName("n1-highmem-64")),
                             false,
                             Some(DiskUpdate.NoPdSizeUpdate(DiskSize(1024))),
                             None,
                             None,
                             Some(tr))
      )
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      updatedRuntimeConfig <- updatedRuntime.traverse(r =>
        RuntimeConfigQueries.getRuntimeConfig(r.runtimeConfigId).transaction
      )
    } yield {
      // runtime should still be Stopped
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopped
      // machine type and disk size should be updated
      updatedRuntimeConfig shouldBe 'defined
      updatedRuntimeConfig.get.machineType shouldBe MachineTypeName("n1-highmem-64")
      updatedRuntimeConfig.get.asInstanceOf[RuntimeConfig.GceConfig].diskSize shouldBe DiskSize(1024)
    }

    res.unsafeRunSync()
  }

  it should "handle CreateDiskMessage and create disk" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
      tr <- traceId.ask[TraceId]
      now <- IO(Instant.now)
      _ <- leoSubscriber.messageResponder(CreateDiskMessage.fromDisk(disk, Some(tr)))
      updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      updatedDisk shouldBe 'defined
      updatedDisk.get.googleId.get.value shouldBe "target"
    }

    res.unsafeRunSync()
  }

  it should "handle DeleteDiskMessage and delete disk" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Deleting).save()
      tr <- traceId.ask[TraceId]

      _ <- leoSubscriber.messageResponder(DeleteDiskMessage(disk.id, Some(tr)))
      updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      updatedDisk shouldBe 'defined
      updatedDisk.get.status shouldBe DiskStatus.Deleting
    }

    res.unsafeRunSync()
  }

  it should "handle DeleteDiskMessage when disk is not in Deleting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Ready).save()
      tr <- traceId.ask[TraceId]
      message = DeleteDiskMessage(disk.id, Some(tr))
      attempt <- leoSubscriber.messageResponder(message).attempt
    } yield {
      attempt shouldBe Right(())
    }

    res.unsafeRunSync()
  }

  it should "handle UpdateDiskMessage and update disk size" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Ready).save()
      tr <- traceId.ask[TraceId]

      _ <- leoSubscriber.messageResponder(UpdateDiskMessage(disk.id, DiskSize(550), Some(tr)))
      updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      updatedDisk shouldBe 'defined
      //TODO: fix tests
//      updatedDisk.get.size shouldBe DiskSize(550)
    }

    res.unsafeRunSync()
  }

  it should "handle create app message with a create cluster" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      getCluster.nodepools.size shouldBe 2
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getApp.app.errors shouldBe List()
      getApp.app.status shouldBe AppStatus.Running
      getApp.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
      getApp.cluster.status shouldBe KubernetesClusterStatus.Running
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.cluster.asyncFields shouldBe Some(
        KubernetesClusterAsyncFields(IP("1.2.3.4"),
                                     IP("0.0.0.0"),
                                     NetworkFields(Config.vpcConfig.networkName,
                                                   Config.vpcConfig.subnetworkName,
                                                   Config.vpcConfig.subnetworkIpRange))
      )
      getDisk.status shouldBe DiskStatus.Ready
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        Some(disk.id),
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  it should "be able to create multiple apps in a cluster" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedNodepool2 = makeNodepool(2, savedCluster1.id).save()
    val disk1 = makePersistentDisk(Some(DiskName("disk1"))).save().unsafeRunSync()
    val disk2 = makePersistentDisk(Some(DiskName("disk2"))).save().unsafeRunSync()

    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk1),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()
    val makeApp2 = makeApp(2, savedNodepool2.id)
    val savedApp2 = makeApp2
      .copy(appResources =
        makeApp2.appResources.copy(
          disk = Some(disk2),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val assertions = for {
      getAppOpt1 <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getAppOpt2 <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp2.appName)
        .transaction
      getApp1 = getAppOpt1.get
      getApp2 = getAppOpt2.get
    } yield {
      getApp1.cluster.status shouldBe KubernetesClusterStatus.Running
      getApp2.cluster.status shouldBe KubernetesClusterStatus.Running
      getApp1.nodepool.status shouldBe NodepoolStatus.Running
      getApp2.nodepool.status shouldBe NodepoolStatus.Running
      getApp1.app.errors shouldBe List()
      getApp1.app.status shouldBe AppStatus.Running
      getApp1.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
      getApp1.cluster.asyncFields shouldBe Some(
        KubernetesClusterAsyncFields(IP("1.2.3.4"),
                                     IP("0.0.0.0"),
                                     NetworkFields(Config.vpcConfig.networkName,
                                                   Config.vpcConfig.subnetworkName,
                                                   Config.vpcConfig.subnetworkIpRange))
      )
      getApp2.app.errors shouldBe List()
      getApp2.app.status shouldBe AppStatus.Running
      getApp2.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg1 = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        Some(disk1.id),
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      msg2 = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateNodepool(savedNodepool2.id)),
        savedApp2.id,
        savedApp2.appName,
        Some(disk2.id),
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg1)
      _ <- leoSubscriber.handleCreateAppMessage(msg2)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  //handle an error in createCluster
  it should "error on create if cluster doesn't exist" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.CreateGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Cluster)
      getApp.app.status shouldBe AppStatus.Error
      //we shouldn't see an error status here because the cluster we passed doesn't exist
      getApp.cluster.status shouldBe KubernetesClusterStatus.Unspecified
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = CreateAppMessage(
        project,
        Some(
          ClusterNodepoolAction.CreateClusterAndNodepool(KubernetesClusterLeoId(-1),
                                                         NodepoolLeoId(-1),
                                                         NodepoolLeoId(-1))
        ),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()
    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  //handle an error in createNodepool
  //update error table and status
  it should "error on create if default nodepool doesn't exist" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppByName(savedCluster1.googleProject, savedApp1.id, includeDeletedClusterApps = true)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.CreateGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Cluster)
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, NodepoolLeoId(-1), NodepoolLeoId(-2))),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  it should "error on create if user nodepool doesn't exist" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppByName(savedCluster1.googleProject, savedApp1.id, includeDeletedClusterApps = true)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.CreateGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Cluster)
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, NodepoolLeoId(-2))),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "error on create if app doesn't exist" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val disk1 = makePersistentDisk(Some(DiskName("disk1"))).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk1),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateNodepool(savedNodepool1.id)),
        savedApp1.id,
        AppName("fakeapp"),
        Some(disk1.id),
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
    } yield ()

    the[AppNotFoundException] thrownBy {
      res.unsafeRunSync()
    }
  }

  it should "handle error in createApp if createDisk is specified with no disk" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.CreateGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Disk)
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateNodepool(savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        Some(DiskId(-1)),
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "delete app without disk" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Deleted
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             None,
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  //delete app and not delete disk when specified
  //update app status and disk id
  it should "delete app and not delete disk when specified" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Deleted
      getApp.app.appResources.disk shouldBe None
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
      getDisk.status shouldBe DiskStatus.Ready
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             None,
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  it should "delete app and delete disk when specified" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).copy(status = DiskStatus.Deleting).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getApp.app.errors shouldBe List()
      getApp.app.status shouldBe AppStatus.Deleted
      getApp.app.appResources.disk shouldBe None
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
      getDisk.status shouldBe DiskStatus.Deleted
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             Some(disk.id),
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  it should "handle an error in delete app" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val mockKubernetesService = new MockKubernetesService(PodStatus.Failed) {
      override def deleteNamespace(
        clusterId: GKEModels.KubernetesClusterId,
        namespace: KubernetesModels.KubernetesNamespace
      )(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.raiseError(new Exception("test error"))
    }
    val makeGKEInterp = for {
      lock <- nodepoolLock
    } yield new GKEInterpreter[IO](Config.gkeInterpConfig,
                                   vpcInterp,
                                   MockGKEService,
                                   mockKubernetesService,
                                   MockHelm,
                                   MockGalaxyDAO,
                                   credentials,
                                   iamDAOKubernetes,
                                   blocker,
                                   lock)

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.status shouldBe AppStatus.Error
      getApp.app.errors.map(_.action) should contain(ErrorAction.DeleteGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.App)
      getApp.nodepool.status shouldBe NodepoolStatus.Unspecified
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             None,
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, makeGKEInterp = makeGKEInterp)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "handle an error in delete nodepool" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val exceptionMessage = "test exception"
    val mockAckConsumer = mock[AckReplyConsumer]

    val mockGKEService = new MockGKEService {
      override def deleteNodepool(nodepoolId: GKEModels.NodepoolId)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[v1.Operation]] = IO.raiseError(new Exception(exceptionMessage))
    }
    val makeGKEInterp = for {
      lock <- nodepoolLock
    } yield new GKEInterpreter[IO](Config.gkeInterpConfig,
                                   vpcInterp,
                                   mockGKEService,
                                   new MockKubernetesService(PodStatus.Succeeded),
                                   MockHelm,
                                   MockGalaxyDAO,
                                   credentials,
                                   iamDAOKubernetes,
                                   blocker,
                                   lock)

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.DeleteGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Nodepool)
      getApp.app.status shouldBe AppStatus.Error
      getApp.nodepool.status shouldBe NodepoolStatus.Error
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             None,
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, makeGKEInterp = makeGKEInterp)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  //error on delete disk if disk doesn't exist
  it should "handle an error in delete app if delete disk = true and no disk exists" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.DeleteGalaxyApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Disk)
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
      getApp.app.status shouldBe AppStatus.Error
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             Some(DiskId(-1)),
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "be able to handle batchCreateNodepool message" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Precreating).save()
    val savedNodepool2 = makeNodepool(3, savedCluster1.id).copy(status = NodepoolStatus.Precreating).save()

    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getMinimalCluster <- kubernetesClusterQuery.getMinimalActiveClusterByName(savedCluster1.googleProject).transaction
    } yield {
      getMinimalCluster.get.status shouldBe KubernetesClusterStatus.Running
      getMinimalCluster.get.nodepools.size shouldBe 3
      getMinimalCluster.get.nodepools.filter(_.isDefault).size shouldBe 1
      getMinimalCluster.get.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getMinimalCluster.get.nodepools.filterNot(_.isDefault).size shouldBe 2
      getMinimalCluster.get.nodepools.filterNot(_.isDefault).map(_.status).distinct.size shouldBe 1
      getMinimalCluster.get.nodepools
        .filterNot(_.isDefault)
        .map(_.status)
        .distinct
        .head shouldBe NodepoolStatus.Unclaimed
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = BatchNodepoolCreateMessage(savedCluster1.id,
                                       List(savedNodepool1.id, savedNodepool2.id),
                                       savedCluster1.googleProject,
                                       Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    assertions.unsafeRunSync()
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "be able to create app with a pre-created nodepool" in isolatedDbTest {
    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      getCluster.nodepools.size shouldBe 2
      //we shouldn't update the default nodepool status here, its already created
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Unspecified
      getApp.app.errors shouldBe List()
      getApp.app.status shouldBe AppStatus.Running
      getApp.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
      getApp.cluster.status shouldBe KubernetesClusterStatus.Running
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getDisk.status shouldBe DiskStatus.Ready
    }
    val res = for {
      tr <- traceId.ask[TraceId]
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        None,
        savedApp1.id,
        savedApp1.appName,
        Some(disk.id),
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  it should "error if nodepools in batch create nodepool message are not in db" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedCluster2 = makeKubeCluster(2).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Precreating).save()
    val savedNodepool2 = makeNodepool(3, savedCluster1.id).copy(status = NodepoolStatus.Precreating).save()
    val savedNodepool3 = makeNodepool(4, savedCluster2.id).copy(status = NodepoolStatus.Unspecified).save()

    val mockAckConsumer = mock[AckReplyConsumer]

    val assertions = for {
      getMinimalCluster <- kubernetesClusterQuery.getMinimalActiveClusterByName(savedCluster1.googleProject).transaction
    } yield {
      getMinimalCluster.get.status shouldBe KubernetesClusterStatus.Error
      getMinimalCluster.get.nodepools.size shouldBe 3
      getMinimalCluster.get.nodepools.count(_.isDefault) shouldBe 1
      getMinimalCluster.get.nodepools.filterNot(_.isDefault).size shouldBe 2
      getMinimalCluster.get.nodepools.filterNot(_.isDefault).map(_.status).distinct.size shouldBe 1
      //we should not have updated the status here, since the nodepools given were faulty
      getMinimalCluster.get.nodepools
        .filterNot(_.isDefault)
        .map(_.status)
        .distinct
        .head shouldBe NodepoolStatus.Precreating
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = BatchNodepoolCreateMessage(savedCluster1.id,
                                       List(savedNodepool1.id, savedNodepool2.id, savedNodepool3.id),
                                       savedCluster1.googleProject,
                                       Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  it should "clean-up google resources on error" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    //using a var here to simplify test code
    //we could use mockito for this functionality, but it would be overly complicated since we wish to override other functionality of the mock as well
    var deleteCalled = false

    val mockKubernetesService = new MockKubernetesService(PodStatus.Succeeded) {
      override def createServiceAccount(
        clusterId: GKEModels.KubernetesClusterId,
        serviceAccount: KubernetesModels.KubernetesServiceAccount,
        namespaceName: KubernetesModels.KubernetesNamespace
      )(implicit ev: Ask[IO, TraceId]): IO[Unit] =
        IO.raiseError(new Exception("this is an intentional test exception"))

      override def deleteNamespace(
        clusterId: GKEModels.KubernetesClusterId,
        namespace: KubernetesModels.KubernetesNamespace
      )(implicit ev: Ask[IO, TraceId]): IO[Unit] =
        IO {
          deleteCalled = true
        }
    }

    val makeGKEInterp = for {
      lock <- nodepoolLock
    } yield new GKEInterpreter[IO](Config.gkeInterpConfig,
                                   vpcInterp,
                                   MockGKEService,
                                   mockKubernetesService,
                                   MockHelm,
                                   MockGalaxyDAO,
                                   credentials,
                                   iamDAOKubernetes,
                                   blocker,
                                   lock)

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppByName(savedCluster1.googleProject, savedApp1.id)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      //only the default should be left, the other has been deleted
      getCluster.nodepools.size shouldBe 1
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getApp.app.errors.size shouldBe 1
      getApp.app.status shouldBe AppStatus.Error
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
      getApp.app.auditInfo.destroyedDate shouldBe None
      getDisk.status shouldBe DiskStatus.Deleted
      deleteCalled shouldBe true
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        Some(disk.id),
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, makeGKEInterp = makeGKEInterp)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions, maxRetry = 50)
    } yield ()

    res.unsafeRunSync()
  }

  it should "not delete a disk that already existing on error" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val mockKubernetesService = new MockKubernetesService(PodStatus.Succeeded) {
      override def createServiceAccount(
        clusterId: GKEModels.KubernetesClusterId,
        serviceAccount: KubernetesModels.KubernetesServiceAccount,
        namespaceName: KubernetesModels.KubernetesNamespace
      )(implicit ev: Ask[IO, TraceId]): IO[Unit] =
        IO.raiseError(new Exception("this is an intentional test exception"))
    }

    val makeGKEInterp = for {
      lock <- nodepoolLock
    } yield new GKEInterpreter[IO](Config.gkeInterpConfig,
                                   vpcInterp,
                                   MockGKEService,
                                   mockKubernetesService,
                                   MockHelm,
                                   MockGalaxyDAO,
                                   credentials,
                                   iamDAOKubernetes,
                                   blocker,
                                   lock)

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppByName(savedCluster1.googleProject, savedApp1.id)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      //only the default should be left, the other has been deleted
      getCluster.nodepools.size shouldBe 1
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getApp.app.errors.size shouldBe 1
      getApp.app.status shouldBe AppStatus.Error
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
      getDisk.status shouldBe disk.status
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, makeGKEInterp = makeGKEInterp)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions, maxRetry = 50)
    } yield ()

    res.unsafeRunSync()
  }

  it should "delete a cluster and put that app in error status on cluster error" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()
    val mockAckConsumer = mock[AckReplyConsumer]

    val mockGKEService = new MockGKEService {
      override def createCluster(request: GKEModels.KubernetesCreateClusterRequest)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[com.google.api.services.container.model.Operation]] = IO.raiseError(new Exception("test exception"))
    }

    val makeGKEInterp = for {
      lock <- nodepoolLock
    } yield new GKEInterpreter[IO](Config.gkeInterpConfig,
                                   vpcInterp,
                                   mockGKEService,
                                   new MockKubernetesService(PodStatus.Succeeded),
                                   MockHelm,
                                   MockGalaxyDAO,
                                   credentials,
                                   iamDAO,
                                   blocker,
                                   lock)

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id, true).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppByName(savedCluster1.googleProject, savedApp1.id, includeDeletedClusterApps = true)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Deleted
      getCluster.nodepools.map(_.status).distinct shouldBe List(NodepoolStatus.Deleted)
      getApp.app.status shouldBe AppStatus.Error
      getApp.app.errors.size shouldBe 1
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        None,
        Map.empty,
        AppType.Galaxy,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, makeGKEInterp = makeGKEInterp)
      _ <- leoSubscriber.messageHandler(Event(msg, None, timestamp, mockAckConsumer))
    } yield ()

    res.unsafeRunSync()
    assertions.unsafeRunSync()
  }

  it should "handle deleteKubernetesClusterMessage" in isolatedDbTest {
    val savedCluster = makeKubeCluster(1).save()

    val makeGKEInterp = for {
      lock <- nodepoolLock
    } yield new GKEInterpreter[IO](Config.gkeInterpConfig,
                                   vpcInterp,
                                   MockGKEService,
                                   WbLibsMockKubernetesService,
                                   MockHelm,
                                   MockGalaxyDAO,
                                   credentials,
                                   iamDAOKubernetes,
                                   blocker,
                                   lock)

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster.id).transaction
      getCluster = clusterOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Deleted
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = DeleteKubernetesClusterMessage(
        savedCluster.id,
        savedCluster.googleProject,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, makeGKEInterp = makeGKEInterp)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleDeleteKubernetesClusterMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions, maxRetry = 50)
    } yield ()

    res.unsafeRunSync()
  }

  it should "handle StopAppMessage" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).copy(status = AppStatus.Stopping).save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Stopped
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.nodepool.autoscalingEnabled shouldBe true
      getApp.nodepool.numNodes shouldBe NumNodes(2)
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = StopAppMessage(savedApp1.id, savedApp1.appName, savedCluster1.googleProject, Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleStopAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  it should "handle StartAppMessage" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).copy(status = AppStatus.Starting).save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Running
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.nodepool.autoscalingEnabled shouldBe true
      getApp.nodepool.numNodes shouldBe NumNodes(2)
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = StartAppMessage(savedApp1.id, savedApp1.appName, savedCluster1.googleProject, Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleStartAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  it should "be idempotent for create app" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.googleProject, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      getCluster.nodepools.size shouldBe 2
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getApp.app.errors shouldBe List()
      getApp.app.status shouldBe AppStatus.Running
      getApp.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
      getApp.cluster.status shouldBe KubernetesClusterStatus.Running
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.cluster.asyncFields shouldBe Some(
        KubernetesClusterAsyncFields(IP("1.2.3.4"),
                                     IP("0.0.0.0"),
                                     NetworkFields(Config.vpcConfig.networkName,
                                                   Config.vpcConfig.subnetworkName,
                                                   Config.vpcConfig.subnetworkIpRange))
      )
      getDisk.status shouldBe DiskStatus.Ready
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
      msg = CreateAppMessage(
        savedCluster1.googleProject,
        Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
        savedApp1.id,
        savedApp1.appName,
        Some(disk.id),
        Map.empty,
        savedApp1.appType,
        Some(tr)
      )
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      // send message twice
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- leoSubscriber.handleCreateAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  it should "be idempotent for delete app" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppByName(savedCluster1.googleProject, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Deleted
      getApp.nodepool.status shouldBe NodepoolStatus.Deleted
    }

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedNodepool1.id,
                             savedCluster1.googleProject,
                             None,
                             Some(tr))
      queue <- InspectableQueue.bounded[IO, Task[IO]](10)
      leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      // send message twice
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield ()

    res.unsafeRunSync()
  }

  it should "be idempotent for create disk" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
      tr <- traceId.ask[TraceId]
      now <- IO(Instant.now)

      message = CreateDiskMessage.fromDisk(disk, Some(tr))
      // send 2 messages
      _ <- leoSubscriber.messageResponder(message)
      _ <- leoSubscriber.messageResponder(message)
      updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      updatedDisk shouldBe 'defined
      updatedDisk.get.googleId.get.value shouldBe "target"
    }

    res.unsafeRunSync()
  }
  it should "be idempotent for delete disk" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      disk <- makePersistentDisk(None).copy(status = DiskStatus.Deleting).save()
      tr <- traceId.ask[TraceId]

      message = DeleteDiskMessage(disk.id, Some(tr))
      // send 2 messages
      _ <- leoSubscriber.messageResponder(message)
      _ <- leoSubscriber.messageResponder(message)
      updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      updatedDisk shouldBe 'defined
      updatedDisk.get.status shouldBe DiskStatus.Deleting
    }

    res.unsafeRunSync()
  }

  def makeGKEInterp(): IO[GKEInterpreter[IO]] =
    for {
      lock <- nodepoolLock
    } yield new GKEInterpreter[IO](Config.gkeInterpConfig,
                                   vpcInterp,
                                   MockGKEService,
                                   new MockKubernetesService(PodStatus.Succeeded),
                                   MockHelm,
                                   MockGalaxyDAO,
                                   credentials,
                                   iamDAOKubernetes,
                                   blocker,
                                   lock)

  def makeLeoSubscriber(runtimeMonitor: RuntimeMonitor[IO, CloudService] = MockRuntimeMonitor,
                        asyncTaskQueue: InspectableQueue[IO, Task[IO]] =
                          InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync,
                        computePollOperation: ComputePollOperation[IO] = new MockComputePollOperation,
                        makeGKEInterp: IO[GKEInterpreter[IO]] = makeGKEInterp): LeoPubsubMessageSubscriber[IO] = {
    val googleSubscriber = new FakeGoogleSubcriber[LeoPubsubMessage]

    implicit val monitor: RuntimeMonitor[IO, CloudService] = runtimeMonitor

    new LeoPubsubMessageSubscriber[IO](
      LeoPubsubMessageSubscriberConfig(1,
                                       30 seconds,
                                       Config.leoPubsubMessageSubscriberConfig.persistentDiskMonitorConfig),
      googleSubscriber,
      asyncTaskQueue,
      MockGoogleDiskService,
      computePollOperation,
      MockAuthProvider,
      makeGKEInterp.unsafeRunSync(),
      org.broadinstitute.dsde.workbench.errorReporting.FakeErrorReporting
    )
  }
}
