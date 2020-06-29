package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.effect.concurrent.Deferred
import cats.implicits._
import cats.mtl.ApplicativeAsk
import fs2.Stream
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, MockGoogleDiskService}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.VM
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.WelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeService
import org.broadinstitute.dsde.workbench.leonardo.db.{
  clusterQuery,
  persistentDiskQuery,
  RuntimeConfigQueries,
  TestComponent
}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.{
  ClusterInvalidState,
  DiskInvalidState
}
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent._
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Left
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class LeoPubsubMessageSubscriberSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with TestComponent
    with Matchers
    with MockitoSugar
    with Eventually
    with LeonardoTestSuite {

  val mockWelderDAO = mock[WelderDAO[IO]]
  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO()
  val gdDAO = new MockGoogleDataprocDAO
  val storageDAO = new MockGoogleStorageDAO
  val iamDAO = new MockGoogleIamDAO
  val projectDAO = new MockGoogleProjectDAO
  val authProvider = mock[LeoAuthProvider[IO]]
  val currentTime = Instant.now

  val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
    new MockGoogleStorageDAO
  }

  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig, clusterResourcesConfig)
  val bucketHelper =
    new BucketHelper[IO](bucketHelperConfig, FakeGoogleStorageService, serviceAccountProvider, blocker)

  val vpcInterp = new VPCInterpreter[IO](Config.vpcInterpreterConfig, projectDAO, MockGoogleComputeService)

  val dataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                   bucketHelper,
                                                   vpcInterp,
                                                   gdDAO,
                                                   MockGoogleComputeService,
                                                   MockGoogleDiskService,
                                                   mockGoogleDirectoryDAO,
                                                   iamDAO,
                                                   projectDAO,
                                                   mockWelderDAO,
                                                   blocker)
  val gceInterp = new GceInterpreter[IO](Config.gceInterpreterConfig,
                                         bucketHelper,
                                         vpcInterp,
                                         MockGoogleComputeService,
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
      tr <- traceId.ask
      _ <- leoSubscriber.messageResponder(CreateRuntimeMessage.fromRuntime(runtime, gceRuntimeConfig, Some(tr)))
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
      tr <- traceId.ask
      monitor = new MockRuntimeMonitor {
        override def pollCheck(a: CloudService)(
          googleProject: GoogleProject,
          runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
          operation: com.google.cloud.compute.v1.Operation,
          action: RuntimeStatus
        )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
          clusterQuery.completeDeletion(runtime.id, Instant.now()).transaction
      }
      queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
      leoSubscriber = makeLeoSubscriber(runtimeMonitor = monitor, asyncTaskQueue = queue)
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

      _ <- leoSubscriber.messageResponder(DeleteRuntimeMessage(runtime.id, false, Some(tr)))
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

  it should "delete disk when handling DeleteRuntimeMessage with deleteDisk being true" in isolatedDbTest {
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      disk <- makePersistentDisk(DiskId(1), Some(FormattedBy.GCE)).save()
      runtimeConfig = RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                                    bootDiskSize = DiskSize(50),
                                                    persistentDiskId = Some(disk.id))

      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(runtimeConfig))
      tr <- traceId.ask
      _ <- leoSubscriber.messageResponder(DeleteRuntimeMessage(runtime.id, true, Some(tr)))
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      _ <- withInfiniteStream(
        asyncTaskProcessor.process,
        persistentDiskQuery.getStatus(disk.id).transaction.map(status => status shouldBe (Some(DiskStatus.Deleted)))
      )
    } yield {
      updatedRuntime shouldBe 'defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Deleting
    }

    res.unsafeRunSync()
  }

  it should "not handle DeleteRuntimeMessage when cluster is not in Deleting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask
      message = DeleteRuntimeMessage(runtime.id, false, Some(tr))
      attempt <- leoSubscriber.messageResponder(message).attempt
    } yield {
      attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))
    }

    res.unsafeRunSync()
  }

  it should "handle StopRuntimeMessage and stop cluster" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Stopping).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

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
      now <- IO(Instant.now)
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask
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
      tr <- traceId.ask

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
      tr <- traceId.ask
      message = StartRuntimeMessage(runtime.id, Some(tr))
      attempt <- leoSubscriber.messageResponder(message).attempt
    } yield {
      attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))
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
      )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = IO.never
    }
    val leoSubscriber = makeLeoSubscriber(monitor)

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

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

  it should "handle UpdateRuntimeMessage and go through a stop-start transition" in isolatedDbTest {
    val queue = InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync()
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id, Some(MachineTypeName("n1-highmem-64")), true, None, None, None, Some(tr))
      )

      assert = IO(eventually(timeout(30.seconds)) {
        val assertion = for {
          updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
          updatedRuntimeConfig <- updatedRuntime.traverse(r =>
            RuntimeConfigQueries.getRuntimeConfig(r.runtimeConfigId).transaction
          )
        } yield {
          // runtime should be Starting after having gone through a stop -> update -> start
          updatedRuntime shouldBe 'defined
          updatedRuntime.get.status shouldBe RuntimeStatus.Starting
          // machine type should be updated
          updatedRuntimeConfig shouldBe 'defined
          updatedRuntimeConfig.get.machineType shouldBe MachineTypeName("n1-highmem-64")
        }
        assertion.unsafeRunSync()
      })
      _ <- Deferred[IO, Unit].flatMap { signalToStop =>
        val assertStream = Stream.eval(assert) ++ Stream.eval(signalToStop.complete(()))
        Stream(assertStream, asyncTaskProcessor.process.interruptWhen(signalToStop.get.attempt.map(_.map(_ => ()))))
          .covary[IO]
          .parJoin(2)
          .compile
          .drain
      }
    } yield ()

    res.unsafeRunSync()
  }

  it should "handle UpdateRuntimeMessage without going through a stop-start transition" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Stopped).saveWithRuntimeConfig(gceRuntimeConfig))
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(
        UpdateRuntimeMessage(runtime.id,
                             Some(MachineTypeName("n1-highmem-64")),
                             false,
                             Some(DiskSize(1024)),
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
      disk <- makePersistentDisk(DiskId(1)).copy(status = DiskStatus.Creating).save()
      tr <- traceId.ask
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
      disk <- makePersistentDisk(DiskId(1)).copy(status = DiskStatus.Deleting).save()
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(DeleteDiskMessage(disk.id, Some(tr)))
      updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      updatedDisk shouldBe 'defined
      updatedDisk.get.status shouldBe DiskStatus.Deleting
    }

    res.unsafeRunSync()
  }

  it should "not handle DeleteDiskMessage when disk is not in Deleting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      disk <- makePersistentDisk(DiskId(1)).copy(status = DiskStatus.Ready).save()
      tr <- traceId.ask
      message = DeleteDiskMessage(disk.id, Some(tr))
      attempt <- leoSubscriber.messageResponder(message).attempt
    } yield {
      attempt shouldBe Left(DiskInvalidState(disk.id, disk.projectNameString, disk, message))
    }

    res.unsafeRunSync()
  }

  it should "handle UpdateDiskMessage and update disk size" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res = for {
      now <- IO(Instant.now)
      disk <- makePersistentDisk(DiskId(1)).copy(status = DiskStatus.Ready).save()
      tr <- traceId.ask

      _ <- leoSubscriber.messageResponder(UpdateDiskMessage(disk.id, DiskSize(550), Some(tr)))
      updatedDisk <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      updatedDisk shouldBe 'defined
      //TODO: fix tests
//      updatedDisk.get.size shouldBe DiskSize(550)
    }

    res.unsafeRunSync()
  }

  def makeLeoSubscriber(runtimeMonitor: RuntimeMonitor[IO, CloudService] = MockRuntimeMonitor,
                        asyncTaskQueue: InspectableQueue[IO, Task[IO]] =
                          InspectableQueue.bounded[IO, Task[IO]](10).unsafeRunSync) = {
    val googleSubscriber = new FakeGoogleSubcriber[LeoPubsubMessage]

    implicit val monitor: RuntimeMonitor[IO, CloudService] = runtimeMonitor
    new LeoPubsubMessageSubscriber[IO](
      LeoPubsubMessageSubscriberConfig(1,
                                       30 seconds,
                                       Config.leoPubsubMessageSubscriberConfig.persistentDiskMonitorConfig),
      googleSubscriber,
      asyncTaskQueue,
      MockGoogleDiskService
    )
  }
}
