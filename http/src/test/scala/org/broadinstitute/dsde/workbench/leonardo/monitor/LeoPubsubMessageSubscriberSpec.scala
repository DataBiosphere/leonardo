package org.broadinstitute.dsde.workbench.leonardo
package monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import bio.terra.workspace.model.JobReport
import cats.data.Kleisli
import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.api.gax.longrunning.OperationFuture
import com.google.cloud.compute.v1.{Disk, Operation}
import com.google.protobuf.Timestamp
import fs2.Stream
import org.broadinstitute.dsde.workbench.azure.mock.{FakeAzureRelayService, FakeAzureVmService}
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, AzureRelayService, AzureVmService}
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.PodStatus
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceAccountName
import org.broadinstitute.dsde.workbench.google2.mock.{MockKubernetesService => _, _}
import org.broadinstitute.dsde.workbench.google2.{
  DiskName,
  GKEModels,
  GoogleDiskService,
  GoogleStorageService,
  KubernetesModels,
  MachineTypeName,
  RegionName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.AppRestore.GalaxyRestore
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{
  makeApp,
  makeAzureCluster,
  makeKubeCluster,
  makeNodepool,
  makeService
}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.BootSource
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.{ApplicationConfig, Config}
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.ClusterInvalidState
import org.broadinstitute.dsde.workbench.leonardo.util.{AzurePubsubHandlerInterp, _}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{IP, TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.messaging.{AckHandler, CloudSubscriber, ReceivedMessage}
import org.broadinstitute.dsp._
import org.broadinstitute.dsp.mocks.MockHelm
import org.http4s.headers.Authorization
import org.mockito.ArgumentMatchers.{any, anyBoolean, startsWith}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent._
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks._
import scalacache.caffeine.CaffeineCache

import java.net.URL
import java.nio.file.Paths
import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Left, Random}

class LeoPubsubMessageSubscriberSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with TestComponent
    with Matchers
    with MockitoSugar
    with Eventually
    with LeonardoTestSuite
    with BeforeAndAfterEach {
  val storageContainerResourceId = WsmControlledResourceId(UUID.randomUUID())

  val mockWelderDAO = mock[WelderDAO[IO]]

  val mockAzurePubsubHandlerInterp = mock[AzurePubsubHandlerAlgebra[IO]]

  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO() {
    override def isGroupMember(groupEmail: WorkbenchEmail,
                               memberEmail: WorkbenchEmail
    ): scala.concurrent.Future[Boolean] =
      scala.concurrent.Future.successful(true)
  }
  val storageDAO = new MockGoogleStorageDAO
  // Kubernetes doesn't actually create a new Service Account when calling googleIamDAO
  val iamDAOKubernetes = new MockGoogleIamDAO {
    override def addIamPolicyBindingOnServiceAccount(serviceAccountProject: GoogleProject,
                                                     serviceAccountEmail: WorkbenchEmail,
                                                     memberEmail: WorkbenchEmail,
                                                     rolesToAdd: Set[String]
    ): Future[Unit] = Future.successful(())
  }
  val iamDAO = new MockGoogleIamDAO
  val resourceService = new FakeGoogleResourceService {
    override def getProjectNumber(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Long]] =
      IO(Some(1L))

    override def getLabels(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Map[String, String]]] =
      IO(Some(Map("gke-default-sa" -> "gke-node-default-sa")))
  }
  val authProvider = mock[LeoAuthProvider[IO]]
  val currentTime = Instant.now
  val timestamp = Timestamp.newBuilder().setSeconds(now.toSeconds).build()

  val instantTimestamp = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos())

  val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => new MockGoogleStorageDAO

  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig)
  val bucketHelper =
    new BucketHelper[IO](bucketHelperConfig, FakeGoogleStorageService, serviceAccountProvider)

  val vpcInterp =
    new VPCInterpreter[IO](Config.vpcInterpreterConfig, resourceService, FakeGoogleComputeService)

  val dataprocInterp = new DataprocInterpreter[IO](
    Config.dataprocInterpreterConfig,
    bucketHelper,
    vpcInterp,
    FakeGoogleDataprocService,
    FakeGoogleComputeService,
    MockGoogleDiskService,
    mockGoogleDirectoryDAO,
    iamDAO,
    resourceService,
    mockWelderDAO
  )
  val gceInterp = new GceInterpreter[IO](Config.gceInterpreterConfig,
                                         bucketHelper,
                                         vpcInterp,
                                         FakeGoogleComputeService,
                                         MockGoogleDiskService,
                                         mockWelderDAO
  )

  val runningCluster = makeCluster(1).copy(
    serviceAccount = serviceAccount,
    asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(hostIp = None)),
    status = RuntimeStatus.Running
  )
  val runnningClusterInstances = List(masterInstance, workerInstance1, workerInstance2)

  val stoppedCluster = makeCluster(2).copy(serviceAccount = serviceAccount,
                                           asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(hostIp = None)),
                                           status = RuntimeStatus.Stopped
  )

  val serviceRegistry = ServicesRegistry()

  override def beforeEach(): Unit = {
    serviceRegistry.clear

    val gcpDependencies = mock[GcpDependencies[IO]]
    when(gcpDependencies.googleDiskService).thenReturn(MockGoogleDiskService)
    serviceRegistry.register[GcpDependencies[IO]](gcpDependencies)
    serviceRegistry.register[GKEAlgebra[IO]](new org.broadinstitute.dsde.workbench.leonardo.MockGKEService)

  }

  it should "handle CreateRuntimeMessage and create cluster" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()
    val res =
      for {
        runtime <- IO(
          makeCluster(1)
            .copy(serviceAccount = serviceAccount, asyncRuntimeFields = None, status = RuntimeStatus.Creating)
            .save()
        )
        tr <- traceId.ask[TraceId]
        gceRuntimeConfigRequest = LeoLenses.runtimeConfigPrism.getOption(gceRuntimeConfig).get
        _ <- leoSubscriber.messageResponder(
          CreateRuntimeMessage.fromRuntime(runtime, gceRuntimeConfigRequest, Some(tr), None)
        )
        updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      } yield {
        updatedRuntime shouldBe defined
        updatedRuntime.get.asyncRuntimeFields shouldBe defined
        updatedRuntime.get.asyncRuntimeFields.get.stagingBucket.value should startWith("leostaging")
        updatedRuntime.get.asyncRuntimeFields.get.hostIp shouldBe None
        updatedRuntime.get.asyncRuntimeFields.get.operationName.value shouldBe "op"
        updatedRuntime.get.asyncRuntimeFields.map(_.proxyHostName).isDefined shouldBe true
        updatedRuntime.get.runtimeImages.map(_.imageType) should contain(BootSource)
      }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle runtime creation failure properly" in isolatedDbTest {
    val runtimeMonitor = new MockRuntimeMonitor {
      override def process(
        a: CloudService
      )(runtimeId: Long, action: RuntimeStatus, checkToolsInterruptAfter: Option[FiniteDuration])(implicit
        ev: Ask[IO, TraceId]
      ): Stream[IO, Unit] = Stream.raiseError[IO](new Exception("failed"))
    }
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val leoSubscriber = makeLeoSubscriber(runtimeMonitor = runtimeMonitor, asyncTaskQueue = queue)
    val res =
      for {
        disk <- makePersistentDisk().save()
        runtime <- IO(
          makeCluster(1)
            .copy(serviceAccount = serviceAccount, asyncRuntimeFields = None, status = RuntimeStatus.Creating)
            .saveWithRuntimeConfig(CommonTestData.defaultGceRuntimeWithPDConfig(Some(disk.id)))
        )
        runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
        tr <- traceId.ask[TraceId]
        gceRuntimeConfigRequest = LeoLenses.runtimeConfigPrism.getOption(gceRuntimeConfig).get
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.messageResponder(
          CreateRuntimeMessage.fromRuntime(runtime, gceRuntimeConfigRequest, Some(tr), None)
        )
        assertions = for {
          error <- clusterErrorQuery.get(runtime.id).transaction
          runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
          diskStatus <- persistentDiskQuery.getStatus(disk.id).transaction
        } yield {
          error.nonEmpty shouldBe true
          runtimeConfig.asInstanceOf[RuntimeConfig.GceWithPdConfig].persistentDiskId shouldBe None
          diskStatus shouldBe Some(DiskStatus.Ready)
        }

        _ <- withInfiniteStream(
          asyncTaskProcessor.process,
          assertions
        )
      } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete creating disk on failed runtime start" in isolatedDbTest {
    val runtimeMonitor = new MockRuntimeMonitor {
      override def process(
        a: CloudService
      )(runtimeId: Long, action: RuntimeStatus, checkToolsInterruptAfter: Option[FiniteDuration])(implicit
        ev: Ask[IO, TraceId]
      ): Stream[IO, Unit] = Stream.raiseError[IO](new Exception("failed"))
    }
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val leoSubscriber = makeLeoSubscriber(runtimeMonitor = runtimeMonitor, asyncTaskQueue = queue)
    val res =
      for {
        disk <- makePersistentDisk().save()
        _ <- persistentDiskQuery.updateStatus(disk.id, DiskStatus.Failed, Instant.now()).transaction
        runtime <- IO(
          makeCluster(1)
            .copy(serviceAccount = serviceAccount, asyncRuntimeFields = None, status = RuntimeStatus.Creating)
            .saveWithRuntimeConfig(CommonTestData.defaultGceRuntimeWithPDConfig(Some(disk.id)))
        )
        runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
        tr <- traceId.ask[TraceId]
        gceRuntimeConfigRequest = LeoLenses.runtimeConfigPrism.getOption(gceRuntimeConfig).get
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.messageResponder(
          CreateRuntimeMessage.fromRuntime(runtime, gceRuntimeConfigRequest, Some(tr), None)
        )
        assertions = for {
          error <- clusterErrorQuery.get(runtime.id).transaction
          runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction
          diskStatus <- persistentDiskQuery.getStatus(disk.id).transaction
        } yield {
          error.nonEmpty shouldBe true
          runtimeConfig.asInstanceOf[RuntimeConfig.GceWithPdConfig].persistentDiskId shouldBe None
          diskStatus shouldBe Some(DiskStatus.Deleted)
        }

        _ <- withInfiniteStream(
          asyncTaskProcessor.process,
          assertions
        )
      } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle dataproc runtime creation failure properly" in isolatedDbTest {
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val dataprocAlg = new BaseMockRuntimeAlgebra {
      override def createRuntime(params: CreateRuntimeParams)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Option[CreateGoogleRuntimeResponse]] = IO.raiseError(new Exception("shit"))
    }
    val leoSubscriber = makeLeoSubscriber(dataprocRuntimeAlgebra = dataprocAlg, asyncTaskQueue = queue)
    val res =
      for {
        runtime <- IO(
          makeCluster(1)
            .copy(serviceAccount = serviceAccount, asyncRuntimeFields = None, status = RuntimeStatus.Creating)
            .saveWithRuntimeConfig(CommonTestData.defaultDataprocRuntimeConfig)
        )
        tr <- traceId.ask[TraceId]
        gceRuntimeConfigRequest = LeoLenses.runtimeConfigPrism
          .getOption(CommonTestData.defaultDataprocRuntimeConfig)
          .get
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.messageResponder(
          CreateRuntimeMessage.fromRuntime(runtime, gceRuntimeConfigRequest, Some(tr), None)
        )
        assertions = for {
          error <- clusterErrorQuery.get(runtime.id).transaction
          status <- clusterQuery.getClusterStatus(runtime.id).transaction
        } yield {
          error.nonEmpty shouldBe true
          status shouldBe Some(RuntimeStatus.Error)
        }

        _ <- withInfiniteStream(
          asyncTaskProcessor.process,
          assertions
        )
      } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  /**
   * When createRuntime gets 409, we shouldn't attempt to update AsyncRuntimeFields.
   * These fields should've been updated in a previous createRuntime request, and
   * this test is to make sure we're not wiping out that info.
   */
  it should "handle CreateRuntimeMessage properly when google returns 409" in isolatedDbTest {
    val runtimeAlgebra = new BaseFakeGceInterp {
      override def createRuntime(params: CreateRuntimeParams)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Option[CreateGoogleRuntimeResponse]] = IO.pure(None)
    }

    val asyncFields = makeAsyncRuntimeFields(1)

    val leoSubscriber = makeLeoSubscriber(gceRuntimeAlgebra = runtimeAlgebra)
    val res =
      for {
        runtime <- IO(
          makeCluster(1)
            .copy(serviceAccount = serviceAccount,
                  asyncRuntimeFields = Some(asyncFields),
                  status = RuntimeStatus.Creating
            )
            .save()
        )
        tr <- traceId.ask[TraceId]
        gceRuntimeConfigRequest = LeoLenses.runtimeConfigPrism.getOption(gceRuntimeConfig).get
        _ <- leoSubscriber.messageResponder(
          CreateRuntimeMessage.fromRuntime(runtime, gceRuntimeConfigRequest, Some(tr), None)
        )
        updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      } yield {
        updatedRuntime shouldBe defined
        updatedRuntime.get.asyncRuntimeFields shouldBe Some(asyncFields)
        updatedRuntime.get.runtimeImages shouldBe runtime.runtimeImages
      }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle DeleteRuntimeMessage and delete cluster" in isolatedDbTest {
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val runtime = makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(gceRuntimeConfig)
    val monitor = new MockRuntimeMonitor {
      override def handlePollCheckCompletion(
        a: CloudService
      )(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig): IO[Unit] =
        clusterQuery.completeDeletion(runtime.id, Instant.now()).transaction
    }
    val leoSubscriber = makeLeoSubscriber(runtimeMonitor = monitor, asyncTaskQueue = queue)
    val res =
      for {
        tr <- traceId.ask[TraceId]
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

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete disk when handling DeleteRuntimeMessage when autoDeleteDisks is set" in isolatedDbTest {
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)
    val res =
      for {
        disk <- makePersistentDisk(None, Some(FormattedBy.GCE)).save()
        runtimeConfig = RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                                      bootDiskSize = DiskSize(50),
                                                      persistentDiskId = Some(disk.id),
                                                      zone = ZoneName("us-central1-a"),
                                                      gpuConfig = None
        )

        runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(runtimeConfig))
        tr <- traceId.ask[TraceId]
        _ <- leoSubscriber.messageResponder(
          DeleteRuntimeMessage(runtime.id, Some(disk.id), Some(tr))
        )
        _ <- withInfiniteStream(
          asyncTaskProcessor.process,
          persistentDiskQuery
            .getStatus(disk.id)
            .transaction
            .map(status => status shouldBe (Some(DiskStatus.Deleted)))
        )
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "persist delete disk error when if fail to delete disk" in isolatedDbTest {
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
    val diskService = new MockGoogleDiskService {
      override def deleteDisk(project: GoogleProject, zone: ZoneName, diskName: DiskName)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[OperationFuture[Operation, Operation]]] = IO.raiseError(new Exception(s"Fail to delete ${diskName}"))
    }
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, diskService = diskService)
    val res =
      for {
        disk <- makePersistentDisk(None, Some(FormattedBy.GCE)).save()
        runtimeConfig = RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                                      bootDiskSize = DiskSize(50),
                                                      persistentDiskId = Some(disk.id),
                                                      zone = ZoneName("us-cetnral1-a"),
                                                      gpuConfig = None
        )

        runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleting).saveWithRuntimeConfig(runtimeConfig))
        tr <- traceId.ask[TraceId]
        _ <- leoSubscriber.messageResponder(DeleteRuntimeMessage(runtime.id, Some(disk.id), Some(tr)))
        _ <- withInfiniteStream(
          asyncTaskProcessor.process,
          clusterErrorQuery.get(runtime.id).transaction.map { error =>
            val dummyNow = Instant.now()
            error.head.copy(timestamp = dummyNow) shouldBe RuntimeError(
              s"Fail to delete ${disk.name}",
              None,
              dummyNow
            )
          }
        )
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not handle DeleteRuntimeMessage when cluster is not in Deleting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()
    val res =
      for {
        runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
        tr <- traceId.ask[TraceId]
        message = DeleteRuntimeMessage(runtime.id, None, Some(tr))
        attempt <- leoSubscriber.messageResponder(message).attempt
      } yield attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle StopRuntimeMessage and stop cluster" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res =
      for {
        runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Stopping).saveWithRuntimeConfig(gceRuntimeConfig))
        tr <- traceId.ask[TraceId]

        _ <- leoSubscriber.messageResponder(StopRuntimeMessage(runtime.id, Some(tr)))
        updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      } yield {
        updatedRuntime shouldBe defined
        updatedRuntime.get.status shouldBe RuntimeStatus.Stopping
      }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not handle StopRuntimeMessage when cluster is not in Stopping status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res =
      for {
        runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
        tr <- traceId.ask[TraceId]
        message = StopRuntimeMessage(runtime.id, Some(tr))
        attempt <- leoSubscriber.messageResponder(message).attempt
      } yield attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle StartRuntimeMessage and start cluster" in isolatedDbTest {
    val gceAlg = MockRuntimeAlgebra
    val leoSubscriber = makeLeoSubscriber(gceRuntimeAlgebra = gceAlg)

    val res =
      for {
        runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Starting).saveWithRuntimeConfig(gceRuntimeConfig))
        tr <- traceId.ask[TraceId]

        _ <- leoSubscriber.messageResponder(StartRuntimeMessage(runtime.id, Some(tr)))
        updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      } yield {
        updatedRuntime shouldBe defined
        updatedRuntime.get.status shouldBe RuntimeStatus.Starting
      }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not handle StartRuntimeMessage when cluster is not in Starting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res =
      for {
        runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
        tr <- traceId.ask[TraceId]
        message = StartRuntimeMessage(runtime.id, Some(tr))
        attempt <- leoSubscriber.messageResponder(message).attempt
      } yield attempt shouldBe Left(ClusterInvalidState(runtime.id, runtime.projectNameString, runtime, message))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle UpdateRuntimeMessage, resize dataproc cluster and setting DB status properly" in isolatedDbTest {
    val monitor = new MockRuntimeMonitor {
      override def process(
        a: CloudService
      )(runtimeId: Long, action: RuntimeStatus, checkToolsInterruptAfter: Option[FiniteDuration])(implicit
        ev: Ask[IO, TraceId]
      ): Stream[IO, Unit] =
        Stream.eval(clusterQuery.setToRunning(runtimeId, IP("0.0.0.0"), Instant.now).transaction.void)
    }
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
    val leoSubscriber = makeLeoSubscriber(runtimeMonitor = monitor, asyncTaskQueue = queue)

    val res =
      for {
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
      } yield updatedRuntimeConfig.get.asInstanceOf[RuntimeConfig.DataprocConfig].numberOfWorkers shouldBe 100

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle UpdateRuntimeMessage and stop the cluster when there's a machine type change" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

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
      updatedRuntime shouldBe defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopping
      // machine type should not be updated yet
      updatedRuntimeConfig shouldBe defined
      updatedRuntimeConfig.get.machineType shouldBe MachineTypeName("n1-standard-4")
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle UpdateRuntimeMessage and go through a stop-start transition for machine type" in isolatedDbTest {
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, gceRuntimeAlgebra = MockRuntimeAlgebra)

    val res =
      for {
        runtime <- IO(makeCluster(1).copy(status = RuntimeStatus.Running).saveWithRuntimeConfig(gceRuntimeConfig))
        tr <- traceId.ask[TraceId]

        _ <- leoSubscriber.messageResponder(
          UpdateRuntimeMessage(runtime.id, Some(MachineTypeName("n1-highmem-64")), true, None, None, None, Some(tr))
        )
        _ <- withInfiniteStream(
          asyncTaskProcessor.process,
          for {
            updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
            patchInProgress <- patchQuery.isInprogress(runtime.id).transaction
          } yield {
            // runtime should be Starting after having gone through a stop -> update -> start
            updatedRuntime shouldBe defined
            updatedRuntime.get.status shouldBe RuntimeStatus.Starting
            patchInProgress shouldBe false
          }
        )
      } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle UpdateRuntimeMessage and restart runtime for persistent disk size update" in isolatedDbTest {
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, gceRuntimeAlgebra = MockRuntimeAlgebra)
    val res =
      for {
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
                               Some(tr)
          )
        )

        assert = for {
          updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
          updatedDisk <- persistentDiskQuery.getById(disk.id)(scala.concurrent.ExecutionContext.global).transaction
        } yield {
          // runtime should be Starting after having gone through a stop -> start
          updatedRuntime shouldBe defined
          updatedRuntime.get.status shouldBe RuntimeStatus.Starting
          // machine type should be updated
          updatedDisk shouldBe defined
          updatedDisk.get.size shouldBe DiskSize(200)
        }

        _ <- withInfiniteStream(
          asyncTaskProcessor.process,
          assert
        )
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
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
                             Some(tr)
        )
      )
      updatedRuntime <- clusterQuery.getClusterById(runtime.id).transaction
      updatedRuntimeConfig <- updatedRuntime.traverse(r =>
        RuntimeConfigQueries.getRuntimeConfig(r.runtimeConfigId).transaction
      )
    } yield {
      // runtime should still be Stopped
      updatedRuntime shouldBe defined
      updatedRuntime.get.status shouldBe RuntimeStatus.Stopped
      // machine type and disk size should be updated
      updatedRuntimeConfig shouldBe defined
      updatedRuntimeConfig.get.machineType shouldBe MachineTypeName("n1-highmem-64")
      updatedRuntimeConfig.get.asInstanceOf[RuntimeConfig.GceConfig].diskSize shouldBe DiskSize(1024)
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle CreateDiskMessage and create disk" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()
    val res =
      for {
        disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
        tr <- traceId.ask[TraceId]
        now <- IO(Instant.now)
        _ <- leoSubscriber.messageResponder(CreateDiskMessage.fromDisk(disk, Some(tr)))
        updatedDisk <- persistentDiskQuery.getById(disk.id)(scala.concurrent.ExecutionContext.global).transaction
      } yield updatedDisk shouldBe defined

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle createDisk timeout properly" in isolatedDbTest {
    val operationFuture = new FakeComputeOperationFuture {
      override def get(timeout: Long, unit: TimeUnit): Operation =
        throw new java.util.concurrent.TimeoutException("fake")
    }
    val diskService = new MockGoogleDiskService {
      override def createDisk(project: GoogleProject, zone: ZoneName, disk: Disk)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[OperationFuture[Operation, Operation]]] = IO.pure(Some(operationFuture))
    }
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, diskService = diskService)
    val res =
      for {
        disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
        tr <- traceId.ask[TraceId]
        _ <- leoSubscriber.messageResponder(CreateDiskMessage.fromDisk(disk, Some(tr)))
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- withInfiniteStream(
          asyncTaskProcessor.process,
          for {
            updatedDisk <- persistentDiskQuery.getById(disk.id)(scala.concurrent.ExecutionContext.global).transaction
          } yield updatedDisk.get.status shouldBe DiskStatus.Failed
        )
        updatedDisk <- persistentDiskQuery.getById(disk.id)(scala.concurrent.ExecutionContext.global).transaction
      } yield updatedDisk shouldBe defined

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle DeleteDiskMessage and delete disk" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()
    val res =
      for {
        disk <- makePersistentDisk(None).copy(status = DiskStatus.Deleting).save()
        tr <- traceId.ask[TraceId]

        _ <- leoSubscriber.messageResponder(DeleteDiskMessage(disk.id, Some(tr)))
        updatedDisk <- persistentDiskQuery.getById(disk.id)(scala.concurrent.ExecutionContext.global).transaction
      } yield {
        updatedDisk shouldBe defined
        updatedDisk.get.status shouldBe DiskStatus.Deleting
      }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle DeleteDiskMessage and delete disk properly even if disk doesn't exist in cloud" in isolatedDbTest {
    val operationFuture = new FakeComputeOperationFuture {
      override def get(): Operation = {
        val exception = new java.util.concurrent.ExecutionException(
          "com.google.api.gax.rpc.NotFoundException: Not Found",
          new Exception("bad")
        )
        throw exception
      }
    }
    val diskService = new MockGoogleDiskService {
      override def deleteDisk(project: GoogleProject, zone: ZoneName, diskName: DiskName)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[OperationFuture[Operation, Operation]]] = IO.pure(Some(operationFuture))
    }
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, diskService = diskService)
    val res =
      for {
        disk <- makePersistentDisk(None).copy(status = DiskStatus.Deleting).save()
        tr <- traceId.ask[TraceId]

        _ <- leoSubscriber.messageResponder(DeleteDiskMessage(disk.id, Some(tr)))
        _ <- withInfiniteStream(
          asyncTaskProcessor.process,
          for {
            updatedDisk <- persistentDiskQuery.getById(disk.id)(scala.concurrent.ExecutionContext.global).transaction
          } yield {
            updatedDisk shouldBe defined
            updatedDisk.get.status shouldBe DiskStatus.Deleted
          }
        )
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle DeleteDiskMessage when disk is not in Deleting status" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()

    val res =
      for {
        disk <- makePersistentDisk(None).copy(status = DiskStatus.Ready).save()
        tr <- traceId.ask[TraceId]
        message = DeleteDiskMessage(disk.id, Some(tr))
        attempt <- leoSubscriber.messageResponder(message).attempt
      } yield attempt shouldBe Right(())

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle UpdateDiskMessage and update disk size" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()
    val res =
      for {
        disk <- makePersistentDisk(None).copy(status = DiskStatus.Ready).save()
        tr <- traceId.ask[TraceId]

        _ <- leoSubscriber.messageResponder(UpdateDiskMessage(disk.id, DiskSize(550), Some(tr)))
        updatedDisk <- persistentDiskQuery.getById(disk.id)(scala.concurrent.ExecutionContext.global).transaction
      } yield updatedDisk shouldBe defined
    // TODO: fix tests
//      updatedDisk.get.size shouldBe DiskSize(550)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle create app message with a create cluster" in isolatedDbTest {

    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val disk = makePersistentDisk(Some(DiskName("disk1")), Some(FormattedBy.Galaxy))
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2)),
          kubernetesServiceAccountName = Some(ServiceAccountName("gxy-ksa"))
        )
      )
      .save()

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
      appRestore <- persistentDiskQuery.getAppDiskRestore(savedApp1.appResources.disk.get.id).transaction
      galaxyRestore = appRestore.map(_.asInstanceOf[GalaxyRestore])
      ipRange = Config.vpcConfig.subnetworkRegionIpRangeMap
        .getOrElse(RegionName("us-central1"), throw new Exception(s"Unsupported Region us-central1"))
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      getCluster.nodepools.size shouldBe 2
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getApp.app.errors shouldBe List.empty
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
                                                   ipRange
                                     )
        )
      )
      getDisk.status shouldBe DiskStatus.Ready
      galaxyRestore shouldBe Some(
        GalaxyRestore(PvcId(s"nfs-pvc-id1"), getApp.app.id)
      )
    }

    implicit val gkeAlg: GKEAlgebra[IO] = makeGKEInterp(nodepoolLock, List(savedApp1.release))
    implicit val googleDiskService = MockGoogleDiskService

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val leoSubscriber =
      makeLeoSubscriber(asyncTaskQueue = queue, gkeAlgebra = gkeAlg)

    val res =
      for {
        tr <- traceId.ask[TraceId]
        dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
        msg = CreateAppMessage(
          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
          Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
          savedApp1.id,
          savedApp1.appName,
          Some(disk.id),
          Map.empty,
          AppType.Galaxy,
          savedApp1.appResources.namespace,
          Some(AppMachineType(5, 4)),
          Some(tr),
          false
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.handleCreateAppMessage(msg)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle create app message with no AppMachineType if for non galaxy app" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val disk = makePersistentDisk(Some(DiskName("disk1")), Some(FormattedBy.Galaxy))
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val makeApp1 = makeApp(1, savedNodepool1.id).copy(appType = AppType.Cromwell)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2)),
          kubernetesServiceAccountName = Some(ServiceAccountName("cromwell-ksa"))
        )
      )
      .save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
    } yield getApp.app.status shouldBe AppStatus.Running

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    implicit val gkeAlgebra = makeGKEInterp(nodepoolLock, List(savedApp1.release))
    implicit val googleDiskService = MockGoogleDiskService

    val leoSubscriber =
      makeLeoSubscriber(asyncTaskQueue = queue, gkeAlgebra = gkeAlgebra)

    val res =
      for {
        tr <- traceId.ask[TraceId]
        dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
        msg = CreateAppMessage(
          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
          Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
          savedApp1.id,
          savedApp1.appName,
          Some(disk.id),
          Map.empty,
          AppType.Galaxy,
          savedApp1.appResources.namespace,
          None,
          Some(tr),
          false
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.handleCreateAppMessage(msg)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "be able to create multiple apps in a cluster" in isolatedDbTest {

    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedNodepool2 = makeNodepool(2, savedCluster1.id).save()
    val disk1 = makePersistentDisk(Some(DiskName("disk1"))).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val disk2 = makePersistentDisk(Some(DiskName("disk2"))).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk1),
          services = List(makeService(1), makeService(2)),
          kubernetesServiceAccountName = Some(ServiceAccountName("gxy-ksa"))
        )
      )
      .save()
    val makeApp2 = makeApp(2, savedNodepool2.id)
    val savedApp2 = makeApp2
      .copy(appResources =
        makeApp2.appResources.copy(
          disk = Some(disk2),
          services = List(makeService(1), makeService(2)),
          kubernetesServiceAccountName = Some(ServiceAccountName("gxy-ksa"))
        )
      )
      .save()

    val assertions = for {
      getAppOpt1 <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
        .transaction
      getAppOpt2 <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.cloudContext, savedApp2.appName)
        .transaction
      getApp1 = getAppOpt1.get
      getApp2 = getAppOpt2.get
      ipRange = Config.vpcConfig.subnetworkRegionIpRangeMap
        .getOrElse(RegionName("us-central1"), throw new Exception(s"Unsupported Region us-central1"))
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
                                                   ipRange
                                     )
        )
      )
      getApp2.app.errors shouldBe List()
      getApp2.app.status shouldBe AppStatus.Running
      getApp2.app.appResources.kubernetesServiceAccountName shouldBe Some(
        ServiceAccountName("gxy-ksa")
      )
    }

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    implicit val gkeAlgebra = makeGKEInterp(nodepoolLock, List(savedApp1.release, savedApp2.release))
    implicit val googleDiskService = MockGoogleDiskService

    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, gkeAlgebra = gkeAlgebra)

    val res =
      for {
        tr <- traceId.ask[TraceId]
        dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
        msg1 = CreateAppMessage(
          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
          Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
          savedApp1.id,
          savedApp1.appName,
          Some(disk1.id),
          Map.empty,
          AppType.Galaxy,
          savedApp1.appResources.namespace,
          Some(AppMachineType(5, 4)),
          Some(tr),
          false
        )
        msg2 = CreateAppMessage(
          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
          Some(ClusterNodepoolAction.CreateNodepool(savedNodepool2.id)),
          savedApp2.id,
          savedApp2.appName,
          Some(disk2.id),
          Map.empty,
          AppType.Galaxy,
          savedApp2.appResources.namespace,
          Some(AppMachineType(5, 4)),
          Some(tr),
          false
        )
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.handleCreateAppMessage(msg1)
        _ <- leoSubscriber.handleCreateAppMessage(msg2)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle error in createApp if createDisk is specified with no disk" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckHandler]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.CreateApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Disk)
    }
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, diskService = makeDetachingDiskInterp())
    val res =
      for {
        tr <- traceId.ask[TraceId]
        msg = CreateAppMessage(
          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
          Some(ClusterNodepoolAction.CreateNodepool(savedNodepool1.id)),
          savedApp1.id,
          savedApp1.appName,
          Some(DiskId(-1)),
          Map.empty,
          AppType.Galaxy,
          savedApp1.appResources.namespace,
          None,
          Some(tr),
          false
        )
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.messageHandler(ReceivedMessage(msg, None, instantTimestamp, mockAckConsumer))
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    verify(mockAckConsumer, times(1)).ack()
  }

  // delete app and not delete disk when specified
  // update app status and disk id
  it should "delete app and not delete disk when diskId not specified" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val disk = makePersistentDisk(None).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
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
      getAppOpt <- KubernetesServiceDbQueries.getFullAppById(savedCluster1.cloudContext, savedApp1.id).transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Deleted
      getApp.app.appResources.disk shouldBe None
      getApp.nodepool.status shouldBe savedNodepool1.status
      getDisk.status shouldBe DiskStatus.Ready
    }

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val googleStorageService: GoogleStorageService[IO] = mock[GoogleStorageService[IO]]
    when(googleStorageService.deleteBucket(any, any, anyBoolean(), any, any, any))
      .thenReturn(fs2.Stream.emit(true).covary[IO])
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue,
                                          storageService = googleStorageService,
                                          diskService = makeDetachingDiskInterp()
    )

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                             None,
                             Some(tr)
      )
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield verify(googleStorageService, never()).deleteBucket(any, any, anyBoolean(), any, any, any)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete app and delete disk when diskId specified" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None)
      .copy(status = DiskStatus.Deleting)
      .save()
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
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
      getAppOpt <- KubernetesServiceDbQueries.getFullAppById(savedCluster1.cloudContext, savedApp1.id).transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getApp.app.errors shouldBe List()
      getApp.app.status shouldBe AppStatus.Deleted
      getApp.app.appResources.disk shouldBe None
      getApp.nodepool.status shouldBe savedNodepool1.status
      getDisk.status shouldBe DiskStatus.Deleted
    }

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    implicit val googleDiskService: GoogleDiskService[IO] = makeDetachingDiskInterp()
    val googleStorageService: GoogleStorageService[IO] = mock[GoogleStorageService[IO]]
    when(googleStorageService.deleteBucket(any, any, anyBoolean(), any, any, any))
      .thenReturn(fs2.Stream.emit(true).covary[IO])
    implicit val gkeAlg: GKEAlgebra[IO] = new org.broadinstitute.dsde.workbench.leonardo.MockGKEService

    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue,
                                          diskService = googleDiskService,
                                          storageService = googleStorageService,
                                          gkeAlgebra = gkeAlg
    )

    val res = for {
      tr <- traceId.ask[TraceId]
      msg = DeleteAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                             Some(disk.id),
                             Some(tr)
      )
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- leoSubscriber.handleDeleteAppMessage(msg)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
    } yield verify(googleStorageService, times(1)).deleteBucket(any, any, anyBoolean(), any, any, any)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle an error in delete app" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckHandler]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppById(savedCluster1.cloudContext, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.status shouldBe AppStatus.Error
      getApp.app.errors.map(_.action) should contain(ErrorAction.DeleteApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.App)
      getApp.nodepool.status shouldBe NodepoolStatus.Unspecified
    }
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val mockKubernetesService = new MockKubernetesService(PodStatus.Failed, appRelease = List(savedApp1.release)) {
      override def deleteNamespace(
        clusterId: GKEModels.KubernetesClusterId,
        namespace: KubernetesModels.KubernetesNamespace
      )(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.raiseError(new Exception("test error"))
    }
    val gkeInter = new GKEInterpreter[IO](
      Config.gkeInterpConfig,
      bucketHelper,
      vpcInterp,
      MockGKEService,
      mockKubernetesService,
      MockHelm,
      MockAppDAO,
      credentials,
      iamDAOKubernetes,
      makeDetachingDiskInterp(),
      MockAppDescriptorDAO,
      nodepoolLock,
      resourceService,
      FakeGoogleComputeService
    )
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, gkeAlgebra = gkeInter)

    val res =
      for {
        tr <- traceId.ask[TraceId]
        msg = DeleteAppMessage(savedApp1.id,
                               savedApp1.appName,
                               savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                               None,
                               Some(tr)
        )
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.messageHandler(ReceivedMessage(msg, None, instantTimestamp, mockAckConsumer))
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "be able to create app with a pre-created nodepool" in isolatedDbTest {
    val disk = makePersistentDisk(None).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2)),
          kubernetesServiceAccountName = Some(ServiceAccountName("gxy-ksa"))
        )
      )
      .save()

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      getCluster.nodepools.size shouldBe 2
      // we shouldn't update the default nodepool status here, its already created
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

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    implicit val gkeAlgebra = makeGKEInterp(nodepoolLock, List(savedApp1.release))
    implicit val googleDiskService = MockGoogleDiskService

    val leoSubscriber =
      makeLeoSubscriber(asyncTaskQueue = queue, gkeAlgebra = gkeAlgebra)

    val res =
      for {
        tr <- traceId.ask[TraceId]
        msg = CreateAppMessage(
          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
          None,
          savedApp1.id,
          savedApp1.appName,
          Some(disk.id),
          Map.empty,
          AppType.Galaxy,
          savedApp1.appResources.namespace,
          Some(AppMachineType(5, 4)),
          Some(tr),
          false
        )
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.handleCreateAppMessage(msg)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    assertions.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "clean-up google resources on error" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()

    // using a var here to simplify test code
    // we could use mockito for this functionality, but it would be overly complicated since we wish to override other functionality of the mock as well
    var deleteCalled = false

    val helmClient = new MockHelm {
      override def installChart(release: Release,
                                chartName: ChartName,
                                chartVersion: ChartVersion,
                                values: Values,
                                createNamespace: Boolean
      ): Kleisli[IO, AuthContext, Unit] =
        if (chartName == Config.gkeInterpConfig.terraAppSetupChartConfig.chartName)
          Kleisli.liftF(IO.raiseError(new Exception("this is an intentional test exception")))
        else Kleisli.liftF(IO.unit)
    }

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppById(savedCluster1.cloudContext, savedApp1.id)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Running
      // The non-default nodepool should still be there, as it is not deleted on app deletion
      getCluster.nodepools.size shouldBe 2
      getCluster.nodepools.filter(_.isDefault).head.status shouldBe NodepoolStatus.Running
      getApp.app.errors.size shouldBe 1
      getApp.app.status shouldBe AppStatus.Error
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.app.auditInfo.destroyedDate shouldBe None
      getDisk.status shouldBe DiskStatus.Deleted
      deleteCalled shouldBe true
    }

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val mockKubernetesService = new MockKubernetesService(PodStatus.Succeeded, List(savedApp1.release)) {
      override def deleteNamespace(
        clusterId: GKEModels.KubernetesClusterId,
        namespace: KubernetesModels.KubernetesNamespace
      )(implicit ev: Ask[IO, TraceId]): IO[Unit] =
        IO {
          deleteCalled = true
        }
    }
    implicit val gkeInterp = new GKEInterpreter[IO](
      Config.gkeInterpConfig,
      bucketHelper,
      vpcInterp,
      MockGKEService,
      mockKubernetesService,
      helmClient,
      MockAppDAO,
      credentials,
      iamDAOKubernetes,
      makeDetachingDiskInterp(),
      MockAppDescriptorDAO,
      nodepoolLock,
      resourceService,
      FakeGoogleComputeService
    )

    implicit val googleDiskService: GoogleDiskService[IO] = makeDetachingDiskInterp()

    val leoSubscriber =
      makeLeoSubscriber(asyncTaskQueue = queue, diskService = googleDiskService, gkeAlgebra = gkeInterp)

    val res =
      for {
        tr <- traceId.ask[TraceId]
        dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
        msg = CreateAppMessage(
          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
          Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
          savedApp1.id,
          savedApp1.appName,
          Some(disk.id),
          Map.empty,
          AppType.Galaxy,
          savedApp1.appResources.namespace,
          None,
          Some(tr),
          false
        )
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.handleCreateAppMessage(msg)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions, maxRetry = 50)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not delete a disk that already existing when app errors out" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
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
        .getFullAppById(savedCluster1.cloudContext, savedApp1.id)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
    } yield {
      // The non-default nodepool should still be there, as it is not deleted on app deletion
      getCluster.nodepools.size shouldBe 2
      getApp.app.errors.size shouldBe 1
      getApp.app.status shouldBe AppStatus.Error
      getDisk.status shouldBe disk.status
    }

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    implicit val gkeAlg = new org.broadinstitute.dsde.workbench.leonardo.MockGKEService {
      override def createAndPollApp(params: CreateAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
        IO.raiseError(new RuntimeException("app creation failed"))
    }
    val leoSubscriber =
      makeLeoSubscriber(asyncTaskQueue = queue, diskService = makeDetachingDiskInterp(), gkeAlgebra = gkeAlg)

    implicit val googleDiskService: GoogleDiskService[IO] =
      serviceRegistry.lookup[GcpDependencies[IO]].get.googleDiskService // googleDiskService

    val res =
      for {
        tr <- traceId.ask[TraceId]
        dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
        msg = CreateAppMessage(
          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
          Some(
            ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)
          ),
          savedApp1.id,
          savedApp1.appName,
          None,
          Map.empty,
          AppType.Galaxy,
          savedApp1.appResources.namespace,
          None,
          Some(tr),
          false
        )
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.handleCreateAppMessage(msg)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions, maxRetry = 50)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete a cluster and put that app in error status on cluster error" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2))
        )
      )
      .save()
    val mockAckConsumer = mock[AckHandler]

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id, true).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getFullAppById(savedCluster1.cloudContext, savedApp1.id)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getCluster.status shouldBe KubernetesClusterStatus.Deleted
      getCluster.nodepools.map(_.status).distinct shouldBe List(NodepoolStatus.Deleted)
      getApp.app.status shouldBe AppStatus.Error
      getApp.app.errors.size shouldBe 1
    }

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val mockGKEService = new MockGKEService {
      override def createCluster(request: GKEModels.KubernetesCreateClusterRequest)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[com.google.api.services.container.model.Operation]] = IO.raiseError(new Exception("test exception"))
    }
    val gkeInterp = new GKEInterpreter[IO](
      Config.gkeInterpConfig,
      bucketHelper,
      vpcInterp,
      mockGKEService,
      new MockKubernetesService(PodStatus.Succeeded),
      MockHelm,
      MockAppDAO,
      credentials,
      iamDAO,
      makeDetachingDiskInterp(),
      MockAppDescriptorDAO,
      nodepoolLock,
      resourceService,
      FakeGoogleComputeService
    )
    val leoSubscriber =
      makeLeoSubscriber(asyncTaskQueue = queue, diskService = makeDetachingDiskInterp(), gkeAlgebra = gkeInterp)

    val res =
      for {
        tr <- traceId.ask[TraceId]
        dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
        msg = CreateAppMessage(
          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
          Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
          savedApp1.id,
          savedApp1.appName,
          None,
          Map.empty,
          AppType.Galaxy,
          savedApp1.appResources.namespace,
          None,
          Some(tr),
          false
        )
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.messageHandler(ReceivedMessage(msg, None, instantTimestamp, mockAckConsumer))
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle StopAppMessage" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).copy(status = AppStatus.Stopping).save()

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)

    implicit val gkeAlg: GKEAlgebra[IO] = serviceRegistry.lookup[GKEAlgebra[IO]].get

    val res =
      for {
        tr <- traceId.ask[TraceId]
        msg = StopAppMessage(savedApp1.id,
                             savedApp1.appName,
                             savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                             Some(tr)
        )
        _ <- leoSubscriber.handleStopAppMessage(msg)
      } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle StartAppMessage" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).copy(status = AppStatus.Starting).save()

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue)

    implicit val gkeAlg: GKEAlgebra[IO] = serviceRegistry.lookup[GKEAlgebra[IO]].get

    val res =
      for {
        tr <- traceId.ask[TraceId]
        msg = StartAppMessage(savedApp1.id,
                              savedApp1.appName,
                              savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                              Some(tr)
        )
        _ <- leoSubscriber.handleStartAppMessage(msg)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle start app timeouts" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).copy(status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).copy(status = AppStatus.Starting).save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppById(savedCluster1.cloudContext, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.head.errorMessage should include(
        "GALAXY startup has failed or timed out for app app1 in cluster"
      )
      getApp.app.errors.head.action shouldBe ErrorAction.StartApp
      getApp.app.errors.head.source shouldBe ErrorSource.App
      getApp.app.status shouldBe AppStatus.Stopped
      getApp.nodepool.status shouldBe NodepoolStatus.Running
      getApp.nodepool.autoscalingEnabled shouldBe true
      getApp.nodepool.numNodes shouldBe NumNodes(2)
    }
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    implicit val gkeInterp = new GKEInterpreter[IO](
      Config.gkeInterpConfig,
      bucketHelper,
      vpcInterp,
      MockGKEService,
      new MockKubernetesService(),
      MockHelm,
      new MockAppDAO(false),
      credentials,
      iamDAOKubernetes,
      makeDetachingDiskInterp(),
      MockAppDescriptorDAO,
      nodepoolLock,
      resourceService,
      FakeGoogleComputeService
    )

    val leoSubscriber =
      makeLeoSubscriber(asyncTaskQueue = queue, diskService = makeDetachingDiskInterp(), gkeAlgebra = gkeInterp)

    val res =
      for {
        tr <- traceId.ask[TraceId]
        msg = StartAppMessage(savedApp1.id,
                              savedApp1.appName,
                              savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                              Some(tr)
        )
        // create a GKEInterpreter instance with a 'down' GalaxyDAO to simulate a timeout
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.handleStartAppMessage(msg)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "be idempotent for create app" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val disk = makePersistentDisk(None).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val makeApp1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = makeApp1
      .copy(appResources =
        makeApp1.appResources.copy(
          disk = Some(disk),
          services = List(makeService(1), makeService(2)),
          kubernetesServiceAccountName = Some(ServiceAccountName("gxy-ksa"))
        )
      )
      .save()

    val assertions = for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id).transaction
      getCluster = clusterOpt.get
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
      getDiskOpt <- persistentDiskQuery.getById(savedApp1.appResources.disk.get.id).transaction
      getDisk = getDiskOpt.get
      ipRange = Config.vpcConfig.subnetworkRegionIpRangeMap
        .getOrElse(RegionName("us-central1"), throw new Exception(s"Unsupported Region us-central1"))
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
                                                   ipRange
                                     )
        )
      )
      getDisk.status shouldBe DiskStatus.Ready
    }

    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    implicit val gkeAlg = makeGKEInterp(nodepoolLock, List(savedApp1.release))
    implicit val googleDiskService = MockGoogleDiskService

    val leoSubscriber =
      makeLeoSubscriber(asyncTaskQueue = queue, gkeAlgebra = gkeAlg)

    val res =
      for {
        tr <- traceId.ask[TraceId]
        dummyNodepool = savedCluster1.nodepools.filter(_.isDefault).head
        msg = CreateAppMessage(
          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
          Some(ClusterNodepoolAction.CreateClusterAndNodepool(savedCluster1.id, dummyNodepool.id, savedNodepool1.id)),
          savedApp1.id,
          savedApp1.appName,
          Some(disk.id),
          Map.empty,
          savedApp1.appType,
          savedApp1.appResources.namespace,
          Some(AppMachineType(5, 4)),
          Some(tr),
          false
        )
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        // send message twice
        _ <- leoSubscriber.handleCreateAppMessage(msg)
        _ <- leoSubscriber.handleCreateAppMessage(msg)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "be idempotent for delete app" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries.getFullAppById(savedCluster1.cloudContext, savedApp1.id).transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 0
      getApp.app.status shouldBe AppStatus.Deleted
      getApp.nodepool.status shouldBe savedNodepool1.status
    }
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, diskService = makeDetachingDiskInterp())

    implicit val googleDiskService: GoogleDiskService[IO] =
      serviceRegistry.lookup[GcpDependencies[IO]].get.googleDiskService // googleDiskService
    implicit val gkeAlg: GKEAlgebra[IO] = serviceRegistry.lookup[GKEAlgebra[IO]].get

    val res =
      for {
        tr <- traceId.ask[TraceId]
        msg = DeleteAppMessage(savedApp1.id,
                               savedApp1.appName,
                               savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                               None,
                               Some(tr)
        )
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        // send message twice
        _ <- leoSubscriber.handleDeleteAppMessage(msg)
        _ <- leoSubscriber.handleDeleteAppMessage(msg)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "be idempotent for create disk" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()
    val res =
      for {
        disk <- makePersistentDisk(None).copy(status = DiskStatus.Creating).save()
        tr <- traceId.ask[TraceId]

        message = CreateDiskMessage.fromDisk(disk, Some(tr))
        // send 2 messages
        _ <- leoSubscriber.messageResponder(message)
        _ <- leoSubscriber.messageResponder(message)
        updatedDisk <- persistentDiskQuery.getById(disk.id)(scala.concurrent.ExecutionContext.global).transaction
      } yield updatedDisk shouldBe defined

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
  it should "be idempotent for delete disk" in isolatedDbTest {
    val leoSubscriber = makeLeoSubscriber()
    val res =
      for {
        disk <- makePersistentDisk(None).copy(status = DiskStatus.Deleting).save()
        tr <- traceId.ask[TraceId]

        message = DeleteDiskMessage(disk.id, Some(tr))
        // send 2 messages
        _ <- leoSubscriber.messageResponder(message)
        _ <- leoSubscriber.messageResponder(message)
        updatedDisk <- persistentDiskQuery.getById(disk.id)(scala.concurrent.ExecutionContext.global).transaction
      } yield {
        updatedDisk shouldBe defined
        updatedDisk.get.status shouldBe DiskStatus.Deleting
      }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle top-level error in create azure vm properly" in isolatedDbTest {
    val exceptionMsg = "test exception"
    val mockWsmDao = new MockWsmDAO {
      override def getCreateVmJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[GetCreateVmJobResult] =
        IO.raiseError(new Exception(exceptionMsg))
    }
    val mockAckConsumer = mock[AckHandler]
    val queue = makeTaskQueue()
    val leoSubscriber = makeLeoSubscriber(azureInterp = makeAzureInterp(asyncTaskQueue = queue, wsmDAO = mockWsmDao),
                                          asyncTaskQueue = queue
    )

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
        )
        runtime = makeCluster(1)
          .copy(
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        jobId <- IO.delay(UUID.randomUUID())
        msg = CreateAzureRuntimeMessage(runtime.id,
                                        workspaceId,
                                        false,
                                        None,
                                        "WorkspaceName",
                                        BillingProfileId("spend-profile")
        )

        _ <- leoSubscriber.messageHandler(ReceivedMessage(msg, None, instantTimestamp, mockAckConsumer))

        assertions = for {
          error <- clusterErrorQuery.get(runtime.id).transaction
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
        } yield {
          getRuntimeOpt.map(_.status) shouldBe Some(RuntimeStatus.Error)
          error.length shouldBe 1
          error.map(_.errorMessage).head should include(exceptionMsg)
        }
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle top-level error in create azure disk properly" in isolatedDbTest {
    val mockAckConsumer = mock[AckHandler]
    val queue = makeTaskQueue()
    val (mockWsm, _, _) = AzureTestUtils.setUpMockWsmApiClientProvider(JobReport.StatusEnum.FAILED)
    val leoSubscriber = makeLeoSubscriber(azureInterp =
                                            makeAzureInterp(asyncTaskQueue = queue, mockWsmClient = mockWsm),
                                          asyncTaskQueue = queue
    )

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
        )
        runtime = makeCluster(1)
          .copy(
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        msg = CreateAzureRuntimeMessage(runtime.id,
                                        workspaceId,
                                        false,
                                        None,
                                        "WorkspaceName",
                                        BillingProfileId("spend-profile")
        )

        _ <- leoSubscriber.messageHandler(ReceivedMessage(msg, None, instantTimestamp, mockAckConsumer))

        assertions = for {
          error <- clusterErrorQuery.get(runtime.id).transaction
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
        } yield {
          getRuntimeOpt.map(_.status) shouldBe Some(RuntimeStatus.Error)
          error.length shouldBe 1
          error.map(_.errorMessage).head should include("Wsm createDisk job failed due to")

        }
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle delete azure vm failure properly" in isolatedDbTest {
    val (mockWsm, _, _) = AzureTestUtils.setUpMockWsmApiClientProvider(vmJobStatus = JobReport.StatusEnum.FAILED)
    val mockAckConsumer = mock[AckHandler]
    val queue = makeTaskQueue()
    val leoSubscriber =
      makeLeoSubscriber(azureInterp = makeAzureInterp(asyncTaskQueue = queue, mockWsmClient = mockWsm),
                        asyncTaskQueue = queue
      )

    val res =
      for {
        ctx <- appContext.ask[AppContext]
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
        )
        runtime = makeCluster(1)
          .copy(
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)
        vmResourceId = WsmControlledResourceId(UUID.randomUUID())

        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(vmResourceId),
                                        BillingProfileId("spend-profile"),
                                        None
        )

        assertions = for {
          errors <- clusterErrorQuery.get(runtime.id).transaction
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
        } yield {
          getRuntimeOpt.map(_.status) shouldBe Some(RuntimeStatus.Error)
          errors.length shouldBe 1
          val error = errors.head
          error.errorMessage should include(
            s"WSM delete VM job failed due to"
          )
          error.traceId shouldBe (Some(ctx.traceId))
        }

        _ <- leoSubscriber.messageHandler(ReceivedMessage(msg, Some(ctx.traceId), instantTimestamp, mockAckConsumer))
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)

      } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle Azure StartRuntimeMessage and start runtime" in isolatedDbTest {
    val azurePubsubHandlerMock = mock[AzurePubsubHandlerInterp[IO]]
    when(azurePubsubHandlerMock.startAndMonitorRuntime(any[Runtime], any[AzureCloudContext])(any()))
      .thenReturn(IO.unit)
    val leoSubscriber = makeLeoSubscriber(azureInterp = azurePubsubHandlerMock)
    val res = for {
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     Some(disk.id),
                                                     None
      )
      runtime <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Starting, cloudContext = CloudContext.Azure(azureCloudContext))
          .saveWithRuntimeConfig(azureRuntimeConfig)
      )
      tr <- traceId.ask[TraceId]
      _ <- leoSubscriber.messageResponder(StartRuntimeMessage(runtime.id, Some(tr)))

    } yield verify(azurePubsubHandlerMock, times(1))
      .startAndMonitorRuntime(any[Runtime], any[AzureCloudContext])(any())

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle Azure StopRuntimeMessage and stop runtime" in isolatedDbTest {
    val azurePubsubHandlerMock = mock[AzurePubsubHandlerInterp[IO]]
    when(azurePubsubHandlerMock.stopAndMonitorRuntime(any[Runtime], any[AzureCloudContext])(any()))
      .thenReturn(IO.unit)
    val leoSubscriber = makeLeoSubscriber(azureInterp = azurePubsubHandlerMock)
    val res = for {
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     Some(disk.id),
                                                     None
      )
      runtime <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Stopping, cloudContext = CloudContext.Azure(azureCloudContext))
          .saveWithRuntimeConfig(azureRuntimeConfig)
      )
      tr <- traceId.ask[TraceId]
      _ <- leoSubscriber.messageResponder(StopRuntimeMessage(runtime.id, Some(tr)))

    } yield verify(azurePubsubHandlerMock, times(1)).stopAndMonitorRuntime(any[Runtime], any[AzureCloudContext])(any())

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle Azure delete disk message" in isolatedDbTest {
    val azurePubsubHandlerMock = mock[AzurePubsubHandlerInterp[IO]]
    when(azurePubsubHandlerMock.deleteDisk(any[DeleteDiskV2Message])(any()))
      .thenReturn(IO.unit)
    val leoSubscriber = makeLeoSubscriber(azureInterp = azurePubsubHandlerMock)
    val res = for {
      disk <- makePersistentDisk(cloudContextOpt = Some(cloudContextAzure)).copy(status = DiskStatus.Ready).save()
      _ <- leoSubscriber.messageResponder(
        DeleteDiskV2Message(disk.id, disk.workspaceId.get, disk.cloudContext, disk.wsmResourceId, None)
      )

    } yield verify(azurePubsubHandlerMock, times(1)).deleteDisk(any[DeleteDiskV2Message])(any())

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  final case class TestExceptionIndexTuple(exception: Throwable, index: Int)
  it should "save an error and transition app to error when a fatal error occurs in azure updateAndPollApp" in isolatedDbTest {
    val errors = Table(
      "exception",
      TestExceptionIndexTuple(HelmException("test helm exception"), 1),
      TestExceptionIndexTuple(AppUpdatePollingException("test polling exception", None), 2)
    )
    forAll(errors) { (tuple: TestExceptionIndexTuple) =>
      val exception = tuple.exception
      val index = tuple.index
      val queue = makeTaskQueue()
      val mockAckConsumer = mock[AckHandler]

      val mockAksInterp = new MockAKSInterp {
        override def updateAndPollApp(params: UpdateAKSAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
          IO.raiseError(exception)
      }

      val leoSubscriber = makeLeoSubscriber(
        azureInterp = makeAzureInterp(asyncTaskQueue = queue, mockWsmClient = mockWsm, mockAksInterp = mockAksInterp),
        asyncTaskQueue = queue
      )

      val savedCluster1 = makeAzureCluster(index).save()
      val savedNodepool1 = makeNodepool(index, savedCluster1.id).save()
      val savedApp1 = makeApp(index, savedNodepool1.id, appType = AppType.Cromwell).save()
      val msg =
        UpdateAppMessage(UpdateAppJobId(UUID.randomUUID()),
                         savedApp1.id,
                         savedApp1.appName,
                         savedCluster1.cloudContext,
                         savedApp1.workspaceId,
                         None,
                         None
        )

      val res =
        for {
          _ <- leoSubscriber.messageHandler(ReceivedMessage(msg, None, instantTimestamp, mockAckConsumer))

          assertions = for {
            getAppOpt <- KubernetesServiceDbQueries
              .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
              .transaction
            getApp = getAppOpt.get
          } yield {
            getApp.app.errors.size shouldBe 1
            getApp.app.errors.map(_.action) should contain(ErrorAction.UpdateApp)
            getApp.app.errors.map(_.source) should contain(ErrorSource.App)
            getApp.app.errors.head.errorMessage should include(exception.getMessage)
            getApp.app.status shouldBe AppStatus.Error
          }
          asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
          _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
        } yield ()

      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      verify(mockAckConsumer, times(1)).ack()
    }
  }

  it should "save an error and transition app to running when a non-fatal error occurs in azure updateAndPollApp" in isolatedDbTest {
    val exception = new RuntimeException("random test exception")
    val queue = makeTaskQueue()
    val mockAckConsumer = mock[AckHandler]

    val mockAksInterp = new MockAKSInterp {
      override def updateAndPollApp(params: UpdateAKSAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
        IO.raiseError(exception)
    }

    val leoSubscriber = makeLeoSubscriber(
      azureInterp = makeAzureInterp(asyncTaskQueue = queue, mockWsmClient = mockWsm, mockAksInterp = mockAksInterp),
      asyncTaskQueue = queue
    )

    val savedCluster1 = makeAzureCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id, appType = AppType.Cromwell).save()
    val msg =
      UpdateAppMessage(UpdateAppJobId(UUID.randomUUID()),
                       savedApp1.id,
                       savedApp1.appName,
                       savedCluster1.cloudContext,
                       savedApp1.workspaceId,
                       None,
                       None
      )

    val res =
      for {
        _ <- leoSubscriber.messageHandler(ReceivedMessage(msg, None, instantTimestamp, mockAckConsumer))

        assertions = for {
          getAppOpt <- KubernetesServiceDbQueries
            .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
            .transaction
          getApp = getAppOpt.get
        } yield {
          getApp.app.errors.size shouldBe 1
          getApp.app.errors.map(_.action) should contain(ErrorAction.UpdateApp)
          getApp.app.errors.map(_.source) should contain(ErrorSource.App)
          getApp.app.errors.head.errorMessage should include("test")
          getApp.app.status shouldBe AppStatus.Running
        }
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "save an error and transition app to error when an error occurs in azure deleteApp" in isolatedDbTest {
    val exception = new RuntimeException("random test exception")
    val queue = makeTaskQueue()
    val mockAckConsumer = mock[AckHandler]

    val mockAksInterp = new MockAKSInterp {
      override def deleteApp(params: DeleteAKSAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
        IO.raiseError(exception)
    }

    val leoSubscriber = makeLeoSubscriber(
      azureInterp = makeAzureInterp(asyncTaskQueue = queue, mockWsmClient = mockWsm, mockAksInterp = mockAksInterp),
      asyncTaskQueue = queue
    )

    val savedCluster1 = makeAzureCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id, appType = AppType.Cromwell).save()
    val msg =
      DeleteAppV2Message(savedApp1.id,
                         savedApp1.appName,
                         savedApp1.workspaceId.get,
                         savedCluster1.cloudContext,
                         None,
                         billingProfileId,
                         None
      )

    val res =
      for {
        _ <- leoSubscriber.messageHandler(ReceivedMessage(msg, None, instantTimestamp, mockAckConsumer))

        assertions = for {
          getAppOpt <- KubernetesServiceDbQueries
            .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
            .transaction
          getApp = getAppOpt.get
        } yield {
          getApp.app.errors.size shouldBe 1
          getApp.app.errors.map(_.action) should contain(ErrorAction.DeleteApp)
          getApp.app.errors.map(_.source) should contain(ErrorSource.App)
          getApp.app.errors.head.errorMessage should include("test")
          getApp.app.status shouldBe AppStatus.Error
        }
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "save an error and transition app to error when an error occurs in azure createApp" in isolatedDbTest {
    val exception = new RuntimeException("random test exception")
    val queue = makeTaskQueue()
    val mockAckConsumer = mock[AckHandler]

    val mockAksInterp = new MockAKSInterp {
      override def createAndPollApp(params: CreateAKSAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
        IO.raiseError(exception)
    }

    val leoSubscriber = makeLeoSubscriber(
      azureInterp = makeAzureInterp(asyncTaskQueue = queue, mockWsmClient = mockWsm, mockAksInterp = mockAksInterp),
      asyncTaskQueue = queue
    )

    val savedCluster1 = makeAzureCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id, appType = AppType.Cromwell).save()
    val msg =
      CreateAppV2Message(savedApp1.id,
                         savedApp1.appName,
                         savedApp1.workspaceId.get,
                         savedCluster1.cloudContext,
                         billingProfileId,
                         None
      )

    val res =
      for {
        _ <- leoSubscriber.messageHandler(ReceivedMessage(msg, None, instantTimestamp, mockAckConsumer))

        assertions = for {
          getAppOpt <- KubernetesServiceDbQueries
            .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
            .transaction
          getApp = getAppOpt.get
        } yield {
          getApp.app.errors.size shouldBe 1
          getApp.app.errors.map(_.action) should contain(ErrorAction.CreateApp)
          getApp.app.errors.map(_.source) should contain(ErrorSource.App)
          getApp.app.errors.head.errorMessage should include("test")
          getApp.app.status shouldBe AppStatus.Error
        }
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    verify(mockAckConsumer, times(1)).ack()
  }

  it should "create a metric for a successful and failed condition" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedApp1 = makeApp(1, savedNodepool1.id).save()
    val mockAckConsumer = mock[AckHandler]

    val assertions = for {
      getAppOpt <- KubernetesServiceDbQueries
        .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
        .transaction
      getApp = getAppOpt.get
    } yield {
      getApp.app.errors.size shouldBe 1
      getApp.app.errors.map(_.action) should contain(ErrorAction.CreateApp)
      getApp.app.errors.map(_.source) should contain(ErrorSource.Disk)
    }
    val queue = Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // we want to validate calls to the metrics system
    implicit val metrics = spy(this.metrics)

    val leoSubscriber = makeLeoSubscriber(asyncTaskQueue = queue, diskService = makeDetachingDiskInterp())
    val res =
      for {
        tr <- traceId.ask[TraceId]
        msg = CreateAppMessage(
          savedCluster1.cloudContext.asInstanceOf[CloudContext.Gcp].value,
          Some(ClusterNodepoolAction.CreateNodepool(savedNodepool1.id)),
          savedApp1.id,
          savedApp1.appName,
          Some(DiskId(-1)),
          Map.empty,
          AppType.Galaxy,
          savedApp1.appResources.namespace,
          None,
          Some(tr),
          false
        )
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- leoSubscriber.messageHandler(ReceivedMessage(msg, None, instantTimestamp, mockAckConsumer))
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    verify(mockAckConsumer, times(1)).ack()
    verify(metrics, times(1)).recordDuration(startsWith(s"pubsub/success"), any(), any(), any())(any())
    verify(metrics, times(1)).recordDuration(startsWith(s"pubsub/fail"), any(), any(), any())(any())
  }

  def makeGKEInterp(lock: KeyLock[IO, GKEModels.KubernetesClusterId],
                    appRelease: List[Release] = List.empty
  ): GKEInterpreter[IO] =
    new GKEInterpreter[IO](
      Config.gkeInterpConfig,
      bucketHelper,
      vpcInterp,
      MockGKEService,
      new MockKubernetesService(PodStatus.Succeeded, appRelease = appRelease),
      MockHelm,
      MockAppDAO,
      credentials,
      iamDAOKubernetes,
      makeDetachingDiskInterp(),
      MockAppDescriptorDAO,
      lock,
      resourceService,
      FakeGoogleComputeService
    )

  def makeLeoSubscriber(
    runtimeMonitor: RuntimeMonitor[IO, CloudService] = MockRuntimeMonitor,
    asyncTaskQueue: Queue[IO, Task[IO]] =
      Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global),
    gkeAlgebra: GKEAlgebra[IO] = new org.broadinstitute.dsde.workbench.leonardo.MockGKEService,
    diskService: GoogleDiskService[IO] = MockGoogleDiskService,
    storageService: GoogleStorageService[IO] = FakeGoogleStorageService,
    dataprocRuntimeAlgebra: RuntimeAlgebra[IO] = dataprocInterp,
    gceRuntimeAlgebra: RuntimeAlgebra[IO] = gceInterp,
    azureInterp: AzurePubsubHandlerAlgebra[IO] = makeAzureInterp()
  )(implicit metrics: OpenTelemetryMetrics[IO]): LeoPubsubMessageSubscriber[IO] = {

    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocRuntimeAlgebra, gceRuntimeAlgebra)
    implicit val monitor: RuntimeMonitor[IO, CloudService] = runtimeMonitor

    val subscriberServicesRegistry = ServicesRegistry()

    subscriberServicesRegistry.register[RuntimeInstances[IO]](runtimeInstances)
    subscriberServicesRegistry.register[RuntimeMonitor[IO, CloudService]](monitor)
    val gcpDependencies = mock[GcpDependencies[IO]]
    when(gcpDependencies.googleDiskService).thenReturn(diskService)
    when(gcpDependencies.googleStorageService).thenReturn(storageService)
    subscriberServicesRegistry.register[GcpDependencies[IO]](gcpDependencies)
    subscriberServicesRegistry.register[GKEAlgebra[IO]](gkeAlgebra)
    val cloudSubscriber = mock[CloudSubscriber[IO, LeoPubsubMessage]]
    when(cloudSubscriber.messages).thenReturn(Stream.empty)

    val underlyingOperationFutureCache =
      Caffeine
        .newBuilder()
        .maximumSize(50L)
        .recordStats()
        .build[Long, scalacache.Entry[OperationFuture[Operation, Operation]]]()
    val operationFutureCache =
      CaffeineCache[IO, Long, OperationFuture[Operation, Operation]](underlyingOperationFutureCache)

    new LeoPubsubMessageSubscriber[IO](
      LeoPubsubMessageSubscriberConfig(1,
                                       30 seconds,
                                       Config.leoPubsubMessageSubscriberConfig.persistentDiskMonitorConfig,
                                       Config.leoPubsubMessageSubscriberConfig.galaxyDiskConfig,
                                       1 seconds
      ),
      cloudSubscriber,
      asyncTaskQueue,
      MockAuthProvider,
      azureInterp,
      operationFutureCache,
      subscriberServicesRegistry
    )
  }
  val (mockWsm, mockControlledResourceApi, mockResourceApi) = AzureTestUtils.setUpMockWsmApiClientProvider()

  // Needs to be made for each test its used in, otherwise queue will overlap
  def makeAzureInterp(asyncTaskQueue: Queue[IO, Task[IO]] = makeTaskQueue(),
                      relayService: AzureRelayService[IO] = FakeAzureRelayService,
                      wsmDAO: MockWsmDAO = new MockWsmDAO,
                      azureVmService: AzureVmService[IO] = FakeAzureVmService,
                      mockWsmClient: WsmApiClientProvider[IO] = mockWsm,
                      mockAksInterp: AKSAlgebra[IO] = new MockAKSInterp()
  ): AzurePubsubHandlerAlgebra[IO] =
    new AzurePubsubHandlerInterp[IO](
      ConfigReader.appConfig.azure.pubsubHandler,
      new ApplicationConfig("test",
                            GoogleProject("test"),
                            Paths.get("x.y"),
                            WorkbenchEmail("z@x.y"),
                            new URL("https://leonardo.foo.org"),
                            "dev",
                            0L
      ),
      contentSecurityPolicy,
      asyncTaskQueue,
      wsmDAO,
      new MockSamDAO(),
      new MockWelderDAO(),
      new MockJupyterDAO(),
      relayService,
      azureVmService,
      mockAksInterp,
      refererConfig,
      mockWsmClient
    )

  def makeTaskQueue(): Queue[IO, Task[IO]] =
    Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

  def makeDetachingDiskInterp(): GoogleDiskService[IO] =
    new MockGoogleDiskService {
      override def getDisk(project: GoogleProject, zone: ZoneName, diskName: DiskName)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Disk]] = IO(Some(Disk.newBuilder().setLastDetachTimestamp(Random.nextInt().toString).build()))
    }
}
