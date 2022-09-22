package org.broadinstitute.dsde.workbench.leonardo
package util

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine, VirtualMachineSizeTypes}
import com.azure.resourcemanager.network.models.PublicIpAddress
import org.broadinstitute.dsde.workbench.azure.mock.{FakeAzureRelayService, FakeAzureVmService}
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, AzureRelayService, AzureVmService, RelayNamespace}
import reactor.core.publisher.Mono
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{ConfigReader, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.http4s.headers.Authorization
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.time.ZonedDateTime
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class AzurePubsubHandlerSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with TestComponent
    with Matchers
    with MockitoSugar
    with Eventually
    with LeonardoTestSuite {
  val storageContainerResourceId = WsmControlledResourceId(UUID.randomUUID())

  it should "create azure vm properly" in isolatedDbTest {
    val vmReturn = mock[VirtualMachine]
    val ipReturn: PublicIpAddress = mock[PublicIpAddress]

    when(vmReturn.powerState()).thenReturn(PowerState.RUNNING)

    val stubIp = "0.0.0.0"
    when(vmReturn.getPrimaryPublicIPAddress()).thenReturn(ipReturn)
    when(ipReturn.ipAddress()).thenReturn(stubIp)

    val queue = QueueFactory.asyncTaskQueue()
    val resourceId = WsmControlledResourceId(UUID.randomUUID())
    val mockWsmDAO = new MockWsmDAO {
      override def getCreateVmJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[GetCreateVmJobResult] =
        IO.pure(
          GetCreateVmJobResult(
            Some(WsmVm(WsmVMMetadata(resourceId))),
            WsmJobReport(WsmJobId("job1"), "", WsmJobStatus.Succeeded, 200, ZonedDateTime.now(), None, "url"),
            None
          )
        )
    }
    val azureInterp =
      makeAzureInterp(asyncTaskQueue = queue, wsmDAO = mockWsmDAO)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       disk.id,
                                                       azureRegion
        )
        runtime = makeCluster(1)
          .copy(
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
        } yield {
          getRuntime.asyncRuntimeFields.flatMap(_.hostIp).isDefined shouldBe true
          getRuntime.status shouldBe RuntimeStatus.Running
        }

        msg = CreateAzureRuntimeMessage(runtime.id,
                                        workspaceId,
                                        RelayNamespace("relay-ns"),
                                        storageContainerResourceId,
                                        None
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
        controlledResources <- controlledResourceQuery.getAllForRuntime(runtime.id).transaction
      } yield {
        controlledResources.length shouldBe 4
        val resourceTypes = controlledResources.map(_.resourceType)
        resourceTypes.contains(WsmResourceType.AzureVm) shouldBe true
        resourceTypes.contains(WsmResourceType.AzureNetwork) shouldBe true
        resourceTypes.contains(WsmResourceType.AzureDisk) shouldBe true
        resourceTypes.contains(WsmResourceType.AzureStorageContainer) shouldBe true
        controlledResources.map(_.resourceId).contains(resourceId) shouldBe true
      }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete azure vm properly" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val mockWsmDao = mock[WsmDao[IO]]
    when {
      mockWsmDao.deleteStorageContainer(any[DeleteWsmResourceRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(None)
    when {
      mockWsmDao.deleteVm(any[DeleteWsmResourceRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(None)
    when {
      mockWsmDao.deleteNetworks(any[DeleteWsmResourceRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(None)
    when {
      mockWsmDao.deleteDisk(any[DeleteWsmResourceRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(None)
    when {
      mockWsmDao.getDeleteVmJobResult(any[GetJobResultRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(
      Some(
        GetDeleteJobResult(
          WsmJobReport(
            WsmJobId("job1"),
            "desc",
            WsmJobStatus.Succeeded,
            200,
            ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
            Some(ZonedDateTime.parse("2022-03-18T15:02:29.264756Z")),
            "resultUrl"
          ),
          None
        )
      )
    )

    val eqWorkspaceId: WorkspaceId = any[UUID].asInstanceOf[WorkspaceId]

    when {
      mockWsmDao.getRelayNamespace(eqWorkspaceId, any[com.azure.core.management.Region], any[Authorization])(
        any[Ask[IO, AppContext]]
      )
    } thenReturn IO.pure(None)
    val azureInterp = makeAzureInterp(asyncTaskQueue = queue, wsmDAO = mockWsmDao)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       disk.id,
                                                       azureRegion
        )
        runtime = makeCluster(2)
          .copy(
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          controlledResources <- controlledResourceQuery.getAllForRuntime(runtime.id).transaction
        } yield {
          verify(mockWsmDao, times(1)).deleteStorageContainer(any[DeleteWsmResourceRequest], any[Authorization])(
            any[Ask[IO, AppContext]]
          )
          getRuntime.status shouldBe RuntimeStatus.Deleted
          controlledResources.length shouldBe 3
        }

        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDisk)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureStorageContainer)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureNetwork)
          .transaction
        msg = DeleteAzureRuntimeMessage(runtime.id, Some(disk.id), workspaceId, Some(wsmResourceId), None)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle error in create azure vm async task properly" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val exceptionMsg = "test exception"
    val mockWsmDao = new MockWsmDAO {
      override def getCreateVmJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[GetCreateVmJobResult] = IO.raiseError(new Exception(exceptionMsg))
    }
    val azureInterp = makeAzureInterp(asyncTaskQueue = queue, wsmDAO = mockWsmDao)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       disk.id,
                                                       azureRegion
        )
        runtime = makeCluster(1)
          .copy(
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          error <- clusterErrorQuery.get(runtime.id).transaction
        } yield {
          getRuntime.status shouldBe RuntimeStatus.Error
          error.length shouldBe 1
          error.map(_.errorMessage).head should include(exceptionMsg)
        }

        msg = CreateAzureRuntimeMessage(runtime.id,
                                        workspaceId,
                                        RelayNamespace("relay-ns"),
                                        storageContainerResourceId,
                                        None
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle error in delete azure vm async task properly" in isolatedDbTest {
    val exceptionMsg = "test exception"
    val queue = QueueFactory.asyncTaskQueue()
    val wsm = new MockWsmDAO {
      override def getDeleteVmJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Option[GetDeleteJobResult]] = IO.raiseError(new Exception("test exception"))
    }
    val azureInterp = makeAzureInterp(asyncTaskQueue = queue, wsmDAO = wsm)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       disk.id,
                                                       azureRegion
        )
        runtime = makeCluster(2)
          .copy(
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        // Here we manually save a controlled resource with the runtime because we want too ensure it isn't deleted on error
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureNetwork)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDisk)
          .transaction

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          error <- clusterErrorQuery.get(runtime.id).transaction
        } yield {
          getRuntime.status shouldBe RuntimeStatus.Error
          error.map(_.errorMessage).head should include(exceptionMsg)
        }

        msg = DeleteAzureRuntimeMessage(runtime.id, Some(disk.id), workspaceId, Some(wsmResourceId), None)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail if WSM delete VM job doesn't complete in time" in isolatedDbTest {
    val exceptionMsg = "WSM delete VM job was not completed within 20 attempts with 1 second delay"
    val queue = QueueFactory.asyncTaskQueue()
    val wsm = new MockWsmDAO {
      override def getDeleteVmJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Option[GetDeleteJobResult]] =
        IO.pure(
          Some(
            GetDeleteJobResult(
              WsmJobReport(
                request.jobId,
                "desc",
                WsmJobStatus.Running,
                200,
                ZonedDateTime.parse("2022-03-18T15:02:29.264756Z"),
                Some(ZonedDateTime.parse("2022-03-18T15:02:29.264756Z")),
                "resultUrl"
              ),
              None
            )
          )
        )
    }
    val azureInterp = makeAzureInterp(asyncTaskQueue = queue, wsmDAO = wsm)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       disk.id,
                                                       azureRegion
        )
        runtime = makeCluster(2)
          .copy(
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        // Here we manually save a controlled resource with the runtime because we want too ensure it isn't deleted on error
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureNetwork)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDisk)
          .transaction

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          error <- clusterErrorQuery.get(runtime.id).transaction
        } yield {
          getRuntime.status shouldBe RuntimeStatus.Error
          error.map(_.errorMessage).head should include(exceptionMsg)
        }

        msg = DeleteAzureRuntimeMessage(runtime.id, Some(disk.id), workspaceId, Some(wsmResourceId), None)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "start azure vm properly" in isolatedDbTest {
    // Set up virtual machine mock.
    val vmReturn = mock[VirtualMachine]
    when(vmReturn.powerState()).thenReturn(PowerState.STOPPED)

    val passAzureVmService = new FakeAzureVmService {
      override def startAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Mono[Void]]] = IO.some(Mono.empty[Void]())
    }

    val queue = QueueFactory.asyncTaskQueue()
    val resourceId = WsmControlledResourceId(UUID.randomUUID())
    val mockWsmDAO = new MockWsmDAO {
      override def getCreateVmJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[GetCreateVmJobResult] =
        IO.pure(
          GetCreateVmJobResult(
            Some(WsmVm(WsmVMMetadata(resourceId))),
            WsmJobReport(WsmJobId("job1"), "", WsmJobStatus.Succeeded, 200, ZonedDateTime.now(), None, "url"),
            None
          )
        )
    }
    val azureInterp =
      makeAzureInterp(asyncTaskQueue = queue, wsmDAO = mockWsmDAO, azureVmService = passAzureVmService)

    val res = for {
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     disk.id,
                                                     azureRegion
      )
      runtime = makeCluster(1)
        .copy(
          cloudContext = CloudContext.Azure(azureCloudContext)
        )
        .saveWithRuntimeConfig(azureRuntimeConfig)

      // Assertions for runtime
      assertions = for {
        getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
        getRuntime = getRuntimeOpt.get
      } yield getRuntime.status shouldBe RuntimeStatus.Running

      // Mocked response, should succeed into the Some(mono) case
      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- azureInterp.startAndMonitorRuntime(runtime, azureCloudContext)

      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)

    } yield {
      // Assertions
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // Needs to be made for each test its used in, otherwise queue will overlap
  def makeAzureInterp(asyncTaskQueue: Queue[IO, Task[IO]] = QueueFactory.asyncTaskQueue(),
                      relayService: AzureRelayService[IO] = FakeAzureRelayService,
                      wsmDAO: WsmDao[IO] = new MockWsmDAO,
                      azureVmService: AzureVmService[IO] = FakeAzureVmService
  ): AzurePubsubHandlerInterp[IO] =
    new AzurePubsubHandlerInterp[IO](
      ConfigReader.appConfig.azure.pubsubHandler,
      contentSecurityPolicy,
      asyncTaskQueue,
      wsmDAO,
      new MockSamDAO(),
      new MockWelderDAO(),
      new MockJupyterDAO(),
      relayService,
      azureVmService
    )

}
