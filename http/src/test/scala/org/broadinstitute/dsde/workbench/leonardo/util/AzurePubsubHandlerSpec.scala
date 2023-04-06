package org.broadinstitute.dsde.workbench.leonardo
package util

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.effect.std.Queue
import cats.implicits._
import cats.mtl.Ask
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine, VirtualMachineSizeTypes}
import com.azure.resourcemanager.network.models.PublicIpAddress
import org.broadinstitute.dsde.workbench.azure.mock.{FakeAzureRelayService, FakeAzureVmService}
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, AzureRelayService, AzureVmService, ContainerName}
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.config.ApplicationConfig
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{ConfigReader, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError.{
  AzureDiskResourceDeletionError,
  AzureRuntimeCreationError,
  AzureRuntimeStartingError,
  AzureRuntimeStoppingError,
  PubsubKubernetesError
}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.broadinstitute.dsp.HelmException
import org.http4s.headers.Authorization
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import reactor.core.publisher.Mono
import java.net.URL
import java.nio.file.Paths
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
    val azurePubsubHandler =
      makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDAO)

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
                                        storageContainerResourceId,
                                        landingZoneResources,
                                        false,
                                        None,
                                        "WorkspaceName",
                                        ContainerName("dummy")
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azurePubsubHandler.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
        controlledResources <- controlledResourceQuery.getAllForRuntime(runtime.id).transaction
      } yield {
        controlledResources.length shouldBe 2
        val resourceTypes = controlledResources.map(_.resourceType)
        resourceTypes.contains(WsmResourceType.AzureDisk) shouldBe true
        resourceTypes.contains(WsmResourceType.AzureStorageContainer) shouldBe true
      }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "create azure vm properly with a persistent disk" in isolatedDbTest {
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
    val azurePubsubHandler =
      makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDAO)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Restoring).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       disk.id,
                                                       azureRegion
        )
        runtime1 = makeCluster(1)
          .copy(
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)
        runtime2 = makeCluster(2)
          .copy(
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        resourceId = WsmControlledResourceId(UUID.randomUUID())
        _ <- controlledResourceQuery
          .save(runtime1.id, resourceId, WsmResourceType.AzureDisk)
          .transaction
        now <- IO.realTimeInstant
        _ <- persistentDiskQuery.updateWSMResourceId(disk.id, resourceId, now).transaction

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime2.id).transaction
          getRuntime = getRuntimeOpt.get
        } yield {
          // check diskId is correct
          getRuntime.asyncRuntimeFields.flatMap(_.hostIp).isDefined shouldBe true
          getRuntime.status shouldBe RuntimeStatus.Running
        }

        msg = CreateAzureRuntimeMessage(runtime2.id,
                                        workspaceId,
                                        storageContainerResourceId,
                                        landingZoneResources,
                                        true,
                                        None,
                                        "WorkspaceName",
                                        ContainerName("dummy")
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azurePubsubHandler.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
        controlledResources <- controlledResourceQuery.getAllForRuntime(runtime2.id).transaction
      } yield {
        controlledResources.length shouldBe 2
        val resourceTypes = controlledResources.map(_.resourceType)
        resourceTypes.contains(WsmResourceType.AzureDisk) shouldBe true
        resourceTypes.contains(WsmResourceType.AzureStorageContainer) shouldBe true
      }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle azure vm creation failure properly" in isolatedDbTest {
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
            WsmJobReport(WsmJobId("job1"), "", WsmJobStatus.Failed, 200, ZonedDateTime.now(), None, "url"),
            None
          )
        )
    }
    val azurePubsubHandler =
      makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDAO)

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
          runtimeStatus <- clusterQuery.getClusterStatus(runtime.id).transaction
          diskIdOpt <- RuntimeConfigQueries.getDiskId(runtime.runtimeConfigId).transaction
          diskStatus <- diskIdOpt.flatTraverse(id => persistentDiskQuery.getStatus(id).transaction)
        } yield {
          runtimeStatus shouldBe Some(RuntimeStatus.Error)
          diskStatus shouldBe (Some(DiskStatus.Deleted))
        }

        msg = CreateAzureRuntimeMessage(runtime.id,
                                        workspaceId,
                                        storageContainerResourceId,
                                        landingZoneResources,
                                        false,
                                        None,
                                        "WorkspaceName",
                                        ContainerName("dummy")
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azurePubsubHandler.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)

      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to create azure vm if welder doesn't come up" in isolatedDbTest {
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
    val fakeWelderDao = new MockWelderDAO() {
      override def isProxyAvailable(cloudContext: CloudContext, clusterName: RuntimeName): IO[Boolean] = IO.pure(false)
    }
    val azurePubsubHandler =
      makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDAO, welderDao = fakeWelderDao)

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
        } yield getRuntime.status shouldBe RuntimeStatus.Error

        msg = CreateAzureRuntimeMessage(runtime.id,
                                        workspaceId,
                                        storageContainerResourceId,
                                        landingZoneResources,
                                        false,
                                        None,
                                        "WorkspaceName",
                                        ContainerName("dummy")
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azurePubsubHandler.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

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
      mockWsmDao.deleteDisk(any[DeleteWsmResourceRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(None)
    when {
      mockWsmDao.getDeleteVmJobResult(any[GetJobResultRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(
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

    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDao)

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
        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        landingZoneResources,
                                        None
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete azure vm but keep the disk if no disk specified" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val mockWsmDao = mock[WsmDao[IO]]
    when {
      mockWsmDao.deleteStorageContainer(any[DeleteWsmResourceRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(None)
    when {
      mockWsmDao.deleteVm(any[DeleteWsmResourceRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(None)
    when {
      mockWsmDao.getDeleteVmJobResult(any[GetJobResultRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(
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

    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDao)

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
          diskStatusOpt <- persistentDiskQuery.getStatus(disk.id).transaction
          diskStatus = diskStatusOpt.get
        } yield {
          verify(mockWsmDao, times(1)).deleteStorageContainer(any[DeleteWsmResourceRequest], any[Authorization])(
            any[Ask[IO, AppContext]]
          )
          getRuntime.status shouldBe RuntimeStatus.Deleted
          controlledResources.length shouldBe 2
          val resourceTypes = controlledResources.map(_.resourceType)
          resourceTypes.contains(WsmResourceType.AzureDisk) shouldBe true
          diskStatus shouldBe DiskStatus.Ready
        }

        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDisk)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureStorageContainer)
          .transaction
        msg = DeleteAzureRuntimeMessage(runtime.id, None, workspaceId, Some(wsmResourceId), landingZoneResources, None)

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
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDao)

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
                                        storageContainerResourceId,
                                        landingZoneResources,
                                        false,
                                        None,
                                        "WorkspaceName",
                                        ContainerName("dummy")
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to create runtime with persistent disk if no resourceId" in isolatedDbTest {
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
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDAO)

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

        msg = CreateAzureRuntimeMessage(runtime.id,
                                        workspaceId,
                                        storageContainerResourceId,
                                        landingZoneResources,
                                        true,
                                        None,
                                        "WorkspaceName",
                                        ContainerName("dummy")
        )

        err <- azureInterp.createAndPollRuntime(msg).attempt

      } yield err shouldBe Left(
        AzureRuntimeCreationError(
          runtime.id,
          workspaceId,
          s"No associated resourceId found for Disk id:${disk.id.value}",
          true
        )
      )

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to create runtime with persistent disk if WSMresource not found" in isolatedDbTest {
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
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDAO)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
        now <- IO.realTimeInstant
        _ <- persistentDiskQuery.updateWSMResourceId(disk.id, resourceId, now).transaction

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       disk.id,
                                                       azureRegion
        )
        runtime = makeCluster(1)
          .copy(
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        msg = CreateAzureRuntimeMessage(runtime.id,
                                        workspaceId,
                                        storageContainerResourceId,
                                        landingZoneResources,
                                        true,
                                        None,
                                        "WorkspaceName",
                                        ContainerName("dummy")
        )

        err <- azureInterp.createAndPollRuntime(msg).attempt

      } yield err shouldBe
        Left(
          AzureRuntimeCreationError(
            runtime.id,
            workspaceId,
            s"WSMResource:${resourceId} not found for disk id:${disk.id.value}",
            true
          )
        )

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle error in delete azure vm async task properly" in isolatedDbTest {
    val exceptionMsg = "test exception"
    val queue = QueueFactory.asyncTaskQueue()
    val wsm = new MockWsmDAO {
      override def getDeleteVmJobResult(request: GetJobResultRequest, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[GetDeleteJobResult] = IO.raiseError(new Exception("test exception"))
    }
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = wsm)

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

        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        landingZoneResources,
                                        None
        )

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
      ): IO[GetDeleteJobResult] =
        IO.pure(
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
    }
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = wsm)

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

        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        landingZoneResources,
                                        None
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update runtime correctly when wsm deleteDisk call errors on runtime deletion" in isolatedDbTest {
    val exceptionMsg = "test exception"
    val queue = QueueFactory.asyncTaskQueue()
    val wsm = new MockWsmDAO {
      override def deleteDisk(request: DeleteWsmResourceRequest, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Option[DeleteWsmResourceResult]] =
        IO.raiseError(new Exception("test exception"))
    }
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = wsm)

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
          getRuntime.auditInfo.destroyedDate shouldBe None
          error.map(_.errorMessage).head should include(exceptionMsg)
        }

        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        landingZoneResources,
                                        None
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "start azure vm" in isolatedDbTest {
    // Set up virtual machine mock.
    val vmReturn = mock[VirtualMachine]
    when(vmReturn.powerState()).thenReturn(PowerState.STOPPED)

    val passAzureVmService = new FakeAzureVmService {
      override def startAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Mono[Void]]] = IO.some(Mono.empty[Void]())
    }

    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = passAzureVmService)

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

      assertions = for {
        getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
        getRuntime = getRuntimeOpt.get
      } yield getRuntime.status shouldBe RuntimeStatus.Running

      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- azureInterp.startAndMonitorRuntime(runtime, azureCloudContext)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)

    } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail on startAzureVm - Azure runtime starting error" in isolatedDbTest {
    val vmReturn = mock[VirtualMachine]
    when(vmReturn.powerState()).thenReturn(PowerState.STOPPED)

    val failAzureVmService = new FakeAzureVmService {
      override def startAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Mono[Void]]] = IO.none
    }

    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = failAzureVmService)

    val res = for {
      ctx <- appContext.ask[AppContext]
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

      startResult <- azureInterp.startAndMonitorRuntime(runtime, azureCloudContext).attempt

    } yield startResult shouldBe Left(
      AzureRuntimeStartingError(runtime.id, s"Starting runtime ${runtime.id} request to Azure failed.", ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "stop azure vm" in isolatedDbTest {
    val vmReturn = mock[VirtualMachine]
    when(vmReturn.powerState()).thenReturn(PowerState.RUNNING)

    val passAzureVmService = new FakeAzureVmService {
      override def stopAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Mono[Void]]] = IO.some(Mono.empty[Void]())
    }
    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = passAzureVmService)

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

      assertions = for {
        getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
        getRuntime = getRuntimeOpt.get
      } yield getRuntime.status shouldBe RuntimeStatus.Stopped

      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- azureInterp.stopAndMonitorRuntime(runtime, azureCloudContext)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)

    } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail on stopAzureVm - Azure runtime stopping error" in isolatedDbTest {
    // Set up virtual machine mock.
    val vmReturn = mock[VirtualMachine]
    when(vmReturn.powerState()).thenReturn(PowerState.RUNNING)

    val failAzureVmService = new FakeAzureVmService {
      override def stopAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Mono[Void]]] = IO.none
    }
    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = failAzureVmService)

    val res = for {
      ctx <- appContext.ask[AppContext]
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

      stopResult <- azureInterp.stopAndMonitorRuntime(runtime, azureCloudContext).attempt

    } yield stopResult shouldBe Left(
      AzureRuntimeStoppingError(runtime.id, s"Stopping runtime ${runtime.id} request to Azure failed.", ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle AKS errors" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()

    val failAksInterp = new MockAKSInterp {
      override def createAndPollApp(params: CreateAKSAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
        IO.raiseError(HelmException("something went wrong"))

      override def deleteApp(params: DeleteAKSAppParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
        IO.raiseError(HelmException("something went wrong"))
    }
    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, aksAlg = failAksInterp)

    val appId = AppId(42)

    val res = for {
      ctx <- appContext.ask[AppContext]
      result <- azureInterp
        .createAndPollApp(appId,
                          AppName("app"),
                          WorkspaceId(UUID.randomUUID()),
                          azureCloudContext,
                          landingZoneResources,
                          None
        )
        .attempt
    } yield result shouldBe Left(
      PubsubKubernetesError(
        AppError(
          s"Error creating Azure app with id ${appId.id} and cloudContext ${azureCloudContext.asString}: something went wrong",
          ctx.now,
          ErrorAction.CreateApp,
          ErrorSource.App,
          None,
          Some(ctx.traceId)
        ),
        Some(appId),
        false,
        None,
        None,
        None
      )
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete azure disk properly" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val mockWsmDao = mock[WsmDao[IO]]
    when {
      mockWsmDao.deleteDisk(any[DeleteWsmResourceRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(None)
    when {
      mockWsmDao.getDeleteDiskJobResult(any[GetJobResultRequest], any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(
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

    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDao)
    val resourceId = WsmControlledResourceId(UUID.randomUUID())

    val res =
      for {
        disk <- makePersistentDisk(wsmResourceId = Some(resourceId)).copy(status = DiskStatus.Ready).save()

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

        _ <- controlledResourceQuery
          .save(runtime.id, resourceId, WsmResourceType.AzureDisk)
          .transaction

        assertions = for {
          diskStatusOpt <- persistentDiskQuery.getStatus(disk.id).transaction
          diskStatus = diskStatusOpt.get
        } yield {
          verify(mockWsmDao, times(1)).deleteDisk(any[DeleteWsmResourceRequest], any[Authorization])(
            any[Ask[IO, AppContext]]
          )

          diskStatus shouldBe DiskStatus.Deleted
        }
        msg = DeleteDiskV2Message(disk.id, workspaceId, cloudContextAzure, disk.wsmResourceId, None)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteDisk(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a disk without a WSMresourceId" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val wsmResourceId = WsmControlledResourceId(UUID.randomUUID())
    val mockWsmDao = mock[WsmDao[IO]]
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDao)

    val res =
      for {
        disk <- makePersistentDisk(wsmResourceId = Some(wsmResourceId)).copy(status = DiskStatus.Ready).save()

        msg = DeleteDiskV2Message(disk.id, workspaceId, cloudContextAzure, Some(wsmResourceId), None)

        err <- azureInterp.deleteDisk(msg).attempt

      } yield err shouldBe Left(
        AzureDiskResourceDeletionError(
          Right(wsmResourceId),
          workspaceId,
          "No disk resource found for delete azure disk. No-op for wsmDao.deleteDisk."
        )
      )

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // Needs to be made for each test its used in, otherwise queue will overlap
  def makeAzurePubsubHandler(asyncTaskQueue: Queue[IO, Task[IO]] = QueueFactory.asyncTaskQueue(),
                             relayService: AzureRelayService[IO] = FakeAzureRelayService,
                             wsmDAO: WsmDao[IO] = new MockWsmDAO,
                             welderDao: WelderDAO[IO] = new MockWelderDAO(),
                             azureVmService: AzureVmService[IO] = FakeAzureVmService,
                             aksAlg: AKSAlgebra[IO] = new MockAKSInterp
  ): AzurePubsubHandlerAlgebra[IO] =
    new AzurePubsubHandlerInterp[IO](
      ConfigReader.appConfig.azure.pubsubHandler,
      new ApplicationConfig("test",
                            GoogleProject("test"),
                            Paths.get("x.y"),
                            WorkbenchEmail("z@x.y"),
                            new URL("https://leonardo.foo.broadinstitute.org"),
                            0L
      ),
      contentSecurityPolicy,
      asyncTaskQueue,
      wsmDAO,
      new MockSamDAO(),
      welderDao,
      new MockJupyterDAO(),
      relayService,
      azureVmService,
      aksAlg
    )

}
