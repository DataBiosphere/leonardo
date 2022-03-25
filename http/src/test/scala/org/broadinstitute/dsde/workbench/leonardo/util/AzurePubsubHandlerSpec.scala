package org.broadinstitute.dsde.workbench.leonardo
package util

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine, VirtualMachineSizeTypes}
import com.azure.resourcemanager.network.models.PublicIpAddress
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{ConfigReader, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.http4s.headers.Authorization
import org.mockito.Mockito.when
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
      override def getCreateVmJobResult(request: GetJobResultRequest, authorization: Authorization)(
        implicit ev: Ask[IO, AppContext]
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
      makeAzureInterp(computeManagerDao = new MockComputeManagerDao(Some(vmReturn)),
                      asyncTaskQueue = queue,
                      wsmDAO = mockWsmDAO)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       disk.id,
                                                       azureRegion)
        runtime = makeCluster(1)
          .copy(
            runtimeImages = Set(azureImage),
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

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, WsmJobId("job1"), None)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
        controlledResources <- controlledResourceQuery.getAllForRuntime(runtime.id).transaction
      } yield {
        controlledResources.length shouldBe 1
        controlledResources.map(_.resourceType) should contain(WsmResourceType.AzureVm)
        controlledResources.map(_.resourceId) shouldBe List(resourceId)
      }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete azure vm properly" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val azureInterp = makeAzureInterp(asyncTaskQueue = queue)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       disk.id,
                                                       azureRegion)
        runtime = makeCluster(2)
          .copy(
            runtimeImages = Set(azureImage),
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          controlledResources <- controlledResourceQuery.getAllForRuntime(runtime.id).transaction
        } yield {
          getRuntime.status shouldBe RuntimeStatus.Deleted
          controlledResources.length shouldBe 0
        }

        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureIp)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDisk)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureNetwork)
          .transaction
        msg = DeleteAzureRuntimeMessage(runtime.id, Some(disk.id), workspaceId, wsmResourceId, None)

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
      override def getCreateVmJobResult(request: GetJobResultRequest, authorization: Authorization)(
        implicit ev: Ask[IO, AppContext]
      ): IO[GetCreateVmJobResult] = IO.raiseError(new Exception(exceptionMsg))
    }
    val azureInterp = makeAzureInterp(asyncTaskQueue = queue, wsmDAO = mockWsmDao)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       disk.id,
                                                       azureRegion)
        runtime = makeCluster(1)
          .copy(
            runtimeImages = Set(azureImage),
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

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, WsmJobId("job1"), None)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle error in delete azure vm async task properly" in isolatedDbTest {
    val exceptionMsg = "test exception"
    val mockComputeManagerDao = new MockComputeManagerDao {
      override def getAzureVm(name: RuntimeName, cloudContext: AzureCloudContext): IO[Option[VirtualMachine]] =
        IO.raiseError(new Exception(exceptionMsg))
    }
    val queue = QueueFactory.asyncTaskQueue()
    val azureInterp = makeAzureInterp(asyncTaskQueue = queue, computeManagerDao = mockComputeManagerDao)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       disk.id,
                                                       azureRegion)
        runtime = makeCluster(2)
          .copy(
            runtimeImages = Set(azureImage),
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        //Here we manually save a controlled resource with the runtime because we want too ensure it isn't deleted on error
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureNetwork)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureIp)
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

        msg = DeleteAzureRuntimeMessage(runtime.id, Some(disk.id), workspaceId, wsmResourceId, None)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
  // Needs to be made for each test its used in, otherwise queue will overlap
  def makeAzureInterp(asyncTaskQueue: Queue[IO, Task[IO]] = QueueFactory.asyncTaskQueue(),
                      computeManagerDao: ComputeManagerDao[IO] = new MockComputeManagerDao(),
                      wsmDAO: MockWsmDAO = new MockWsmDAO): AzureInterpreter[IO] =
    new AzureInterpreter[IO](
      ConfigReader.appConfig.azure.monitor,
      asyncTaskQueue,
      wsmDAO,
      new MockSamDAO(),
      computeManagerDao
    )

}
