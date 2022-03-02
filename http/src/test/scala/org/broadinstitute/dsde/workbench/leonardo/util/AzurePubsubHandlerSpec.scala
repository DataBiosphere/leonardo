package org.broadinstitute.dsde.workbench.leonardo
package util

import java.util.UUID

import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import akka.actor.ActorSystem
import org.broadinstitute.dsde.workbench.leonardo.db.{
  clusterErrorQuery,
  clusterQuery,
  controlledResourceQuery,
  TestComponent,
  WsmResourceType
}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.concurrent.Eventually
import akka.testkit.TestKit
import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.dao.{
  ComputeManagerDao,
  CreateVmResult,
  GetJobResultRequest,
  MockComputeManagerDao,
  MockWsmDAO
}
import org.broadinstitute.dsde.workbench.leonardo.http.ConfigReader
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine, VirtualMachineSizeTypes}
import com.azure.resourcemanager.network.models.PublicIpAddress
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.mockito.Mockito.when

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

    val queue = makeTaskQueue()
    val azureInterp =
      makeAzureInterp(computeManagerDao = new MockComputeManagerDao(Some(vmReturn)), asyncTaskQueue = queue)

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

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, azureImage, None)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.createAndPollRuntime(msg)

        controlledResources <- controlledResourceQuery.getAllForRuntime(runtime.id).transaction
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield {
        controlledResources.length shouldBe 4
        controlledResources.map(_.resourceType) should contain(WsmResourceType.AzureVm)
        controlledResources.map(_.resourceType) should contain(WsmResourceType.AzureNetwork)
        controlledResources.map(_.resourceType) should contain(WsmResourceType.AzureDisk)
        controlledResources.map(_.resourceType) should contain(WsmResourceType.AzureIp)
      }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete azure vm properly" in isolatedDbTest {
    val queue = makeTaskQueue()
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

        msg = DeleteAzureRuntimeMessage(runtime.id, Some(disk.id), workspaceId, wsmResourceId, None)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle error in create azure vm async task properly" in isolatedDbTest {
    val queue = makeTaskQueue()
    val exceptionMsg = "test exception"
    val mockWsmDao = new MockWsmDAO {
      override def getCreateVmJobResult(request: GetJobResultRequest)(
        implicit ev: Ask[IO, AppContext]
      ): IO[CreateVmResult] = IO.raiseError(new Exception(exceptionMsg))
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

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, azureImage, None)

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
    val queue = makeTaskQueue()
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

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          controlledResources <- controlledResourceQuery.getAllForRuntime(runtime.id).transaction
          error <- clusterErrorQuery.get(runtime.id).transaction
        } yield {
          getRuntime.status shouldBe RuntimeStatus.Error
          controlledResources.length shouldBe 1
          error.length shouldBe 1
          error.map(_.errorMessage).head should include(exceptionMsg)
        }

        msg = DeleteAzureRuntimeMessage(runtime.id, Some(disk.id), workspaceId, wsmResourceId, None)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  def makeTaskQueue(): Queue[IO, Task[IO]] =
    Queue.bounded[IO, Task[IO]](10).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

  // Needs to be made for each test its used in, otherwise queue will overlap
  def makeAzureInterp(asyncTaskQueue: Queue[IO, Task[IO]] = makeTaskQueue(),
                      computeManagerDao: ComputeManagerDao[IO] = new MockComputeManagerDao(),
                      wsmDAO: MockWsmDAO = new MockWsmDAO): AzureInterpreter[IO] =
    new AzureInterpreter[IO](
      ConfigReader.appConfig.azure.runtimeDefaults,
      ConfigReader.appConfig.azure.monitor,
      asyncTaskQueue,
      wsmDAO,
      computeManagerDao
    )

}
