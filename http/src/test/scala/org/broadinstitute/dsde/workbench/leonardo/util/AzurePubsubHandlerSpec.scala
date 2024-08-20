package org.broadinstitute.dsde.workbench.leonardo
package util

import akka.actor.ActorSystem
import akka.testkit.TestKit
import bio.terra.workspace.model.{DeleteControlledAzureResourceRequest, JobReport}
import cats.effect.IO
import cats.effect.std.Queue
import cats.implicits._
import cats.mtl.Ask
import com.azure.resourcemanager.compute.models.{PowerState, VirtualMachine, VirtualMachineSizeTypes}
import com.azure.resourcemanager.network.models.PublicIpAddress
import org.broadinstitute.dsde.workbench.azure.mock.{FakeAzureRelayService, FakeAzureVmService}
import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, AzureRelayService, AzureVmService}
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.AsyncTaskProcessor.Task
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.PrivateAzureStorageAccountSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.ApplicationConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.{WsmApiClientProvider, _}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{ConfigReader, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.PubsubHandleMessageError._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.broadinstitute.dsp.HelmException
import org.http4s.headers.Authorization
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{spy, times, verify, when}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import reactor.core.publisher.Mono

import java.net.URL
import java.nio.file.Paths
import java.time.{Instant, ZonedDateTime}
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

  val (mockWsm, mockControlledResourceApi, mockResourceApi, workspaceApi) =
    AzureTestUtils.setUpMockWsmApiClientProvider()

  it should "generate an Azure VM password properly" in {
    // Test this with a short password to make sure that the while loop works properly
    val password = AzurePubsubHandler.generateAzureVMSecurePassword(4)
    password.length shouldBe 4
    password.exists(_.isLower) shouldBe true
    password.exists(_.isUpper) shouldBe true
    password.exists(_.isDigit) shouldBe true
    password.exists(IndexedSeq('!', '@', '#', '$', '&', '*', '?', '^', '(', ')').contains) shouldBe true
  }

  it should "not use the shared Azure VM credentials in prod" in {
    val password = AzurePubsubHandler.getAzureVMSecurePassword("prod", "sharedPassword")
    assert(password != "sharedPassword")
    password.length shouldBe 25
  }

  it should "create azure vm properly" in isolatedDbTest {
    val vmReturn = mock[VirtualMachine]
    val ipReturn: PublicIpAddress = mock[PublicIpAddress]

    when(vmReturn.powerState()).thenReturn(PowerState.RUNNING)

    val stubIp = "0.0.0.0"
    when(vmReturn.getPrimaryPublicIPAddress()).thenReturn(ipReturn)
    when(ipReturn.ipAddress()).thenReturn(stubIp)

    val queue = QueueFactory.asyncTaskQueue()
    val resourceId = WsmControlledResourceId(UUID.randomUUID())
    val azurePubsubHandler =
      makeAzurePubsubHandler(asyncTaskQueue = queue)

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

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          getRuntimeConfig <- RuntimeConfigQueries.getRuntimeConfig(getRuntime.runtimeConfigId).transaction
        } yield {
          getRuntime.asyncRuntimeFields.flatMap(_.hostIp).isDefined shouldBe true
          getRuntime.status shouldBe RuntimeStatus.Running
          getRuntimeConfig shouldBe azureRuntimeConfig.copy(region = Some(RegionName("southcentralus")))
        }

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, false, None, "WorkspaceName", billingProfileId)

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

  it should "create azure vm properly when WSM is slow" in isolatedDbTest {
    val vmReturn = mock[VirtualMachine]
    val ipReturn: PublicIpAddress = mock[PublicIpAddress]

    when(vmReturn.powerState()).thenReturn(PowerState.RUNNING)

    val stubIp = "0.0.0.0"
    when(vmReturn.getPrimaryPublicIPAddress()).thenReturn(ipReturn)
    when(ipReturn.ipAddress()).thenReturn(stubIp)

    val queue = QueueFactory.asyncTaskQueue()
    val resourceId = WsmControlledResourceId(UUID.randomUUID())
    val startTime: Instant = Instant.now
    val mockLatencyMillis = 5000
    val azurePubsubHandler =
      makeAzurePubsubHandler(asyncTaskQueue = queue)

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

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          getRuntimeConfig <- RuntimeConfigQueries.getRuntimeConfig(getRuntime.runtimeConfigId).transaction
        } yield {
          getRuntime.asyncRuntimeFields.flatMap(_.hostIp).isDefined shouldBe true
          getRuntime.status shouldBe RuntimeStatus.Running
          getRuntime.auditInfo.dateAccessed.isAfter(startTime.plusMillis(mockLatencyMillis)) shouldBe true
          getRuntimeConfig shouldBe azureRuntimeConfig.copy(region = Some(RegionName("southcentralus")))
        }

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, false, None, "WorkspaceName", billingProfileId)

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
    val azurePubsubHandler =
      makeAzurePubsubHandler(asyncTaskQueue = queue)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Restoring).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
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
          getRuntimeConfig <- RuntimeConfigQueries.getRuntimeConfig(getRuntime.runtimeConfigId).transaction
        } yield {
          // check diskId is correct
          getRuntime.asyncRuntimeFields.flatMap(_.hostIp).isDefined shouldBe true
          getRuntime.status shouldBe RuntimeStatus.Running
          getRuntimeConfig shouldBe azureRuntimeConfig.copy(region = Some(RegionName("southcentralus")))
        }

        msg = CreateAzureRuntimeMessage(runtime2.id, workspaceId, true, None, "WorkspaceName", billingProfileId)

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

  it should "create an azure vm properly with an action managed identity" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val resourceId = WsmControlledResourceId(UUID.randomUUID())
    val mockSamDAO = new MockSamDAO {
      override def getAzureActionManagedIdentity(authHeader: Authorization,
                                                 resource: PrivateAzureStorageAccountSamResourceId,
                                                 action: PrivateAzureStorageAccountAction
      )(implicit ev: Ask[IO, TraceId]): IO[Option[String]] = IO(Some("awesome-identity"))
    }
    val azurePubsubHandler =
      makeAzurePubsubHandler(asyncTaskQueue = queue, samDAO = mockSamDAO)

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

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          getRuntimeConfig <- RuntimeConfigQueries.getRuntimeConfig(getRuntime.runtimeConfigId).transaction
        } yield {
          getRuntime.asyncRuntimeFields.flatMap(_.hostIp).isDefined shouldBe true
          getRuntime.status shouldBe RuntimeStatus.Running
          getRuntimeConfig shouldBe azureRuntimeConfig.copy(region = Some(RegionName("southcentralus")))
        }

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, false, None, "WorkspaceName", billingProfileId)

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

  it should "handle azure vm creation failure properly" in isolatedDbTest {
    val vmReturn = mock[VirtualMachine]
    val ipReturn: PublicIpAddress = mock[PublicIpAddress]

    when(vmReturn.powerState()).thenReturn(PowerState.RUNNING)

    val stubIp = "0.0.0.0"
    when(vmReturn.getPrimaryPublicIPAddress()).thenReturn(ipReturn)
    when(ipReturn.ipAddress()).thenReturn(stubIp)

    val queue = QueueFactory.asyncTaskQueue()
    val resourceId = WsmControlledResourceId(UUID.randomUUID())
    val (mockWsm, _, _, _) = AzureTestUtils.setUpMockWsmApiClientProvider(vmJobStatus = JobReport.StatusEnum.FAILED)

    val azurePubsubHandler =
      makeAzurePubsubHandler(asyncTaskQueue = queue, wsmClient = mockWsm)

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

        assertions = for {
          runtimeStatus <- clusterQuery.getClusterStatus(runtime.id).transaction
          diskStatus <- persistentDiskQuery.getStatus(disk.id).transaction
        } yield {
          runtimeStatus shouldBe Some(RuntimeStatus.Error)
          diskStatus shouldBe Some(DiskStatus.Deleted)
        }

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, false, None, "WorkspaceName", billingProfileId)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azurePubsubHandler.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)

      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle azure disk creation failure from wsm properly" in isolatedDbTest {
    val (mockWsm, _, _, _) = AzureTestUtils.setUpMockWsmApiClientProvider(diskJobStatus = JobReport.StatusEnum.FAILED)

    val queue = QueueFactory.asyncTaskQueue()

    val azurePubsubHandler =
      makeAzurePubsubHandler(asyncTaskQueue = queue, wsmClient = mockWsm)

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

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, false, None, "WorkspaceName", billingProfileId)

        _ <- azurePubsubHandler.createAndPollRuntime(msg)

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

  it should "fail to create azure vm if welder doesn't come up" in isolatedDbTest {
    val vmReturn = mock[VirtualMachine]
    val ipReturn: PublicIpAddress = mock[PublicIpAddress]

    when(vmReturn.powerState()).thenReturn(PowerState.RUNNING)

    val stubIp = "0.0.0.0"
    when(vmReturn.getPrimaryPublicIPAddress()).thenReturn(ipReturn)
    when(ipReturn.ipAddress()).thenReturn(stubIp)

    val queue = QueueFactory.asyncTaskQueue()
    val resourceId = WsmControlledResourceId(UUID.randomUUID())
    val fakeWelderDao = new MockWelderDAO() {
      override def isProxyAvailable(cloudContext: CloudContext, clusterName: RuntimeName): IO[Boolean] = IO.pure(false)
    }
    val azurePubsubHandler =
      makeAzurePubsubHandler(asyncTaskQueue = queue, welderDao = fakeWelderDao)

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

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
        } yield getRuntime.status shouldBe RuntimeStatus.Error

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, false, None, "WorkspaceName", billingProfileId)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azurePubsubHandler.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete azure vm properly" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val mockWsmDao = mock[WsmDao[IO]]
    val (mockWsm, mockControlledResourceApi, _, _) =
      AzureTestUtils.setUpMockWsmApiClientProvider()
    when {
      mockWsmDao.getLandingZoneResources(BillingProfileId(any[String]), any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(landingZoneResources)

    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDao, wsmClient = mockWsm)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
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
          verify(mockControlledResourceApi, times(1)).deleteAzureStorageContainer(any, any, any)
          verify(mockControlledResourceApi, times(1)).deleteAzureDisk(any, any, any)
          verify(mockControlledResourceApi, times(1)).deleteAzureVm(any, any, any)
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
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDatabase)
          .transaction
        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        billingProfileId,
                                        None
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "poll on azure delete vm and disk" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val mockWsmDao = mock[WsmDao[IO]]
    when {
      mockWsmDao.getLandingZoneResources(BillingProfileId(any[String]), any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(landingZoneResources)

    val (mockWsm, mockControlledResourceApi, _, _) = AzureTestUtils.setUpMockWsmApiClientProvider()
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDao, wsmClient = mockWsm)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
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
          verify(mockControlledResourceApi, times(1)).getDeleteAzureDiskResult(any[UUID], any[String])
          verify(mockControlledResourceApi, times(1)).getDeleteAzureVmResult(any[UUID], any[String])
          verify(mockControlledResourceApi, times(1)).getDeleteAzureStorageContainerResult(any[UUID], any[String])
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
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDatabase)
          .transaction
        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        billingProfileId,
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
      mockWsmDao.getLandingZoneResources(BillingProfileId(any[String]), any[Authorization])(any[Ask[IO, AppContext]])
    } thenReturn IO.pure(landingZoneResources)
    val (mockWsm, mockControlledResourceApi, _, _) = AzureTestUtils.setUpMockWsmApiClientProvider()

    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmDAO = mockWsmDao, wsmClient = mockWsm)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
        )
        runtime = makeCluster(2)
          .copy(
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        wsmDiskId = WsmControlledResourceId(UUID.randomUUID())
        wsmStorageContainerId = WsmControlledResourceId(UUID.randomUUID())

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          controlledResources <- controlledResourceQuery.getAllForRuntime(runtime.id).transaction
          diskStatusOpt <- persistentDiskQuery.getStatus(disk.id).transaction
          diskStatus = diskStatusOpt.get
          isAttached <- persistentDiskQuery.isDiskAttached(disk.id).transaction
        } yield {
          verify(mockControlledResourceApi, times(1)).deleteAzureStorageContainer(
            any[DeleteControlledAzureResourceRequest],
            mockitoEq(workspaceId.value),
            mockitoEq(wsmStorageContainerId.value)
          )
          verify(mockControlledResourceApi, times(1)).getDeleteAzureStorageContainerResult(mockitoEq(workspaceId.value),
                                                                                           any[String]
          )
          verify(mockControlledResourceApi, times(1)).getDeleteAzureVmResult(mockitoEq(workspaceId.value), any[String])
          verify(mockControlledResourceApi, times(0)).deleteAzureDisk(any[DeleteControlledAzureResourceRequest],
                                                                      any[UUID],
                                                                      any[UUID]
          )
          verify(mockControlledResourceApi, times(1)).deleteAzureVm(any[DeleteControlledAzureResourceRequest],
                                                                    mockitoEq(workspaceId.value),
                                                                    mockitoEq(wsmResourceId.value)
          )
          getRuntime.status shouldBe RuntimeStatus.Deleted
          controlledResources.length shouldBe 2
          val resourceTypes = controlledResources.map(_.resourceType)
          resourceTypes.contains(WsmResourceType.AzureDisk) shouldBe true
          diskStatus shouldBe DiskStatus.Ready
          isAttached shouldBe false
        }

        _ <- controlledResourceQuery
          .save(runtime.id, wsmDiskId, WsmResourceType.AzureDisk)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, wsmStorageContainerId, WsmResourceType.AzureStorageContainer)
          .transaction
        msg = DeleteAzureRuntimeMessage(runtime.id, None, workspaceId, Some(wsmResourceId), billingProfileId, None)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)

        isAttachedBeforeInterp <- persistentDiskQuery.isDiskAttached(disk.id).transaction
        _ = isAttachedBeforeInterp shouldBe true
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle storage container creation error in async task properly" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val exceptionMsg = "storage container failed to create"
    val (mockWsm, _, _, _) =
      AzureTestUtils.setUpMockWsmApiClientProvider(storageContainerJobStatus = JobReport.StatusEnum.FAILED)

    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmClient = mockWsm)

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

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          error <- clusterErrorQuery.get(runtime.id).transaction
          getDiskOpt <- persistentDiskQuery.getById(disk.id).transaction
          getDisk = getDiskOpt.get
        } yield {
          getRuntime.status shouldBe RuntimeStatus.Error
          error.length shouldBe 1
          error.map(_.errorMessage).head should include(exceptionMsg)
          getDisk.status shouldBe DiskStatus.Deleted
        }

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId2, false, None, "WorkspaceName", billingProfileId)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle vm creation error in async task properly" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val exceptionMsg = "test exception"
    val (mockWsm, controlledResourceApi, _, _) =
      AzureTestUtils.setUpMockWsmApiClientProvider(JobReport.StatusEnum.FAILED)

    when {
      controlledResourceApi.getCreateAzureVmResult(any, any)
    } thenAnswer {
      throw new Exception(exceptionMsg)
    }
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmClient = mockWsm)

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

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          error <- clusterErrorQuery.get(runtime.id).transaction
          getDiskOpt <- persistentDiskQuery.getById(disk.id).transaction
          getDisk = getDiskOpt.get
        } yield {
          getRuntime.status shouldBe RuntimeStatus.Error
          error.length shouldBe 1
          error.map(_.errorMessage).head should include(exceptionMsg)
          getDisk.status shouldBe DiskStatus.Deleted
        }

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, false, None, "WorkspaceName", billingProfileId)

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.createAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to create runtime with persistent disk if no resourceId" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val resourceId = WsmControlledResourceId(UUID.randomUUID())
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue)

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

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, true, None, "WorkspaceName", billingProfileId)

        err <- azureInterp.createAndPollRuntime(msg).attempt

      } yield err shouldBe Left(
        AzureRuntimeCreationError(
          runtime.id,
          workspaceId,
          s"WSMResource record:${wsmResourceId.value} not found for disk id:${disk.id.value}",
          true
        )
      )

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to create runtime with persistent disk if WSMresource not found" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val resourceId = WsmControlledResourceId(UUID.randomUUID())

    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
        now <- IO.realTimeInstant
        _ <- persistentDiskQuery.updateWSMResourceId(disk.id, resourceId, now).transaction

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
        )
        runtime = makeCluster(1)
          .copy(
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        msg = CreateAzureRuntimeMessage(runtime.id, workspaceId, true, None, "WorkspaceName", billingProfileId)

        err <- azureInterp.createAndPollRuntime(msg).attempt

      } yield err shouldBe
        Left(
          AzureRuntimeCreationError(
            runtime.id,
            workspaceId,
            s"WSMResource record:${resourceId.value} not found for disk id:${disk.id.value}",
            true
          )
        )

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "handle error in delete azure vm async task properly" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val (mockWsm, _, _, _) = AzureTestUtils.setUpMockWsmApiClientProvider(vmJobStatus = JobReport.StatusEnum.FAILED)
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmClient = mockWsm)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
        )
        runtime = makeCluster(2)
          .copy(
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        // Here we manually save a controlled resource with the runtime because we want too ensure it isn't deleted on error
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDatabase)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDisk)
          .transaction

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          error <- clusterErrorQuery.get(runtime.id).transaction
          getDiskOpt <- persistentDiskQuery.getById(disk.id).transaction
          getDisk = getDiskOpt.get
        } yield {
          getRuntime.status shouldBe RuntimeStatus.Error
          error.map(_.errorMessage).head should include("WSM delete VM job failed due")
          getDisk.status shouldBe DiskStatus.Error
        }

        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        billingProfileId,
                                        None
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail if WSM delete VM job doesn't complete in time" in isolatedDbTest {
    val exceptionMsg = "WSM delete VM job was not completed within"
    val queue = QueueFactory.asyncTaskQueue()
    val (mockWsm, _, _, _) = AzureTestUtils.setUpMockWsmApiClientProvider(vmJobStatus = JobReport.StatusEnum.RUNNING)
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmClient = mockWsm)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
        )
        runtime = makeCluster(2)
          .copy(
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        // Here we manually save a controlled resource with the runtime because we want too ensure it isn't deleted on error
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDatabase)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDisk)
          .transaction

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          error <- clusterErrorQuery.get(runtime.id).transaction
          getDiskOpt <- persistentDiskQuery.getById(disk.id).transaction
          getDisk = getDiskOpt.get
        } yield {
          getRuntime.status shouldBe RuntimeStatus.Error
          error.map(_.errorMessage).head should include(exceptionMsg)
          getDisk.status shouldBe DiskStatus.Error
        }

        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        billingProfileId,
                                        None
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)
        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update state correctly if WSM deleteDisk job fails during runtime deletion" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()
    val (mockWsm, _, _, _) = AzureTestUtils.setUpMockWsmApiClientProvider(diskJobStatus = JobReport.StatusEnum.FAILED)
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmClient = mockWsm)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
        )
        runtime = makeCluster(1)
          .copy(
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        // Here we manually save a controlled resource with the runtime because we want too ensure it isn't deleted on error
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDatabase)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDisk)
          .transaction

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          runtime = getRuntimeOpt.get
          getDiskOpt <- persistentDiskQuery.getById(disk.id).transaction
          getDisk = getDiskOpt.get
          diskAttached <- persistentDiskQuery.isDiskAttached(disk.id).transaction
        } yield {
          // VM must be deleted successfully for deleteDisk action to start
          // disk can then be deleted from the cloud environment page if desired
          runtime.status shouldBe RuntimeStatus.Error
          getDisk.status shouldBe DiskStatus.Error
          diskAttached shouldBe true
        }

        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        billingProfileId,
                                        None
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update runtime correctly when wsm deleteDisk call errors on runtime deletion" in isolatedDbTest {
    val exceptionMsg = "Wsm deleteDisk job failed due to"
    val queue = QueueFactory.asyncTaskQueue()
    val (mockWsm, _, _, _) = AzureTestUtils.setUpMockWsmApiClientProvider(diskJobStatus = JobReport.StatusEnum.FAILED)
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmClient = mockWsm)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
        )
        runtime = makeCluster(2)
          .copy(
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        // Here we manually save a controlled resource with the runtime because we want too ensure it isn't deleted on error
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDatabase)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDisk)
          .transaction

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          error <- clusterErrorQuery.get(runtime.id).transaction
          getDiskOpt <- persistentDiskQuery.getById(disk.id).transaction
          getDisk = getDiskOpt.get
        } yield {
          getRuntime.status shouldBe RuntimeStatus.Error
          getRuntime.auditInfo.destroyedDate shouldBe None
          error.map(_.errorMessage).head should include(exceptionMsg)
          getDisk.status shouldBe DiskStatus.Error
        }

        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        billingProfileId,
                                        None
        )

        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update runtime correctly when wsm deleteStorageContainer call errors on runtime deletion" in isolatedDbTest {
    val exceptionMsg = "WSM storage container delete job failed due to"
    val queue = QueueFactory.asyncTaskQueue()
    val (mockWsm, _, _, _) =
      AzureTestUtils.setUpMockWsmApiClientProvider(storageContainerJobStatus = JobReport.StatusEnum.FAILED)
    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmClient = mockWsm)

    val res =
      for {
        disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
        )
        runtime = makeCluster(2)
          .copy(
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        // Here we manually save a controlled resource with the runtime because we want too ensure it isn't deleted on error
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDatabase)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureDisk)
          .transaction
        _ <- controlledResourceQuery
          .save(runtime.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureStorageContainer)
          .transaction

        assertions = for {
          getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
          getRuntime = getRuntimeOpt.get
          error <- clusterErrorQuery.get(runtime.id).transaction
          getDiskOpt <- persistentDiskQuery.getById(disk.id).transaction
          getDisk = getDiskOpt.get
        } yield {
          getRuntime.status shouldBe RuntimeStatus.Error
          getRuntime.auditInfo.destroyedDate shouldBe None
          error.map(_.errorMessage).head should include(exceptionMsg)
          getDisk.status shouldBe DiskStatus.Error
        }

        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        billingProfileId,
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
    val fakeAzureVmService = AzureTestUtils.setupFakeAzureVmService(vmState = PowerState.DEALLOCATED)

    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = fakeAzureVmService)

    val res = for {
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

  it should "start azure vm - in starting status" in isolatedDbTest {
    // Set up virtual machine mock.
    val fakeAzureVmService = AzureTestUtils.setupFakeAzureVmService(vmState = PowerState.STARTING)

    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = fakeAzureVmService)

    val res = for {
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

  it should "start azure vm - no-op if already running " in isolatedDbTest {
    val vmName = "RunningRuntime"
    val fakeAzureVmService = AzureTestUtils.setupFakeAzureVmService(vmState = PowerState.RUNNING)

    val spyAzureVmService = spy(fakeAzureVmService)

    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = fakeAzureVmService)

    val res = for {
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     Some(disk.id),
                                                     None
      )
      runtime = makeCluster(1)
        .copy(
          cloudContext = CloudContext.Azure(azureCloudContext),
          runtimeName = RuntimeName(vmName)
        )
        .saveWithRuntimeConfig(azureRuntimeConfig)

      assertions = for {
        getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
        getRuntime = getRuntimeOpt.get
      } yield {
        verify(spyAzureVmService, times(0)).startAzureVm(InstanceName(vmName), azureCloudContext)
        getRuntime.status shouldBe RuntimeStatus.Running
      }

      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- azureInterp.startAndMonitorRuntime(runtime, azureCloudContext)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)

    } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail on startAzureVm - Azure runtime starting error" in isolatedDbTest {

    val failAzureVmService = AzureTestUtils.setupFakeAzureVmService(startVm = false, vmState = PowerState.DEALLOCATED)

    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = failAzureVmService)

    val res = for {
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

      startResult <- azureInterp.startAndMonitorRuntime(runtime, azureCloudContext).attempt

    } yield startResult shouldBe Left(
      AzureRuntimeStartingError(runtime.id, s"Starting runtime ${runtime.id} request to Azure failed.", ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail on startAzureVm - Azure vm in an unstartable status" in isolatedDbTest {
    val fakeAzureVmService = AzureTestUtils.setupFakeAzureVmService(vmState = PowerState.UNKNOWN)

    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = fakeAzureVmService)

    val res = for {
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

      startResult <- azureInterp.startAndMonitorRuntime(runtime, azureCloudContext).attempt

    } yield startResult shouldBe Left(
      AzureRuntimeStartingError(
        runtime.id,
        s"Runtime ${runtime.runtimeName.asString} cannot be started in a ${PowerState.UNKNOWN.toString} state, starting runtime request failed",
        ctx.traceId
      )
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "stop azure vm" in isolatedDbTest {
    val vmName = "RunningRuntime"
    val vmReturn = mock[VirtualMachine]
    when(vmReturn.powerState())
      .thenReturn(PowerState.RUNNING)
      .thenReturn(PowerState.DEALLOCATED)

    val passAzureVmService = new FakeAzureVmService {
      override def stopAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Mono[Void]]] = IO.some(Mono.empty[Void]())

      override def getAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[VirtualMachine]] = IO.some(vmReturn)
    }

    val spyAzureVmService = spy(passAzureVmService)

    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = spyAzureVmService)

    val res = for {
      ctx <- appContext.ask[AppContext]
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     Some(disk.id),
                                                     None
      )
      runtime = makeCluster(1)
        .copy(
          cloudContext = CloudContext.Azure(azureCloudContext),
          runtimeName = RuntimeName(vmName)
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

  it should "stop azure vm - in stopping status" in isolatedDbTest {
    val vmName = "StoppingRuntime"
    val vmReturn = mock[VirtualMachine]
    when(vmReturn.powerState())
      .thenReturn(PowerState.DEALLOCATING)
      .thenReturn(PowerState.DEALLOCATED)

    val passAzureVmService = new FakeAzureVmService {
      override def stopAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[Mono[Void]]] = IO.some(Mono.empty[Void]())

      override def getAzureVm(name: InstanceName, cloudContext: AzureCloudContext)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[VirtualMachine]] = IO.some(vmReturn)
    }

    val spyAzureVmService = spy(passAzureVmService)

    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = spyAzureVmService)

    val res = for {
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     Some(disk.id),
                                                     None
      )
      runtime = makeCluster(1)
        .copy(
          cloudContext = CloudContext.Azure(azureCloudContext),
          runtimeName = RuntimeName(vmName)
        )
        .saveWithRuntimeConfig(azureRuntimeConfig)

      assertions = for {
        getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
        getRuntime = getRuntimeOpt.get
      } yield {
        verify(spyAzureVmService, times(0)).stopAzureVm(InstanceName(vmName), azureCloudContext)
        getRuntime.status shouldBe RuntimeStatus.Stopped
      }

      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- azureInterp.stopAndMonitorRuntime(runtime, azureCloudContext)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)

    } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "stop azure vm - no-op if already stopped" in isolatedDbTest {
    val vmName = "StoppedRuntime"

    val fakeAzureVmService = AzureTestUtils.setupFakeAzureVmService(vmState = PowerState.STOPPED)

    val spyAzureVmService = spy(fakeAzureVmService)

    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = spyAzureVmService)

    val res = for {
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()
      azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                     Some(disk.id),
                                                     None
      )
      runtime = makeCluster(1)
        .copy(
          cloudContext = CloudContext.Azure(azureCloudContext),
          runtimeName = RuntimeName(vmName)
        )
        .saveWithRuntimeConfig(azureRuntimeConfig)

      assertions = for {
        getRuntimeOpt <- clusterQuery.getClusterById(runtime.id).transaction
        getRuntime = getRuntimeOpt.get
      } yield {
        verify(spyAzureVmService, times(0)).stopAzureVm(InstanceName(vmName), azureCloudContext)
        getRuntime.status shouldBe RuntimeStatus.Stopped
      }

      asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
      _ <- azureInterp.stopAndMonitorRuntime(runtime, azureCloudContext)
      _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)

    } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail on stopAzureVm - Azure runtime stopping error" in isolatedDbTest {
    val failAzureVmService = AzureTestUtils.setupFakeAzureVmService(stopVm = false)

    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = failAzureVmService)

    val res = for {
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

      stopResult <- azureInterp.stopAndMonitorRuntime(runtime, azureCloudContext).attempt

    } yield stopResult shouldBe Left(
      AzureRuntimeStoppingError(runtime.id, s"Stopping runtime ${runtime.id} request to Azure failed.", ctx.traceId)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail on stopAzureVm - Azure runtime not in stoppable status" in isolatedDbTest {
    val fakeAzureVmService = AzureTestUtils.setupFakeAzureVmService(vmState = PowerState.UNKNOWN)
    val queue = QueueFactory.asyncTaskQueue()

    val azureInterp =
      makeAzurePubsubHandler(asyncTaskQueue = queue, azureVmService = fakeAzureVmService)

    val res = for {
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

      stopResult <- azureInterp.stopAndMonitorRuntime(runtime, azureCloudContext).attempt

    } yield stopResult shouldBe Left(
      AzureRuntimeStoppingError(
        runtime.id,
        s"Runtime ${runtime.runtimeName.asString} cannot be stopped in a ${PowerState.UNKNOWN.toString} state, stopping runtime request failed",
        ctx.traceId
      )
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
        .createAndPollApp(appId, AppName("app"), WorkspaceId(UUID.randomUUID()), azureCloudContext, billingProfileId)
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

    val (mockWsm, mockControlledResourceApi, _, _) =
      AzureTestUtils.setUpMockWsmApiClientProvider(storageContainerJobStatus = JobReport.StatusEnum.SUCCEEDED)

    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmClient = mockWsm)

    val resourceId = WsmControlledResourceId(UUID.randomUUID())

    val res =
      for {
        disk <- makePersistentDisk(wsmResourceId = Some(resourceId)).copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
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
          verify(mockControlledResourceApi, times(1)).deleteAzureDisk(any[DeleteControlledAzureResourceRequest],
                                                                      mockitoEq(workspaceId.value),
                                                                      mockitoEq(disk.wsmResourceId.get.value)
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

  it should "not send delete to WSM without a disk record" in isolatedDbTest {
    val queue = QueueFactory.asyncTaskQueue()

    val (mockWsm, mockControlledResourceApi, _, _) =
      AzureTestUtils.setUpMockWsmApiClientProvider(storageContainerJobStatus = JobReport.StatusEnum.FAILED)

    val azureInterp = makeAzurePubsubHandler(asyncTaskQueue = queue, wsmClient = mockWsm)

    val res =
      for {
        disk <- makePersistentDisk(wsmResourceId = None).copy(status = DiskStatus.Ready).save()

        azureRuntimeConfig = RuntimeConfig.AzureConfig(MachineTypeName(VirtualMachineSizeTypes.STANDARD_A1.toString),
                                                       Some(disk.id),
                                                       None
        )
        runtime = makeCluster(2)
          .copy(
            status = RuntimeStatus.Running,
            cloudContext = CloudContext.Azure(azureCloudContext)
          )
          .saveWithRuntimeConfig(azureRuntimeConfig)

        assertions = for {
          diskStatusOpt <- persistentDiskQuery.getStatus(disk.id).transaction
          diskStatus = diskStatusOpt.get
        } yield {
          verify(mockControlledResourceApi, times(0)).deleteAzureDisk(any[DeleteControlledAzureResourceRequest],
                                                                      any[UUID],
                                                                      any[UUID]
          )
          diskStatus shouldBe DiskStatus.Deleted
        }
        msg = DeleteAzureRuntimeMessage(runtime.id,
                                        Some(disk.id),
                                        workspaceId,
                                        Some(wsmResourceId),
                                        billingProfileId,
                                        None
        )
        asyncTaskProcessor = AsyncTaskProcessor(AsyncTaskProcessor.Config(10, 10), queue)
        _ <- azureInterp.deleteAndPollRuntime(msg)

        _ <- withInfiniteStream(asyncTaskProcessor.process, assertions)
      } yield ()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
  // Needs to be made for each test its used in, otherwise queue will overlap
  def makeAzurePubsubHandler(asyncTaskQueue: Queue[IO, Task[IO]] = QueueFactory.asyncTaskQueue(),
                             relayService: AzureRelayService[IO] = FakeAzureRelayService,
                             wsmDAO: WsmDao[IO] = new MockWsmDAO,
                             welderDao: WelderDAO[IO] = new MockWelderDAO(),
                             azureVmService: AzureVmService[IO] = FakeAzureVmService,
                             aksAlg: AKSAlgebra[IO] = new MockAKSInterp,
                             wsmClient: WsmApiClientProvider[IO] = mockWsm,
                             samDAO: SamDAO[IO] = new MockSamDAO()
  ): AzurePubsubHandlerAlgebra[IO] =
    new AzurePubsubHandlerInterp[IO](
      ConfigReader.appConfig.azure.pubsubHandler,
      new ApplicationConfig("test",
                            GoogleProject("test"),
                            Paths.get("x.y"),
                            WorkbenchEmail("z@x.y"),
                            new URL("https://leonardo.foo.broadinstitute.org"),
                            "dev",
                            0L
      ),
      contentSecurityPolicy,
      asyncTaskQueue,
      wsmDAO,
      samDAO,
      welderDao,
      new MockJupyterDAO(),
      relayService,
      azureVmService,
      aksAlg,
      refererConfig,
      wsmClient
    )

}
