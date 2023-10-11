package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.api.ControlledAzureResourceApi
import bio.terra.workspace.model.{AzureVmResource, ResourceMetadata, State}
import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import org.broadinstitute.dsde.workbench.azure.{ContainerName, RelayNamespace}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestTags.SlickPlainQueryTest
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{RuntimeSamResourceId, WsmResourceSamResourceId}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.{appContext, defaultMockitoAnswer}
import org.broadinstitute.dsde.workbench.leonardo.auth.AllowlistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage,
  StartRuntimeMessage,
  StopRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{LeoPubsubMessage, UpdateDateAccessedMessage, UpdateTarget}
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.http4s.headers.Authorization
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class RuntimeV2ServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent with MockitoSugar {
  val serviceConfig = RuntimeServiceConfig(
    Config.proxyConfig.proxyUrlBase,
    imageConfig,
    autoFreezeConfig,
    dataprocConfig,
    Config.gceConfig,
    azureServiceConfig
  )

  val wsmDao = new MockWsmDAO

  // set up wsm to return an deletable vm state
  val state = State.READY
  val resourceMetaData = new ResourceMetadata()
  resourceMetaData.setState(state)
  val azureVmResource = new AzureVmResource()
  azureVmResource.setMetadata(resourceMetaData)

  val wsmResourceApi = mock[ControlledAzureResourceApi]
  when(wsmResourceApi.getAzureVm(any(), any())).thenReturn(azureVmResource)

  val wsmClientProvider = new MockWsmClientProvider(wsmResourceApi)

  // used when we care about queue state
  def makeInterp(queue: Queue[IO, LeoPubsubMessage],
                 allowlistAuthProvider: AllowlistAuthProvider = allowListAuthProvider,
                 wsmDao: WsmDao[IO] = wsmDao,
                 dateAccessedQueue: Queue[IO, UpdateDateAccessedMessage] = QueueFactory.makeDateAccessedQueue(),
                 wsmClientProvider: WsmApiClientProvider[IO] = wsmClientProvider
  ) =
    new RuntimeV2ServiceInterp[IO](serviceConfig,
                                   allowlistAuthProvider,
                                   wsmDao,
                                   queue,
                                   dateAccessedQueue,
                                   wsmClientProvider
    )

  // need to set previous runtime to deleted status before creating next to avoid exception
  def setRuntimetoDeleted(workspaceId: WorkspaceId, name: RuntimeName): IO[Long] =
    for {
      now <- IO.realTimeInstant
      runtime <- RuntimeServiceDbQueries
        .getRuntimeByWorkspaceId(workspaceId, name)
        .transaction

      _ <- clusterQuery
        .completeDeletion(runtime.id, now)
        .transaction
    } yield runtime.id

  val runtimeV2Service =
    new RuntimeV2ServiceInterp[IO](
      serviceConfig,
      allowListAuthProvider,
      new MockWsmDAO,
      QueueFactory.makePublisherQueue(),
      QueueFactory.makeDateAccessedQueue(),
      wsmClientProvider
    )

  val runtimeV2Service2 =
    new RuntimeV2ServiceInterp[IO](
      serviceConfig,
      allowListAuthProvider2,
      new MockWsmDAO,
      QueueFactory.makePublisherQueue(),
      QueueFactory.makeDateAccessedQueue(),
      wsmClientProvider
    )

  it should "submit a create azure runtime message properly" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())
    val relayNamespace = RelayNamespace("relay-ns")

    val publisherQueue = QueueFactory.makePublisherQueue()
    val storageContainerResourceId = WsmControlledResourceId(UUID.randomUUID())

    val wsmDao = new MockWsmDAO {
      override def getWorkspaceStorageContainer(workspaceId: WorkspaceId, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Option[StorageContainerResponse]] =
        IO.pure(Some(StorageContainerResponse(ContainerName("dummy"), storageContainerResourceId)))

      override def getLandingZoneResources(billingProfileId: BillingProfileId, userToken: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[LandingZoneResources] =
        IO.pure(landingZoneResources)
    }
    val azureService = makeInterp(publisherQueue, wsmDao = wsmDao)
    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      context <- appContext.ask[AppContext]

      r <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
        .attempt
      workspaceDesc <- wsmDao.getWorkspace(workspaceId, dummyAuth)
      cloudContext = CloudContext.Azure(workspaceDesc.get.azureContext.get)
      clusterRecOpt <- clusterQuery
        .getActiveClusterRecordByName(cloudContext, runtimeName)(scala.concurrent.ExecutionContext.global)
        .transaction
      clusterRec = clusterRecOpt.get
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContext, runtimeName)(scala.concurrent.ExecutionContext.global)
        .transaction
      cluster = clusterOpt.get
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
      message <- publisherQueue.take
      azureRuntimeConfig = runtimeConfig.asInstanceOf[RuntimeConfig.AzureConfig]
      fullClusterOpt <- clusterQuery.getClusterById(cluster.id).transaction

      diskOpt <- persistentDiskQuery.getById(azureRuntimeConfig.persistentDiskId.get).transaction
      disk = diskOpt.get
    } yield {
      r shouldBe Right(CreateRuntimeResponse(context.traceId))
      cluster.cloudContext shouldBe cloudContext
      cluster.runtimeName shouldBe runtimeName
      cluster.status shouldBe RuntimeStatus.PreCreating
      clusterRec.workspaceId shouldBe Some(workspaceId)

      azureRuntimeConfig.machineType.value shouldBe VirtualMachineSizeTypes.STANDARD_A1.toString
      azureRuntimeConfig.region shouldBe None
      disk.name.value shouldBe defaultCreateAzureRuntimeReq.azureDiskConfig.name.value

      val expectedRuntimeImage = Set(
        RuntimeImage(
          RuntimeImageType.Azure,
          "microsoft-dsvm, ubuntu-2004, 2004-gen2, 23.01.06",
          None,
          context.now
        ),
        RuntimeImage(
          RuntimeImageType.Listener,
          ConfigReader.appConfig.azure.pubsubHandler.runtimeDefaults.listenerImage,
          None,
          context.now
        ),
        RuntimeImage(
          RuntimeImageType.Welder,
          ConfigReader.appConfig.azure.pubsubHandler.welderImageHash,
          None,
          context.now
        )
      )

      fullClusterOpt.map(_.runtimeImages) shouldBe Some(expectedRuntimeImage)

      val expectedMessage = CreateAzureRuntimeMessage(
        cluster.id,
        workspaceId,
        false,
        Some(context.traceId),
        workspaceDesc.get.displayName,
        BillingProfileId("spend-profile")
      )
      message shouldBe expectedMessage
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to create a runtime when caller has no permission" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("badUser"), WorkbenchEmail("badEmail"), 0)
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val thrown = the[ForbiddenError] thrownBy {
      runtimeV2Service
        .createRuntime(userInfo, runtimeName, workspaceId, false, defaultCreateAzureRuntimeReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    thrown shouldBe ForbiddenError(userInfo.userEmail)
  }

  it should "throw RuntimeAlreadyExistsException when creating a runtime with same name and context as an existing runtime" in isolatedDbTest {
    runtimeV2Service
      .createRuntime(userInfo, name0, workspaceId, false, defaultCreateAzureRuntimeReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val exc = runtimeV2Service
      .createRuntime(userInfo2, name0, workspaceId, false, defaultCreateAzureRuntimeReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
    exc shouldBe a[RuntimeAlreadyExistsException]
  }

  it should "fail to create a runtime with existing disk if there are multiple disks" in isolatedDbTest {

    runtimeV2Service
      .createRuntime(userInfo, name0, workspaceId, false, defaultCreateAzureRuntimeReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // set runtime status to deleted before creating next
    setRuntimetoDeleted(workspaceId, name0).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    runtimeV2Service
      .createRuntime(
        userInfo,
        name1,
        workspaceId,
        false,
        CreateAzureRuntimeRequest(
          Map.empty,
          VirtualMachineSizeTypes.STANDARD_A1,
          Map.empty,
          CreateAzureDiskRequest(
            Map.empty,
            AzureDiskName("diskName2"),
            Some(DiskSize(100)),
            None
          ),
          Some(0)
        )
      )
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // set runtime status to deleted before creating next
    setRuntimetoDeleted(workspaceId, name1).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val exc = runtimeV2Service
      .createRuntime(userInfo, name2, workspaceId, true, defaultCreateAzureRuntimeReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get

    exc shouldBe a[MultiplePersistentDisksException]
  }

  it should "fail to create a runtime with existing disk if there are 0 disks" in isolatedDbTest {
    val exc = runtimeV2Service
      .createRuntime(userInfo, name0, workspaceId, true, defaultCreateAzureRuntimeReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
    exc shouldBe a[NoPersistentDiskException]
  }

  it should "fail to create a runtime with existing disk if disk isn't ready" in isolatedDbTest {
    val res = for {
      _ <- runtimeV2Service
        .createRuntime(userInfo, name0, workspaceId, false, defaultCreateAzureRuntimeReq)

      disks <- DiskServiceDbQueries
        .listDisks(Map.empty, includeDeleted = false, Some(userInfo.userEmail), None, Some(workspaceId))
        .transaction
      disk = disks.head
      now <- IO.realTimeInstant
      _ <- persistentDiskQuery.updateStatus(disk.id, DiskStatus.Creating, now).transaction

      // set runtime to deleted so they dont get hit by `OnlyOneRuntimePerWorkspacePerCreator`
      runtime <- clusterQuery.getClusterWithDiskId(disk.id).transaction
      _ <- clusterQuery.updateClusterStatus(runtime.get.id, RuntimeStatus.Deleted, now).transaction

      err <- runtimeV2Service
        .createRuntime(userInfo, name1, workspaceId, true, defaultCreateAzureRuntimeReq)
    } yield ()

    val exc = res.attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get

    exc shouldBe a[PersistentDiskNotReadyException]
  }

  it should "fail to create a runtime with existing disk if disk is attached" in isolatedDbTest {
    val res = for {
      _ <- runtimeV2Service
        .createRuntime(userInfo, name0, workspaceId, false, defaultCreateAzureRuntimeReq)

      disks <- DiskServiceDbQueries
        .listDisks(Map.empty, includeDeleted = false, Some(userInfo.userEmail), None, Some(workspaceId))
        .transaction
      disk = disks.head
      now <- IO.realTimeInstant
      _ <- persistentDiskQuery.updateStatus(disk.id, DiskStatus.Ready, now).transaction

      // set runtime to deleted so they dont get hit by `OnlyOneRuntimePerWorkspacePerCreator`
      runtime <- clusterQuery.getClusterWithDiskId(disk.id).transaction
      _ <- clusterQuery.updateClusterStatus(runtime.get.id, RuntimeStatus.Deleted, now).transaction

      err <- runtimeV2Service
        .createRuntime(userInfo, name1, workspaceId, true, defaultCreateAzureRuntimeReq)

    } yield ()

    val exc = res.attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get

    exc shouldBe a[DiskAlreadyAttachedException]
  }

  it should "fail to create a runtime with existing disk if disk is attached to non-deleted runtime" in isolatedDbTest {
    val res = for {
      _ <- runtimeV2Service
        .createRuntime(userInfo, name0, workspaceId, false, defaultCreateAzureRuntimeReq)

      disks <- DiskServiceDbQueries
        .listDisks(Map.empty, includeDeleted = false, Some(userInfo.userEmail), None, Some(workspaceId))
        .transaction
      disk = disks.head
      now <- IO.realTimeInstant
      _ <- persistentDiskQuery.updateStatus(disk.id, DiskStatus.Ready, now).transaction

      runtime <- clusterQuery.getClusterWithDiskId(disk.id).transaction

      err <- runtimeV2Service
        .createRuntime(userInfo, name1, workspaceId, true, defaultCreateAzureRuntimeReq)
        .attempt

    } yield err shouldBe Left(
      OnlyOneRuntimePerWorkspacePerCreator(workspaceId, userInfo.userEmail, runtime.get.runtimeName, runtime.get.status)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to create a runtime if one exists in the workspace" in isolatedDbTest {
    runtimeV2Service
      .createRuntime(userInfo, name0, workspaceId, false, defaultCreateAzureRuntimeReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val exc = runtimeV2Service
      .createRuntime(userInfo, name2, workspaceId, false, defaultCreateAzureRuntimeReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get

    exc shouldBe a[OnlyOneRuntimePerWorkspacePerCreator]
  }

  it should "create a runtime if one exists in the workspace but for another user" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val storageContainerResourceId = WsmControlledResourceId(UUID.randomUUID())

    val wsmDao = new MockWsmDAO {
      override def getWorkspaceStorageContainer(workspaceId: WorkspaceId, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Option[StorageContainerResponse]] =
        IO.pure(Some(StorageContainerResponse(ContainerName("dummy"), storageContainerResourceId)))

      override def getLandingZoneResources(billingProfileId: BillingProfileId, userToken: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[LandingZoneResources] =
        IO.pure(landingZoneResources)
    }
    val azureService = makeInterp(publisherQueue, wsmDao = wsmDao)

    azureService
      .createRuntime(userInfo, name0, workspaceId, false, defaultCreateAzureRuntimeReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      context <- appContext.ask[AppContext]

      r <- azureService
        .createRuntime(
          userInfo2,
          name2,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq.copy(
            azureDiskConfig = defaultCreateAzureRuntimeReq.azureDiskConfig.copy(name = AzureDiskName("diskName2"))
          )
        )
        .attempt
      workspaceDesc <- wsmDao.getWorkspace(workspaceId, dummyAuth)
      cloudContext = CloudContext.Azure(workspaceDesc.get.azureContext.get)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContext, name2)(scala.concurrent.ExecutionContext.global)
        .transaction
      cluster = clusterOpt.get
      message <- publisherQueue.take
    } yield {
      r shouldBe Right(CreateRuntimeResponse(context.traceId))

      val expectedMessage = CreateAzureRuntimeMessage(
        cluster.id,
        workspaceId,
        false,
        Some(context.traceId),
        workspaceDesc.get.displayName,
        BillingProfileId("spend-profile")
      )
      message shouldBe expectedMessage
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "create a runtime if one exists but in deleting status" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val storageContainerResourceId = WsmControlledResourceId(UUID.randomUUID())

    val wsmDao = new MockWsmDAO {
      override def getWorkspaceStorageContainer(workspaceId: WorkspaceId, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Option[StorageContainerResponse]] =
        IO.pure(Some(StorageContainerResponse(ContainerName("dummy"), storageContainerResourceId)))

      override def getLandingZoneResources(billingProfileId: BillingProfileId, userToken: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[LandingZoneResources] =
        IO.pure(landingZoneResources)
    }
    val azureService = makeInterp(publisherQueue, wsmDao = wsmDao)

    azureService
      .createRuntime(userInfo, name0, workspaceId, false, defaultCreateAzureRuntimeReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val updateRuntimeStatus = for {
      now <- IO.realTimeInstant
      runtime <- RuntimeServiceDbQueries
        .getRuntimeByWorkspaceId(workspaceId, name0)
        .transaction
      _ <- clusterQuery
        .updateClusterStatus(runtime.id, RuntimeStatus.Deleting, now)
        .transaction
    } yield ()
    updateRuntimeStatus.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      context <- appContext.ask[AppContext]

      r <- azureService
        .createRuntime(
          userInfo,
          name2,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq.copy(
            azureDiskConfig = defaultCreateAzureRuntimeReq.azureDiskConfig.copy(name = AzureDiskName("diskName2"))
          )
        )
        .attempt
      workspaceDesc <- wsmDao.getWorkspace(workspaceId, dummyAuth)
      cloudContext = CloudContext.Azure(workspaceDesc.get.azureContext.get)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(cloudContext, name2)(scala.concurrent.ExecutionContext.global)
        .transaction
      cluster = clusterOpt.get
      message <- publisherQueue.take
    } yield {
      r shouldBe Right(CreateRuntimeResponse(context.traceId))

      val expectedMessage = CreateAzureRuntimeMessage(
        cluster.id,
        workspaceId,
        false,
        Some(context.traceId),
        workspaceDesc.get.displayName,
        BillingProfileId("spend-profile")
      )
      message shouldBe expectedMessage
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "get a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      getResponse <- azureService.getRuntime(userInfo, runtimeName, workspaceId)
    } yield {
      getResponse.clusterName shouldBe runtimeName
      getResponse.auditInfo.creator shouldBe userInfo.userEmail

    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "get a runtime when caller is creator" in isolatedDbTest {

    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted

    implicit val mockSamResourceAction: SamResourceAction[WsmResourceSamResourceId, WsmResourceAction] =
      mock[SamResourceAction[WsmResourceSamResourceId, WsmResourceAction]]

    // test: user does not have access permission for this resource (but they are the creator)
    val mockAuthProvider = mock[AllowlistAuthProvider](defaultMockitoAnswer[IO])
    // User passes isUserWorkspaceReader
    when(mockAuthProvider.isUserWorkspaceReader(any(), any())(any())).thenReturn(IO.pure(true))
    // Calls to a method on a mock which is not stubbed explicitly will return null;
    // the user cannot pass mockAuthProvider.hasPermission unless we stub it

    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()

    val setupAzureService = makeInterp(publisherQueue)
    val testAzureService = makeInterp(publisherQueue, mockAuthProvider)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

      _ <- setupAzureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      getResponse <- testAzureService.getRuntime(userInfo, runtimeName, workspaceId)
    } yield {
      getResponse.clusterName shouldBe runtimeName
      getResponse.auditInfo.creator shouldBe userInfo.userEmail

    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to get a runtime when caller has workspace permission, lacks resource permissions, and is not creator" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val userInfoNoncreator =
      UserInfo(OAuth2BearerToken(""), WorkbenchUserId("anotherUserId"), WorkbenchEmail("another_user@example.com"), 0)

    implicit val mockSamResourceAction: SamResourceAction[WsmResourceSamResourceId, WsmResourceAction] =
      mock[SamResourceAction[WsmResourceSamResourceId, WsmResourceAction]]

    // test: user does not have access permission for this resource (and they are not the creator)
    val mockAuthProvider = mock[AllowlistAuthProvider](defaultMockitoAnswer[IO])
    // User passes isUserWorkspaceReader
    when(mockAuthProvider.isUserWorkspaceReader(any(), any())(any())).thenReturn(IO.pure(true))
    when(mockAuthProvider.hasPermission(any(), any(), any())(any(), any())).thenReturn(IO.pure(false))

    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()

    val setupAzureService = makeInterp(publisherQueue)
    val testAzureService = makeInterp(publisherQueue, mockAuthProvider)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

      _ <- setupAzureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      _ <- testAzureService.getRuntime(userInfoNoncreator, runtimeName, workspaceId)
    } yield ()

    the[RuntimeNotFoundException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail to get a runtime when caller has no permission" in isolatedDbTest {
    val badUserInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("badUser"), WorkbenchEmail("badEmail"), 0)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      _ <- azureService.getRuntime(badUserInfo, runtimeName, workspaceId)
    } yield ()

    the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail to get a runtime if the creator loses access to workspace" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue, allowListAuthProvider)
    val azureService2 = makeInterp(publisherQueue, allowListAuthProvider2)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      _ <- azureService2.getRuntime(userInfo, runtimeName, workspaceId)
    } yield ()

    the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "publish start a runtime message properly" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("user1@example.com"), 0)
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)
    val res = for {
      ctx <- appContext.ask[AppContext]
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Stopped,
                workspaceId = Some(workspaceId),
                auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      _ <- azureService
        .startRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
      msg <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

    } yield msg shouldBe Some(StartRuntimeMessage(runtime.id, Some(ctx.traceId)))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to start a runtime if permission denied" in isolatedDbTest {
    // User is runtime creator, but does not have access ot the workspace
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Running,
                workspaceId = Some(workspaceId),
                auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      r <- runtimeV2Service
        .startRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
        .attempt
    } yield {
      val exception = r.swap.toOption.get
      exception.getMessage shouldBe s"email is unauthorized. If you have proper permissions to use the workspace, make sure you are also added to the billing account"
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to start a runtime when runtime doesn't exist in DB" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("user1@example.com"), 0)
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val res =
      runtimeV2Service
        .startRuntime(userInfo, runtimeName, workspaceId)
        .attempt
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val exception = res.swap.toOption.get
    exception.isInstanceOf[RuntimeNotFoundByWorkspaceIdException] shouldBe true
    exception.getMessage shouldBe s"Runtime clusterName1 not found in workspace ${workspaceId.value}"
  }

  it should "fail to start a runtime when runtime is not in startable statuses" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("user1@example.com"), 0)

    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Running,
                workspaceId = Some(workspaceId),
                auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      res <- runtimeV2Service
        .startRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
        .attempt
    } yield {
      val exception = res.swap.toOption.get
      exception.isInstanceOf[RuntimeCannotBeStartedException] shouldBe true
      exception.getMessage shouldBe "Runtime Gcp/dsp-leo-test cannot be started in Running status"
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "publish stop a runtime message properly" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("user1@example.com"), 0)
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)
    val res = for {
      ctx <- appContext.ask[AppContext]
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Running,
                workspaceId = Some(workspaceId),
                auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      _ <- azureService
        .stopRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
      msg <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

    } yield msg shouldBe Some(StopRuntimeMessage(runtime.id, Some(ctx.traceId)))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to stop a runtime if permission denied" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Running,
                workspaceId = Some(workspaceId),
                auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      r <- runtimeV2Service
        .stopRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
        .attempt
    } yield {
      val exception = r.swap.toOption.get
      exception.getMessage shouldBe s"email is unauthorized. If you have proper permissions to use the workspace, make sure you are also added to the billing account"
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to stop a runtime when runtime doesn't exist in DB" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("user1@example.com"), 0)
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val res =
      runtimeV2Service
        .stopRuntime(userInfo, runtimeName, workspaceId)
        .attempt
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val exception = res.swap.toOption.get
    exception.isInstanceOf[RuntimeNotFoundByWorkspaceIdException] shouldBe true
    exception.getMessage shouldBe s"Runtime clusterName1 not found in workspace ${workspaceId.value}"
  }

  it should "fail to stop a runtime when runtime is not in startable statuses" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("user1@example.com"), 0)

    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Stopped,
                workspaceId = Some(workspaceId),
                auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      res <- runtimeV2Service
        .stopRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
        .attempt
    } yield {
      val exception = res.swap.toOption.get
      exception.isInstanceOf[RuntimeCannotBeStoppedException] shouldBe true
      exception.getMessage shouldBe s"Runtime Gcp/dsp-leo-test/${runtime.runtimeName.asString} cannot be stopped in Stopped status"
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      context <- appContext.ask[AppContext]

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      _ <- publisherQueue.tryTake // clean out create msg
      preDeleteCluster <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName).transaction

      _ <- clusterQuery.updateClusterStatus(preDeleteCluster.id, RuntimeStatus.Running, context.now).transaction

      _ <- azureService.deleteRuntime(userInfo, runtimeName, workspaceId, true)

      message <- publisherQueue.take

      postDeleteClusterOpt <- clusterQuery
        .getClusterById(preDeleteCluster.id)
        .transaction

      wsmResourceId = WsmControlledResourceId(UUID.fromString(preDeleteCluster.internalId))

      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(preDeleteCluster.runtimeConfigId).transaction
      diskOpt <- persistentDiskQuery
        .getById(runtimeConfig.asInstanceOf[RuntimeConfig.AzureConfig].persistentDiskId.get)
        .transaction
      disk = diskOpt.get
    } yield {
      postDeleteClusterOpt.map(_.status) shouldBe Some(RuntimeStatus.Deleting)
      disk.status shouldBe DiskStatus.Deleting

      val expectedMessage =
        DeleteAzureRuntimeMessage(
          preDeleteCluster.id,
          Some(disk.id),
          workspaceId,
          Some(wsmResourceId),
          BillingProfileId("spend-profile"),
          Some(context.traceId)
        )
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not delete a runtime in a creating status" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      preDeleteClusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      preDeleteCluster = preDeleteClusterOpt.get
      _ <- azureService.deleteRuntime(userInfo, runtimeName, workspaceId, true)
    } yield ()

    the[RuntimeCannotBeDeletedException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "delete a runtime and not send wsmResourceId if runtime in a DELETING WSM status" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    // set up wsm to return an un-deletable vm state
    val state = State.DELETING
    val resourceMetaData = new ResourceMetadata()
    resourceMetaData.setState(state)
    val azureVmResource = new AzureVmResource()
    azureVmResource.setMetadata(resourceMetaData)

    val wsmResourceApi = mock[ControlledAzureResourceApi]
    when(wsmResourceApi.getAzureVm(any(), any())).thenReturn(azureVmResource)

    val wsmClientProvider = new MockWsmClientProvider(wsmResourceApi)
    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue, wsmClientProvider = wsmClientProvider)

    val res = for {
      context <- appContext.ask[AppContext]

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      _ <- publisherQueue.tryTake // clean out create msg
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      preDeleteClusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      preDeleteCluster = preDeleteClusterOpt.get
      _ <- clusterQuery.updateClusterStatus(preDeleteCluster.id, RuntimeStatus.Running, context.now).transaction

      _ <- azureService.deleteRuntime(userInfo, runtimeName, workspaceId, true)

      message <- publisherQueue.take

      postDeleteClusterOpt <- clusterQuery
        .getClusterById(preDeleteCluster.id)
        .transaction

      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(preDeleteCluster.runtimeConfigId).transaction
      diskOpt <- persistentDiskQuery
        .getById(runtimeConfig.asInstanceOf[RuntimeConfig.AzureConfig].persistentDiskId.get)
        .transaction
      disk = diskOpt.get
    } yield {
      postDeleteClusterOpt.map(_.status) shouldBe Some(RuntimeStatus.Deleting)
      disk.status shouldBe DiskStatus.Deleting

      val expectedMessage =
        DeleteAzureRuntimeMessage(preDeleteCluster.id,
                                  Some(disk.id),
                                  workspaceId,
                                  None,
                                  BillingProfileId("spend-profile"),
                                  Some(context.traceId)
        )
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete a runtime and not send wsmResourceId if runtime not in wsm" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    // set up wsm to return null
    val wsmResourceApi = mock[ControlledAzureResourceApi]
    when(wsmResourceApi.getAzureVm(any(), any())).thenThrow(new Error("Runtime not found"))

    val wsmClientProvider = new MockWsmClientProvider(wsmResourceApi)
    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue, wsmClientProvider = wsmClientProvider)

    val res = for {
      context <- appContext.ask[AppContext]

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      _ <- publisherQueue.tryTake // clean out create msg
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      preDeleteClusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      preDeleteCluster = preDeleteClusterOpt.get
      _ <- clusterQuery.updateClusterStatus(preDeleteCluster.id, RuntimeStatus.Running, context.now).transaction

      _ <- azureService.deleteRuntime(userInfo, runtimeName, workspaceId, true)

      message <- publisherQueue.take

      postDeleteClusterOpt <- clusterQuery
        .getClusterById(preDeleteCluster.id)
        .transaction

      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(preDeleteCluster.runtimeConfigId).transaction
      diskOpt <- persistentDiskQuery
        .getById(runtimeConfig.asInstanceOf[RuntimeConfig.AzureConfig].persistentDiskId.get)
        .transaction
      disk = diskOpt.get
    } yield {
      postDeleteClusterOpt.map(_.status) shouldBe Some(RuntimeStatus.Deleting)
      disk.status shouldBe DiskStatus.Deleting

      val expectedMessage =
        DeleteAzureRuntimeMessage(preDeleteCluster.id,
                                  Some(disk.id),
                                  workspaceId,
                                  None,
                                  BillingProfileId("spend-profile"),
                                  Some(context.traceId)
        )
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete a runtime but keep the disk if specified" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      context <- appContext.ask[AppContext]

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      _ <- publisherQueue.tryTake // clean out create msg
      preDeleteCluster <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName).transaction

      wsmResourceId = WsmControlledResourceId(UUID.fromString(preDeleteCluster.internalId))

      _ <- clusterQuery.updateClusterStatus(preDeleteCluster.id, RuntimeStatus.Running, context.now).transaction

      _ <- azureService.deleteRuntime(userInfo, runtimeName, workspaceId, false)

      message <- publisherQueue.take

      postDeleteClusterOpt <- clusterQuery
        .getClusterById(preDeleteCluster.id)
        .transaction

      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(preDeleteCluster.runtimeConfigId).transaction
      diskOpt <- persistentDiskQuery
        .getById(runtimeConfig.asInstanceOf[RuntimeConfig.AzureConfig].persistentDiskId.get)
        .transaction
      disk = diskOpt.get
    } yield {
      postDeleteClusterOpt.map(_.status) shouldBe Some(RuntimeStatus.Deleting)
      disk.status shouldBe DiskStatus.Creating

      val expectedMessage =
        DeleteAzureRuntimeMessage(preDeleteCluster.id,
                                  None,
                                  workspaceId,
                                  Some(wsmResourceId),
                                  BillingProfileId("spend-profile"),
                                  Some(context.traceId)
        )
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a runtime when caller has no permission" in isolatedDbTest {
    val badUserInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("badUser"), WorkbenchEmail("badEmail"), 0)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      jobUUID <- IO.delay(UUID.randomUUID().toString()).map(WsmJobId)

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      now <- IO.realTimeInstant
      _ <- clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.Running, now).transaction
      _ <- azureService.deleteRuntime(badUserInfo, runtimeName, workspaceId, true)
    } yield ()

    the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail to delete a runtime when creator has lost workspace permission" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue, allowListAuthProvider)
    val azureService2 = makeInterp(publisherQueue, allowListAuthProvider2)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      jobUUID <- IO.delay(UUID.randomUUID().toString()).map(WsmJobId)

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      now <- IO.realTimeInstant
      _ <- clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.Running, now).transaction
      _ <- azureService2.deleteRuntime(userInfo, runtimeName, workspaceId, true)
    } yield ()

    the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "Azure V2 - deleteAllRuntimes, update all status appropriately, and queue multiple messages" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName_1 = RuntimeName("clusterName1")
    val runtimeName_2 = RuntimeName("clusterName2")
    val runtimeName_3 = RuntimeName("clusterName3")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      context <- appContext.ask[AppContext]

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName_1,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )

      _ <- azureService
        .createRuntime(
          userInfo2,
          runtimeName_2,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq.copy(
            azureDiskConfig = defaultCreateAzureRuntimeReq.azureDiskConfig.copy(name = AzureDiskName("diskName2"))
          )
        )

      _ <- azureService
        .createRuntime(
          userInfo3,
          runtimeName_3,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq.copy(
            azureDiskConfig = defaultCreateAzureRuntimeReq.azureDiskConfig.copy(name = AzureDiskName("diskName3"))
          )
        )

      _ <- publisherQueue.tryTakeN(Some(3)) // clean out create msg
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      preDeleteCluster_1 <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName_1).transaction

      preDeleteCluster_2 <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName_2).transaction

      preDeleteCluster_3 <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName_3).transaction

      _ <- clusterQuery.updateClusterStatus(preDeleteCluster_1.id, RuntimeStatus.Deleted, context.now).transaction
      _ <- clusterQuery.updateClusterStatus(preDeleteCluster_2.id, RuntimeStatus.Running, context.now).transaction
      _ <- clusterQuery.updateClusterStatus(preDeleteCluster_3.id, RuntimeStatus.Error, context.now).transaction

      wsmResourceId_1 = WsmControlledResourceId(UUID.fromString(preDeleteCluster_1.internalId))
      wsmResourceId_2 = WsmControlledResourceId(UUID.fromString(preDeleteCluster_2.internalId))
      wsmResourceId_3 = WsmControlledResourceId(UUID.fromString(preDeleteCluster_3.internalId))

      _ <- azureService.deleteAllRuntimes(userInfo, workspaceId, true)

      delete_messages <- publisherQueue.tryTakeN(Some(3))

      postDeleteClusterOpt_1 <- clusterQuery
        .getClusterById(preDeleteCluster_1.id)
        .transaction
      postDeleteClusterOpt_2 <- clusterQuery
        .getClusterById(preDeleteCluster_2.id)
        .transaction
      postDeleteClusterOpt_3 <- clusterQuery
        .getClusterById(preDeleteCluster_3.id)
        .transaction

      runtimeConfig_2 <- RuntimeConfigQueries.getRuntimeConfig(preDeleteCluster_2.runtimeConfigId).transaction
      runtimeConfig_3 <- RuntimeConfigQueries.getRuntimeConfig(preDeleteCluster_3.runtimeConfigId).transaction

      diskOpt_2 <- persistentDiskQuery
        .getById(runtimeConfig_2.asInstanceOf[RuntimeConfig.AzureConfig].persistentDiskId.get)
        .transaction
      diskOpt_3 <- persistentDiskQuery
        .getById(runtimeConfig_3.asInstanceOf[RuntimeConfig.AzureConfig].persistentDiskId.get)
        .transaction

      disk_2 = diskOpt_2.get
      disk_3 = diskOpt_3.get
    } yield {
      postDeleteClusterOpt_1.map(_.status) shouldBe Some(RuntimeStatus.Deleted)
      postDeleteClusterOpt_2.map(_.status) shouldBe Some(RuntimeStatus.Deleting)
      disk_2.status shouldBe DiskStatus.Deleting
      postDeleteClusterOpt_3.map(_.status) shouldBe Some(RuntimeStatus.Deleting)
      disk_3.status shouldBe DiskStatus.Deleting

      val expectedMessages = List(
        DeleteAzureRuntimeMessage(preDeleteCluster_2.id,
                                  Some(disk_2.id),
                                  workspaceId,
                                  Some(wsmResourceId_2),
                                  BillingProfileId("spend-profile"),
                                  Some(context.traceId)
        ),
        DeleteAzureRuntimeMessage(preDeleteCluster_3.id,
                                  Some(disk_3.id),
                                  workspaceId,
                                  Some(wsmResourceId_3),
                                  BillingProfileId("spend-profile"),
                                  Some(context.traceId)
        )
      )
      delete_messages shouldBe expectedMessages
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "Azure V2 - deleteAllRuntimes, error out if any runtime is not in a deletable status" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName_1 = RuntimeName("clusterName1")
    val runtimeName_2 = RuntimeName("clusterName2")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      context <- appContext.ask[AppContext]

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName_1,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )

      _ <- azureService
        .createRuntime(
          userInfo2,
          runtimeName_2,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq.copy(
            azureDiskConfig = defaultCreateAzureRuntimeReq.azureDiskConfig.copy(name = AzureDiskName("diskName2"))
          )
        )

      _ <- publisherQueue.tryTakeN(Some(2)) // clean out create msg
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      preDeleteClusterOpt_1 <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName_1)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      preDeleteClusterOpt_2 <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName_2)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      // Let's keep the first cluster in the creating status so we won't deleted it in this test
      preDeleteCluster_1 = preDeleteClusterOpt_1.get
      _ <- clusterQuery.updateClusterStatus(preDeleteCluster_1.id, RuntimeStatus.Creating, context.now).transaction
      preDeleteCluster_2 = preDeleteClusterOpt_2.get
      _ <- clusterQuery.updateClusterStatus(preDeleteCluster_2.id, RuntimeStatus.Running, context.now).transaction

      _ <- azureService.deleteAllRuntimes(userInfo, workspaceId, true)

    } yield ()

    the[NonDeletableRuntimesInWorkspaceFoundException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "list runtimes" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      _ <- IO(makeCluster(1).copy(samResource = samResource1).save())
      _ <- IO(
        makeCluster(2)
          .copy(samResource = samResource2, cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext))
          .save()
      )
      listResponse <- runtimeV2Service.listRuntimes(userInfo, None, None, Map.empty)
    } yield listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource2)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to list runtimes if user loses access to workspace" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      _ <- IO(makeCluster(1).copy(samResource = samResource1).save())
      _ <- IO(
        makeCluster(2)
          .copy(samResource = samResource2, cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext))
          .save()
      )
      listResponse <- runtimeV2Service2.listRuntimes(userInfo, None, None, Map.empty)
    } yield listResponse.map(_.samResource).toSet shouldBe Set()

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with a workspace" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val workspace = Some(workspaceId)

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      _ <- IO(makeCluster(1).copy(samResource = samResource1, workspaceId = workspace).save())
      _ <- IO(makeCluster(2).copy(samResource = samResource2).save())
      listResponse <- runtimeV2Service.listRuntimes(userInfo, workspace, None, Map.empty)
    } yield listResponse.map(_.samResource).toSet shouldBe Set(samResource1)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with a workspace and cloudContext" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val workspace = Some(workspaceId)

    val workspace2 = Some(WorkspaceId(UUID.randomUUID()))

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      samResource3 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      _ <- IO(
        makeCluster(1)
          .copy(samResource = samResource1,
                workspaceId = workspace,
                cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext)
          )
          .save()
      )
      _ <- IO(
        makeCluster(2)
          .copy(samResource = samResource2,
                workspaceId = workspace2,
                cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext)
          )
          .save()
      )
      _ <- IO(makeCluster(3).copy(samResource = samResource3, workspaceId = workspace2).save())
      listResponse1 <- runtimeV2Service.listRuntimes(userInfo, workspace, Some(CloudProvider.Azure), Map.empty)
      listResponse2 <- runtimeV2Service.listRuntimes(userInfo, workspace2, Some(CloudProvider.Azure), Map.empty)
      listResponse3 <- runtimeV2Service.listRuntimes(userInfo, workspace2, Some(CloudProvider.Gcp), Map.empty)
      listResponse4 <- runtimeV2Service.listRuntimes(userInfo, workspace, Some(CloudProvider.Gcp), Map.empty)
    } yield {
      listResponse1.map(_.samResource).toSet shouldBe Set(samResource1)
      listResponse2.map(_.samResource) shouldBe List(samResource2)
      listResponse3.map(_.samResource) shouldBe List(samResource3)
      listResponse4.isEmpty shouldBe true
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with parameters" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      runtime1 <- IO(makeCluster(1).copy(samResource = samResource1).save())
      _ <- IO(makeCluster(2).copy(samResource = samResource2).save())
      _ <- labelQuery.save(runtime1.id, LabelResourceType.Runtime, "foo", "bar").transaction
      listResponse <- runtimeV2Service.listRuntimes(userInfo, None, None, Map("foo" -> "bar"))
    } yield listResponse.map(_.samResource).toSet shouldBe Set(samResource1)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // See https://broadworkbench.atlassian.net/browse/PROD-440
  // AoU relies on the ability for project owners to list other users' runtimes.
  it should "list runtimes belonging to other users" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted

    // Make runtimes belonging to different users than the calling user
    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      runtime1 = LeoLenses.runtimeToCreator.set(WorkbenchEmail("different_user1@example.com"))(
        makeCluster(1).copy(samResource = samResource1)
      )
      runtime2 = LeoLenses.runtimeToCreator.set(WorkbenchEmail("different_user2@example.com"))(
        makeCluster(2).copy(samResource = samResource2)
      )
      _ <- IO(runtime1.save())
      _ <- IO(runtime2.save())
      listResponse <- runtimeV2Service.listRuntimes(userInfo, None, None, Map.empty)
    } yield
    // Since the calling user is allowlisted in the auth provider, it should return
    // the runtimes belonging to other users.
    listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource2)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with labels" taggedAs SlickPlainQueryTest in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
    val wsmJobId1 = WsmJobId("job1")
    val req = defaultCreateAzureRuntimeReq.copy(
      labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar")
    )
    runtimeV2Service
      .createRuntime(userInfo, clusterName1, workspaceId, false, req)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val setupControlledResource1 = for {
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), clusterName1)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction

      cluster = clusterOpt.get
    } yield ()
    setupControlledResource1.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val runtime1 = runtimeV2Service
      .getRuntime(userInfo, clusterName1, workspaceId)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val listRuntimeResponse1 = ListRuntimeResponse2(
      runtime1.id,
      Some(workspaceId),
      runtime1.samResource,
      runtime1.clusterName,
      runtime1.cloudContext,
      runtime1.auditInfo,
      runtime1.runtimeConfig,
      runtime1.clusterUrl,
      runtime1.status,
      runtime1.labels,
      runtime1.patchInProgress
    )

    val clusterName2 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
    val wsmJobId2 = WsmJobId("job2")
    runtimeV2Service
      .createRuntime(
        userInfo2,
        clusterName2,
        workspaceId,
        false,
        req.copy(labels = Map("a" -> "b", "foo" -> "bar"),
                 azureDiskConfig = req.azureDiskConfig.copy(name = AzureDiskName("disk2"))
        )
      )
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val setupControlledResource2 = for {
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), clusterName2)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
    } yield ()
    setupControlledResource2.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val runtime2 = runtimeV2Service
      .getRuntime(userInfo, clusterName2, workspaceId)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val listRuntimeResponse2 = ListRuntimeResponse2(
      runtime2.id,
      Some(workspaceId),
      runtime2.samResource,
      runtime2.clusterName,
      runtime2.cloudContext,
      runtime2.auditInfo,
      runtime2.runtimeConfig,
      runtime2.clusterUrl,
      runtime2.status,
      runtime2.labels,
      runtime2.patchInProgress
    )

    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse1,
      listRuntimeResponse2
    )
    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar,bam=yes"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse1
    )
    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar,bam=yes,vcf=no"))
      .unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
      .futureValue
      .toSet shouldBe Set(listRuntimeResponse1)
    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "a=b"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse2
    )
    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "baz=biz"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set.empty
    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "A=B"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse2
    ) // labels are not case sensitive because MySQL
    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo%3Dbar"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true
    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar;bam=yes"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true
    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar,bam"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true

    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "bogus"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true

    runtimeV2Service
      .listRuntimes(userInfo, None, None, Map("_labels" -> "a,b"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true
  }

  it should "update date accessed when user has permission" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val dateAccessedQueue = QueueFactory.makeDateAccessedQueue()
    val azureService = makeInterp(publisherQueue, dateAccessedQueue = dateAccessedQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get

      ctx <- appContext.ask[AppContext]
      _ <- azureService.updateDateAccessed(userInfo, workspaceId, runtimeName)
      msg <- dateAccessedQueue.tryTake
    } yield msg shouldBe Some(
      UpdateDateAccessedMessage(UpdateTarget.Runtime(runtimeName), cluster.cloudContext, ctx.now)
    )
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not update date accessed when user doesn't have permission" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("bad1"),
                            WorkbenchEmail("bad1@example.com"),
                            0
    ) // this email is not allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val dateAccessedQueue = QueueFactory.makeDateAccessedQueue()
    val azureService = makeInterp(publisherQueue, dateAccessedQueue = dateAccessedQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      _ <- azureService.updateDateAccessed(userInfo, workspaceId, runtimeName)
    } yield ()

    val thrown = the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    thrown shouldBe ForbiddenError(userInfo.userEmail)
  }

  it should "not update date accessed when user has lost access to workspace" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val dateAccessedQueue = QueueFactory.makeDateAccessedQueue()
    val azureService =
      makeInterp(publisherQueue, allowListAuthProvider, dateAccessedQueue = dateAccessedQueue)
    val azureService2 = makeInterp(publisherQueue, allowListAuthProvider2, dateAccessedQueue = dateAccessedQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      _ <- azureService2.updateDateAccessed(userInfo, workspaceId, runtimeName)
    } yield ()

    the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }
}
