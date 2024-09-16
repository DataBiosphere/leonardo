package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.azure._
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec.{
  projectSamResourceDecoder,
  runtimeSamResourceDecoder,
  workspaceSamResourceIdDecoder,
  wsmResourceSamResourceIdDecoder
}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{
  ProjectSamResourceId,
  RuntimeSamResourceId,
  WorkspaceResourceSamResourceId,
  WsmResourceSamResourceId
}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.{appContext, defaultMockitoAnswer}
import org.broadinstitute.dsde.workbench.leonardo.auth.AllowlistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.SamResourceAction.{
  projectSamResourceAction,
  runtimeSamResourceAction,
  workspaceSamResourceAction,
  wsmResourceSamResourceAction,
  AppSamResourceAction
}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage,
  StartRuntimeMessage,
  StopRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{LeoPubsubMessage, UpdateDateAccessedMessage, UpdateTarget}
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.mockito.ArgumentMatchers.{any, argThat, eq => isEq}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatestplus.mockito.MockitoSugar
import org.typelevel.log4cats.StructuredLogger

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

  val wsmClientProvider = new MockWsmClientProvider()

  // used when we care about queue state
  def makeInterp(
    queue: Queue[IO, LeoPubsubMessage] = QueueFactory.makePublisherQueue(),
    authProvider: AllowlistAuthProvider = allowListAuthProvider,
    dateAccessedQueue: Queue[IO, UpdateDateAccessedMessage] = QueueFactory.makeDateAccessedQueue(),
    wsmClientProvider: WsmApiClientProvider[IO] = wsmClientProvider
  ) =
    new RuntimeV2ServiceInterp[IO](serviceConfig,
                                   authProvider,
                                   queue,
                                   dateAccessedQueue,
                                   wsmClientProvider,
                                   MockSamService
    )

  // need to set previous runtime to deleted status before creating next to avoid exception
  def setRuntimeDeleted(workspaceId: WorkspaceId, name: RuntimeName): IO[Long] =
    for {
      now <- IO.realTimeInstant
      runtime <- RuntimeServiceDbQueries
        .getRuntimeByWorkspaceId(workspaceId, name)
        .transaction

      _ <- clusterQuery
        .completeDeletion(runtime.id, now)
        .transaction
    } yield runtime.id

  /**
   * Generate a mocked AuthProvider which will permit action on the given resource IDs by the given user.
   * TODO: cover actions beside `checkUserEnabled` and `listResourceIds`
   * @param userInfo
   * @param readerRuntimeSamIds
   * @param readerWorkspaceSamIds
   * @param readerProjectSamIds
   * @param ownerWorkspaceSamIds
   * @param ownerProjectSamIds
   * @return
   */
  def mockAuthorize(
    userInfo: UserInfo,
    readerRuntimeSamIds: Set[RuntimeSamResourceId] = Set.empty,
    readerWsmSamIds: Set[WsmResourceSamResourceId] = Set.empty,
    readerWorkspaceSamIds: Set[WorkspaceResourceSamResourceId] = Set.empty,
    readerProjectSamIds: Set[ProjectSamResourceId] = Set.empty,
    ownerWorkspaceSamIds: Set[WorkspaceResourceSamResourceId] = Set.empty,
    ownerProjectSamIds: Set[ProjectSamResourceId] = Set.empty
  ): AllowlistAuthProvider = {
    val mockAuthProvider: AllowlistAuthProvider = mock[AllowlistAuthProvider](defaultMockitoAnswer[IO])

    when(mockAuthProvider.checkUserEnabled(isEq(userInfo))(any)).thenReturn(IO.unit)
    when(
      mockAuthProvider.listResourceIds[RuntimeSamResourceId](isEq(true), isEq(userInfo))(
        any(runtimeSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[RuntimeSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(readerRuntimeSamIds))
    when(
      mockAuthProvider.listResourceIds[WsmResourceSamResourceId](isEq(false), isEq(userInfo))(
        any(wsmResourceSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[WsmResourceSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(readerWsmSamIds))
    when(
      mockAuthProvider.listResourceIds[WorkspaceResourceSamResourceId](isEq(false), isEq(userInfo))(
        any(workspaceSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[WorkspaceResourceSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(readerWorkspaceSamIds))
    when(
      mockAuthProvider.listResourceIds[ProjectSamResourceId](isEq(false), isEq(userInfo))(
        any(projectSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[ProjectSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    )
      .thenReturn(IO.pure(readerProjectSamIds))
    when(
      mockAuthProvider.listResourceIds[WorkspaceResourceSamResourceId](isEq(true), isEq(userInfo))(
        any(workspaceSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[WorkspaceResourceSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    )
      .thenReturn(IO.pure(ownerWorkspaceSamIds))
    when(
      mockAuthProvider.listResourceIds[ProjectSamResourceId](isEq(true), isEq(userInfo))(
        any(projectSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[ProjectSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    )
      .thenReturn(IO.pure(ownerProjectSamIds))

    mockAuthProvider
  }

  /**
   * Generate a mocked AuthProvider which will permit action on the given resource IDs by the given user,
   * when the list request is restricted to one workspace. Expects isUserWorkspace* instead of listResourceIds.
   * TODO: cover actions beside `checkUserEnabled` and `listResourceIds`
   *
   * @param userInfo
   * @param readerRuntimeSamIds
   * @param readerWorkspaceSamIds
   * @param readerProjectSamIds
   * @param ownerWorkspaceSamIds
   * @param ownerProjectSamIds
   * @return
   */
  def mockAuthorizeForOneWorkspace(
    userInfo: UserInfo,
    readerRuntimeSamIds: Set[RuntimeSamResourceId] = Set.empty,
    readerWsmSamIds: Set[WsmResourceSamResourceId] = Set.empty,
    readerWorkspaceSamIds: Set[WorkspaceResourceSamResourceId] = Set.empty,
    readerProjectSamIds: Set[ProjectSamResourceId] = Set.empty,
    ownerWorkspaceSamIds: Set[WorkspaceResourceSamResourceId] = Set.empty,
    ownerProjectSamIds: Set[ProjectSamResourceId] = Set.empty
  ): AllowlistAuthProvider = {
    val mockAuthProvider: AllowlistAuthProvider = mock[AllowlistAuthProvider](defaultMockitoAnswer[IO])

    when(mockAuthProvider.checkUserEnabled(isEq(userInfo))(any)).thenReturn(IO.unit)
    when(
      mockAuthProvider.listResourceIds[RuntimeSamResourceId](isEq(true), isEq(userInfo))(
        any(runtimeSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[RuntimeSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(readerRuntimeSamIds))
    when(
      mockAuthProvider.listResourceIds[WsmResourceSamResourceId](isEq(false), isEq(userInfo))(
        any(wsmResourceSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[WsmResourceSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(readerWsmSamIds))
    when(
      mockAuthProvider.isUserWorkspaceReader(any, isEq(userInfo))(
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(false))
    when(
      mockAuthProvider.isUserWorkspaceReader(argThat(readerWorkspaceSamIds.contains(_)), isEq(userInfo))(
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(true))
    when(
      mockAuthProvider.listResourceIds[ProjectSamResourceId](isEq(false), isEq(userInfo))(
        any(projectSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[ProjectSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    )
      .thenReturn(IO.pure(readerProjectSamIds))
    when(
      mockAuthProvider.isUserWorkspaceOwner(any, isEq(userInfo))(
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(false))
    when(
      mockAuthProvider.isUserWorkspaceOwner(argThat(ownerWorkspaceSamIds.contains(_)), isEq(userInfo))(
        any(Ask[IO, TraceId].getClass)
      )
    ).thenReturn(IO.pure(true))
    when(
      mockAuthProvider.listResourceIds[ProjectSamResourceId](isEq(true), isEq(userInfo))(
        any(projectSamResourceAction.getClass),
        any(AppSamResourceAction.getClass),
        any(Decoder[ProjectSamResourceId].getClass),
        any(Ask[IO, TraceId].getClass)
      )
    )
      .thenReturn(IO.pure(ownerProjectSamIds))

    mockAuthProvider
  }

  def mockUserInfo(email: String = userEmail.toString()): UserInfo =
    UserInfo(OAuth2BearerToken(""), WorkbenchUserId(s"userId-${email}"), WorkbenchEmail(email), 0)

  val runtimeV2Service =
    new RuntimeV2ServiceInterp[IO](
      serviceConfig,
      allowListAuthProvider,
      QueueFactory.makePublisherQueue(),
      QueueFactory.makeDateAccessedQueue(),
      wsmClientProvider,
      MockSamService
    )

  val runtimeV2Service2 =
    new RuntimeV2ServiceInterp[IO](
      serviceConfig,
      allowListAuthProvider2,
      QueueFactory.makePublisherQueue(),
      QueueFactory.makeDateAccessedQueue(),
      wsmClientProvider,
      MockSamService
    )

  it should "submit a create azure runtime message properly" in isolatedDbTest {
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()

    val azureService = makeInterp(publisherQueue)
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
      workspaceDesc <- wsmClientProvider.getWorkspace("token", workspaceId)
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
          "microsoft-dsvm, ubuntu-2004, 2004-gen2, 23.04.24",
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
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val thrown = the[ForbiddenError] thrownBy {
      runtimeV2Service
        .createRuntime(unauthorizedUserInfo, runtimeName, workspaceId, false, defaultCreateAzureRuntimeReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    thrown shouldBe ForbiddenError(unauthorizedEmail)
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
    setRuntimeDeleted(workspaceId, name0).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

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
    setRuntimeDeleted(workspaceId, name1).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

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

      _ <- runtimeV2Service
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
    val azureService = makeInterp(publisherQueue)

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
      workspaceDesc <- wsmClientProvider.getWorkspace("token", workspaceId)
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

    val azureService = makeInterp(publisherQueue)

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
      workspaceDesc <- wsmClientProvider.getWorkspace("token", workspaceId)
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
    val userInfo = UserInfo(
      OAuth2BearerToken(""),
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
      azureCloudContext <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
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

    val userInfo = UserInfo(
      OAuth2BearerToken(""),
      WorkbenchUserId("userId"),
      WorkbenchEmail("user1@example.com"),
      0
    ) // this email is allowlisted

    implicit val mockSamResourceAction: SamResourceAction[WsmResourceSamResourceId, WsmResourceAction] =
      mock[SamResourceAction[WsmResourceSamResourceId, WsmResourceAction]]

    // test: user does not have access permission for this resource (but they are the creator)
    val mockAuthProvider = mock[AllowlistAuthProvider](defaultMockitoAnswer[IO])
    // User passes isUserWorkspaceReader
    when(mockAuthProvider.isUserWorkspaceReader(any, any)(any)).thenReturn(IO.pure(true))
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
      azureCloudContext <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
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
      azureCloudContext <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
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
      azureCloudContext <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      _ <- azureService.getRuntime(unauthorizedUserInfo, runtimeName, workspaceId)
    } yield ()

    the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail to get a runtime if the creator loses access to workspace" in isolatedDbTest {
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
      azureCloudContext <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      _ <- azureService2.getRuntime(userInfo, runtimeName, workspaceId)
    } yield ()

    the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "publish start a runtime message properly" in isolatedDbTest {
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)
    val res = for {
      ctx <- appContext.ask[AppContext]
      runtime <- IO(
        makeCluster(0)
          .copy(
            status = RuntimeStatus.Stopped,
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
    // User is runtime creator, but does not have access to the workspace
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(
            status = RuntimeStatus.Running,
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
    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(
            status = RuntimeStatus.Running,
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
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)
    val res = for {
      ctx <- appContext.ask[AppContext]
      runtime <- IO(
        makeCluster(0)
          .copy(
            status = RuntimeStatus.Running,
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
          .copy(
            status = RuntimeStatus.Running,
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
    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(
            status = RuntimeStatus.Stopped,
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
      disks <- DiskServiceDbQueries
        .listDisks(Map.empty, includeDeleted = false, Some(userInfo.userEmail), None, Some(workspaceId))
        .transaction
      disk = disks.head
      _ <- persistentDiskQuery.updateWSMResourceId(disk.id, wsmResourceId, context.now).transaction

      _ <- publisherQueue.tryTake // clean out create msg
      preDeleteCluster <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName).transaction

      _ <- clusterQuery.updateClusterStatus(preDeleteCluster.id, RuntimeStatus.Running, context.now).transaction
      _ <- persistentDiskQuery.updateStatus(disk.id, DiskStatus.Ready, context.now).transaction

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

  it should "delete a runtime and not delete the disk if the disk is already deleted" in isolatedDbTest {
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
      disks <- DiskServiceDbQueries
        .listDisks(Map.empty, includeDeleted = false, Some(userInfo.userEmail), None, Some(workspaceId))
        .transaction
      disk = disks.head
      _ <- persistentDiskQuery.updateWSMResourceId(disk.id, wsmResourceId, context.now).transaction
      _ <- persistentDiskQuery.delete(disk.id, context.now).transaction

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

      val expectedMessage =
        DeleteAzureRuntimeMessage(
          preDeleteCluster.id,
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

  it should "not delete a runtime in a creating status in Wsm" in isolatedDbTest {
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val wsmClientProvider = new MockWsmClientProvider() {
      override def getWsmState(token: String,
                               workspaceId: WorkspaceId,
                               wsmResourceId: WsmControlledResourceId,
                               wsmResourceType: WsmResourceType
      )(implicit ev: Ask[IO, AppContext], log: StructuredLogger[IO]): IO[WsmState] =
        IO.pure(WsmState(Some("CREATING")))
    }
    val azureService = makeInterp(wsmClientProvider = wsmClientProvider)

    val res = for {
      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      _ <- azureService.deleteRuntime(userInfo, runtimeName, workspaceId, true)
    } yield ()

    the[RuntimeCannotBeDeletedWsmException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "not delete a runtime in a updating status in Wsm" in isolatedDbTest {
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val wsmClientProvider = new MockWsmClientProvider() {
      override def getWsmState(token: String,
                               workspaceId: WorkspaceId,
                               wsmResourceId: WsmControlledResourceId,
                               wsmResourceType: WsmResourceType
      )(implicit ev: Ask[IO, AppContext], log: StructuredLogger[IO]): IO[WsmState] =
        IO.pure(WsmState(Some("UPDATING")))
    }
    val azureService = makeInterp(wsmClientProvider = wsmClientProvider)

    val res = for {
      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      _ <- azureService.deleteRuntime(userInfo, runtimeName, workspaceId, true)
    } yield ()

    the[RuntimeCannotBeDeletedWsmException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "not delete a runtime in a deleting status in Wsm" in isolatedDbTest {
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val wsmClientProvider = new MockWsmClientProvider() {
      override def getWsmState(token: String,
                               workspaceId: WorkspaceId,
                               wsmResourceId: WsmControlledResourceId,
                               wsmResourceType: WsmResourceType
      )(implicit ev: Ask[IO, AppContext], log: StructuredLogger[IO]): IO[WsmState] =
        IO.pure(WsmState(Some("DELETING")))
    }
    val azureService = makeInterp(wsmClientProvider = wsmClientProvider)

    val res = for {
      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      _ <- azureService.deleteRuntime(userInfo, runtimeName, workspaceId, true)
    } yield ()

    the[RuntimeCannotBeDeletedWsmException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "not delete a runtime if the disk cannot be deleted in Wsm" in isolatedDbTest {
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val wsmClientProvider = new MockWsmClientProvider() {
      override def getWsmState(token: String,
                               workspaceId: WorkspaceId,
                               wsmResourceId: WsmControlledResourceId,
                               wsmResourceType: WsmResourceType
      )(implicit ev: Ask[IO, AppContext], log: StructuredLogger[IO]): IO[WsmState] =
        if (wsmResourceType == WsmResourceType.AzureDisk) IO.pure(WsmState(Some("CREATING")))
        else IO.pure(WsmState(Some("RUNNING")))
    }
    val azureService = makeInterp(wsmClientProvider = wsmClientProvider)

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
      disks <- DiskServiceDbQueries
        .listDisks(Map.empty, includeDeleted = false, Some(userInfo.userEmail), None, Some(workspaceId))
        .transaction
      disk = disks.head
      _ <- persistentDiskQuery.updateWSMResourceId(disk.id, wsmResourceId, context.now).transaction

      _ <- azureService.deleteRuntime(userInfo, runtimeName, workspaceId, true)
    } yield ()

    the[RuntimeCannotBeDeletedWsmException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "delete a runtime and not send wsmResourceId if runtime is deleted in WSM" in isolatedDbTest {
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()

    // make VM be deleted in WSM
    val wsmClientProvider = new MockWsmClientProvider() {
      override def getWsmState(token: String,
                               workspaceId: WorkspaceId,
                               wsmResourceId: WsmControlledResourceId,
                               wsmResourceType: WsmResourceType
      )(implicit ev: Ask[IO, AppContext], log: StructuredLogger[IO]): IO[WsmState] =
        IO.pure(WsmState(None))
    }
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
      disks <- DiskServiceDbQueries
        .listDisks(Map.empty, includeDeleted = false, Some(userInfo.userEmail), None, Some(workspaceId))
        .transaction
      disk = disks.head
      _ <- persistentDiskQuery.updateWSMResourceId(disk.id, wsmResourceId, context.now).transaction
      _ <- persistentDiskQuery.updateStatus(disk.id, DiskStatus.Ready, context.now).transaction

      _ <- publisherQueue.tryTake // clean out create msg
      azureCloudContext <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
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
        DeleteAzureRuntimeMessage(
          preDeleteCluster.id,
          None,
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
        DeleteAzureRuntimeMessage(
          preDeleteCluster.id,
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
      azureCloudContext <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      now <- IO.realTimeInstant
      _ <- clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.Running, now).transaction
      _ <- azureService.deleteRuntime(unauthorizedUserInfo, runtimeName, workspaceId, true)
    } yield ()

    the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail to delete a runtime when creator has lost workspace permission" in isolatedDbTest {
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
      azureCloudContext <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
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
    val runtimeName_1 = RuntimeName("clusterName1")
    val runtimeName_2 = RuntimeName("clusterName2")
    val runtimeName_3 = RuntimeName("clusterName3")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      context <- appContext.ask[AppContext]
      azureCloudContextOpt <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
      azureCloudContext = CloudContext.Azure(azureCloudContextOpt.get)

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

      disks <- DiskServiceDbQueries
        .listDisks(Map.empty, includeDeleted = false, Some(userInfo.userEmail), None, Some(workspaceId))
        .transaction

      disk1 <- persistentDiskQuery.getActiveByName(azureCloudContext, DiskName("diskName1")).transaction
      disk2 <- persistentDiskQuery.getActiveByName(azureCloudContext, DiskName("diskName2")).transaction
      disk3 <- persistentDiskQuery.getActiveByName(azureCloudContext, DiskName("diskName3")).transaction

      _ <- persistentDiskQuery.updateWSMResourceId(disk1.get.id, wsmResourceId, context.now).transaction
      _ <- persistentDiskQuery.updateWSMResourceId(disk2.get.id, wsmResourceId, context.now).transaction
      _ <- persistentDiskQuery.updateWSMResourceId(disk3.get.id, wsmResourceId, context.now).transaction

      _ <- persistentDiskQuery.updateStatus(disk1.get.id, DiskStatus.Ready, context.now).transaction
      _ <- persistentDiskQuery.updateStatus(disk2.get.id, DiskStatus.Ready, context.now).transaction
      _ <- persistentDiskQuery.updateStatus(disk3.get.id, DiskStatus.Ready, context.now).transaction

      _ <- publisherQueue.tryTakeN(Some(3)) // clean out create msg

      preDeleteCluster_1 <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName_1).transaction
      preDeleteCluster_2 <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName_2).transaction
      preDeleteCluster_3 <- RuntimeServiceDbQueries.getActiveRuntimeRecord(workspaceId, runtimeName_3).transaction

      _ <- clusterQuery.updateClusterStatus(preDeleteCluster_1.id, RuntimeStatus.Deleted, context.now).transaction
      _ <- clusterQuery.updateClusterStatus(preDeleteCluster_2.id, RuntimeStatus.Running, context.now).transaction
      _ <- clusterQuery.updateClusterStatus(preDeleteCluster_3.id, RuntimeStatus.Error, context.now).transaction

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
        DeleteAzureRuntimeMessage(
          preDeleteCluster_2.id,
          Some(disk_2.id),
          workspaceId,
          Some(wsmResourceId_2),
          BillingProfileId("spend-profile"),
          Some(context.traceId)
        ),
        DeleteAzureRuntimeMessage(
          preDeleteCluster_3.id,
          Some(disk_3.id),
          workspaceId,
          Some(wsmResourceId_3),
          BillingProfileId("spend-profile"),
          Some(context.traceId)
        )
      )
      delete_messages should contain theSameElementsAs expectedMessages
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "Azure V2 - deleteAllRuntimes, error out if any runtime is not in a deletable status" in isolatedDbTest {
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
      azureCloudContext <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
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

  it should "list runtimes" in isolatedDbTest {
    val runtimeId1 = UUID.randomUUID.toString
    val runtimeId2 = UUID.randomUUID.toString
    val projectIdGcp = cloudContextGcp.asString
    val workspaceIdAzure = UUID.randomUUID.toString

    val mockAuthProvider = mockAuthorize(
      userInfo,
      Set(RuntimeSamResourceId(runtimeId1), RuntimeSamResourceId(runtimeId2)),
      Set.empty,
      Set(WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(workspaceIdAzure)))),
      Set(ProjectSamResourceId(GoogleProject(projectIdGcp)))
    )

    val testService = makeInterp(authProvider = mockAuthProvider)

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(runtimeId1))
      samResource2 <- IO(RuntimeSamResourceId(runtimeId2))
      // GCP runtime
      runtime1 <- IO(makeCluster(1).copy(samResource = samResource1, workspaceId = workspaceIdOpt).save())
      // Azure runtime
      runtime2 <- IO(
        makeCluster(2)
          .copy(
            samResource = samResource2,
            cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext),
            workspaceId = Some(WorkspaceId(UUID.fromString(workspaceIdAzure)))
          )
          .save()
      )
      listResponse <- testService.listRuntimes(userInfo, None, None, Map.empty)
    } yield {
      listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource2)
      listResponse should contain(
        ListRuntimeResponse2(
          id = runtime1.id,
          workspaceId = workspaceIdOpt,
          samResource = runtime1.samResource,
          clusterName = runtime1.runtimeName,
          cloudContext = runtime1.cloudContext,
          auditInfo = runtime1.auditInfo,
          runtimeConfig = defaultDataprocRuntimeConfig,
          proxyUrl = Runtime
            .getProxyUrl(proxyUrlBase, cloudContextGcp, runtime1.runtimeName, Set(jupyterImage), None, Map.empty),
          runtime1.status,
          runtime1.labels,
          runtime1.patchInProgress
        )
      )
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes, omitting runtimes for workspaces and projects user cannot read" in isolatedDbTest {
    val runtimeId1 = UUID.randomUUID.toString
    val runtimeId2 = UUID.randomUUID.toString
    val runtimeId3 = UUID.randomUUID.toString
    val runtimeId4 = UUID.randomUUID.toString
    val projectIdGcp1 = "gcp-context-1"
    val projectIdGcp2 = "gcp-context-2"
    val workspaceIdAzure1 = UUID.randomUUID.toString
    val workspaceIdAzure2 = UUID.randomUUID.toString

    val userInfo = mockUserInfo("jerome@vore.gov")
    val mockAuthProvider = mockAuthorize(
      userInfo,
      // user can read runtimes which are 'notebook-cluster' aka Runtimes
      Set(RuntimeSamResourceId(runtimeId1), RuntimeSamResourceId(runtimeId2), RuntimeSamResourceId(runtimeId3)),
      // user can read runtimes which are WsmResources
      Set(
        WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(runtimeId3))),
        WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(runtimeId4)))
      ),
      // user can only read workspace1
      Set(WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(workspaceIdAzure1)))),
      // user can only read project1
      Set(ProjectSamResourceId(GoogleProject(projectIdGcp1)))
    )
    val testService = makeInterp(authProvider = mockAuthProvider)

    val res = for {
      // GCP runtime 1 (in project1): seen
      samResource1 <- IO(RuntimeSamResourceId(runtimeId1))
      runtime1 <- IO(
        makeCluster(1)
          .copy(samResource = samResource1, cloudContext = CloudContext.Gcp(GoogleProject(projectIdGcp1)))
          .save()
      )
      // GCP runtime 2 (in project2): hidden
      samResource2 <- IO(RuntimeSamResourceId(runtimeId2))
      runtime2 <- IO(
        makeCluster(2)
          .copy(samResource = samResource2, cloudContext = CloudContext.Gcp(GoogleProject(projectIdGcp2)))
          .save()
      )
      // Azure runtime 3 (in workspace1): seen
      samResource3 <- IO(RuntimeSamResourceId(runtimeId3))
      runtime3 <- IO(
        makeCluster(3)
          .copy(
            samResource = samResource3,
            cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext),
            workspaceId = Some(WorkspaceId(UUID.fromString(workspaceIdAzure1)))
          )
          .save()
      )
      // Azure runtime 4 (in workspace2): hidden
      samResource4 <- IO(RuntimeSamResourceId(runtimeId4))
      runtime4 <- IO(
        makeCluster(4)
          .copy(
            samResource = samResource4,
            cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext),
            workspaceId = Some(WorkspaceId(UUID.fromString(workspaceIdAzure2)))
          )
          .save()
      )
      listResponse <- testService.listRuntimes(userInfo, None, None, Map.empty)
    } yield listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource3)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes given different user permissions" in isolatedDbTest {
    forAll(
      Table(
        ("context", "runtimeAccess", "contextAccess", "isListed"),
        // Any runtime access; no access to wrapper context => hidden
        (TestContext.GoogleProject, TestRuntimeAccess.Nothing, TestContextAccess.Nothing, false),
        (TestContext.GoogleWorkspace, TestRuntimeAccess.Nothing, TestContextAccess.Nothing, false),
        (TestContext.AzureWorkspace, TestRuntimeAccess.Nothing, TestContextAccess.Nothing, false),
        (TestContext.GoogleProject, TestRuntimeAccess.Reader, TestContextAccess.Nothing, false),
        (TestContext.GoogleWorkspace, TestRuntimeAccess.Reader, TestContextAccess.Nothing, false),
        (TestContext.AzureWorkspace, TestRuntimeAccess.Reader, TestContextAccess.Nothing, false),
        // No access to runtime; read access to wrapper context => hidden
        (TestContext.GoogleProject, TestRuntimeAccess.Nothing, TestContextAccess.Reader, false),
        (TestContext.GoogleWorkspace, TestRuntimeAccess.Nothing, TestContextAccess.Reader, false),
        (TestContext.AzureWorkspace, TestRuntimeAccess.Nothing, TestContextAccess.Reader, false),
        // Read access to runtime; read access to wrapper context => shown
        (TestContext.GoogleProject, TestRuntimeAccess.Reader, TestContextAccess.Reader, true),
        (TestContext.GoogleWorkspace, TestRuntimeAccess.Reader, TestContextAccess.Reader, true),
        (TestContext.AzureWorkspace, TestRuntimeAccess.Reader, TestContextAccess.Reader, true),
        // Any runtime access; owner of wrapper context => shown
        (TestContext.GoogleProject, TestRuntimeAccess.Nothing, TestContextAccess.Owner, true),
        (TestContext.GoogleWorkspace, TestRuntimeAccess.Nothing, TestContextAccess.Owner, true),
        (TestContext.AzureWorkspace, TestRuntimeAccess.Nothing, TestContextAccess.Owner, true),
        (TestContext.GoogleProject, TestRuntimeAccess.Reader, TestContextAccess.Owner, true),
        (TestContext.GoogleWorkspace, TestRuntimeAccess.Reader, TestContextAccess.Owner, true),
        (TestContext.AzureWorkspace, TestRuntimeAccess.Reader, TestContextAccess.Owner, true)
      )
    ) {
      (
        context: TestContext.Context,
        runtimeAccess: TestRuntimeAccess.Role,
        contextAccess: TestContextAccess.Role,
        isListed: Boolean
      ) =>
        val runtimeId = UUID.randomUUID.toString
        val contextId = UUID.randomUUID.toString

        val userInfo = mockUserInfo("jerome@vore.gov")
        val mockAuthProvider = mockAuthorize(
          userInfo,
          readerRuntimeSamIds = Set(RuntimeSamResourceId(runtimeId))
            .filter(_ => runtimeAccess == TestRuntimeAccess.Reader),
          Set.empty,
          readerWorkspaceSamIds = Set(WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(contextId))))
            .filter(_ => context != TestContext.GoogleProject && contextAccess != TestContextAccess.Nothing),
          readerProjectSamIds = Set(ProjectSamResourceId(GoogleProject(contextId)))
            .filter(_ => context == TestContext.GoogleProject && contextAccess != TestContextAccess.Nothing),
          ownerWorkspaceSamIds = Set(WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(contextId))))
            .filter(_ => context != TestContext.GoogleProject && contextAccess == TestContextAccess.Owner),
          ownerProjectSamIds = Set(ProjectSamResourceId(GoogleProject(contextId)))
            .filter(_ => context == TestContext.GoogleProject && contextAccess == TestContextAccess.Owner)
        )
        val testService = makeInterp(authProvider = mockAuthProvider)

        val res = for {
          projectRuntime <- IO(
            makeCluster(1)
              .copy(
                samResource = RuntimeSamResourceId(runtimeId),
                cloudContext = CloudContext.Gcp(GoogleProject(contextId))
              )
          )
          googleWorkspaceRuntime <- IO(
            makeCluster(2)
              .copy(
                samResource = RuntimeSamResourceId(runtimeId),
                cloudContext = CloudContext.Gcp(GoogleProject(contextId)),
                workspaceId = Some(WorkspaceId(UUID.fromString(contextId)))
              )
          )
          azureWorkspaceRuntime <- IO(
            makeCluster(3)
              .copy(
                samResource = RuntimeSamResourceId(runtimeId),
                cloudContext = CloudContext.Azure(
                  AzureCloudContext(TenantId(contextId), SubscriptionId(contextId), ManagedResourceGroupName(contextId))
                ),
                workspaceId = Some(WorkspaceId(UUID.fromString(contextId)))
              )
          )
          runtime = context match {
            case TestContext.GoogleProject   => projectRuntime
            case TestContext.GoogleWorkspace => googleWorkspaceRuntime
            case TestContext.AzureWorkspace  => azureWorkspaceRuntime
          }
          _ = runtime.save()
          expectedResults = if (isListed) Set(runtime.samResource) else Set.empty

          listResponse <- testService.listRuntimes(userInfo, None, None, Map.empty)
        } yield listResponse.map(_.samResource).toSet shouldBe expectedResults

        res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "list runtimes with a workspace and/or cloudProvider" in isolatedDbTest {
    val runtimeId1 = UUID.randomUUID.toString
    val runtimeId2 = UUID.randomUUID.toString
    val runtimeId3 = UUID.randomUUID.toString
    val runtimeId4 = UUID.randomUUID.toString
    val runtimeId5 = UUID.randomUUID.toString
    val projectIdGcp1 = "gcp-context-1"
    val projectIdGcp2 = "gcp-context-2"
    val workspaceId1 = UUID.randomUUID.toString
    val workspaceId2 = UUID.randomUUID.toString
    val workspaceId3 = UUID.randomUUID.toString

    val userInfo = mockUserInfo("jerome@vore.gov")
    val mockAuthProvider = mockAuthorize(
      userInfo,
      // user can read runtimes 3, 4, and 5
      Set(RuntimeSamResourceId(runtimeId3), RuntimeSamResourceId(runtimeId4), RuntimeSamResourceId(runtimeId5)),
      Set.empty,
      // user can read all workspaces
      Set(
        WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(workspaceId1))),
        WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(workspaceId2))),
        WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(workspaceId3)))
      ),
      // user can read all projects
      Set(ProjectSamResourceId(GoogleProject(projectIdGcp1)), ProjectSamResourceId(GoogleProject(projectIdGcp2))),
      // user owns workspace 1
      Set(WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(workspaceId1)))),
      // user owns project 1
      Set(ProjectSamResourceId(GoogleProject(projectIdGcp1)))
    )
    val mockAuthProviderForOneWorkspace = mockAuthorizeForOneWorkspace(
      userInfo,
      // user can read runtimes 3, 4, and 5
      Set(RuntimeSamResourceId(runtimeId3), RuntimeSamResourceId(runtimeId4), RuntimeSamResourceId(runtimeId5)),
      Set.empty,
      // user can read all workspaces
      Set(
        WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(workspaceId1))),
        WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(workspaceId2))),
        WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(workspaceId3)))
      ),
      // user can read all projects
      Set(ProjectSamResourceId(GoogleProject(projectIdGcp1)), ProjectSamResourceId(GoogleProject(projectIdGcp2))),
      // user owns workspace 1
      Set(WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(workspaceId1)))),
      // user owns project 1
      Set(ProjectSamResourceId(GoogleProject(projectIdGcp1)))
    )
    val testService = makeInterp(authProvider = mockAuthProvider)
    val testServiceForOneWorkspace = makeInterp(authProvider = mockAuthProviderForOneWorkspace)

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(runtimeId1))
      samResource2 <- IO(RuntimeSamResourceId(runtimeId2))
      samResource3 <- IO(RuntimeSamResourceId(runtimeId3))
      samResource4 <- IO(RuntimeSamResourceId(runtimeId4))
      samResource5 <- IO(RuntimeSamResourceId(runtimeId5))
      workspace1 <- IO(WorkspaceId(UUID.fromString(workspaceId1)))
      workspace2 <- IO(WorkspaceId(UUID.fromString(workspaceId2)))
      workspace3 <- IO(WorkspaceId(UUID.fromString(workspaceId3)))

      // hidden runtime 1, owned workspace 1, Azure
      _ <- IO(
        makeCluster(1)
          .copy(
            samResource = samResource1,
            workspaceId = Some(workspace1),
            cloudContext = CloudContext.Azure(
              AzureCloudContext(
                TenantId(workspaceId1),
                SubscriptionId(workspaceId1),
                ManagedResourceGroupName(workspaceId1)
              )
            )
          )
          .save()
      )
      // hidden runtime 2, read workspace 2, owned project 1, Gcp
      _ <- IO(
        makeCluster(2)
          .copy(
            samResource = samResource2,
            workspaceId = Some(workspace2),
            cloudContext = CloudContext.Gcp(GoogleProject(projectIdGcp1))
          )
          .save()
      )
      // read runtime 3, read workspace 2, owned project 1, Gcp
      _ <- IO(
        makeCluster(3)
          .copy(
            samResource = samResource3,
            workspaceId = Some(workspace2),
            cloudContext = CloudContext.Gcp(GoogleProject(projectIdGcp1))
          )
          .save()
      )
      // read runtime 4, read workspace 3, Azure
      _ <- IO(
        makeCluster(4)
          .copy(
            samResource = samResource4,
            workspaceId = Some(workspace3),
            cloudContext = CloudContext.Azure(
              AzureCloudContext(
                TenantId(workspaceId3),
                SubscriptionId(workspaceId3),
                ManagedResourceGroupName(workspaceId3)
              )
            )
          )
          .save()
      )
      // read runtime 5, read project 2, Gcp
      _ <- IO(
        makeCluster(5)
          .copy(samResource = samResource5, cloudContext = CloudContext.Gcp(GoogleProject(projectIdGcp2)))
          .save()
      )

      // Test the service call and pluck Sam resource IDs to compare with expected results
      getResultIds = (
        userInfo: UserInfo,
        workspaceId: Option[WorkspaceId],
        cloudProvider: Option[CloudProvider],
        params: Map[String, String]
      ) => {
        val service = if (workspaceId.isEmpty) testService else testServiceForOneWorkspace
        service
          .listRuntimes(userInfo, workspaceId, cloudProvider, params)
          .flatMap(result => IO(result.map(_.samResource).toSet))
      }

      responseIdsWorkspace1 <- getResultIds(userInfo, Some(workspace1), None, Map.empty)
      responseIdsWorkspace2 <- getResultIds(userInfo, Some(workspace2), None, Map.empty)
      responseIdsWorkspace3 <- getResultIds(userInfo, Some(workspace3), None, Map.empty)
      responseIdsAzure <- getResultIds(userInfo, None, Some(CloudProvider.Azure), Map.empty)
      responseIdsGcp <- getResultIds(userInfo, None, Some(CloudProvider.Gcp), Map.empty)
      responseIdsAzureWorkspace1 <- getResultIds(userInfo, Some(workspace1), Some(CloudProvider.Azure), Map.empty)
      responseIdsAzureWorkspace2 <- getResultIds(userInfo, Some(workspace2), Some(CloudProvider.Azure), Map.empty)
      responseIdsGcpWorkspace1 <- getResultIds(userInfo, Some(workspace1), Some(CloudProvider.Gcp), Map.empty)
      responseIdsGcpWorkspace2 <- getResultIds(userInfo, Some(workspace2), Some(CloudProvider.Gcp), Map.empty)
    } yield {
      responseIdsWorkspace1 shouldBe Set(samResource1)
      responseIdsWorkspace2 shouldBe Set(samResource2, samResource3)
      responseIdsWorkspace3 shouldBe Set(samResource4)
      responseIdsAzure shouldBe Set(samResource1, samResource4)
      responseIdsGcp shouldBe Set(samResource2, samResource3, samResource5)
      responseIdsAzureWorkspace1 shouldBe Set(samResource1)
      responseIdsAzureWorkspace2 shouldBe Set.empty
      responseIdsGcpWorkspace1 shouldBe Set.empty
      responseIdsGcpWorkspace2 shouldBe Set(samResource2, samResource3)
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with parameters" in isolatedDbTest {
    val runtimeId1 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val runtimeId2 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val workspaceId1 = WorkspaceId(UUID.randomUUID)
    val userInfo = mockUserInfo("karen@styx.hel")
    val mockAuthProvider = mockAuthorize(
      userInfo,
      // can read all runtimes
      Set(runtimeId1, runtimeId2),
      Set.empty,
      // can read all workspaces
      Set(WorkspaceResourceSamResourceId(workspaceId1))
    )
    val testService = makeInterp(authProvider = mockAuthProvider)
    val res = for {
      samResource1 <- IO(runtimeId1)
      samResource2 <- IO(runtimeId2)
      runtime1 <- IO(
        makeCluster(1)
          .copy(samResource = samResource1, workspaceId = Some(workspaceId1))
          .save()
      )
      _ <- setRuntimeDeleted(workspaceId1, runtime1.runtimeName)

      _ <- IO(makeCluster(2).copy(samResource = samResource2, workspaceId = Some(workspaceId1)).save())
      _ <- labelQuery.save(runtime1.id, LabelResourceType.Runtime, "foo", "bar").transaction
      listResponse1 <- testService.listRuntimes(
        userInfo,
        None,
        None,
        Map("foo" -> "bar", "includeDeleted" -> "true")
      ) // hit
      listResponse2 <- testService.listRuntimes(
        userInfo,
        None,
        None,
        Map("FOO" -> "BAR", "includeDeleted" -> "true")
      ) // hit, case insensitive
      listResponse3 <- testService.listRuntimes(
        userInfo,
        None,
        None,
        Map("foo!@#$%^&*()_+=';:\"" -> "!@#$%^&*()_+=';:\"bar", "includeDeleted" -> "true")
      ) // miss, with weird characters
      listResponse4 <- testService.listRuntimes(
        userInfo,
        None,
        None,
        Map("foo" -> "not-bar", "includeDeleted" -> "true")
      ) // miss value
      listResponse5 <- testService.listRuntimes(
        userInfo,
        None,
        None,
        Map("not-foo" -> "bar", "includeDeleted" -> "true")
      ) // miss key
      listResponse6 <- testService.listRuntimes(
        userInfo,
        None,
        None,
        Map("foo" -> "bar")
      ) // miss because includeDeleted defaults false
    } yield {
      listResponse1.map(_.samResource).toSet shouldBe Set(samResource1)
      listResponse2.map(_.samResource).toSet shouldBe Set(samResource1)
      listResponse3.map(_.samResource).toSet shouldBe Set.empty
      listResponse4.map(_.samResource).toSet shouldBe Set.empty
      listResponse5.map(_.samResource).toSet shouldBe Set.empty
      listResponse6.map(_.samResource).toSet shouldBe Set.empty
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes filtered by creator" in isolatedDbTest {
    val wsmId1 = WsmResourceSamResourceId(WsmControlledResourceId(UUID.randomUUID))
    val runtimeId2 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val runtimeId3 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val runtimeId4 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val workspaceId1 = WorkspaceId(UUID.randomUUID)
    val userInfoCreator = mockUserInfo("karen@styx.hel")
    val userInfoOther = mockUserInfo("mike@heavn.io")
    val mockAuthProvider = mockAuthorize(
      userInfoCreator,
      // user has auth permission for runtime1
      Set.empty,
      Set(wsmId1),
      // user can read workspace1
      Set(WorkspaceResourceSamResourceId(workspaceId1))
    )
    val testService = makeInterp(authProvider = mockAuthProvider)
    val res = for {
      // runtime 1: I created, in a workspace I can read => visible
      samResource1 <- IO(RuntimeSamResourceId(wsmId1.resourceId.toString))
      runtime1 <- IO(
        makeCluster(1, Some(userInfoCreator.userEmail))
          .copy(samResource = samResource1, workspaceId = Some(workspaceId1))
          .save()
      )

      // runtime 2: I created, but in a workspace I cannot read, and I DO NOT HAVE SAM PERMISSION => hidden
      samResource2 <- IO(runtimeId2)
      runtime2 <- IO(
        makeCluster(2, Some(userInfoCreator.userEmail))
          .copy(samResource = samResource2, workspaceId = Some(WorkspaceId(UUID.randomUUID)))
          .save()
      )

      // runtime 3: someone else created, in a workspace I can read => hidden
      samResource3 <- IO(runtimeId3)
      runtime3 <- IO(
        makeCluster(3, Some(userInfoOther.userEmail))
          .copy(samResource = samResource3, workspaceId = Some(workspaceId1))
          .save()
      )

      // runtime 4: I created, in a workspace I can read, and I DO NOT HAVE SAM PERMISSION => seen if role=creator, else hid
      samResource4 <- IO(runtimeId4)
      runtime4 <- IO(
        makeCluster(4, Some(userInfoCreator.userEmail))
          .copy(samResource = samResource4, workspaceId = Some(workspaceId1))
          .save()
      )

      listResponseCreator <- testService.listRuntimes(userInfoCreator, None, None, Map("role" -> "creator"))
      listResponseAny <- testService.listRuntimes(userInfoCreator, None, None, Map.empty)
    } yield {
      listResponseCreator.map(_.samResource).toSet shouldBe Set(samResource1, samResource4)
      listResponseAny.map(_.samResource).toSet shouldBe Set(samResource1)
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // See https://broadworkbench.atlassian.net/browse/PROD-440
  // AoU relies on the ability for project owners to list other users' runtimes.
  it should "list runtimes belonging to other users" in isolatedDbTest {
    val runtimeId1 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val runtimeId2 = RuntimeSamResourceId(UUID.randomUUID.toString)
    val workspaceId1 = WorkspaceId(UUID.randomUUID)
    val userInfo = mockUserInfo("karen@styx.hel")
    val mockAuthProvider = mockAuthorize(
      userInfo,
      // can read all runtimes
      Set(runtimeId1, runtimeId2),
      Set.empty,
      // can read workspace
      Set(WorkspaceResourceSamResourceId(workspaceId1)),
      // owns workspace
      ownerWorkspaceSamIds = Set(WorkspaceResourceSamResourceId(workspaceId1))
    )
    val testService = makeInterp(authProvider = mockAuthProvider)

    // Make runtimes belonging to different users than the calling user
    val res = for {
      samResource1 <- IO(runtimeId1)
      samResource2 <- IO(runtimeId2)
      runtime1 = LeoLenses.runtimeToCreator.replace(WorkbenchEmail("different_user1@example.com"))(
        makeCluster(1).copy(samResource = samResource1, workspaceId = Some(workspaceId1))
      )
      runtime2 = LeoLenses.runtimeToCreator.replace(WorkbenchEmail("different_user2@example.com"))(
        makeCluster(2).copy(samResource = samResource2, workspaceId = Some(workspaceId1))
      )
      _ <- IO(runtime1.save())
      _ <- IO(runtime2.save())
      listResponse <- testService.listRuntimes(userInfo, None, None, Map.empty)
    } yield listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource2)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes, rejecting invalid label parameters" in isolatedDbTest {
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
      azureCloudContext <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
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
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val dateAccessedQueue = QueueFactory.makeDateAccessedQueue()
    val azureService = makeInterp(publisherQueue, dateAccessedQueue = dateAccessedQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with

      _ <- azureService
        .createRuntime(
          unauthorizedUserInfo, // this email is not allowlisted
          runtimeName,
          workspaceId,
          false,
          defaultCreateAzureRuntimeReq
        )
      _ <- azureService.updateDateAccessed(unauthorizedUserInfo, workspaceId, runtimeName)
    } yield ()

    val thrown = the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    thrown shouldBe ForbiddenError(unauthorizedEmail)
  }

  it should "not update date accessed when user has lost access to workspace" in isolatedDbTest {
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)
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
      azureCloudContext <- wsmClientProvider.getWorkspace("token", workspaceId).map(_.get.azureContext)
      _ <- azureService2.updateDateAccessed(userInfo, workspaceId, runtimeName)
    } yield ()

    the[ForbiddenError] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

}

object TestContext extends Enumeration {
  type Context = Value
  val GoogleProject, GoogleWorkspace, AzureWorkspace = Value
}

object TestContextAccess extends Enumeration {
  type Role = Value
  val Nothing, Reader, Owner = Value
}

object TestRuntimeAccess extends Enumeration {
  type Role = Value
  val Nothing, Reader = Value
}
