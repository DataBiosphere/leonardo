package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.effect.std.Queue
import com.azure.resourcemanager.compute.models.VirtualMachineSizeTypes
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockWsmDAO, StorageContainerResponse, WsmDao}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.{
  ForbiddenError,
  ParseLabelsException,
  RuntimeAlreadyExistsException,
  RuntimeCannotBeDeletedException,
  RuntimeCannotBeStartedException,
  RuntimeCannotBeStoppedException,
  RuntimeNotFoundByWorkspaceIdException,
  RuntimeNotFoundException
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage,
  StartRuntimeMessage,
  StopRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import cats.mtl.Ask
import com.azure.core.management.Region
import org.broadinstitute.dsde.workbench.azure.{ContainerName, RelayNamespace}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.RuntimeSamResourceId
import org.http4s.headers.Authorization

import scala.concurrent.ExecutionContext.Implicits.global

class RuntimeServiceV2InterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {
  val serviceConfig = RuntimeServiceConfig(
    Config.proxyConfig.proxyUrlBase,
    imageConfig,
    autoFreezeConfig,
    dataprocConfig,
    Config.gceConfig,
    azureServiceConfig
  )

  val wsmDao = new MockWsmDAO

  // used when we care about queue state
  def makeInterp(queue: Queue[IO, LeoPubsubMessage], wsmDao: WsmDao[IO] = wsmDao) =
    new RuntimeV2ServiceInterp[IO](serviceConfig, whitelistAuthProvider, wsmDao, mockSamDAO, queue)

  val defaultAzureService =
    new RuntimeV2ServiceInterp[IO](serviceConfig,
                                   whitelistAuthProvider,
                                   new MockWsmDAO,
                                   mockSamDAO,
                                   QueueFactory.makePublisherQueue()
    )

  it should "submit a create azure runtime message properly" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())
    val relayNamespace = RelayNamespace("relay-ns")

    val publisherQueue = QueueFactory.makePublisherQueue()
    val storageContainerResourceId = WsmControlledResourceId(UUID.randomUUID())

    val wsmDao = new MockWsmDAO {
      override def getRelayNamespace(workspaceId: WorkspaceId, region: Region, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Option[RelayNamespace]] =
        IO.pure(Some(relayNamespace))

      override def getWorkspaceStorageContainer(workspaceId: WorkspaceId, authorization: Authorization)(implicit
        ev: Ask[IO, AppContext]
      ): IO[Option[StorageContainerResponse]] =
        IO.pure(Some(StorageContainerResponse(ContainerName("dummy"), storageContainerResourceId)))

    }
    val azureService = makeInterp(publisherQueue, wsmDao)
    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      context <- appContext.ask[AppContext]

      r <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
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

      diskOpt <- persistentDiskQuery.getById(azureRuntimeConfig.persistentDiskId).transaction
      disk = diskOpt.get
    } yield {
      r shouldBe Right(CreateRuntimeResponse(context.traceId))
      cluster.cloudContext shouldBe cloudContext
      cluster.runtimeName shouldBe runtimeName
      cluster.status shouldBe RuntimeStatus.PreCreating
      clusterRec.workspaceId shouldBe Some(workspaceId)

      azureRuntimeConfig.machineType.value shouldBe VirtualMachineSizeTypes.STANDARD_A1.toString
      azureRuntimeConfig.region shouldBe azureRegion
      disk.name.value shouldBe defaultCreateAzureRuntimeReq.azureDiskConfig.name.value

      val expectedRuntimeImage = Set(
        RuntimeImage(
          RuntimeImageType.Azure,
          "microsoft-dsvm, ubuntu-2004, 2004-gen2, 22.04.27",
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
        storageContainerResourceId,
        landingZoneResources,
        Some(context.traceId)
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
      defaultAzureService
        .createRuntime(userInfo, runtimeName, workspaceId, defaultCreateAzureRuntimeReq)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    thrown shouldBe ForbiddenError(userInfo.userEmail)
  }

  it should "throw RuntimeAlreadyExistsException when creating a runtime with same name and context as an existing runtime" in isolatedDbTest {
    defaultAzureService
      .createRuntime(userInfo, name0, workspaceId, defaultCreateAzureRuntimeReq)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val exc = defaultAzureService
      .createRuntime(userInfo, name0, workspaceId, defaultCreateAzureRuntimeReq)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
    exc shouldBe a[RuntimeAlreadyExistsException]
  }

  it should "get a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed
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
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      _ <- controlledResourceQuery.save(cluster.id, wsmResourceId, WsmResourceType.AzureVm).transaction
      getResponse <- azureService.getRuntime(userInfo, runtimeName, workspaceId)
    } yield {
      getResponse.clusterName shouldBe runtimeName
      getResponse.auditInfo.creator shouldBe userInfo.userEmail

    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "publish start a runtime message properly" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)
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
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Running,
                workspaceId = Some(workspaceId),
                auditInfo = auditInfo.copy(creator = WorkbenchEmail("otherUser"))
          )
          .save()
      )
      _ <- controlledResourceQuery.save(runtime.id, wsmResourceId, WsmResourceType.AzureVm).transaction
      r <- defaultAzureService
        .startRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
        .attempt
    } yield {
      val exception = r.swap.toOption.get
      exception.getMessage shouldBe s"Runtime Gcp/dsp-leo-test/${runtime.runtimeName.asString} not found"
      exception.asInstanceOf[RuntimeNotFoundException].msg shouldBe "permission denied"
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to start a runtime when runtime doesn't exist in DB" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val res =
      defaultAzureService
        .startRuntime(userInfo, runtimeName, workspaceId)
        .attempt
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val exception = res.swap.toOption.get
    exception.isInstanceOf[RuntimeNotFoundByWorkspaceIdException] shouldBe true
    exception.getMessage shouldBe s"Runtime ${workspaceId} clusterName1 not found"
  }

  it should "fail to start a runtime when runtime is not in startable statuses" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)

    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Running,
                workspaceId = Some(workspaceId),
                auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      res <- defaultAzureService
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
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)
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
                auditInfo = auditInfo.copy(creator = WorkbenchEmail("otherUser"))
          )
          .save()
      )
      _ <- controlledResourceQuery.save(runtime.id, wsmResourceId, WsmResourceType.AzureVm).transaction
      r <- defaultAzureService
        .stopRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
        .attempt
    } yield {
      val exception = r.swap.toOption.get
      exception.getMessage shouldBe s"Runtime Gcp/dsp-leo-test/${runtime.runtimeName.asString} not found"
      exception.asInstanceOf[RuntimeNotFoundException].msg shouldBe "permission denied"
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to stop a runtime when runtime doesn't exist in DB" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val res =
      defaultAzureService
        .stopRuntime(userInfo, runtimeName, workspaceId)
        .attempt
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val exception = res.swap.toOption.get
    exception.isInstanceOf[RuntimeNotFoundByWorkspaceIdException] shouldBe true
    exception.getMessage shouldBe s"Runtime ${workspaceId} clusterName1 not found"
  }

  it should "fail to stop a runtime when runtime is not in startable statuses" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("user"), WorkbenchEmail("email"), 0)

    val res = for {
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Stopped,
                workspaceId = Some(workspaceId),
                auditInfo = auditInfo.copy(creator = userInfo.userEmail)
          )
          .save()
      )
      res <- defaultAzureService
        .stopRuntime(userInfo, runtime.runtimeName, runtime.workspaceId.get)
        .attempt
    } yield {
      val exception = res.swap.toOption.get
      exception.isInstanceOf[RuntimeCannotBeStoppedException] shouldBe true
      exception.getMessage shouldBe s"Runtime Gcp/dsp-leo-test/${runtime.runtimeName.asString} cannot be stopped in Stopped status"
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to get a runtime when no controlled resource is saved for runtime" in isolatedDbTest {
    val badUserInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("badUser"), WorkbenchEmail("badEmail"), 0)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed
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

    the[AzureRuntimeControlledResourceNotFoundException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail to get a runtime when caller has no permission" in isolatedDbTest {
    val badUserInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("badUser"), WorkbenchEmail("badEmail"), 0)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed
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
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      _ <- controlledResourceQuery.save(cluster.id, wsmResourceId, WsmResourceType.AzureVm).transaction
      _ <- azureService.getRuntime(badUserInfo, runtimeName, workspaceId)
    } yield ()

    the[RuntimeNotFoundException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "delete a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed
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
      _ <- controlledResourceQuery.save(preDeleteCluster.id, wsmResourceId, WsmResourceType.AzureVm).transaction
      _ <- clusterQuery.updateClusterStatus(preDeleteCluster.id, RuntimeStatus.Running, context.now).transaction

      _ <- azureService.deleteRuntime(userInfo, runtimeName, workspaceId)

      message <- publisherQueue.take

      postDeleteClusterOpt <- clusterQuery
        .getClusterById(preDeleteCluster.id)
        .transaction

      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(preDeleteCluster.runtimeConfigId).transaction
      diskOpt <- persistentDiskQuery
        .getById(runtimeConfig.asInstanceOf[RuntimeConfig.AzureConfig].persistentDiskId)
        .transaction
      disk = diskOpt.get
    } yield {
      postDeleteClusterOpt.map(_.status) shouldBe Some(RuntimeStatus.Deleting)
      disk.status shouldBe DiskStatus.Deleting

      val expectedMessage =
        DeleteAzureRuntimeMessage(preDeleteCluster.id,
                                  Some(disk.id),
                                  workspaceId,
                                  Some(wsmResourceId),
                                  Some(context.traceId)
        )
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not delete a runtime in a non-deletable status" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed
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
          defaultCreateAzureRuntimeReq
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      preDeleteClusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      preDeleteCluster = preDeleteClusterOpt.get
      _ <- controlledResourceQuery.save(preDeleteCluster.id, wsmResourceId, WsmResourceType.AzureVm).transaction
      _ <- azureService.deleteRuntime(userInfo, runtimeName, workspaceId)
    } yield ()

    the[RuntimeCannotBeDeletedException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail to delete a runtime when caller has no permission" in isolatedDbTest {
    val badUserInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("badUser"), WorkbenchEmail("badEmail"), 0)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed
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
      _ <- controlledResourceQuery.save(cluster.id, wsmResourceId, WsmResourceType.AzureVm).transaction
      _ <- azureService.deleteRuntime(badUserInfo, runtimeName, workspaceId)
    } yield ()

    the[RuntimeNotFoundException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "list runtimes" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      _ <- IO(makeCluster(1).copy(samResource = samResource1).save())
      _ <- IO(
        makeCluster(2)
          .copy(samResource = samResource2, cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext))
          .save()
      )
      listResponse <- defaultAzureService.listRuntimes(userInfo, None, None, Map.empty)
    } yield listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource2)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with a workspace" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed
    val workspace = Some(workspaceId)

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      _ <- IO(makeCluster(1).copy(samResource = samResource1, workspaceId = workspace).save())
      _ <- IO(makeCluster(2).copy(samResource = samResource2).save())
      listResponse <- defaultAzureService.listRuntimes(userInfo, workspace, None, Map.empty)
    } yield listResponse.map(_.samResource).toSet shouldBe Set(samResource1)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with a workspace and cloudContext" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed
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
      listResponse1 <- defaultAzureService.listRuntimes(userInfo, workspace, Some(CloudProvider.Azure), Map.empty)
      listResponse2 <- defaultAzureService.listRuntimes(userInfo, workspace2, Some(CloudProvider.Azure), Map.empty)
      listResponse3 <- defaultAzureService.listRuntimes(userInfo, workspace2, Some(CloudProvider.Gcp), Map.empty)
      listResponse4 <- defaultAzureService.listRuntimes(userInfo, workspace, Some(CloudProvider.Gcp), Map.empty)
    } yield {
      listResponse1.map(_.samResource).toSet shouldBe Set(samResource1)
      listResponse2.map(_.samResource) shouldBe List(samResource2)
      listResponse3.map(_.samResource) shouldBe List(samResource3)
      listResponse4.isEmpty shouldBe true
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with parameters" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed

    val res = for {
      samResource1 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      samResource2 <- IO(RuntimeSamResourceId(UUID.randomUUID.toString))
      runtime1 <- IO(makeCluster(1).copy(samResource = samResource1).save())
      _ <- IO(makeCluster(2).copy(samResource = samResource2).save())
      _ <- labelQuery.save(runtime1.id, LabelResourceType.Runtime, "foo", "bar").transaction
      listResponse <- defaultAzureService.listRuntimes(userInfo, None, None, Map("foo" -> "bar"))
    } yield listResponse.map(_.samResource).toSet shouldBe Set(samResource1)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  // See https://broadworkbench.atlassian.net/browse/PROD-440
  // AoU relies on the ability for project owners to list other users' runtimes.
  it should "list runtimes belonging to other users" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is white listed

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
      listResponse <- defaultAzureService.listRuntimes(userInfo, None, None, Map.empty)
    } yield
    // Since the calling user is whitelisted in the auth provider, it should return
    // the runtimes belonging to other users.
    listResponse.map(_.samResource).toSet shouldBe Set(samResource1, samResource2)

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with labels" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
    val wsmJobId1 = WsmJobId("job1")
    val req = defaultCreateAzureRuntimeReq.copy(
      labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar")
    )
    defaultAzureService
      .createRuntime(userInfo, clusterName1, workspaceId, req)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val setupControlledResource1 = for {
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.get.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext.get), clusterName1)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction

      cluster = clusterOpt.get
      _ <- controlledResourceQuery.save(cluster.id, wsmResourceId, WsmResourceType.AzureVm).transaction
    } yield ()
    setupControlledResource1.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val runtime1 = defaultAzureService
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
    defaultAzureService
      .createRuntime(
        userInfo,
        clusterName2,
        workspaceId,
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

      cluster = clusterOpt.get
      _ <- controlledResourceQuery
        .save(cluster.id, WsmControlledResourceId(UUID.randomUUID()), WsmResourceType.AzureVm)
        .transaction
    } yield ()
    setupControlledResource2.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val runtime2 = defaultAzureService
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

    defaultAzureService
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse1,
      listRuntimeResponse2
    )
    defaultAzureService
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar,bam=yes"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse1
    )
    defaultAzureService
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar,bam=yes,vcf=no"))
      .unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
      .futureValue
      .toSet shouldBe Set(listRuntimeResponse1)
    defaultAzureService
      .listRuntimes(userInfo, None, None, Map("_labels" -> "a=b"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse2
    )
    defaultAzureService
      .listRuntimes(userInfo, None, None, Map("_labels" -> "baz=biz"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set.empty
    defaultAzureService
      .listRuntimes(userInfo, None, None, Map("_labels" -> "A=B"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .toSet shouldBe Set(
      listRuntimeResponse2
    ) // labels are not case sensitive because MySQL
    defaultAzureService
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo%3Dbar"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true
    defaultAzureService
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar;bam=yes"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true
    defaultAzureService
      .listRuntimes(userInfo, None, None, Map("_labels" -> "foo=bar,bam"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true

    defaultAzureService
      .listRuntimes(userInfo, None, None, Map("_labels" -> "bogus"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true

    defaultAzureService
      .listRuntimes(userInfo, None, None, Map("_labels" -> "a,b"))
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
      .isInstanceOf[ParseLabelsException] shouldBe true
  }

}
