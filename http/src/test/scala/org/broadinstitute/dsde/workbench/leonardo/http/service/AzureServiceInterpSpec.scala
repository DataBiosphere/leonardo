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
import org.broadinstitute.dsde.workbench.leonardo.dao.MockWsmDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.{
  ForbiddenError,
  RuntimeAlreadyExistsException,
  RuntimeCannotBeDeletedException,
  RuntimeNotFoundException
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{
  CreateAzureRuntimeMessage,
  DeleteAzureRuntimeMessage
}
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class AzureServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {
  val serviceConfig = RuntimeServiceConfig(
    Config.proxyConfig.proxyUrlBase,
    imageConfig,
    autoFreezeConfig,
    dataprocConfig,
    Config.gceConfig,
    azureServiceConfig,
    ConfigReader.appConfig.azure.runtimeDefaults
  )

  val wsmDao = new MockWsmDAO

  //used when we care about queue state
  def makeInterp(queue: Queue[IO, LeoPubsubMessage]) =
    new AzureServiceInterp[IO](serviceConfig,
                               whitelistAuthProvider,
                               wsmDao,
                               mockSamDAO,
                               QueueFactory.asyncTaskQueue,
                               queue)

  val defaultAzureService =
    new AzureServiceInterp[IO](serviceConfig,
                               whitelistAuthProvider,
                               new MockWsmDAO,
                               mockSamDAO,
                               QueueFactory.asyncTaskQueue,
                               QueueFactory.makePublisherQueue())

  it should "submit a create azure runtime message properly" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      context <- appContext.ask[AppContext]

      jobUUID <- IO.delay(UUID.randomUUID()).map(WsmJobId)
      r <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          defaultCreateAzureRuntimeReq,
          jobUUID
        )
        .attempt
      workspaceDesc <- wsmDao.getWorkspace(workspaceId, dummyAuth)
      cloudContext = CloudContext.Azure(workspaceDesc.azureContext)
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
      r shouldBe Right(())
      cluster.cloudContext shouldBe cloudContext
      cluster.runtimeName shouldBe runtimeName
      cluster.status shouldBe RuntimeStatus.PreCreating
      clusterRec.workspaceId shouldBe Some(workspaceId)

      azureRuntimeConfig.machineType.value shouldBe VirtualMachineSizeTypes.STANDARD_A1.toString
      azureRuntimeConfig.region shouldBe azureRegion
      disk.name.value shouldBe defaultCreateAzureRuntimeReq.azureDiskConfig.name.value

      val expectedRuntimeImage = RuntimeImage(
        RuntimeImageType.Azure,
        azureImage.imageUrl,
        None,
        context.now
      )

      fullClusterOpt.map(_.runtimeImages) shouldBe Some(Set(expectedRuntimeImage))

      val expectedMessage = CreateAzureRuntimeMessage(
        cluster.id,
        workspaceId,
        jobUUID,
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
    val jobUUID = WsmJobId(UUID.randomUUID())

    val thrown = the[ForbiddenError] thrownBy {
      defaultAzureService
        .createRuntime(userInfo, runtimeName, workspaceId, defaultCreateAzureRuntimeReq, jobUUID)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }

    thrown shouldBe ForbiddenError(userInfo.userEmail)
  }

  it should "throw RuntimeAlreadyExistsException when creating a runtime with same name and context as an existing runtime" in isolatedDbTest {
    val jobId = WsmJobId(UUID.randomUUID())
    defaultAzureService
      .createRuntime(userInfo, name0, workspaceId, defaultCreateAzureRuntimeReq, jobId)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val jobId2 = WsmJobId(UUID.randomUUID())

    val exc = defaultAzureService
      .createRuntime(userInfo, name0, workspaceId, defaultCreateAzureRuntimeReq, jobId2)
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .swap
      .toOption
      .get
    exc shouldBe a[RuntimeAlreadyExistsException]
  }

  it should "get a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      context <- appContext.ask[AppContext]
      jobUUID <- IO.delay(UUID.randomUUID()).map(WsmJobId)

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          defaultCreateAzureRuntimeReq,
          jobUUID
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      _ <- controlledResourceQuery.save(cluster.id, wsmResourceId, WsmResourceType.AzureVm).transaction
      getRuntime <- azureService.getRuntime(userInfo, runtimeName, workspaceId)
    } yield {
      getRuntime.clusterName shouldBe runtimeName
      getRuntime.auditInfo.creator shouldBe userInfo.userEmail
      getRuntime.clusterImages shouldBe Set(
        RuntimeImage(
          RuntimeImageType.Azure,
          defaultCreateAzureRuntimeReq.imageUri.get.value,
          None,
          context.now
        )
      )
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to get a runtime when no controlled resource is saved for runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      jobUUID <- IO.delay(UUID.randomUUID()).map(WsmJobId)

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          defaultCreateAzureRuntimeReq,
          jobUUID
        )
      _ <- azureService.getRuntime(userInfo, runtimeName, workspaceId)
    } yield ()

    the[AzureRuntimeControlledResourceNotFoundException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail to get a runtime when caller has no permission" in isolatedDbTest {
    val badUserInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("badUser"), WorkbenchEmail("badEmail"), 0)
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      jobUUID <- IO.delay(UUID.randomUUID()).map(WsmJobId)

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          defaultCreateAzureRuntimeReq,
          jobUUID
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext), runtimeName)(
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
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      context <- appContext.ask[AppContext]
      jobUUID <- IO.delay(UUID.randomUUID()).map(WsmJobId)

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          defaultCreateAzureRuntimeReq,
          jobUUID
        )
      _ <- publisherQueue.tryTake //clean out create msg
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.azureContext)
      preDeleteClusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext), runtimeName)(
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
        DeleteAzureRuntimeMessage(preDeleteCluster.id, Some(disk.id), workspaceId, wsmResourceId, Some(context.traceId))
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not delete a runtime in a non-deletable status" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      jobUUID <- IO.delay(UUID.randomUUID()).map(WsmJobId)

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          defaultCreateAzureRuntimeReq,
          jobUUID
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.azureContext)
      preDeleteClusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext), runtimeName)(
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
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val runtimeName = RuntimeName("clusterName1")
    val workspaceId = WorkspaceId(UUID.randomUUID())

    val publisherQueue = QueueFactory.makePublisherQueue()
    val azureService = makeInterp(publisherQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      jobUUID <- IO.delay(UUID.randomUUID()).map(WsmJobId)

      _ <- azureService
        .createRuntime(
          userInfo,
          runtimeName,
          workspaceId,
          defaultCreateAzureRuntimeReq,
          jobUUID
        )
      azureCloudContext <- wsmDao.getWorkspace(workspaceId, dummyAuth).map(_.azureContext)
      clusterOpt <- clusterQuery
        .getActiveClusterByNameMinimal(CloudContext.Azure(azureCloudContext), runtimeName)(
          scala.concurrent.ExecutionContext.global
        )
        .transaction
      cluster = clusterOpt.get
      _ <- controlledResourceQuery.save(cluster.id, wsmResourceId, WsmResourceType.AzureVm).transaction
      _ <- azureService.deleteRuntime(badUserInfo, runtimeName, workspaceId)
    } yield ()

    the[RuntimeNotFoundException] thrownBy {
      res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

}
