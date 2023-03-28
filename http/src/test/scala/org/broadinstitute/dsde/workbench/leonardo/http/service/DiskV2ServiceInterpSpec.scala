package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.effect.std.Queue
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.PersistentDiskSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockWsmDAO, WsmDao}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.DeleteDiskV2Message
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
class DiskV2ServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {
  val wsmDao = new MockWsmDAO

  private def makeDiskV2Service(queue: Queue[IO, LeoPubsubMessage], wsmDao: WsmDao[IO] = wsmDao) =
    new DiskV2ServiceInterp[IO](
      ConfigReader.appConfig.persistentDisk.copy(),
      allowListAuthProvider,
      wsmDao,
      mockSamDAO,
      queue
    )

  val diskV2Service =
    new DiskV2ServiceInterp[IO](ConfigReader.appConfig.persistentDisk.copy(),
                                allowListAuthProvider,
                                new MockWsmDAO,
                                mockSamDAO,
                                QueueFactory.makePublisherQueue()
    )

  it should "get a disk" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allowlisted
    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskV2Service = makeDiskV2Service(publisherQueue)

    val res = for {
      _ <- publisherQueue.tryTake // just to make sure there's no messages in the queue to start with
      pd <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

      getResponse <- diskV2Service
        .getDisk(
          userInfo,
          workspaceId,
          pd.id
        )
    } yield {
      getResponse.id shouldBe pd.id
      getResponse.auditInfo.creator shouldBe userInfo.userEmail

    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail with DiskNotFound if disk does not exist" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskV2Service = makeDiskV2Service(publisherQueue)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed
    val diskId = DiskId(-1)

    val res = for {
      ctx <- appContext.ask[AppContext]

      getResponse <- diskV2Service
        .getDisk(
          userInfo,
          workspaceId,
          diskId
        )
        .attempt
    } yield getResponse shouldBe Left(DiskNotFoundByIdWorkspaceException(diskId, workspaceId, ctx.traceId))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail with DiskNotFound if user doesn't have permission" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskV2Service = makeDiskV2Service(publisherQueue)
    val userInfo =
      UserInfo(OAuth2BearerToken(""), WorkbenchUserId("stranger"), WorkbenchEmail("stranger@example.com"), 0)

    val res = for {
      ctx <- appContext.ask[AppContext]
      pd <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

      getResponse <- diskV2Service
        .getDisk(
          userInfo,
          workspaceId,
          pd.id
        )
        .attempt
    } yield getResponse shouldBe Left(DiskNotFoundByIdWorkspaceException(pd.id, workspaceId, ctx.traceId))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete a disk" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskV2Service = makeDiskV2Service(publisherQueue)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      ctx <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(None).copy(samResource = diskSamResource).save()

      _ <- diskV2Service.deleteDisk(userInfo, workspaceId, disk.id)
      dbDiskOpt <- persistentDiskQuery
        .getActiveByName(disk.cloudContext, disk.name)
        .transaction
      dbDisk = dbDiskOpt.get
      message <- publisherQueue.take
    } yield {
      dbDisk.status shouldBe DiskStatus.Deleting
      message shouldBe DeleteDiskV2Message(disk.id,
                                           workspaceId,
                                           disk.cloudContext,
                                           disk.wsmResourceId,
                                           Some(ctx.traceId)
      )
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a disk if its already deleting" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskV2Service = makeDiskV2Service(publisherQueue)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      ctx <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(cloudContextOpt = Some(cloudContextAzure)).copy(samResource = diskSamResource).save()

      _ <- diskV2Service.deleteDisk(userInfo, workspaceId, disk.id)
      _ <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig.AzureConfig(MachineTypeName("n1-standard-4"), disk.id, azureRegion)
        )
      )
      err <- diskV2Service.deleteDisk(userInfo, workspaceId, disk.id).attempt
    } yield err shouldBe Left(
      DiskCannotBeDeletedException(disk.id, DiskStatus.Deleting, cloudContextAzure, ctx.traceId)
    )

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a disk if it is attached to a runtime" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskV2Service = makeDiskV2Service(publisherQueue)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      ctx <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(None).copy(samResource = diskSamResource).save()

      _ <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"),
                                        Some(disk.id),
                                        bootDiskSize = DiskSize(50),
                                        zone = ZoneName("us-west2-b"),
                                        None
          )
        )
      )
      err <- diskV2Service.deleteDisk(userInfo, workspaceId, disk.id).attempt
    } yield err shouldBe Left(DiskCannotBeDeletedAttachedException(disk.id, workspaceId, ctx.traceId))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
