package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.effect.std.Queue
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.PersistentDiskSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.auth.AllowlistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockWsmClientProvider, MockWsmDAO, WsmApiClientProvider, WsmDao}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.ForbiddenError
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.DeleteDiskV2Message
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.flatspec.AnyFlatSpec
import org.typelevel.log4cats.StructuredLogger

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class DiskV2ServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {
  val wsmDao = new MockWsmDAO
  val wsmClientProvider = new MockWsmClientProvider

  private def makeDiskV2Service(queue: Queue[IO, LeoPubsubMessage],
                                allowlistAuthProvider: AllowlistAuthProvider = allowListAuthProvider,
                                wsmDao: WsmDao[IO] = wsmDao,
                                wsmClientProvider: WsmApiClientProvider[IO] = wsmClientProvider
  ) =
    new DiskV2ServiceInterp[IO](
      allowlistAuthProvider,
      queue,
      wsmClientProvider
    )

  val diskV2Service = makeDiskV2Service(QueueFactory.makePublisherQueue(), wsmDao = new MockWsmDAO)

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
          diskId
        )
        .attempt
    } yield getResponse shouldBe Left(DiskNotFoundByIdException(diskId, ctx.traceId))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail with ForbiddenError if user doesn't have permission" in isolatedDbTest {
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
          pd.id
        )
        .attempt
    } yield getResponse shouldBe Left(ForbiddenError(WorkbenchEmail("stranger@example.com")))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail with DiskNotFound if creator loses workspace access" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskV2service2 = makeDiskV2Service(publisherQueue, allowListAuthProvider2)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is the disk creator, but NOT allow-listed in allowListAuthProvider2
    val res = for {
      ctx <- appContext.ask[AppContext]
      pd <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

      getResponse <- diskV2service2
        .getDisk(
          userInfo,
          pd.id
        )
        .attempt
    } yield getResponse shouldBe Left(ForbiddenError(WorkbenchEmail("user1@example.com")))
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete a disk" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskV2Service = makeDiskV2Service(publisherQueue)

    val res = for {
      ctx <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk()
        .copy(samResource = diskSamResource)
        .save()

      _ <- persistentDiskQuery.updateWSMResourceId(disk.id, wsmResourceId, ctx.now).transaction

      _ <- diskV2Service.deleteDisk(userInfo, disk.id)
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

  it should "fail to delete a disk if its creating" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val wsmClientProvider = new MockWsmClientProvider() {
      override def getWsmState(token: String,
                               workspaceId: WorkspaceId,
                               wsmResourceId: WsmControlledResourceId,
                               wsmResourceType: WsmResourceType
      )(implicit ev: Ask[IO, AppContext], log: StructuredLogger[IO]): IO[WsmState] =
        IO.pure(WsmState(Some("CREATING")))
    }

    val diskV2Service = makeDiskV2Service(publisherQueue, wsmClientProvider = wsmClientProvider)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      ctx <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(cloudContextOpt = Some(cloudContextAzure)).copy(samResource = diskSamResource).save()

      err <- diskV2Service.deleteDisk(userInfo, disk.id).attempt
    } yield err shouldBe Left(
      DiskCannotBeDeletedWsmException(disk.id, WsmState(Some("CREATING")), cloudContextAzure, ctx.traceId)
    )

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete a disk and not send wsmResourceId if disk is deleted in WSM" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val wsmClientProvider = new MockWsmClientProvider() {
      override def getWsmState(token: String,
                               workspaceId: WorkspaceId,
                               wsmResourceId: WsmControlledResourceId,
                               wsmResourceType: WsmResourceType
      )(implicit ev: Ask[IO, AppContext], log: StructuredLogger[IO]): IO[WsmState] =
        IO.pure(WsmState(None))
    }

    val diskV2Service = makeDiskV2Service(publisherQueue, wsmClientProvider = wsmClientProvider)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is allow-listed

    val res = for {
      ctx <- appContext.ask[AppContext]
      diskSamResource <- IO(PersistentDiskSamResourceId(UUID.randomUUID.toString))
      disk <- makePersistentDisk(cloudContextOpt = Some(cloudContextAzure)).copy(samResource = diskSamResource).save()

      _ <- diskV2Service.deleteDisk(userInfo, disk.id)
      message <- publisherQueue.take
    } yield message shouldBe DeleteDiskV2Message(
      disk.id,
      workspaceId,
      cloudContextAzure,
      None,
      Some(ctx.traceId)
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
      err <- diskV2Service.deleteDisk(userInfo, disk.id).attempt
    } yield err shouldBe Left(DiskCannotBeDeletedAttachedException(disk.id, workspaceId, ctx.traceId))

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a disk if it has no workspaceId" in isolatedDbTest {
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
      disk <- makePersistentDisk(workspaceId = None).copy(samResource = diskSamResource).save()

      _ <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig.AzureConfig(MachineTypeName("n1-standard-4"), Some(disk.id), None)
        )
      )
      err <- diskV2Service.deleteDisk(userInfo, disk.id).attempt
    } yield err shouldBe Left(
      DiskWithoutWorkspaceException(disk.id, ctx.traceId)
    )

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "fail to delete a disk if its creator lost access to the workspace" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val diskV2service2 = makeDiskV2Service(publisherQueue, allowListAuthProvider2)
    val userInfo = UserInfo(OAuth2BearerToken(""),
                            WorkbenchUserId("userId"),
                            WorkbenchEmail("user1@example.com"),
                            0
    ) // this email is the disk creator, but NOT allow-listed in allowListAuthProvider2
    val res = for {
      ctx <- appContext.ask[AppContext]
      disk <- makePersistentDisk().copy(status = DiskStatus.Ready).save()

      _ <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig.AzureConfig(MachineTypeName("n1-standard-4"), Some(disk.id), None)
        )
      )
      err <- diskV2service2.deleteDisk(userInfo, disk.id).attempt
    } yield err shouldBe Left(
      DiskNotFoundByIdException(disk.id, ctx.traceId)
    )

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
