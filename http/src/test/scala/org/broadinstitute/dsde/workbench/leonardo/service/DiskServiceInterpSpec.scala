package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.PersistentDiskSamResource
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.api.UpdateDiskRequest
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.Implicits.global

class DiskServiceInterpSpec extends FlatSpec with LeonardoTestSuite with TestComponent {
  val publisherQueue = QueueFactory.makePublisherQueue()
  val diskService = new DiskServiceInterp(
    Config.persistentDiskConfig,
    whitelistAuthProvider,
    serviceAccountProvider,
    publisherQueue
  )
  val emptyCreateDiskReq = CreateDiskRequest(
    Map.empty,
    None,
    None,
    None
  )

  implicit val ctx: ApplicativeAsk[IO, AppContext] = ApplicativeAsk.const[IO, AppContext](
    AppContext(model.TraceId("traceId"), Instant.now())
  )

  "DiskService" should "fail with AuthorizationError if user doesn't have project level permission" in {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("email"), 0)
    val googleProject = GoogleProject("googleProject")

    val res = for {
      d <- diskService
        .createDisk(
          userInfo,
          googleProject,
          DiskName("diskName1"),
          emptyCreateDiskReq
        )
        .attempt
    } yield {
      d shouldBe (Left(AuthorizationError(Some(userInfo.userEmail))))
    }
    res.unsafeRunSync()
  }

  it should "successfully create a persistent disk" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
    val googleProject = GoogleProject("googleProject")
    val diskName = DiskName("diskName1")

    val res = for {
      context <- ctx.ask
      d <- diskService
        .createDisk(
          userInfo,
          googleProject,
          diskName,
          emptyCreateDiskReq
        )
        .attempt
      diskOpt <- persistentDiskQuery.getActiveByName(googleProject, diskName).transaction
      disk = diskOpt.get
      message <- publisherQueue.dequeue1
    } yield {
      d shouldBe Right(())
      disk.googleProject shouldBe (googleProject)
      disk.name shouldBe (diskName)
      val expectedMessage = CreateDiskMessage.fromDisk(disk, Some(context.traceId))

      message shouldBe expectedMessage
    }
    res.unsafeRunSync()
  }

  it should "get a disk" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      samResource <- IO(PersistentDiskSamResource(UUID.randomUUID.toString))
      disk <- makePersistentDisk(DiskId(1)).copy(samResource = samResource).save()
      _ <- labelQuery.save(disk.id.value, LabelResourceType.PersistentDisk, "label1", "value1").transaction
      _ <- labelQuery.save(disk.id.value, LabelResourceType.PersistentDisk, "label2", "value2").transaction
      labelResp <- labelQuery.getAllForResource(disk.id.value, LabelResourceType.PersistentDisk).transaction
      getResponse <- diskService.getDisk(userInfo, disk.googleProject, disk.name)
    } yield {
      getResponse.samResource shouldBe disk.samResource
      getResponse.labels shouldBe labelResp
    }
    res.unsafeRunSync()
  }

  it should "list disks" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      disk1 <- makePersistentDisk(DiskId(1)).save()
      disk2 <- makePersistentDisk(DiskId(2)).save()
      listResponse <- diskService.listDisks(userInfo, None, Map.empty)
    } yield {
      listResponse.map(_.id).toSet shouldBe Set(disk1.id, disk2.id)
    }

    res.unsafeRunSync()
  }

  it should "list disks with a project" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      disk1 <- makePersistentDisk(DiskId(1)).save()
      disk2 <- makePersistentDisk(DiskId(2)).save()
      _ <- makePersistentDisk(DiskId(3)).copy(googleProject = project2).save()
      listResponse <- diskService.listDisks(userInfo, Some(project), Map.empty)
    } yield {
      listResponse.map(_.id).toSet shouldBe Set(disk1.id, disk2.id)
    }

    res.unsafeRunSync()
  }

  it should "list disks with parameters" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      disk1 <- makePersistentDisk(DiskId(1)).save()
      _ <- makePersistentDisk(DiskId(2)).save()
      _ <- labelQuery.save(disk1.id.value, LabelResourceType.PersistentDisk, "foo", "bar").transaction
      listResponse <- diskService.listDisks(userInfo, None, Map("foo" -> "bar"))
    } yield {
      listResponse.map(_.id).toSet shouldBe Set(disk1.id)
    }

    res.unsafeRunSync()
  }

  it should "delete a disk" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      context <- ctx.ask
      diskSamResource <- IO(PersistentDiskSamResource(UUID.randomUUID.toString))
      disk <- makePersistentDisk(DiskId(1)).copy(samResource = diskSamResource).save()

      _ <- diskService.deleteDisk(userInfo, disk.googleProject, disk.name)
      dbDiskOpt <- persistentDiskQuery
        .getActiveByName(disk.googleProject, disk.name)
        .transaction
      dbDisk = dbDiskOpt.get
      message <- publisherQueue.dequeue1
    } yield {
      dbDisk.status shouldBe DiskStatus.Deleting
      val expectedMessage = DeleteDiskMessage(disk.id, Some(context.traceId))
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()
  }

  it should "fail to delete a disk if it is attached to a runtime" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      t <- ctx.ask
      diskSamResource <- IO(PersistentDiskSamResource(UUID.randomUUID.toString))
      disk <- makePersistentDisk(DiskId(1)).copy(samResource = diskSamResource).save()
      _ <- IO(
        makeCluster(1).saveWithRuntimeConfig(
          RuntimeConfig.GceWithPdConfig(MachineTypeName("n1-standard-4"), Some(disk.id))
        )
      )
      err <- diskService.deleteDisk(userInfo, disk.googleProject, disk.name).attempt
    } yield {
      err shouldBe Left(DiskAlreadyAttachedException(project, disk.name, t.traceId))
    }

    res.unsafeRunSync()
  }

  List(DiskStatus.Creating, DiskStatus.Restoring, DiskStatus.Failed, DiskStatus.Deleting, DiskStatus.Deleted).foreach {
    status =>
      val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed
      it should s"fail to update a disk in $status status" in isolatedDbTest {
        val res = for {
          t <- ctx.ask
          diskSamResource <- IO(PersistentDiskSamResource(UUID.randomUUID.toString))
          disk <- makePersistentDisk(DiskId(1)).copy(samResource = diskSamResource, status = status).save()
          req = UpdateDiskRequest(Map.empty, DiskSize(600))
          fail <- diskService
            .updateDisk(userInfo, disk.googleProject, disk.name, req)
            .attempt
        } yield {
          fail shouldBe Left(DiskCannotBeUpdatedException(disk.projectNameString, disk.status, traceId = t.traceId))
        }
        res.unsafeRunSync()
      }
  }

  it should "update a disk in Ready status" in isolatedDbTest {
    val userInfo = UserInfo(OAuth2BearerToken(""), WorkbenchUserId("userId"), WorkbenchEmail("user1@example.com"), 0) // this email is white listed

    val res = for {
      context <- ctx.ask
      diskSamResource <- IO(PersistentDiskSamResource(UUID.randomUUID.toString))
      disk <- makePersistentDisk(DiskId(1)).copy(samResource = diskSamResource).save()
      req = UpdateDiskRequest(Map.empty, DiskSize(600))
      _ <- diskService.updateDisk(userInfo, disk.googleProject, disk.name, req)
      message <- publisherQueue.dequeue1
    } yield {
      val expectedMessage = UpdateDiskMessage(disk.id, DiskSize(600), Some(context.traceId))
      message shouldBe expectedMessage
    }

    res.unsafeRunSync()
  }
}
