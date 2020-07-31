package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import java.time.Instant

import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.DiskType.SSD
import org.broadinstitute.dsde.workbench.leonardo.db.{persistentDiskQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.diskEq

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

class PersistentDiskComponentSpec extends AnyFlatSpecLike with TestComponent {

  "PersistentDiskComponent" should "save and get records" in isolatedDbTest {
    val disk1 = makePersistentDisk(Some(DiskName("d1")))
    val disk2 =
      makePersistentDisk(Some(DiskName("d2"))).copy(size = DiskSize(1000), blockSize = BlockSize(16384), diskType = SSD)

    val res = for {
      savedDisk1 <- disk1.save()
      savedDisk2 <- disk2.save()
      d1 <- persistentDiskQuery.getById(savedDisk1.id).transaction
      d2 <- persistentDiskQuery.getById(savedDisk2.id).transaction
      d3 <- persistentDiskQuery.getById(DiskId(-1)).transaction
    } yield {
      d1.get shouldEqual disk1
      d2.get shouldEqual disk2
      d3 shouldEqual None
    }

    res.unsafeRunSync()
  }

  it should "get by name" in isolatedDbTest {
    val deletedDisk =
      LeoLenses.diskToDestroyedDate.modify(_ => Some(Instant.now))(makePersistentDisk(Some(DiskName("d2"))))

    val res = for {
      disk <- makePersistentDisk(Some(DiskName("d1"))).save()
      _ <- deletedDisk.save()
      d1 <- persistentDiskQuery.getActiveByName(disk.googleProject, disk.name).transaction
      d2 <- persistentDiskQuery.getActiveByName(deletedDisk.googleProject, deletedDisk.name).transaction
    } yield {
      d1.get shouldEqual disk
      d2 shouldEqual None
    }

    res.unsafeRunSync()
  }

  it should "update status" in isolatedDbTest {
    val res = for {
      savedDisk <- makePersistentDisk().save()
      now <- nowInstant
      d1 <- persistentDiskQuery.updateStatus(savedDisk.id, DiskStatus.Restoring, now).transaction
      d2 <- persistentDiskQuery.getById(savedDisk.id).transaction
    } yield {
      d1 shouldEqual 1
      d2.get.status shouldEqual DiskStatus.Restoring
      d2.get.auditInfo.dateAccessed should not equal savedDisk.auditInfo.dateAccessed
    }

    res.unsafeRunSync()
  }

  it should "delete records" in isolatedDbTest {
    val res = for {
      savedDisk <- makePersistentDisk(None).save()
      now <- nowInstant
      d1 <- persistentDiskQuery.delete(savedDisk.id, now).transaction
      d2 <- persistentDiskQuery.getById(savedDisk.id).transaction
    } yield {
      d1 shouldEqual 1
      d2.get.status shouldEqual DiskStatus.Deleted
      d2.get.auditInfo.dateAccessed should not equal savedDisk.auditInfo.dateAccessed
      d2.get.auditInfo.destroyedDate should not equal savedDisk.auditInfo.destroyedDate
    }

    res.unsafeRunSync()
  }

}
