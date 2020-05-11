package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.DiskType.SSD
import org.broadinstitute.dsde.workbench.leonardo.db.{persistentDiskQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.diskEq
import org.scalatest.FlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class PersistentDiskComponentSpec extends FlatSpecLike with TestComponent {

  "PersistentDiskComponent" should "save and get records" in isolatedDbTest {
    val disk1 = makePersistentDisk(DiskId(1))
    val disk2 = makePersistentDisk(DiskId(2)).copy(size = DiskSize(1000), blockSize = BlockSize(16384), diskType = SSD)

    val res = for {
      id1 <- disk1.save()
      id2 <- disk2.save()
      d1 <- persistentDiskQuery.getById(id1).transaction
      d2 <- persistentDiskQuery.getById(id2).transaction
      d3 <- persistentDiskQuery.getById(DiskId(-1)).transaction
    } yield {
      d1.get shouldEqual disk1
      d2.get shouldEqual disk2
      d3 shouldEqual None
    }

    res.unsafeRunSync()
  }

  it should "get by name" in isolatedDbTest {
    val disk = makePersistentDisk(DiskId(1))
    val deletedDisk = LeoLenses.diskToDestroyedDate.modify(_ => Some(Instant.now))(makePersistentDisk(DiskId(2)))

    val res = for {
      _ <- disk.save()
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
    val disk = makePersistentDisk(DiskId(1))

    val res = for {
      id <- disk.save()
      now <- nowInstant
      d1 <- persistentDiskQuery.updateStatus(id, DiskStatus.Restoring, now).transaction
      d2 <- persistentDiskQuery.getById(id).transaction
    } yield {
      d1 shouldEqual 1
      d2.get.status shouldEqual DiskStatus.Restoring
      d2.get.auditInfo.dateAccessed should not equal disk.auditInfo.dateAccessed
    }

    res.unsafeRunSync()
  }

  it should "delete records" in isolatedDbTest {
    val disk = makePersistentDisk(DiskId(1))

    val res = for {
      id <- disk.save()
      now <- nowInstant
      d1 <- persistentDiskQuery.delete(id, now).transaction
      d2 <- persistentDiskQuery.getById(id).transaction
    } yield {
      d1 shouldEqual 1
      d2.get.status shouldEqual DiskStatus.Deleted
      d2.get.auditInfo.dateAccessed should not equal disk.auditInfo.dateAccessed
      d2.get.auditInfo.destroyedDate should not equal disk.auditInfo.destroyedDate
    }

    res.unsafeRunSync()
  }

}
