package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.DiskType.SSD
import org.broadinstitute.dsde.workbench.leonardo.db.{persistentDiskQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.diskEq
import org.scalatest.FlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class PersistentDiskComponentSpec extends FlatSpecLike with TestComponent {

  "PersistentDiskComponent" should "save and get records" in isolatedDbTest {
    val disk1 = makePersistentDisk(1)
    val disk2 = makePersistentDisk(2).copy(size = DiskSize(1000), blockSize = BlockSize(16384), diskType = SSD)

    val res = for {
      id1 <- persistentDiskQuery.save(disk1).transaction
      id2 <- persistentDiskQuery.save(disk2).transaction
      d1 <- persistentDiskQuery.getById(id1).transaction
      d2 <- persistentDiskQuery.getById(id2).transaction
      d3 <- persistentDiskQuery.getById(-1).transaction
    } yield {
      d1.get shouldEqual disk1
      d2.get shouldEqual disk2
      d3 shouldEqual None
    }

    res.unsafeRunSync()
  }

  it should "get by name" in isolatedDbTest {
    val disk = makePersistentDisk(1)
    val deletedDisk = LeoLenses.diskToDestroyedDate.modify(_ => Some(Instant.now))(makePersistentDisk(2))

    val res = for {
      _ <- persistentDiskQuery.save(disk).transaction
      _ <- persistentDiskQuery.save(deletedDisk).transaction
      d1 <- persistentDiskQuery.getActiveByName(disk.googleProject, disk.name).transaction
      d2 <- persistentDiskQuery.getActiveByName(deletedDisk.googleProject, deletedDisk.name).transaction
    } yield {
      d1.get shouldEqual disk
      d2 shouldEqual None
    }

    res.unsafeRunSync()
  }

  it should "update status" in isolatedDbTest {
    val disk = makePersistentDisk(1)

    val res = for {
      id <- persistentDiskQuery.save(disk).transaction
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
    val disk = makePersistentDisk(1)

    val res = for {
      id <- persistentDiskQuery.save(disk).transaction
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
