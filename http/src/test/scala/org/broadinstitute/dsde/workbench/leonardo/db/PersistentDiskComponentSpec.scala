package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.DiskType.SSD
import org.broadinstitute.dsde.workbench.leonardo.db.{persistentDiskQuery, TestComponent}
import org.scalatest.FlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class PersistentDiskComponentSpec extends FlatSpecLike with TestComponent {

  "PersistentDiskComponent" should "save and get records" in isolatedDbTest {
    val disk1 = makePersistentDisk(1)
    val disk2 = makePersistentDisk(2).copy(size = DiskSize(1000), blockSize = BlockSize(16384), diskType = SSD)

    val res = for {
      d1 <- persistentDiskQuery.save(disk1).transaction
      d2 <- persistentDiskQuery.save(disk2).transaction
      d3 <- persistentDiskQuery.getById(disk1.id).transaction
      d4 <- persistentDiskQuery.getById(disk2.id).transaction
      d5 <- persistentDiskQuery.getById(-1).transaction
    } yield {
      d1 shouldBe 1
      d2 shouldBe 1
      d3 shouldBe Some(disk1)
      d4 shouldBe Some(disk2)
      d5 shouldBe None
    }

    res.unsafeRunSync()
  }

  it should "get by name" in isolatedDbTest {
    val disk = makePersistentDisk(1)
    val deletedDisk = makePersistentDisk(2).copy(status = DiskStatus.Deleted)

    val res = for {
      d1 <- persistentDiskQuery.save(disk).transaction
      d2 <- persistentDiskQuery.save(deletedDisk).transaction
      d3 <- persistentDiskQuery.getActiveByName(disk.googleProject, disk.name).transaction
      d4 <- persistentDiskQuery.getActiveByName(deletedDisk.googleProject, deletedDisk.name).transaction
    } yield {
      d1 shouldBe 1
      d2 shouldBe 1
      d3 shouldBe Some(disk)
      d4 shouldBe None
    }

    res.unsafeRunSync()
  }

  it should "update status" in isolatedDbTest {
    val disk = makePersistentDisk(1)

    val res = for {
      d1 <- persistentDiskQuery.save(disk).transaction
      now <- nowInstant
      d2 <- persistentDiskQuery.updateStatus(disk.id, DiskStatus.Restoring, now).transaction
      d3 <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      d1 shouldBe 1
      d2 shouldBe 1
      val expected = LeoLenses.diskToDateAccessed.modify(_ => now)(disk.copy(status = DiskStatus.Restoring))
      d3 shouldBe Some(expected)
    }

    res.unsafeRunSync()
  }

  it should "delete records" in isolatedDbTest {
    val disk = makePersistentDisk(1)

    val res = for {
      d1 <- persistentDiskQuery.save(disk).transaction
      now <- nowInstant
      d2 <- persistentDiskQuery.delete(disk.id, now).transaction
      d3 <- persistentDiskQuery.getById(disk.id).transaction
    } yield {
      d1 shouldBe 1
      d2 shouldBe 1
      val expected = LeoLenses.diskToDateAccessed.modify(_ => now)(disk.copy(status = DiskStatus.Deleted))
      d3 shouldBe Some(expected)
    }

    res.unsafeRunSync()
  }

}
