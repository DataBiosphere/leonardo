package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.db.{persistentDiskLabelQuery, persistentDiskQuery, TestComponent}
import org.scalatest.FlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class PersistentDiskLabelComponentSpec extends FlatSpecLike with TestComponent {

  "PersistentDiskLabelComponent" should "save, list, and delete" in isolatedDbTest {
    val disk = makePersistentDisk(1)
    val labels = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3")

    val res = for {
      _ <- persistentDiskQuery.save(disk).transaction
      l1 <- persistentDiskLabelQuery.getAllForDisk(disk.id).transaction
      l2 <- persistentDiskLabelQuery.saveAllForDisk(disk.id, labels).transaction
      l3 <- persistentDiskLabelQuery.getAllForDisk(disk.id).transaction
      l4 <- persistentDiskLabelQuery.deleteAllForDisk(disk.id).transaction
      l5 <- persistentDiskLabelQuery.getAllForDisk(disk.id).transaction
    } yield {
      l1 shouldBe Map.empty
      l2 shouldBe 3
      l3 shouldBe labels
      l4 shouldBe 3
      l5 shouldBe Map.empty
    }

    res.unsafeRunSync()
  }

  it should "fail to write 2 labels with the same key" in isolatedDbTest {
    val disk = makePersistentDisk(1)
    val labels = Map("k1" -> "v1", "k2" -> "v2", "k1" -> "v3")

    val res = for {
      _ <- persistentDiskQuery.save(disk).transaction
      _ <- persistentDiskLabelQuery.saveAllForDisk(disk.id, labels).transaction
    } yield ()

    res.attempt.unsafeRunSync().isLeft shouldBe true
  }

}
