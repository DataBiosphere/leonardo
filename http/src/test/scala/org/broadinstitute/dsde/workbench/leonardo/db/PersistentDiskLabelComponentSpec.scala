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
      id <- persistentDiskQuery.save(disk).transaction
      l1 <- persistentDiskLabelQuery.getAllForDisk(id).transaction
      _ <- persistentDiskLabelQuery.saveAllForDisk(id, labels).transaction
      l2 <- persistentDiskLabelQuery.getAllForDisk(id).transaction
      _ <- persistentDiskLabelQuery.deleteAllForDisk(id).transaction
      l3 <- persistentDiskLabelQuery.getAllForDisk(id).transaction
    } yield {
      l1 shouldBe Map.empty
      l2 shouldBe labels
      l3 shouldBe Map.empty
    }

    res.unsafeRunSync()
  }
}
