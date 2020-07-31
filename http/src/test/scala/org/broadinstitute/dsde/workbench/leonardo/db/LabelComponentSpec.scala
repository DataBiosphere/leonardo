package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.db.{labelQuery, LabelResourceType, TestComponent}

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

class LabelComponentSpec extends AnyFlatSpecLike with TestComponent with GcsPathUtils {

  List(LabelResourceType.Runtime, LabelResourceType.PersistentDisk, LabelResourceType.App).foreach { resourceType =>
    it should s"save, get, update, and delete ${resourceType.asString} labels" in isolatedDbTest {
      for {
        id1 <- makeResource(1, resourceType)
        id2 <- makeResource(2, resourceType)

        cluster2Map = Map("bam" -> "true", "sample" -> "NA12878")
        missingId = -1

        missing <- labelQuery.getAllForResource(missingId, LabelResourceType.Runtime).transaction
        missingErr <- labelQuery.save(missingId, LabelResourceType.Runtime, "key1", "value1").transaction.attempt

        save1 <- labelQuery.save(id1, LabelResourceType.Runtime, "key1", "value1").transaction
        get1 <- labelQuery.getAllForResource(id1, LabelResourceType.Runtime).transaction

        _ <- labelQuery.saveAllForResource(id2, LabelResourceType.Runtime, cluster2Map).transaction
        get2 <- labelQuery.getAllForResource(id2, LabelResourceType.Runtime).transaction
        get1Again <- labelQuery.getAllForResource(id1, LabelResourceType.Runtime).transaction

        update1 <- labelQuery.save(id1, LabelResourceType.Runtime, "key1", "value2").transaction
        get1YetAgain <- labelQuery.getAllForResource(id1, LabelResourceType.Runtime).transaction

        delete1 <- labelQuery.deleteAllForResource(id1, LabelResourceType.Runtime).transaction
        delete2 <- labelQuery.deleteAllForResource(id2, LabelResourceType.Runtime).transaction

        afterDelete1 <- labelQuery.getAllForResource(id1, LabelResourceType.Runtime).transaction
        afterDelete2 <- labelQuery.getAllForResource(id2, LabelResourceType.Runtime).transaction
      } yield {
        missing shouldBe Map.empty
        missingErr.isLeft shouldBe true

        save1 shouldBe 1
        get1 shouldBe Map("key1" -> "value1")

        get2 shouldBe cluster2Map
        get1Again shouldBe Map("key1" -> "value1")

        update1 shouldBe 1
        get1YetAgain shouldBe Map("key1" -> "value2")

        delete1 shouldBe 1
        delete2 shouldBe 2

        afterDelete1 shouldBe Map.empty
        afterDelete2 shouldBe Map.empty
      }
    }
  }

  private def makeResource(index: Int, lblType: LabelResourceType): IO[Long] =
    lblType match {
      case LabelResourceType.Runtime        => IO(makeCluster(index).save()).map(_.id)
      case LabelResourceType.PersistentDisk => makePersistentDisk(None).save().map(_.id.value)
      case LabelResourceType.App =>
        for {
          clusterId <- IO(makeKubeCluster(index).save()).map(_.id)
          nodepoolId <- IO(makeNodepool(index, clusterId).save()).map(_.id)
          appId <- IO(makeApp(index, nodepoolId).save()).map(_.id)
        } yield appId.id
    }
}
