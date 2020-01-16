package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException

import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.scalatest.FlatSpecLike
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random
import CommonTestData._

class LabelComponentSpec extends TestComponent with FlatSpecLike with GcsPathUtils {

  "LabelComponent" should "save, get,and delete" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).save()
    val savedCluster2 = makeCluster(2).save()

    val cluster2Map = Map("bam" -> "true", "sample" -> "NA12878")

    val missingId = Random.nextLong()
    dbFutureValue { labelQuery.getAllForCluster(missingId) } shouldEqual Map.empty
    dbFutureValue { labelQuery.get(missingId, "missing") } shouldEqual None
    dbFailure { labelQuery.save(missingId, "key1", "value1") } shouldBe a[SQLException]

    val cluster1Id = savedCluster1.id

    dbFutureValue { labelQuery.save(cluster1Id, "key1", "value1") } shouldEqual 1
    dbFutureValue { labelQuery.getAllForCluster(cluster1Id) } shouldEqual Map("key1" -> "value1")
    dbFutureValue { labelQuery.get(cluster1Id, "key1") } shouldEqual Some("value1")

    val cluster2Id = savedCluster2.id

    dbFutureValue { labelQuery.saveAllForCluster(cluster2Id, cluster2Map) }
    dbFutureValue { labelQuery.getAllForCluster(cluster2Id) } shouldEqual cluster2Map
    dbFutureValue { labelQuery.get(cluster2Id, "bam") } shouldEqual Some("true")
    dbFutureValue { labelQuery.getAllForCluster(cluster1Id) } shouldEqual Map("key1" -> "value1")

    // (cluster, key) unique key test

    val cluster2NewMap = Map("sample" -> "NA12879")

    dbFailure { labelQuery.save(cluster1Id, "key1", "newvalue") } shouldBe a[SQLException]
    dbFailure { labelQuery.saveAllForCluster(cluster2Id, cluster2NewMap) } shouldBe a[SQLException]

    dbFutureValue { labelQuery.delete(cluster1Id, "key1") } shouldEqual 1
    dbFutureValue { labelQuery.delete(cluster1Id, "key1") } shouldEqual 0
    dbFutureValue { labelQuery.getAllForCluster(cluster1Id) } shouldEqual Map.empty

    dbFutureValue { labelQuery.deleteAllForCluster(cluster2Id) } shouldEqual 2
    dbFutureValue { labelQuery.deleteAllForCluster(cluster2Id) } shouldEqual 0
    dbFutureValue { labelQuery.getAllForCluster(cluster2Id) } shouldEqual Map.empty
  }
}
