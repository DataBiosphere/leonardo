package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException

import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.scalatest.FlatSpecLike

import scala.util.Random

class LabelComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {

  "LabelComponent" should "save, get,and delete" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).save()
    val savedCluster2 = makeCluster(2).save()

    val cluster2Map = Map("bam" -> "true", "sample" -> "NA12878")

    val missingId = Random.nextLong()
    dbFutureValue { _.labelQuery.getAllForCluster(missingId) } shouldEqual Map.empty
    dbFutureValue { _.labelQuery.get(missingId, "missing") } shouldEqual None
    dbFailure { _.labelQuery.save(missingId, "key1", "value1") } shouldBe a [SQLException]

    val cluster1Id = savedCluster1.id

    dbFutureValue { _.labelQuery.save(cluster1Id, "key1", "value1") } shouldEqual 1
    dbFutureValue { _.labelQuery.getAllForCluster(cluster1Id) } shouldEqual Map("key1" -> "value1")
    dbFutureValue { _.labelQuery.get(cluster1Id, "key1") } shouldEqual Some("value1")

    val cluster2Id = savedCluster2.id

    dbFutureValue { _.labelQuery.saveAllForCluster(cluster2Id, cluster2Map) }
    dbFutureValue { _.labelQuery.getAllForCluster(cluster2Id) } shouldEqual cluster2Map
    dbFutureValue { _.labelQuery.get(cluster2Id, "bam") } shouldEqual Some("true")
    dbFutureValue { _.labelQuery.getAllForCluster(cluster1Id) } shouldEqual Map("key1" -> "value1")

    // (cluster, key) unique key test

    val cluster2NewMap = Map("sample" -> "NA12879")

    dbFailure { _.labelQuery.save(cluster1Id, "key1", "newvalue") } shouldBe a[SQLException]
    dbFailure { _.labelQuery.saveAllForCluster(cluster2Id, cluster2NewMap) } shouldBe a[SQLException]

    dbFutureValue { _.labelQuery.delete(cluster1Id, "key1") } shouldEqual 1
    dbFutureValue { _.labelQuery.delete(cluster1Id, "key1") } shouldEqual 0
    dbFutureValue { _.labelQuery.getAllForCluster(cluster1Id) } shouldEqual Map.empty

    dbFutureValue { _.labelQuery.deleteAllForCluster(cluster2Id) } shouldEqual 2
    dbFutureValue { _.labelQuery.deleteAllForCluster(cluster2Id) } shouldEqual 0
    dbFutureValue { _.labelQuery.getAllForCluster(cluster2Id) } shouldEqual Map.empty
   }
}
