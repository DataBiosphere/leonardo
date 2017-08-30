package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterStatus}
import org.scalatest.FlatSpecLike

import scala.util.Random

class LabelComponentSpec extends TestComponent with FlatSpecLike {

  "LabelComponent" should "save, get,and delete" in isolatedDbTest {

    val c1 = Cluster(
      clusterName = "name1",
      googleId = UUID.randomUUID(),
      googleProject = "dsp-leo-test",
      googleServiceAccount = "not-a-service-acct@google.com",
      googleBucket = "bucket1",
      clusterUrl = Cluster.getClusterUrl("dsp-leo-test", "name1"),
      operationName = "op1",
      status = ClusterStatus.Creating,
      hostIp = None,
      createdDate = Instant.now(),
      destroyedDate = Option(Instant.now()),
      labels = Map.empty)

    val c2 = Cluster(
      clusterName = "name2",
      googleId = UUID.randomUUID(),
      googleProject = "dsp-leo-test",
      googleServiceAccount = "not-a-service-acct@google.com",
      googleBucket = "bucket2",
      clusterUrl = Cluster.getClusterUrl("dsp-leo-test", "name2"),
      operationName = "op2",
      status = ClusterStatus.Unknown,
      hostIp = Some("sure, this is an IP address"),
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty)

    val c2Map = Map("bam" -> "true", "sample" -> "NA12878")

    val missingId = Random.nextLong()
    dbFutureValue { _.labelQuery.getAllForCluster(missingId) } shouldEqual Map.empty
    dbFutureValue { _.labelQuery.get(missingId, "missing") } shouldEqual None
    dbFailure { _.labelQuery.save(missingId, "key1", "value1") } shouldBe a [SQLException]

    dbFutureValue { _.clusterQuery.save(c1) } shouldEqual c1
    val c1Id = dbFutureValue { _.clusterQuery.getIdByGoogleId(c1.googleId) }.get

    dbFutureValue { _.labelQuery.save(c1Id, "key1", "value1") } shouldEqual 1
    dbFutureValue { _.labelQuery.getAllForCluster(c1Id) } shouldEqual Map("key1" -> "value1")
    dbFutureValue { _.labelQuery.get(c1Id, "key1") } shouldEqual Some("value1")

    dbFutureValue { _.clusterQuery.save(c2) } shouldEqual c2
    val c2Id = dbFutureValue { _.clusterQuery.getIdByGoogleId(c2.googleId) }.get

    dbFutureValue { _.labelQuery.saveAllForCluster(c2Id, c2Map) }
    dbFutureValue { _.labelQuery.getAllForCluster(c2Id) } shouldEqual c2Map
    dbFutureValue { _.labelQuery.get(c2Id, "bam") } shouldEqual Some("true")
    dbFutureValue { _.labelQuery.getAllForCluster(c1Id) } shouldEqual Map("key1" -> "value1")

    // (cluster, key) unique key test

    val c2NewMap = Map("sample" -> "NA12879")

    dbFailure { _.labelQuery.save(c1Id, "key1", "newvalue") } shouldBe a[SQLException]
    dbFailure { _.labelQuery.saveAllForCluster(c2Id, c2NewMap) } shouldBe a[SQLException]

    dbFutureValue { _.labelQuery.delete(c1Id, "key1") } shouldEqual 1
    dbFutureValue { _.labelQuery.delete(c1Id, "key1") } shouldEqual 0
    dbFutureValue { _.labelQuery.getAllForCluster(c1Id) } shouldEqual Map.empty

    dbFutureValue { _.labelQuery.deleteAllForCluster(c2Id) } shouldEqual 2
    dbFutureValue { _.labelQuery.deleteAllForCluster(c2Id) } shouldEqual 0
    dbFutureValue { _.labelQuery.getAllForCluster(c2Id) } shouldEqual Map.empty
   }
}
