package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.Cluster

class LabelComponentSpec extends TestComponent {

  "LabelComponent" should "save, get,and delete" in isolatedDbTest {

    val c1 = Cluster(clusterId = UUID.randomUUID(),
      clusterName = "name1",
      googleProject = "dsp-leo-test",
      googleServiceAccount = "not-a-service-acct@google.com",
      googleBucket = "bucket1",
      operationName = "op1",
      createdDate = Instant.now(),
      destroyedDate = Option(Instant.now()),
      labels = Map.empty)

    val c2 = Cluster(clusterId = UUID.randomUUID(),
      clusterName = "name2",
      googleProject = "dsp-leo-test",
      googleServiceAccount = "not-a-service-acct@google.com",
      googleBucket = "bucket2",
      operationName = "op2",
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty)

    val c1Id = c1.clusterId
    val c2Id = c2.clusterId
    val c2Map = Map("bam" -> "true", "sample" -> "NA12878")

    dbFutureValue { _.labelQuery.getAll(c1Id) } shouldEqual Map.empty
    dbFutureValue { _.labelQuery.get(c1Id, "missing") } shouldEqual None

    // Cluster c1 doesn't exist in the DB
    dbFailure { _.labelQuery.save(c1.clusterId, "key1", "value1") } shouldBe a [SQLException]
    dbFutureValue { _.clusterQuery.save(c1) } shouldEqual c1

    dbFutureValue { _.labelQuery.save(c1Id, "key1", "value1") } shouldEqual 1
    dbFutureValue { _.labelQuery.getAll(c1Id) } shouldEqual Map("key1" -> "value1")
    dbFutureValue { _.labelQuery.get(c1Id, "key1") } shouldEqual Some("value1")
    dbFutureValue { _.labelQuery.getAll(c2Id) } shouldEqual Map.empty
    dbFutureValue { _.labelQuery.get(c2Id, "key1") } shouldEqual None

    // Cluster c2 doesn't exist in the DB
    dbFailure { _.labelQuery.saveAll(c2.clusterId, c2Map) } shouldBe a [SQLException]
    dbFutureValue { _.clusterQuery.save(c2) } shouldEqual c2

    dbFutureValue { _.labelQuery.saveAll(c2Id, c2Map) }
    dbFutureValue { _.labelQuery.getAll(c2Id) } shouldEqual c2Map
    dbFutureValue { _.labelQuery.get(c2Id, "bam") } shouldEqual Some("true")
    dbFutureValue { _.labelQuery.getAll(c1Id) } shouldEqual Map("key1" -> "value1")

    dbFutureValue { _.labelQuery.delete(c1Id, "key1") } shouldEqual 1
    dbFutureValue { _.labelQuery.delete(c1Id, "key1") } shouldEqual 0
    dbFutureValue { _.labelQuery.getAll(c1Id) } shouldEqual Map.empty

    dbFutureValue { _.labelQuery.deleteAll(c2Id) } shouldEqual 2
    dbFutureValue { _.labelQuery.deleteAll(c2Id) } shouldEqual 0
    dbFutureValue { _.labelQuery.getAll(c2Id) } shouldEqual Map.empty
   }
}
