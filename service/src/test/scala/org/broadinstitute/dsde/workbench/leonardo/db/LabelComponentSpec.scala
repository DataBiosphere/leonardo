package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.FlatSpecLike

import scala.util.Random

class LabelComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {

  "LabelComponent" should "save, get,and delete" in isolatedDbTest {
    val c1 = Cluster(
      clusterName = name1,
      googleId = UUID.randomUUID(),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
      operationName = OperationName("op1"),
      status = ClusterStatus.Creating,
      hostIp = None,
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = Option(Instant.now()),
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      Some(GcsBucketName("testStagingBucket1")),
      List.empty)

    val c2 = Cluster(
      clusterName = name2,
      googleId = UUID.randomUUID(),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name2, clusterUrlBase),
      operationName = OperationName("op2"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("sure, this is an IP address")),
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      None,
      None,
      Some(GcsBucketName("testStagingBucket2")),
      List.empty)

    val c2Map = Map("bam" -> "true", "sample" -> "NA12878")

    val missingId = Random.nextLong()
    dbFutureValue { _.labelQuery.getAllForCluster(missingId) } shouldEqual Map.empty
    dbFutureValue { _.labelQuery.get(missingId, "missing") } shouldEqual None
    dbFailure { _.labelQuery.save(missingId, "key1", "value1") } shouldBe a [SQLException]

    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), Some(serviceAccountKey.id)) } shouldEqual c1
    val c1Id = dbFutureValue { _.clusterQuery.getIdByGoogleId(c1.googleId) }.get

    dbFutureValue { _.labelQuery.save(c1Id, "key1", "value1") } shouldEqual 1
    dbFutureValue { _.labelQuery.getAllForCluster(c1Id) } shouldEqual Map("key1" -> "value1")
    dbFutureValue { _.labelQuery.get(c1Id, "key1") } shouldEqual Some("value1")

    dbFutureValue { _.clusterQuery.save(c2, gcsPath("gs://bucket2"), Some(serviceAccountKey.id)) } shouldEqual c2
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
