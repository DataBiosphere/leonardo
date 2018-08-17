package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
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
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      dataprocInfo = DataprocInfo(Option(UUID.randomUUID()), Option(OperationName("op1")), Some(GcsBucketName("testStagingBucket1")), None),
      auditInfo = AuditInfo(userEmail, Instant.now(), Option(Instant.now()), Instant.now()),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
      status = ClusterStatus.Creating,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      autopauseThreshold = 0,
      defaultClientId = None)

    val c2 = Cluster(
      clusterName = name2,
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      dataprocInfo = DataprocInfo(Option(UUID.randomUUID()), Option(OperationName("op2")), Some(GcsBucketName("testStagingBucket2")), Some(IP("sure, this is an IP address"))),
      auditInfo = AuditInfo(userEmail, Instant.now(), None, Instant.now()),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name2, clusterUrlBase),
      status = ClusterStatus.Unknown,
      labels = Map.empty,
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      autopauseThreshold = 0,
      defaultClientId = None)

    val c2Map = Map("bam" -> "true", "sample" -> "NA12878")

    val missingId = Random.nextLong()
    dbFutureValue { _.labelQuery.getAllForCluster(missingId) } shouldEqual Map.empty
    dbFutureValue { _.labelQuery.get(missingId, "missing") } shouldEqual None
    dbFailure { _.labelQuery.save(missingId, "key1", "value1") } shouldBe a [SQLException]

    val savedC1 = dbFutureValue { _.clusterQuery.save(c1, Option(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) }

    savedC1 shouldEqual c1

    val c1Id = savedC1.id

    dbFutureValue { _.labelQuery.save(c1Id, "key1", "value1") } shouldEqual 1
    dbFutureValue { _.labelQuery.getAllForCluster(c1Id) } shouldEqual Map("key1" -> "value1")
    dbFutureValue { _.labelQuery.get(c1Id, "key1") } shouldEqual Some("value1")

    val savedC2 = dbFutureValue { _.clusterQuery.save(c2, Option(gcsPath("gs://bucket2")), Some(serviceAccountKey.id)) }

    savedC2 shouldEqual c2

    val c2Id = savedC2.id

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
