package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterStatus}
import org.scalatest.FlatSpecLike

class ClusterComponentSpec extends TestComponent with FlatSpecLike {

  "ClusterComponent" should "list, save, get, and delete" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.list() } shouldEqual Seq()

    val c1 = Cluster(
      clusterName = "name1",
      googleId = UUID.randomUUID(),
      googleProject = "dsp-leo-test",
      googleServiceAccount = "not-a-service-acct@google.com",
      googleBucket = "bucket1",
      clusterUrl = Cluster.getClusterUrl("dsp-leo-test", "name1"),
      operationName = "op1",
      status = ClusterStatus.Unknown,
      hostIp = Some("numbers.and.dots"),
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no"))

    val c2 = Cluster(
      clusterName = "name2",
      googleId = UUID.randomUUID(),
      googleProject = "dsp-leo-test",
      googleServiceAccount = "not-a-service-acct@google.com",
      googleBucket = "bucket2",
      clusterUrl = Cluster.getClusterUrl("dsp-leo-test", "name2"),
      operationName = "op2",
      status = ClusterStatus.Creating,
      hostIp = None,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty)

    dbFutureValue { _.clusterQuery.save(c1) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2) } shouldEqual c2
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs Seq(c1, c2)
    dbFutureValue { _.clusterQuery.getByName(c1.googleProject, c1.clusterName) } shouldEqual Some(c1)
    dbFutureValue { _.clusterQuery.getByName(c1.googleProject, c2.clusterName) } shouldEqual Some(c2)
    dbFutureValue { _.clusterQuery.getByGoogleId(c1.googleId) } shouldEqual Some(c1)
    dbFutureValue { _.clusterQuery.getByGoogleId(c2.googleId) } shouldEqual Some(c2)

    // (project, name) unique key test

    val c3 = Cluster(
      clusterName = c1.clusterName,
      googleId = UUID.randomUUID(),
      googleProject = c1.googleProject,
      googleServiceAccount = "something-new@google.com",
      googleBucket = "bucket3",
      clusterUrl = Cluster.getClusterUrl(c1.googleProject, c1.clusterName),
      operationName = "op3",
      status = ClusterStatus.Unknown,
      hostIp = Some("1.2.3.4"),
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty)
    dbFailure { _.clusterQuery.save(c3) } shouldBe a[SQLException]

    // googleId unique key test

    val c4 = Cluster(
      clusterName = "name4",
      googleId = c1.googleId,
      googleProject = "project4",
      googleServiceAccount = "something-new@google.com",
      googleBucket = "bucket3",
      clusterUrl = Cluster.getClusterUrl("project4", "name4"),
      operationName = "op3",
      status = ClusterStatus.Unknown,
      hostIp = Some("1.2.3.4"),
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty)
    dbFailure { _.clusterQuery.save(c4) } shouldBe a[SQLException]

    dbFutureValue { _.clusterQuery.markPendingDeletion(c1.googleId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq(c2)
    val c1status = dbFutureValue { _.clusterQuery.getByGoogleId(c1.googleId) }.get
    c1status.status shouldEqual ClusterStatus.Deleting
    assert(c1status.destroyedDate.nonEmpty)

    dbFutureValue { _.clusterQuery.markPendingDeletion(c2.googleId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq()
    val c2status = dbFutureValue { _.clusterQuery.getByGoogleId(c2.googleId) }.get
    c2status.status shouldEqual ClusterStatus.Deleting
    assert(c2status.destroyedDate.nonEmpty)
  }
}
