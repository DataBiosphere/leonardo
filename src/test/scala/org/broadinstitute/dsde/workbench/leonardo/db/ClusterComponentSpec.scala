package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model._

class ClusterComponentSpec extends TestComponent {

  "ClusterComponent" should "list, save, get, and delete" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.list() } shouldEqual Seq()

    val name1 = ClusterName("name1")
    val name2 = ClusterName("name2")
    val project = GoogleProject("dsp-leo-test")

    val c1 = Cluster(
      clusterName = name1,
      googleId = UUID.randomUUID(),
      googleProject = project,
      googleServiceAccount = GoogleServiceAccount("not-a-service-acct@google.com"),
      googleBucket = GoogleBucket("bucket1"),
      clusterUrl = Cluster.getClusterUrl(project, name1),
      operationName = OperationName("op1"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("numbers.and.dots")),
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no"))

    val c2 = Cluster(
      clusterName = name2,
      googleId = UUID.randomUUID(),
      googleProject = project,
      googleServiceAccount = GoogleServiceAccount("not-a-service-acct@google.com"),
      googleBucket = GoogleBucket("bucket2"),
      clusterUrl = Cluster.getClusterUrl(project, name2),
      operationName = OperationName("op2"),
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
      googleServiceAccount = GoogleServiceAccount("something-new@google.com"),
      googleBucket = GoogleBucket("bucket3"),
      clusterUrl = Cluster.getClusterUrl(c1.googleProject, c1.clusterName),
      operationName = OperationName("op3"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("1.2.3.4")),
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty)

    dbFailure { _.clusterQuery.save(c3) } shouldBe a[SQLException]

    // googleId unique key test

    val name4 = ClusterName("name4")
    val c4 = Cluster(
      clusterName = name4,
      googleId = c1.googleId,
      googleProject = project,
      googleServiceAccount = GoogleServiceAccount("something-new@google.com"),
      googleBucket = GoogleBucket("bucket4"),
      clusterUrl = Cluster.getClusterUrl(project, name4),
      operationName = OperationName("op4"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("1.2.3.4")),
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty)

    dbFailure { _.clusterQuery.save(c4) } shouldBe a[SQLException]

    dbFutureValue { _.clusterQuery.deleteByGoogleId(c1.googleId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.list() } shouldEqual Seq(c2)

    dbFutureValue { _.clusterQuery.deleteByGoogleId(c1.googleId) } shouldEqual 0
    dbFutureValue { _.clusterQuery.list() } shouldEqual Seq(c2)

    dbFutureValue { _.clusterQuery.deleteByGoogleId(c2.googleId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.list() } shouldEqual Seq()
  }
}
