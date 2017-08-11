package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterStatus}

class ClusterComponentSpec extends TestComponent {

  "ClusterComponent" should "list, save, get, and delete" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.list() } shouldEqual Seq()

    val c1 = Cluster(
      clusterName = "name1",
      googleId = UUID.randomUUID(),
      googleProject = "dsp-leo-test",
      googleServiceAccount = "not-a-service-acct@google.com",
      googleBucket = "bucket1",
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

    // can't update/overwrite
    dbFailure { _.clusterQuery.save(c1.copy(googleId = UUID.randomUUID())) } shouldBe a[SQLException]

    dbFutureValue { _.clusterQuery.deleteByGoogleId(c1.googleId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs Seq(c2)

    dbFutureValue { _.clusterQuery.deleteByGoogleId(c1.googleId) } shouldEqual 0
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs Seq(c2)

    dbFutureValue { _.clusterQuery.deleteByGoogleId(c2.googleId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs Seq()
  }
}
