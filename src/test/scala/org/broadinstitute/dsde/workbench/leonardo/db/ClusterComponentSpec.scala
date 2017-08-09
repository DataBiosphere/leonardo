package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterStatus}

class ClusterComponentSpec extends TestComponent {

  "ClusterComponent" should "list, save, and delete" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.list() } shouldEqual Seq()

    val c1 = Cluster(clusterId = UUID.randomUUID(),
      clusterName = "name1",
      googleProject = "dsp-leo-test",
      googleServiceAccount = "not-a-service-acct@google.com",
      googleBucket = "bucket1",
      operationName = "op1",
      status = ClusterStatus.Unknown,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no"))

    val c2 = Cluster(clusterId = UUID.randomUUID(),
      clusterName = "name2",
      googleProject = "dsp-leo-test",
      googleServiceAccount = "not-a-service-acct@google.com",
      googleBucket = "bucket2",
      operationName = "op2",
      status = ClusterStatus.Creating,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty)

    dbFutureValue { _.clusterQuery.save(c1) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2) } shouldEqual c2
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs Seq(c1, c2)

    dbFutureValue { _.clusterQuery.delete(c1.clusterId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs Seq(c2)

    dbFutureValue { _.clusterQuery.delete(c1.clusterId) } shouldEqual 0
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs Seq(c2)

    dbFutureValue { _.clusterQuery.delete(c2.clusterId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs Seq()

  }
}
