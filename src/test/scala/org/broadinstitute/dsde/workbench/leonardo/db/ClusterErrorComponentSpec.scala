package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import java.time.temporal.ChronoUnit

import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.scalatest.FlatSpecLike

class ClusterErrorComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {

  "ClusterErrorComponent" should "save, and get" in isolatedDbTest {
    val cluster1 = getCluster(1)

    lazy val timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val clusterError = ClusterError("Some Error", 10, timestamp)

    val savedCluster1 = dbFutureValue { _.clusterQuery.save(cluster1, Option(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) }
    savedCluster1 shouldEqual cluster1

    dbFutureValue {_.clusterErrorQuery.get(savedCluster1.id)} shouldEqual List.empty
    dbFutureValue {_.clusterErrorQuery.save(savedCluster1.id, clusterError)}
    dbFutureValue {_.clusterErrorQuery.get(savedCluster1.id)} shouldEqual List(clusterError)
  }
}
