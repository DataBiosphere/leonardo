package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import java.time.temporal.ChronoUnit

import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.scalatest.FlatSpecLike

class ClusterErrorComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {

  "ClusterErrorComponent" should "save, and get" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).save()

    lazy val timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val clusterError = ClusterError("Some Error", 10, timestamp)

    dbFutureValue {_.clusterErrorQuery.get(savedCluster1.id)} shouldEqual List.empty
    dbFutureValue {_.clusterErrorQuery.save(savedCluster1.id, clusterError)}
    dbFutureValue {_.clusterErrorQuery.get(savedCluster1.id)} shouldEqual List(clusterError)
  }
}
