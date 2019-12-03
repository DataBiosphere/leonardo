package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant
import java.time.temporal.ChronoUnit

import org.scalatest.FlatSpecLike
import CommonTestData._

class ClusterErrorComponentSpec extends TestComponent with FlatSpecLike with GcsPathUtils {

  "ClusterErrorComponent" should "save, and get" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).save()

    lazy val timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val clusterError = ClusterError("Some Error", 10, timestamp)

    dbFutureValue { dbRef.dataAccess.clusterErrorQuery.get(savedCluster1.id) } shouldEqual List.empty
    dbFutureValue { dbRef.dataAccess.clusterErrorQuery.save(savedCluster1.id, clusterError) }
    dbFutureValue { dbRef.dataAccess.clusterErrorQuery.get(savedCluster1.id) } shouldEqual List(clusterError)
  }
}
