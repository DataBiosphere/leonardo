package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant
import java.time.temporal.ChronoUnit

import CommonTestData._
import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

class ClusterErrorComponentSpec extends AnyFlatSpecLike with TestComponent with GcsPathUtils {

  "ClusterErrorComponent" should "save, and get" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).save()

    lazy val timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val clusterError = RuntimeError("Some Error", Some(10), timestamp)

    dbFutureValue(clusterErrorQuery.get(savedCluster1.id)) shouldEqual List.empty
    dbFutureValue(clusterErrorQuery.save(savedCluster1.id, clusterError))
    val res = dbFutureValue(clusterErrorQuery.get(savedCluster1.id))
    res shouldEqual List(clusterError)
  }
}
