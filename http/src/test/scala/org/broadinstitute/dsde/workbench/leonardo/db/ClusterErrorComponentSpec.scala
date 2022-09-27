package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant
import java.time.temporal.ChronoUnit
import CommonTestData._
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.UUID

class ClusterErrorComponentSpec extends AnyFlatSpecLike with TestComponent with GcsPathUtils {

  "ClusterErrorComponent" should "save, and get" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).save()

    val traceId = TraceId(UUID.randomUUID())
    lazy val timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val clusterError = RuntimeError("Some Error", Some(10), timestamp, traceId = Some(traceId))

    dbFutureValue(clusterErrorQuery.get(savedCluster1.id)) shouldEqual List.empty
    dbFutureValue(clusterErrorQuery.save(savedCluster1.id, clusterError))
    val res = dbFutureValue(clusterErrorQuery.get(savedCluster1.id))
    res shouldEqual List(clusterError)
  }
}
