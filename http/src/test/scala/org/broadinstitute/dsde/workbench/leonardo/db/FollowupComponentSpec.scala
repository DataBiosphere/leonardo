package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.db.{followupQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.ClusterFollowupDetails
import org.scalatest.FlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class FollowupComponentSpec extends FlatSpecLike with TestComponent {
  "FollowupComponent" should "update a record when there's already an existing record" in isolatedDbTest {
    val cluster = makeCluster(1).save()
    val followUpDetails = ClusterFollowupDetails(cluster.id, RuntimeStatus.Stopped)

    val res = for {
      r1 <- followupQuery.save(followUpDetails, Some(MachineTypeName("machineType1"))).transaction
      r2 <- followupQuery.getFollowupAction(followUpDetails).transaction
      r3 <- followupQuery.save(followUpDetails, Some(MachineTypeName("machineType2"))).transaction
      r4 <- followupQuery.getFollowupAction(followUpDetails).transaction
    } yield {
      r1 shouldBe (1)
      r2 shouldBe (Some(MachineTypeName("machineType1")))
      r3 shouldBe (1)
      r4 shouldBe (Some(MachineTypeName("machineType2")))
    }
    res.unsafeRunSync()
  }
}
