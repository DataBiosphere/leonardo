package org.broadinstitute.dsde.workbench.leonardo
package db

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class PatchComponentSpec extends AnyFlatSpecLike with TestComponent {
  "PatchComponent" should "update a record when there's already an existing record" in isolatedDbTest {
    implicit dbRef =>
      val cluster = makeCluster(1).save()
      val patchDetails = RuntimePatchDetails(cluster.id, RuntimeStatus.Stopped)

      val res = for {
        r1 <- patchQuery.save(patchDetails, Some(MachineTypeName("machineType1"))).transaction
        r2 <- patchQuery.getPatchAction(patchDetails.runtimeId).transaction
        r3 <- patchQuery.save(patchDetails, Some(MachineTypeName("machineType2"))).transaction
        r4 <- patchQuery.getPatchAction(patchDetails.runtimeId).transaction
      } yield {
        r1 shouldBe (1)
        r2 shouldBe (Some(MachineTypeName("machineType1")))
        r3 shouldBe (1)
        r4 shouldBe (Some(MachineTypeName("machineType2")))
      }
      res.unsafeRunSync()
  }
}
