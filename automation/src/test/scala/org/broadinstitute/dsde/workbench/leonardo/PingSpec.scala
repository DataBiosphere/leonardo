package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.auth.UserAuthToken
import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.{DoNotDiscover, FreeSpec, Matchers}

@DoNotDiscover
class PingSpec(val billingProject: GoogleProject) extends FreeSpec with Matchers with LeonardoTestUtils with BillingFixtures {
  "Leonardo" - {
    "should ping" in {
      implicit val token: UserAuthToken = ronAuthToken
      Leonardo.test.ping() shouldBe "OK"
    }
  }
}
