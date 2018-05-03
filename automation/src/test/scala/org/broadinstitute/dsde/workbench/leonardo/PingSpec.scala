package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.fixture.BillingFixtures
import org.scalatest.{FreeSpec, Matchers}

class PingSpec extends FreeSpec with Matchers with LeonardoTestUtils with BillingFixtures {
  "Leonardo" - {
    "should ping" in {
      implicit val token = ronAuthToken
      Leonardo.test.ping() shouldBe "OK"
    }
  }
}
