package org.broadinstitute.dsde.workbench.leonardo

import org.scalatest.{FreeSpec, Matchers}

class PingSpec extends FreeSpec with Matchers with LeonardoTestUtils {
  "Leonardo" - {
    "should ping" in {
      implicit val token = ronAuthToken
      Leonardo.test.ping() shouldBe "OK"
    }
  }
}
