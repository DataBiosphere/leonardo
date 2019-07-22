package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.auth.UserAuthToken
import org.scalatest.{DoNotDiscover, FreeSpec, Matchers}

@DoNotDiscover
class PingSpec extends FreeSpec with Matchers with LeonardoTestUtils {
  "Leonardo" - {
    "should ping" in {
      implicit val token: UserAuthToken = ronAuthToken
      Leonardo.test.ping() shouldBe "OK"
    }
  }
}
