package org.broadinstitute.dsde.workbench.leonardo.model

import org.broadinstitute.dsde.workbench.leonardo.{LeonardoTestSuite, WsmState}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class ModelsSpec extends LeonardoTestSuite with Matchers with AnyFlatSpecLike {
  it should "convert WSM state to value correctly" in {
    WsmState(None).value shouldBe "NONE"
    WsmState(Some("CREATING")).value shouldBe "CREATING"
    WsmState(Some("UPDATING")).value shouldBe "UPDATING"
    WsmState(Some("BROKEN")).value shouldBe "BROKEN"
    WsmState(Some("READY")).value shouldBe "READY"
  }
  it should "determine if WSM state is deletable" in {
    WsmState(None).isDeletable shouldBe true
    WsmState(Some("BROKEN")).isDeletable shouldBe true
    WsmState(Some("READY")).isDeletable shouldBe true
    WsmState(Some("CREATING")).isDeletable shouldBe false
    WsmState(Some("UPDATING")).isDeletable shouldBe false
  }
  it should "determine if WSM state is deleted" in {
    WsmState(None).isDeleted shouldBe true
    WsmState(Some("READY")).isDeleted shouldBe false
  }
}
