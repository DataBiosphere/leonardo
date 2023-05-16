package org.broadinstitute.dsde.workbench.leonardo
package config
import org.scalatest.flatspec.AnyFlatSpecLike

class RefererConfigSpec extends LeonardoTestSuite with AnyFlatSpecLike {
  it should "parse config values correctly" in {
    CommonTestData.refererConfig.validHosts shouldBe Set("example.com", "localhost:3000")
    CommonTestData.refererConfig.enabled shouldBe true
    CommonTestData.refererConfig.originStrict shouldBe false
  }
}
