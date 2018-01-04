package org.broadinstitute.dsde.workbench.leonardo.auth

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.scalatest.FlatSpec
import org.scalatest.concurrent.ScalaFutures

class SamAuthProviderSpec extends FlatSpec with CommonTestData with ScalaFutures {
  private val samAuth = new SamAuthProvider(config.getConfig("auth.samAuthProviderConfig"))

  "SamAuthProvider" should "construct a sane resourcesApi" in {
    val rAPI = samAuth.resourcesApi(defaultUserInfo)
    rAPI.resourceAction("aaa", "bbbb", "cccc")
    println("")
  }
}
