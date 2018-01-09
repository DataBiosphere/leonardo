package org.broadinstitute.dsde.workbench.leonardo.auth

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.scalatest.FlatSpec
import org.scalatest.concurrent.ScalaFutures

class SamAuthProviderSpec extends FlatSpec with CommonTestData with ScalaFutures {
  private val samAuth = new SamAuthProvider(config.getConfig("auth.samAuthProviderConfig"), serviceAccountProvider)

  "SamAuthProvider" should "construct a sane resourcesApi" in {
    val rAPI = samAuth.resourcesApiAsPet(defaultUserInfo.userEmail)
    rAPI.resourceAction("resource-type", "project", "launch_notebook_cluster")
  }
}