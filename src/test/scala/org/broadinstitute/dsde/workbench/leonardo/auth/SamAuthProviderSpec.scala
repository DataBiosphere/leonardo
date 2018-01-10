package org.broadinstitute.dsde.workbench.leonardo.auth

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.scalatest.FlatSpec
import org.scalatest.concurrent.ScalaFutures

class SamAuthProviderSpec extends FlatSpec with CommonTestData with ScalaFutures {
  private val samAuth = new SamAuthProvider(config.getConfig("auth.samAuthProviderConfig"), serviceAccountProvider)

  "SamAuthProvider" should "construct a sane resourcesApi" in {
    val resourcesAPI = samAuth.resourcesApiAsPet(defaultUserInfo.userEmail, project.value)
    resourcesAPI.resourceAction("billing-project", project.value, "launch_notebook_cluster")
  }
}