package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import org.broadinstitute.dsde.workbench.leonardo.http.service.ProxyService._
import akka.http.scaladsl.model.Uri
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ProxyServiceSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite {
  "ProxyService" should "be able to rewrite Jupyter path correctly" in {
    val pathBeforeRewrite1 = Uri.Path("/notebooks/project1/cluster1/jupyter")
    val expectedPath1 = Uri.Path("/notebooks/project1/cluster1/")
    val res1 = rewriteJupyterPath(pathBeforeRewrite1)
    res1 shouldBe (expectedPath1)

    val pathBeforeRewrite2 = Uri.Path("/proxy/project2/cluster2/jupyter")
    val expectedPath2 = Uri.Path("/notebooks/project2/cluster2/")
    val res2 = rewriteJupyterPath(pathBeforeRewrite2)
    res2 shouldBe (expectedPath2)
  }
}
