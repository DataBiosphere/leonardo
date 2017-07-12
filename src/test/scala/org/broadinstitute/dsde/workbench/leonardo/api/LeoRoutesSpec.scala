package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.{FlatSpec, Matchers}

class LeoRoutesSpec extends FlatSpec with Matchers with ScalatestRouteTest {

  class TestLeoRoutes()
    extends LeoRoutes

  "LeoRoutes" should "200 on ping" in {
    val leoRoutes = new TestLeoRoutes()

    Get("/ping") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }
}
