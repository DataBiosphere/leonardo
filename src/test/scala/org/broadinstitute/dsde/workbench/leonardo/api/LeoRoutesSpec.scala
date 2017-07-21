package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.leonardo.config.SwaggerConfig
import org.scalatest.{FlatSpec, Matchers}

class LeoRoutesSpec extends FlatSpec with Matchers with ScalatestRouteTest {

  class TestLeoRoutes
    extends LeoRoutes(SwaggerConfig())

  "LeoRoutes" should "200 on ping" in {
    val leoRoutes = new TestLeoRoutes()

    Get("/api/ping") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  "Cluster" should "200" in {
    val leoRoutes = new TestLeoRoutes()

    Put("/cluster") ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }
}
