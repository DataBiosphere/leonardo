package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LivelinessRoutesSpec
  extends AnyFlatSpec
    with Matchers
    with ScalatestRouteTest
    with LeonardoTestSuite
    with TestComponent
    with TestLeoRoutes {
  val livelinessRoutes = new LivelinessRoutes

  "GET /liveliness" should "give 200" in {
    eventually {
      Get("/liveliness") ~> livelinessRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }
  }
}
