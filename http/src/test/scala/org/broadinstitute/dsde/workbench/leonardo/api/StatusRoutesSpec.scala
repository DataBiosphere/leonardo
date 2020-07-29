package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.http.service.StatusService
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.util.health.StatusJsonSupport._
import org.broadinstitute.dsde.workbench.util.health.Subsystems._
import org.broadinstitute.dsde.workbench.util.health.{HealthMonitor, StatusCheckResponse, SubsystemStatus}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

/**
 * Created by rtitle on 10/26/17.
 */
class StatusRoutesSpec
    extends AnyFlatSpec
    with Matchers
    with ScalatestRouteTest
    with LeonardoTestSuite
    with TestComponent
    with TestLeoRoutes {
  implicit override val patienceConfig = PatienceConfig(timeout = 1.second)

  "GET /version" should "give 200 for ok" in {
    eventually {
      Get("/version") ~> statusRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] should include("n/a")
      }
    }
  }

  "GET /status" should "give 200 for ok" in {
    eventually {
      Get("/status") ~> statusRoutes.route ~> check {
        responseAs[StatusCheckResponse] shouldEqual StatusCheckResponse(true,
                                                                        Map(GoogleDataproc -> HealthMonitor.OkStatus,
                                                                            Database -> HealthMonitor.OkStatus,
                                                                            Sam -> HealthMonitor.OkStatus))
        status shouldEqual StatusCodes.OK
      }
    }
  }

  it should "give 500 for not ok" in {
    val badSam = new MockSamDAO {
      override def getStatus(implicit ev: ApplicativeAsk[IO, TraceId]): IO[StatusCheckResponse] =
        IO.pure(StatusCheckResponse(false, Map(OpenDJ -> SubsystemStatus(false, Some(List("OpenDJ is down. Panic!"))))))
    }
    val badDataproc = new MockGoogleDataprocDAO(false)
    val statusService =
      new StatusService(badDataproc, badSam, testDbRef, applicationConfig, pollInterval = 1.second)
    val statusRoute = new StatusRoutes(statusService) with MockUserInfoDirectives {
      override val userInfo: UserInfo = defaultUserInfo
    }

    eventually {
      Get("/status") ~> statusRoute.route ~> check {
        responseAs[StatusCheckResponse].ok shouldEqual false
        responseAs[StatusCheckResponse].systems.keySet shouldEqual Set(GoogleDataproc, Database, Sam)
        responseAs[StatusCheckResponse].systems(GoogleDataproc).ok shouldBe false
        responseAs[StatusCheckResponse].systems(GoogleDataproc).messages shouldBe 'defined
        responseAs[StatusCheckResponse].systems(Sam).ok shouldBe false
        responseAs[StatusCheckResponse].systems(Sam).messages shouldBe 'defined
        responseAs[StatusCheckResponse].systems(Database).ok shouldBe true
        responseAs[StatusCheckResponse].systems(Database).messages shouldBe None
        status shouldEqual StatusCodes.InternalServerError
      }
    }
  }
}
