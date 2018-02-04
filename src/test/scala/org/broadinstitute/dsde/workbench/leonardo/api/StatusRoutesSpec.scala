package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.service.StatusService
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.util.health.{HealthMonitor, StatusCheckResponse}
import org.broadinstitute.dsde.workbench.util.health.Subsystems._
import org.broadinstitute.dsde.workbench.util.health.StatusJsonSupport._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.Eventually.eventually

import scala.concurrent.duration._

/**
  * Created by rtitle on 10/26/17.
  */
class StatusRoutesSpec extends FlatSpec with Matchers with ScalatestRouteTest with TestLeoRoutes with TestComponent {
  override implicit val patienceConfig = PatienceConfig(timeout = 1.second)

  "GET /status" should "give 200 for ok" in {
    eventually {
      Get("/status") ~> leoRoutes.route ~> check {
        responseAs[StatusCheckResponse] shouldEqual StatusCheckResponse(
          true, Map(GoogleDataproc -> HealthMonitor.OkStatus, Database -> HealthMonitor.OkStatus, Sam -> HealthMonitor.OkStatus))
        status shouldEqual StatusCodes.OK
      }
    }
  }

  it should "give 500 for not ok" in {
    val badSam = new MockSamDAO(false)
    val badDataproc = new MockGoogleDataprocDAO(false)
    val statusService = new StatusService(badDataproc, badSam, DbSingleton.ref, dataprocConfig, pollInterval = 1.second)
    val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig) with MockUserInfoDirectives {
      override val userInfo: UserInfo = defaultUserInfo
    }

    eventually {
      Get("/status") ~> leoRoutes.route ~> check {
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
