package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.util.health.StatusJsonSupport._
import org.broadinstitute.dsde.workbench.util.health.Subsystems.{GoogleIam, OpenDJ}
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.broadinstitute.dsde.workbench.model.ErrorReportJsonSupport._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.Future

/**
  * Created by rtitle on 10/16/17.
  */
class HttpSamDAOSpec extends FlatSpec with Matchers with BeforeAndAfterAll with ScalatestRouteTest with ScalaFutures {
  implicit val patience = PatienceConfig(timeout = scaled(Span(10, Seconds)))
  implicit val errorReportSource = ErrorReportSource("test")
  var bindingFutureHealthy: Future[ServerBinding] = _
  var bindingFutureUnhealthy: Future[ServerBinding] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    bindingFutureHealthy = Http().bindAndHandle(backendRoute, "0.0.0.0", 9090)
    bindingFutureUnhealthy = Http().bindAndHandle(unhealthyRoute, "0.0.0.0", 9091)
  }

  override def afterAll(): Unit = {
    bindingFutureHealthy.flatMap(_.unbind())
    bindingFutureUnhealthy.flatMap(_.unbind())
    super.afterAll()
  }

  val returnedOkStatus = StatusCheckResponse(true, Map(OpenDJ -> SubsystemStatus(true, None), GoogleIam -> SubsystemStatus(true, None)))
  val expectedOkStatus = StatusCheckResponse(true, Map.empty)
  val notOkStatus = StatusCheckResponse(false, Map(OpenDJ -> SubsystemStatus(false, Option(List("OpenDJ is down. Panic!"))), GoogleIam -> SubsystemStatus(true, None)))

  val expectedPet = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val defaultUserInfo = UserInfo(OAuth2BearerToken("accessToken1"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)
  val errorUserInfo = UserInfo(OAuth2BearerToken("accessToken2"), WorkbenchUserId("user2"), WorkbenchEmail("user2@example.com"), 0)
  val unknownUserInfo = UserInfo(OAuth2BearerToken("accessToken3"), WorkbenchUserId("user3"), WorkbenchEmail("user3@example.com"), 0)
  val googleProject = GoogleProject("my-google-proj")

  def getPet(auth: Option[Authorization]): ToResponseMarshallable = {
    auth match {
      case Some(Authorization(credentials)) if credentials.token() == defaultUserInfo.accessToken.token => expectedPet
      case Some(Authorization(credentials)) if credentials.token() == errorUserInfo.accessToken.token => StatusCodes.InternalServerError -> ErrorReport(new Exception("my message"))
      case _ => StatusCodes.Unauthorized
    }
  }

  val unhealthyRoute: Route =
    path("status") {
      get {
        complete(StatusCodes.InternalServerError -> notOkStatus)
      }
    }

  val backendRoute: Route =
    path("status") {
      get {
        complete(returnedOkStatus)
      }
    } ~
    // deprecated!
    pathPrefix("api") {
      path("user" / "petServiceAccount") {
        get {
          extractRequest { request =>
            complete(getPet(request.header[Authorization]))
          }
        }
      } ~
      path("google" / "user" / "petServiceAccount" / Segment) { googleProject =>
        get {
          extractRequest { request =>
            complete(getPet(request.header[Authorization]))
          }
        }
      }
    }

  val dao = new HttpSamDAO("http://localhost:9090")
  val unhealthyDAO = new HttpSamDAO("http://localhost:9091")
  val unknownDAO = new HttpSamDAO("http://localhost:9092")

  "HttpSamDAO" should "get Sam status" in {
    dao.getStatus().futureValue shouldBe expectedOkStatus
    unhealthyDAO.getStatus().futureValue shouldBe notOkStatus
    unknownDAO.getStatus().failed.futureValue shouldBe a [CallToSamFailedException]
  }

  it should "get pet service accounts" in {
    dao.getPetServiceAccount(defaultUserInfo).futureValue shouldBe expectedPet
    val exception = dao.getPetServiceAccount(errorUserInfo).failed.futureValue
    exception shouldBe a [CallToSamFailedException]
    exception.asInstanceOf[CallToSamFailedException].status shouldBe StatusCodes.InternalServerError
    exception.asInstanceOf[CallToSamFailedException].msg shouldBe Some("my message")  // matches returned ErrorReport

    val exception2 = dao.getPetServiceAccount(unknownUserInfo).failed.futureValue
    exception2 shouldBe a [CallToSamFailedException]
    exception2.asInstanceOf[CallToSamFailedException].status shouldBe StatusCodes.Unauthorized
    exception2.asInstanceOf[CallToSamFailedException].msg shouldBe None  // no ErrorReport
  }

  it should "get pet service accounts per project" in {
    dao.getPetServiceAccountForProject(defaultUserInfo, googleProject).futureValue shouldBe expectedPet
    val exception = dao.getPetServiceAccountForProject(errorUserInfo, googleProject).failed.futureValue
    exception shouldBe a [CallToSamFailedException]
    exception.asInstanceOf[CallToSamFailedException].status shouldBe StatusCodes.InternalServerError
    exception.asInstanceOf[CallToSamFailedException].msg shouldBe Some("my message")  // matches returned ErrorReport

    val exception2 = dao.getPetServiceAccountForProject(unknownUserInfo, googleProject).failed.futureValue
    exception2 shouldBe a [CallToSamFailedException]
    exception2.asInstanceOf[CallToSamFailedException].status shouldBe StatusCodes.Unauthorized
    exception2.asInstanceOf[CallToSamFailedException].msg shouldBe None  // no ErrorReport
  }

  it should "fail gracefully if it can't connect" in {
    val badDao = new HttpSamDAO("http://localhost:9099")
    val exception = badDao.getPetServiceAccountForProject(defaultUserInfo, googleProject).failed.futureValue
    exception shouldBe a [CallToSamFailedException]
    exception.asInstanceOf[CallToSamFailedException].status shouldBe StatusCodes.InternalServerError
  }
}
