package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.workbench.leonardo.model.UserInfo
import org.broadinstitute.dsde.workbench.model.{WorkbenchUserEmail, WorkbenchUserId, WorkbenchUserServiceAccountEmail}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.util.health.StatusJsonSupport._
import org.broadinstitute.dsde.workbench.util.health.SubsystemStatus
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import scala.concurrent.Future

/**
  * Created by rtitle on 10/16/17.
  */
class HttpSamDAOSpec extends FlatSpec with Matchers with BeforeAndAfterAll with ScalatestRouteTest with ScalaFutures {
  implicit val patience = PatienceConfig(timeout = scaled(Span(10, Seconds)))
  var bindingFuture: Future[ServerBinding] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    bindingFuture = Http().bindAndHandle(backendRoute, "0.0.0.0", 9090)
  }

  override def afterAll(): Unit = {
    bindingFuture.flatMap(_.unbind())
    super.afterAll()
  }

  val expectedStatus = SubsystemStatus(true, None)
  val expectedPet = WorkbenchUserServiceAccountEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val defaultUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchUserEmail("user1@example.com"), 0)
  val otherUserInfo = UserInfo(OAuth2BearerToken("tokenFoo"), WorkbenchUserId("user2"), WorkbenchUserEmail("user2@example.com"), 0)

  val backendRoute: Route =
    path("status") {
      get {
        complete(expectedStatus)
      }
    } ~
    pathPrefix("api") {
      path("user" / "petServiceAccount") {
        get {
          extractRequest { request =>
            request.header[Authorization] match {
              case Some(Authorization(credentials)) if credentials.token() == defaultUserInfo.accessToken.token =>
                complete(expectedPet)
              case _ =>
                complete(StatusCodes.BadRequest)
            }
          }
        }
      }
    }

  val dao = new HttpSamDAO("http://localhost:9090")

  "HttpSamDAO" should "deserialize Sam status" in {
    dao.getStatus().futureValue shouldBe expectedStatus
  }

  it should "deserialize pet service accounts" in {
    dao.getPetServiceAccount(defaultUserInfo).futureValue shouldBe expectedPet
    val exception = dao.getPetServiceAccount(otherUserInfo).failed.futureValue
    exception shouldBe a [CallToSamFailedException]
    exception.asInstanceOf[CallToSamFailedException].status shouldBe StatusCodes.BadRequest
  }

  it should "fail gracefully" in {
    val badDao = new HttpSamDAO("http://localhost:9091")
    val exception = badDao.getPetServiceAccount(defaultUserInfo).failed.futureValue
    exception shouldBe a [CallToSamFailedException]
    exception.asInstanceOf[CallToSamFailedException].status shouldBe StatusCodes.InternalServerError
  }
}
