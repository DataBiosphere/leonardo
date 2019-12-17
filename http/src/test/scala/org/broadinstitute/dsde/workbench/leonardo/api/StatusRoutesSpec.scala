package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.dao.{ResourceTypeName, SamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterInternalId
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.service.StatusService
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, ValueObject, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.StatusJsonSupport._
import org.broadinstitute.dsde.workbench.util.health.Subsystems._
import org.broadinstitute.dsde.workbench.util.health.{HealthMonitor, StatusCheckResponse, SubsystemStatus}
import org.http4s.EntityDecoder
import org.http4s.headers.Authorization
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

/**
 * Created by rtitle on 10/26/17.
 */
class StatusRoutesSpec
    extends FlatSpec
    with Matchers
    with ScalatestRouteTest
    with CommonTestData
    with TestComponent
    with TestLeoRoutes {
  implicit override val patienceConfig = PatienceConfig(timeout = 1.second)

  "GET /version" should "give 200 for ok" in {
    eventually {
      Get("/version") ~> leoRoutes.route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] should include("n/a")
      }
    }
  }

  "GET /status" should "give 200 for ok" in {
    eventually {
      Get("/status") ~> leoRoutes.route ~> check {
        responseAs[StatusCheckResponse] shouldEqual StatusCheckResponse(true,
                                                                        Map(GoogleDataproc -> HealthMonitor.OkStatus,
                                                                            Database -> HealthMonitor.OkStatus,
                                                                            Sam -> HealthMonitor.OkStatus))
        status shouldEqual StatusCodes.OK
      }
    }
  }

  it should "give 500 for not ok" in {
    val badSam = new SamDAO[IO] {
      override def getStatus(implicit ev: ApplicativeAsk[IO, TraceId]): IO[StatusCheckResponse] =
        IO.pure(StatusCheckResponse(false, Map(OpenDJ -> SubsystemStatus(false, Some(List("OpenDJ is down. Panic!"))))))

      override def hasResourcePermission(
        resourceId: ValueObject,
        action: String,
        resourceTypeName: ResourceTypeName,
        authHeader: Authorization
      )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Boolean] = ???

      override def getResourcePolicies[A](authHeader: Authorization, resourseTypeName: ResourceTypeName)(
        implicit decoder: EntityDecoder[IO, List[A]],
        ev: ApplicativeAsk[IO, TraceId]
      ): IO[List[A]] = ???

      override def createClusterResource(internalId: ClusterInternalId,
                                         creatorEmail: WorkbenchEmail,
                                         googleProject: GoogleProject,
                                         clusterName: ClusterName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
        ???

      override def deleteClusterResource(internalId: ClusterInternalId,
                                         userEmail: WorkbenchEmail,
                                         creatorEmail: WorkbenchEmail,
                                         googleProject: GoogleProject,
                                         clusterName: ClusterName)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
        ???

      override def getPetServiceAccount(authorization: Authorization, googleProject: GoogleProject)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[Option[WorkbenchEmail]] = ???

      override def getUserProxy(userEmail: WorkbenchEmail)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[Option[WorkbenchEmail]] = ???

      override def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[Option[String]] = ???
    }
    val badDataproc = new MockGoogleDataprocDAO(false)
    val statusService = new StatusService(badDataproc, badSam, DbSingleton.ref, dataprocConfig, pollInterval = 1.second)
    val leoRoutes = new LeoRoutes(leonardoService, proxyService, statusService, swaggerConfig, contentSecurityPolicy)
    with MockUserInfoDirectives {
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
