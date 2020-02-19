package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{contentSecurityPolicy, swaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.{LeonardoTestSuite, RuntimeName}
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.http.api.{
  CreateRuntime2Request,
  HttpRoutes,
  RuntimeServiceContext,
  TestLeoRoutes
}
import org.broadinstitute.dsde.workbench.leonardo.http.service.RuntimeService
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpec, Matchers}

class HttpRoutesSpec
    extends FlatSpec
    with ScalatestRouteTest
    with LeonardoTestSuite
    with ScalaFutures
    with Matchers
    with TestComponent
    with TestLeoRoutes {
  val clusterName = "test"
  val googleProject = "dsp-leo-test"
  "HttpRoutes" should "200 on ping" in {
    Get("/ping") ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "create runtime" in {
    val routes = new HttpRoutes(
      swaggerConfig,
      statusService,
      proxyService,
      leonardoService,
      new RuntimeService[IO] {
        override def createRuntime(
          userInfo: UserInfo,
          googleProject: GoogleProject,
          runtimeName: RuntimeName,
          req: CreateRuntime2Request
        )(implicit as: ApplicativeAsk[IO, RuntimeServiceContext]): IO[Unit] = IO.unit
      },
      userInfoDirectives,
      contentSecurityPolicy
    )
    Post("/api/google/v1/runtime/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, "{}") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }
  }
}
