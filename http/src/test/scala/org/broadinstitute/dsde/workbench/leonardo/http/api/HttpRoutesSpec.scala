package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.mtl.Ask
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.Decoder
import io.circe.parser.decode
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.RefererConfig
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.http.AdminRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.AppRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.DiskRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.RuntimeRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.api.RuntimeRoutes._
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.util.ServicesRegistry
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{ErrorReport, ErrorReportSource, UserInfo}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._

/**
 * Tests the full(ish) logical flow of calls to various Leonardo API endpoints.
 * NOTE that you may have to run the whole spec, not individual tests,
 * to get the expected results.
 */
class HttpRoutesSpec
    extends AnyFlatSpec
    with ScalatestRouteTest
    with LeonardoTestSuite
    with ScalaFutures
    with Matchers
    with TestComponent
    with TestLeoRoutes
    with MockitoSugar {
  val clusterName = "test"
  val googleProject = "dsp-leo-test"

  def createGcpOnlyServicesRegistry() = {
    val registry = ServicesRegistry()
    registry.register[ProxyService](proxyService)
    registry.register[RuntimeService[IO]](MockRuntimeServiceInterp)
    registry.register[DiskService[IO]](MockDiskServiceInterp)
    registry.register[ResourcesService[IO]](MockResourcesService)
    registry
  }

  val routes =
    new HttpRoutes(
      openIdConnectionConfiguration,
      statusService,
      createGcpOnlyServicesRegistry(),
      MockDiskV2ServiceInterp,
      MockAppService,
      new MockRuntimeV2Interp,
      MockAdminServiceInterp,
      timedUserInfoDirectives,
      contentSecurityPolicy,
      refererConfig
    )

  val httpRoutesAzureOnly = new HttpRoutes(
    openIdConnectionConfiguration,
    statusService,
    createGcpOnlyServicesRegistry(),
    MockDiskV2ServiceInterp,
    MockAppService,
    new MockRuntimeV2Interp,
    MockAdminServiceInterp,
    timedUserInfoDirectives,
    contentSecurityPolicy,
    refererConfig,
    true
  )

  val routesWithStrictRefererConfig =
    new HttpRoutes(
      openIdConnectionConfiguration,
      statusService,
      createGcpOnlyServicesRegistry(),
      MockDiskV2ServiceInterp,
      MockAppService,
      new MockRuntimeV2Interp,
      MockAdminServiceInterp,
      timedUserInfoDirectives,
      contentSecurityPolicy,
      RefererConfig(Set("bvdp-saturn-dev.appspot.com/"), true)
    )

  val routesWithWildcardReferer =
    new HttpRoutes(
      openIdConnectionConfiguration,
      statusService,
      createGcpOnlyServicesRegistry(),
      MockDiskV2ServiceInterp,
      MockAppService,
      new MockRuntimeV2Interp,
      MockAdminServiceInterp,
      timedUserInfoDirectives,
      contentSecurityPolicy,
      RefererConfig(Set("*", "bvdp-saturn-dev.appspot.com/"), true)
    )

  val routesWithDisabledRefererConfig =
    new HttpRoutes(
      openIdConnectionConfiguration,
      statusService,
      createGcpOnlyServicesRegistry(),
      MockDiskV2ServiceInterp,
      MockAppService,
      new MockRuntimeV2Interp,
      MockAdminServiceInterp,
      timedUserInfoDirectives,
      contentSecurityPolicy,
      RefererConfig(Set.empty, false)
    )

  implicit val errorReportDecoder: Decoder[ErrorReport] = Decoder.instance { h =>
    for {
      message <- h.downField("message").as[String]
    } yield ErrorReport(message)(ErrorReportSource("leonardo"))
  }

  "RuntimeRoutes" should "create runtime id1" in {
    Post("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`,
                  defaultCreateRuntimeRequest.asJson.spaces2
      ) ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "reject if saturn-iframe-extension is invalid" in {
    val req = defaultCreateRuntimeRequest.copy(userJupyterExtensionConfig =
      Some(UserJupyterExtensionConfig(Map("saturn-iframe-extension" -> "random"), Map.empty, Map.empty, Map.empty))
    )
    val validReq = defaultCreateRuntimeRequest.copy(userJupyterExtensionConfig =
      Some(
        UserJupyterExtensionConfig(
          Map("saturn-iframe-extension" -> s"https://bvdp-saturn-dev.appspot.com/jupyter-iframe-extension.js"),
          Map.empty,
          Map.empty,
          Map.empty
        )
      )
    )

    // Fail with saturn-iframe-extension outside of strict referer allowlist
    Post("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, req.asJson.spaces2) ~> routesWithStrictRefererConfig.route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseAs[ErrorReport].message.contains("Invalid `saturn-iframe-extension`") shouldBe true
    }

    // Succeed with saturn-iframe-extension in strict allowlist
    Post("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`,
                  validReq.asJson.spaces2
      ) ~> routesWithStrictRefererConfig.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }

    // Succeed with permissive allowlist (has wildcard *)
    Post("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, req.asJson.spaces2) ~> routesWithWildcardReferer.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }

    // Succeed with disabled allowlist
    Post("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`,
                  req.asJson.spaces2
      ) ~> routesWithDisabledRefererConfig.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }
  }

  it should "create a runtime with default parameters" in {
    Post("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, "{}") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should s"reject create a cluster if cluster name is invalid" in {
    val invalidClusterName = "MyCluster"
    Post(s"/api/google/v1/runtimes/googleProject1/$invalidClusterName",
         defaultCreateRuntimeRequest.asJson.spaces2
    ) ~> httpRoutes.route ~> check {
      val expectedResponse =
        """Invalid name MyCluster. Only lowercase alphanumeric characters, numbers and dashes are allowed in leo names"""
      responseEntity.toStrict(5 seconds).futureValue.data.utf8String shouldBe expectedResponse

      status shouldEqual StatusCodes.BadRequest
    }
  }

  it should "get a runtime" in {
    Get("/api/google/v1/runtimes/googleProject/runtime1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[GetRuntimeResponse].id shouldBe CommonTestData.testCluster.id
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "return 404 when getting a non-existent runtime" in {
    Get("/api/google/v1/runtimes/googleProject/runtime-non-existent") ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "list runtimes with a project" in {
    Get("/api/google/v1/runtimes/googleProject") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Vector[ListRuntimeResponse2]].map(_.id) shouldBe Vector(CommonTestData.testCluster.id)
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list runtimes without a project" in {
    Get("/api/google/v1/runtimes") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      val response = responseAs[Vector[ListRuntimeResponse2]]
      response.map(_.id) shouldBe Vector(CommonTestData.testCluster.id)
      response.map(_.runtimeConfig.asInstanceOf[RuntimeConfig.GceConfig]) shouldBe Vector(
        CommonTestData.defaultGceRuntimeConfig
      )
      validateRawCookie(header("Set-Cookie"))
    }
  }

  // Tests only parameter parsing, not service logic.
  it should "list runtimes with labels" in isolatedDbTest {
    def runtimesWithLabels(i: Int) =
      defaultCreateRuntimeRequest
        .copy(startUserScriptUri = None)
        .copy(labels = Map(s"label$i" -> s"value$i"))

    for (i <- 1 to 10)
      Post(s"/api/google/v1/runtimes/${googleProject}/${clusterName}-$i",
           runtimesWithLabels(i).asJson
      ) ~> httpRoutes.route ~> check {
        status shouldEqual StatusCodes.Accepted
      }

    Get("/api/google/v1/runtimes?label6=value6") ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
    }

    Get("/api/google/v1/runtimes?_labels=label4%3Dvalue4") ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
    }

    Get("/api/google/v1/runtimes?_labels=bad") ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.BadRequest
    }
  }

  it should "list runtimes with parameters" in {
    Get("/api/google/v1/runtimes?project=foo&creator=bar") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Vector[ListRuntimeResponse2]].map(_.id) shouldBe Vector(CommonTestData.testCluster.id)
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "delete a runtime" in {
    Delete("/api/google/v1/runtimes/googleProject1/runtime1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "delete a runtime and disk if deleteDisk is true" in {
    val runtimeService = new BaseMockRuntimeServiceInterp {
      override def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(implicit
        as: Ask[IO, AppContext]
      ): IO[Unit] = IO {
        val expectedDeleteRuntime =
          DeleteRuntimeRequest(timedUserInfo, GoogleProject("googleProject1"), RuntimeName("runtime1"), true)
        deleteRuntimeRequest shouldBe expectedDeleteRuntime
      }
    }
    Delete("/api/google/v1/runtimes/googleProject1/runtime1?deleteDisk=true") ~> fakeRoutes(
      runtimeService
    ).route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "return 404 when deleting a non-existent runtime" in {
    Delete("/api/google/v1/runtimes/googleProject/runtime-non-existent") ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "keep disk when deleting runtime if deleteDisk is false" in {
    val runtimeService = new BaseMockRuntimeServiceInterp {
      override def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(implicit
        as: Ask[IO, AppContext]
      ): IO[Unit] = IO {
        val expectedDeleteRuntime =
          DeleteRuntimeRequest(timedUserInfo, GoogleProject("googleProject1"), RuntimeName("runtime1"), false)
        deleteRuntimeRequest shouldBe expectedDeleteRuntime
      }
    }
    Delete("/api/google/v1/runtimes/googleProject1/runtime1?deleteDisk=false") ~> fakeRoutes(
      runtimeService
    ).route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }

  }

  it should "not delete disk when deleting a runtime with PD enabled if deleteDisk is not set" in {
    val runtimeService = new BaseMockRuntimeServiceInterp {
      override def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(implicit
        as: Ask[IO, AppContext]
      ): IO[Unit] = IO {
        val expectedDeleteRuntime =
          DeleteRuntimeRequest(timedUserInfo, GoogleProject("googleProject1"), RuntimeName("runtime1"), false)
        deleteRuntimeRequest shouldBe expectedDeleteRuntime
      }
    }
    Delete("/api/google/v1/runtimes/googleProject1/runtime1") ~> fakeRoutes(runtimeService).route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "not delete disk when deleting a kubernetes app with PD enabled if deleteDisk is not set" in {
    val kubernetesService = new MockAppService {
      override def deleteApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName, deleteDisk: Boolean)(
        implicit as: Ask[IO, AppContext]
      ): IO[Unit] = IO {
        cloudContext shouldBe CloudContext.Gcp(GoogleProject("googleProject1"))
        appName shouldBe AppName("app1")
        deleteDisk shouldBe false
      }
    }
    Delete("/api/google/v1/apps/googleProject1/app1") ~> fakeRoutes(kubernetesService).route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "stop a runtime" in {
    Post("/api/google/v1/runtimes/googleProject1/runtime1/stop") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "return 404 when stopping a non-existent runtime" in {
    Post("/api/google/v1/runtimes/googleProject1/runtime-non-existent/stop") ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "start a runtime" in {
    Post("/api/google/v1/runtimes/googleProject1/runtime1/start") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "return 404 when starting a non-existent runtime" in {
    Post("/api/google/v1/runtimes/googleProject1/runtime-non-existent/start") ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "update a runtime" in {
    val request = UpdateRuntimeRequest(
      Some(UpdateRuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-micro-2")), Some(DiskSize(50)))),
      true,
      Some(true),
      Some(5.minutes),
      Map.empty,
      Set.empty
    )
    Patch("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, request.asJson.spaces2) ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "update a runtime with default parameters" in {
    Patch("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, "{}") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "not handle patch with invalid runtime config" in {
    val negative =
      UpdateRuntimeRequest(Some(UpdateRuntimeConfigRequest.GceConfig(None, Some(DiskSize(-100)))),
                           false,
                           None,
                           None,
                           Map.empty,
                           Set.empty
      )
    Patch("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, negative.asJson.spaces2) ~> routes.route ~> check {
      status shouldBe StatusCodes.BadRequest
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "The request content was malformed:\nDecodingFailure at .runtimeConfig.diskSize: Minimum required disk size is 5GB"
    }
    val oneWorker =
      UpdateRuntimeRequest(Some(UpdateRuntimeConfigRequest.DataprocConfig(None, None, Some(1), None)),
                           false,
                           None,
                           None,
                           Map.empty,
                           Set.empty
      )
    Patch("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, oneWorker.asJson.spaces2) ~> routes.route ~> check {
      status shouldBe StatusCodes.BadRequest
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "The request content was malformed:\nDecodingFailure at : Google Dataproc does not support clusters with 1 non-preemptible worker. Must be 0, 2 or more."
    }
  }

  "RuntimeRoutesV2" should "create azure runtime" in {
    Post(s"/api/v2/runtimes/${workspaceId.value.toString}/azure/azureruntime1")
      .withEntity(ContentTypes.`application/json`,
                  defaultCreateAzureRuntimeReq.asJson.spaces2
      ) ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "reject azure runtime with invalid workspaceId" in {
    Post(s"/api/v2/runtimes/invalidWorkspaceId/azure/azureruntime1")
      .withEntity(ContentTypes.`application/json`,
                  defaultCreateAzureRuntimeReq.asJson.spaces2
      ) ~> routes.route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseEntity.toStrict(5 seconds).futureValue.data.utf8String should include(
        "Invalid workspace id invalidWorkspaceId, workspace id must be a valid UUID"
      )
    }
  }

  it should "reject azure runtime with invalid runtimeName" in {
    Post(s"/api/v2/runtimes/${workspaceId.value.toString}/azure/invalidRuntime")
      .withEntity(ContentTypes.`application/json`,
                  defaultCreateAzureRuntimeReq.asJson.spaces2
      ) ~> routes.route ~> check {
      status shouldEqual StatusCodes.BadRequest
      responseEntity.toStrict(5 seconds).futureValue.data.utf8String should include(
        "Invalid runtime name invalidRuntime"
      )
    }
  }

  it should "get an azure runtime" in {
    Get(s"/api/v2/runtimes/${workspaceId.value.toString}/azure/azureruntime1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[GetRuntimeResponse].clusterName shouldBe RuntimeName("azureruntime1")
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "404 on get azure runtime when it does not exist" in {
    Get(s"/api/v2/runtimes/${workspaceId.value.toString}/azure/fakeruntime") ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "delete an azure runtime" in {
    Delete(s"/api/v2/runtimes/${workspaceId.value.toString}/azure/azureruntime1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }
  }

  it should "404 on delete azure runtime when it does not exist" in {
    Delete(s"/api/v2/runtimes/${workspaceId.value.toString}/azure/azureruntime1") ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "list runtimes v2 with a workspace" in {
    Get(s"/api/v2/runtimes/${workspaceId.value.toString}") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Vector[ListRuntimeResponse2]].map(_.clusterName) shouldBe Vector(RuntimeName("azureruntime1"))
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list runtimes v2 without a workspace or cloudContext" in {
    Get("/api/v2/runtimes") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      val response = responseAs[Vector[ListRuntimeResponse2]]
      response.map(_.clusterName) shouldBe Vector(RuntimeName("azureruntime1"))
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list runtimes v2 with a workspace and cloud context" in {
    Get(s"/api/v2/runtimes/${workspaceId.value.toString}/azure") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      val response = responseAs[Vector[ListRuntimeResponse2]]
      response.map(_.clusterName) shouldBe Vector(RuntimeName("azureruntime1"))
      validateRawCookie(header("Set-Cookie"))
    }
  }

  // Tests only parameter parsing, not service logic.
  it should "list runtimes v2 with labels" in isolatedDbTest {
    Get(
      s"/api/v2/runtimes/${workspaceId.value.toString}/azure?foo=bar&includeDeleted=true"
    ) ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseClusters = responseAs[List[ListRuntimeResponse2]]
      responseClusters should have size 1
      validateRawCookie(header("Set-Cookie"))
    }

    Get(s"/api/v2/runtimes/${workspaceId.value.toString}/azure?_labels=foo%3Dbar") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK

      val responseClusters = responseAs[List[ListRuntimeResponse2]]
      responseClusters should have size 1
    }

    Get(s"/api/v2/runtimes/${workspaceId.value.toString}/azure?_labels=bad") ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.BadRequest
    }
  }

  it should "list runtimes v2 with parameters" in {
    Get(s"/api/v2/runtimes/${workspaceId.value.toString}/azure?project=foo&creator=bar") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Vector[ListRuntimeResponse2]].map(_.clusterName) shouldBe Vector(RuntimeName("azureruntime1"))
      validateRawCookie(header("Set-Cookie"))
    }
  }

  "DiskRoutes" should "create a disk" in {
    val diskCreateRequest = CreateDiskRequest(
      Map("foo" -> "bar"),
      Some(DiskSize(500)),
      Some(DiskType.Standard),
      Some(BlockSize(65536)),
      None,
      None
    )
    Post("/api/google/v1/disks/googleProject1/disk1")
      .withEntity(ContentTypes.`application/json`, diskCreateRequest.asJson.spaces2) ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "create a disk with default parameters" in {
    Post("/api/google/v1/disks/googleProject1/disk1")
      .withEntity(ContentTypes.`application/json`, "{}") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "get a disk" in {
    Get("/api/google/v1/disks/googleProject/disk1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[GetPersistentDiskResponse].name shouldBe CommonTestData.diskName
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list all disks" in {
    Get("/api/google/v1/disks") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Vector[ListPersistentDiskResponse]].map(_.name) shouldBe Vector(CommonTestData.diskName)
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list all disks within a project" in {
    Get("/api/google/v1/disks/googleProject") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Vector[ListPersistentDiskResponse]].map(_.name) shouldBe Vector(CommonTestData.diskName)
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list disks with parameters" in {
    Get("/api/google/v1/disks?project=foo&creator=bar") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Vector[ListPersistentDiskResponse]].map(_.name) shouldBe Vector(CommonTestData.diskName)
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "delete a disk" in {
    Delete("/api/google/v1/disks/googleProject1/disk1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "update a disk" in {
    val request = UpdateDiskRequest(
      Map("foo" -> "bar"),
      DiskSize(1024)
    )
    Patch("/api/google/v1/disks/googleProject1/disk1", request.asJson) ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "get a disk v2" in {
    Get(s"/api/v2/disks/-1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[GetPersistentDiskV2Response].name shouldBe CommonTestData.diskName
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "delete a disk v2" in {
    Delete(s"/api/v2/disks/-1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "reject get a disk if id is invalid" in {
    Get(s"/api/v2/disks/diskid") ~> routes.route ~> check {
      val expectedResponse =
        """Invalid workspace id workspaceId, workspace id must be a valid UUID"""
      responseEntity.toStrict(5 seconds).futureValue.data.utf8String.contains(expectedResponse)
      status shouldEqual StatusCodes.InternalServerError
    }
  }

  "HttpRoutes" should "not handle unrecognized routes" in {
    Post("/api/google/v1/runtime/googleProject1/runtime1/unhandled") ~> routes.route ~> check {
      status shouldBe StatusCodes.NotFound
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "\"API not found. Make sure you're calling the correct endpoint with correct method\""
    }
    Get("/api/google/v1/badruntime/googleProject1/runtime1") ~> routes.route ~> check {
      status shouldBe StatusCodes.NotFound
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "\"API not found. Make sure you're calling the correct endpoint with correct method\""
    }
    Get("/api/google/v2/runtime/googleProject1/runtime1") ~> routes.route ~> check {
      status shouldBe StatusCodes.NotFound
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "\"API not found. Make sure you're calling the correct endpoint with correct method\""
    }
    Post("/api/google/v1/runtime/googleProject1/runtime1") ~> routes.route ~> check {
      status shouldBe StatusCodes.NotFound
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "\"API not found. Make sure you're calling the correct endpoint with correct method\""
    }
    Post("/api/google/v1/disk/googleProject1/disk1") ~> routes.route ~> check {
      status shouldBe StatusCodes.NotFound
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "\"API not found. Make sure you're calling the correct endpoint with correct method\""
    }
    Get("/api/google/v1/disks/googleProject1/disk1/foo") ~> routes.route ~> check {
      status shouldBe StatusCodes.NotFound
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "\"API not found. Make sure you're calling the correct endpoint with correct method\""
    }
    Get(s"/api/disks/v2/${workspaceId.value.toString}/disk1") ~> routes.route ~> check {
      status shouldBe StatusCodes.NotFound
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "\"API not found. Make sure you're calling the correct endpoint with correct method\""
    }
  }

  it should "have expected azure routes when azure hosting mode is true" in {

    val adminRoute = "/api/admin/v2/apps/update"
    val appsV2Route = s"/api/apps/v2/${workspaceId.value.toString}/app1"
    val diskV2Route = "/api/v2/disks/-1"
    val runtimeV2Route = "/api/v2/runtimes"
    val statusRoute = "/status"

    Get(adminRoute) ~> httpRoutesAzureOnly.route ~> check {
      status should not be StatusCodes.NotFound
    }

    Get(appsV2Route) ~> httpRoutesAzureOnly.route ~> check {
      status should not be StatusCodes.NotFound
    }

    Get(diskV2Route) ~> httpRoutesAzureOnly.route ~> check {
      status should not be StatusCodes.NotFound
    }

    Get(runtimeV2Route) ~> httpRoutesAzureOnly.route ~> check {
      status should not be StatusCodes.NotFound
    }

    Get(statusRoute) ~> httpRoutesAzureOnly.route ~> check {
      status should not be StatusCodes.NotFound
    }
  }

  "Kubernetes Routes" should "create an app" in {
    Post("/api/google/v1/apps/googleProject1/app1")
      .withEntity(ContentTypes.`application/json`, createAppRequest.asJson.spaces2) ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list apps with project" in {
    Get("/api/google/v1/apps/googleProject1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
      val response = responseAs[Vector[ListAppResponse]]
      response shouldBe listAppResponse
    }
  }

  it should "list apps with no project" in {
    Get("/api/google/v1/apps/") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list apps with labels" in {
    Get("/api/google/v1/apps?project=foo&creator=bar&includeDeleted=true") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "get app" in {
    Get("/api/google/v1/apps/googleProject1/app1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
      responseAs[GetAppResponse] shouldBe getAppResponse
    }
  }

  it should "delete app" in {
    Delete("/api/google/v1/apps/googleProject1/app1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "validate app name" in {
    Get("/api/google/v1/apps/googleProject1/1badApp") ~> routes.route ~> check {
      status.intValue shouldBe 400
    }
  }

  it should "validate create app request" in {
    Post("/api/google/v1/apps/googleProject1/app1")
      .withEntity(
        ContentTypes.`application/json`,
        createAppRequest
          .copy(kubernetesRuntimeConfig =
            createAppRequest.kubernetesRuntimeConfig.map(c => c.copy(numNodes = NumNodes(-1)))
          )
          .asJson
          .spaces2
      ) ~> httpRoutes.route ~> check {
      status shouldBe StatusCodes.BadRequest
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "The request content was malformed:\nDecodingFailure at .kubernetesRuntimeConfig.numNodes: Minimum number of nodes is 1"
    }
  }

  it should "stop an app" in {
    Post("/api/google/v1/apps/googleProject1/app1/stop") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "start an app" in {
    Post("/api/google/v1/apps/googleProject1/app1/start") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "create an app V2" in {
    Post(s"/api/apps/v2/${workspaceId.value.toString}/app1")
      .withEntity(ContentTypes.`application/json`,
                  createAppRequest.copy(accessScope = Some(AppAccessScope.WorkspaceShared)).asJson.spaces2
      ) ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list apps v2 with project" in {
    Get(s"/api/apps/v2/${workspaceId.value.toString}") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
      val response = responseAs[Vector[ListAppResponse]]
      response shouldBe listAppResponse
    }
  }

  it should "get app V2" in {
    Get(s"/api/apps/v2/${workspaceId.value.toString}/app1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
      responseAs[GetAppResponse] shouldBe getAppResponse
    }
  }

  it should "delete app V2" in {
    Delete(s"/api/apps/v2/${workspaceId.value.toString}/app1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "validate create appV2 request" in {
    Post(s"/api/apps/v2/${workspaceId.value.toString}/app1")
      .withEntity(
        ContentTypes.`application/json`,
        createAppRequest
          .copy(kubernetesRuntimeConfig =
            createAppRequest.kubernetesRuntimeConfig.map(c => c.copy(numNodes = NumNodes(-1)))
          )
          .asJson
          .spaces2
      ) ~> httpRoutes.route ~> check {
      status shouldBe StatusCodes.BadRequest
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "The request content was malformed:\nDecodingFailure at .kubernetesRuntimeConfig.numNodes: Minimum number of nodes is 1"
    }
  }

  it should "run a basic app update request" in {
    Post(s"/api/admin/v2/apps/update")
      .withEntity(
        ContentTypes.`application/json`,
        UpdateAppsRequest(None,
                          AppType.Galaxy,
                          CloudProvider.Gcp,
                          List.empty,
                          List.empty,
                          None,
                          None,
                          List.empty,
                          dryRun = false
        ).asJson.spaces2
      ) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
    }
  }

  it should "run a dry run app update request" in {
    Post(s"/api/admin/v2/apps/update")
      .withEntity(
        ContentTypes.`application/json`,
        UpdateAppsRequest(None,
                          AppType.Galaxy,
                          CloudProvider.Gcp,
                          List.empty,
                          List.empty,
                          None,
                          None,
                          List.empty,
                          dryRun = true
        ).asJson.spaces2
      ) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
    }
  }

  it should "run a basic delete all resources request" in {
    Delete(
      "/api/google/v1/resources/googleProject1/deleteAll?deleteDisk=false"
    ) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "run a basic cleanup all resources request" in {
    Delete(
      "/api/google/v1/resources/googleProject1/cleanupAll"
    ) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
    }
  }

  // Validate encoding/decoding of RuntimeConfigRequest types
  // TODO should these decoders move to JsonCodec in core module?

  it should "decode and encode RuntimeConfigRequest.GceConfig" in {
    val test =
      RuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-standard-8")),
                                     Some(DiskSize(100)),
                                     Some(ZoneName("europe-west1-b")),
                                     None
      )
    decode[RuntimeConfigRequest](test.asJson.noSpaces) shouldBe Right(test)
  }

  it should "decode and encode RuntimeConfigRequest.GceWithPdConfig" in {
    val test = RuntimeConfigRequest.GceWithPdConfig(
      Some(MachineTypeName("n1-standard-8")),
      PersistentDiskRequest(DiskName("disk"), Some(DiskSize(100)), Some(DiskType.Standard), Map.empty),
      Some(ZoneName("europe-west1-b")),
      None
    )
    decode[RuntimeConfigRequest](test.asJson.noSpaces) shouldBe Right(test)
  }

  it should "decode and encode RuntimeConfigRequest.DataprocConfig" in {
    val test = RuntimeConfigRequest.DataprocConfig(
      Some(100),
      Some(MachineTypeName("n1-standard-16")),
      Some(DiskSize(500)),
      Some(MachineTypeName("n1-highmem-4")),
      Some(DiskSize(100)),
      Some(0),
      Some(100),
      Map.empty,
      Some(RegionName("europe-west1")),
      true,
      true
    )
    decode[RuntimeConfigRequest](test.asJson.noSpaces) shouldBe Right(test)
  }

  def fakeRoutes(runtimeService: RuntimeService[IO]): HttpRoutes = {
    val gcpOnlyServicesRegistry = ServicesRegistry()
    gcpOnlyServicesRegistry.register[ProxyService](proxyService)
    gcpOnlyServicesRegistry.register[RuntimeService[IO]](runtimeService)
    gcpOnlyServicesRegistry.register[DiskService[IO]](MockDiskServiceInterp)
    gcpOnlyServicesRegistry.register[ResourcesService[IO]](MockResourcesService)

    new HttpRoutes(
      openIdConnectionConfiguration,
      statusService,
      gcpOnlyServicesRegistry,
      MockDiskV2ServiceInterp,
      MockAppService,
      runtimev2Service,
      MockAdminServiceInterp,
      timedUserInfoDirectives,
      contentSecurityPolicy,
      refererConfig
    )
  }

  def fakeRoutes(kubernetesService: AppService[IO]): HttpRoutes = {
    val gcpOnlyServicesRegistry = ServicesRegistry()
    gcpOnlyServicesRegistry.register[ProxyService](proxyService)
    gcpOnlyServicesRegistry.register[RuntimeService[IO]](runtimeService)
    gcpOnlyServicesRegistry.register[DiskService[IO]](MockDiskServiceInterp)
    gcpOnlyServicesRegistry.register[ResourcesService[IO]](MockResourcesService)

    new HttpRoutes(
      openIdConnectionConfiguration,
      statusService,
      gcpOnlyServicesRegistry,
      MockDiskV2ServiceInterp,
      kubernetesService,
      runtimev2Service,
      MockAdminServiceInterp,
      timedUserInfoDirectives,
      contentSecurityPolicy,
      refererConfig
    )
  }
}
