package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.net.URL
import java.time.Instant

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.http.DiskRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.RuntimeRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.api.HttpRoutesSpec._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{contentSecurityPolicy, swaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.api.KubernetesRoutes._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.RuntimeSamResource
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  DeleteRuntimeRequest,
  GetAppResponse,
  GetRuntimeResponse,
  ListAppResponse,
  RuntimeService
}
import org.broadinstitute.dsde.workbench.leonardo.service.{
  BaseMockRuntimeServiceInterp,
  MockDiskServiceInterp,
  MockKubernetesServiceInterp,
  MockRuntimeServiceInterp
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, GoogleProject}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HttpRoutesSpec
    extends AnyFlatSpec
    with ScalatestRouteTest
    with LeonardoTestSuite
    with ScalaFutures
    with Matchers
    with TestComponent
    with TestLeoRoutes {
  val clusterName = "test"
  val googleProject = "dsp-leo-test"

  val routes = new HttpRoutes(
    swaggerConfig,
    statusService,
    proxyService,
    leonardoService,
    MockRuntimeServiceInterp,
    MockDiskServiceInterp,
    MockKubernetesServiceInterp,
    timedUserInfoDirectives,
    contentSecurityPolicy
  )

  "RuntimeRoutes" should "create runtime" in {
    val request = CreateRuntime2Request(
      Map("lbl1" -> "true"),
      None,
      Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket"), GcsObjectName("script.sh")))),
      Some(RuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-standard-4")), Some(DiskSize(100)))),
      None,
      Some(true),
      Some(30.minutes),
      None,
      Some(ContainerImage.DockerHub("myrepo/myimage")),
      Some(ContainerImage.DockerHub("broadinstitute/welder")),
      Set.empty,
      Map.empty
    )
    Post("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, request.asJson.spaces2) ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "create a runtime with default parameters" in {
    Post("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, "{}") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "get a runtime" in {
    Get("/api/google/v1/runtimes/googleProject/runtime1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[GetRuntimeResponse].id shouldBe CommonTestData.testCluster.id
      validateRawCookie(header("Set-Cookie"))
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
      override def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(
        implicit as: ApplicativeAsk[IO, AppContext]
      ): IO[Unit] = IO {
        val expectedDeleteRuntime =
          DeleteRuntimeRequest(timedUserInfo, GoogleProject("googleProject1"), RuntimeName("runtime1"), true)
        deleteRuntimeRequest shouldBe expectedDeleteRuntime
      }
    }
    val routes = fakeRoutes(runtimeService)
    Delete("/api/google/v1/runtimes/googleProject1/runtime1?deleteDisk=true") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "keep disk when deleting runtime if deleteDisk is false" in {
    val runtimeService = new BaseMockRuntimeServiceInterp {
      override def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(
        implicit as: ApplicativeAsk[IO, AppContext]
      ): IO[Unit] = IO {
        val expectedDeleteRuntime =
          DeleteRuntimeRequest(timedUserInfo, GoogleProject("googleProject1"), RuntimeName("runtime1"), false)
        deleteRuntimeRequest shouldBe expectedDeleteRuntime
      }
    }
    val routes = fakeRoutes(runtimeService)
    Delete("/api/google/v1/runtimes/googleProject1/runtime1?deleteDisk=false") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "not delete disk when deleting a runtime with PD enabled if deleteDisk is not set" in {
    val runtimeService = new BaseMockRuntimeServiceInterp {
      override def deleteRuntime(deleteRuntimeRequest: DeleteRuntimeRequest)(
        implicit as: ApplicativeAsk[IO, AppContext]
      ): IO[Unit] = IO {
        val expectedDeleteRuntime =
          DeleteRuntimeRequest(timedUserInfo, GoogleProject("googleProject1"), RuntimeName("runtime1"), false)
        deleteRuntimeRequest shouldBe expectedDeleteRuntime
      }
    }
    val routes = fakeRoutes(runtimeService)
    Delete("/api/google/v1/runtimes/googleProject1/runtime1") ~> routes.route ~> check {
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

  it should "start a runtime" in {
    Post("/api/google/v1/runtimes/googleProject1/runtime1/start") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "update a runtime" in {
    val request = UpdateRuntimeRequest(
      Some(UpdateRuntimeConfigRequest.GceConfig(Some(MachineTypeName("n1-micro-2")), Some(DiskSize(50)))),
      true,
      Some(true),
      Some(5.minutes)
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
      UpdateRuntimeRequest(Some(UpdateRuntimeConfigRequest.GceConfig(None, Some(DiskSize(-100)))), false, None, None)
    Patch("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, negative.asJson.spaces2) ~> routes.route ~> check {
      handled shouldBe false
    }
    val oneWorker =
      UpdateRuntimeRequest(Some(UpdateRuntimeConfigRequest.DataprocConfig(None, None, Some(1), None)),
                           false,
                           None,
                           None)
    Patch("/api/google/v1/runtimes/googleProject1/runtime1")
      .withEntity(ContentTypes.`application/json`, oneWorker.asJson.spaces2) ~> routes.route ~> check {
      handled shouldBe false
    }
  }

  "DiskRoutes" should "create a disk" in {
    val diskCreateRequest = CreateDiskRequest(
      Map("foo" -> "bar"),
      Some(DiskSize(500)),
      Some(DiskType.Standard),
      Some(BlockSize(65536))
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

  "HttpRoutes" should "not handle unrecognized routes" in {
    Post("/api/google/v1/runtime/googleProject1/runtime1/unhandled") ~> routes.route ~> check {
      handled shouldBe false
    }
    Get("/api/google/v1/badruntime/googleProject1/runtime1") ~> routes.route ~> check {
      handled shouldBe false
    }
    Get("/api/google/v2/runtime/googleProject1/runtime1") ~> routes.route ~> check {
      handled shouldBe false
    }
    Post("/api/google/v1/runtime/googleProject1/runtime1") ~> routes.route ~> check {
      handled shouldBe false
    }
    Post("/api/google/v1/disk/googleProject1/disk1") ~> routes.route ~> check {
      handled shouldBe false
    }
    Get("/api/google/v1/disks/googleProject1/disk1/foo") ~> routes.route ~> check {
      handled shouldBe false
    }
  }

  "Kubernetes Routes" should "create an app" in {
    Post("/api/google/v1/app/googleProject1/app1")
      .withEntity(ContentTypes.`application/json`, createAppRequest.asJson.spaces2) ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list apps with project" in {
    Get("/api/google/v1/app/googleProject1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
      responseAs[Vector[ListAppResponse]] shouldBe listAppResponse
    }
  }

  it should "list apps with no project" in {
    Get("/api/google/v1/app/") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "list apps with labels" in {
    Get("/api/google/v1/app?project=foo&creator=bar&includeDeleted=true") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "get app" in {
    Get("/api/google/v1/app/googleProject1/app1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.OK
      validateRawCookie(header("Set-Cookie"))
      responseAs[GetAppResponse] shouldBe getAppResponse
    }
  }

  it should "delete app" in {
    Delete("/api/google/v1/app/googleProject1/app1") ~> routes.route ~> check {
      status shouldEqual StatusCodes.Accepted
      validateRawCookie(header("Set-Cookie"))
    }
  }

  it should "validate app name" in {
    Get("/api/google/v1/app/googleProject1/1badApp") ~> routes.route ~> check {
      status.intValue shouldBe 500
    }
  }

  it should "validate create app request" in {
    Post("/api/google/v1/app/googleProject1/app1")
      .withEntity(
        ContentTypes.`application/json`,
        createAppRequest
          .copy(kubernetesRuntimeConfig =
            createAppRequest.kubernetesRuntimeConfig.map(c => c.copy(numNodes = NumNodes(-1)))
          )
          .asJson
          .spaces2
      ) ~> routes.route ~> check {
      handled shouldBe false
    }
  }

  def fakeRoutes(runtimeService: RuntimeService[IO]): HttpRoutes =
    new HttpRoutes(
      swaggerConfig,
      statusService,
      proxyService,
      leonardoService,
      runtimeService,
      MockDiskServiceInterp,
      MockKubernetesServiceInterp,
      timedUserInfoDirectives,
      contentSecurityPolicy
    )
}

object HttpRoutesSpec {
  implicit val updateGceConfigRequestEncoder: Encoder[UpdateRuntimeConfigRequest.GceConfig] = Encoder.forProduct3(
    "cloudService",
    "machineType",
    "diskSize"
  )(x => (x.cloudService, x.updatedMachineType, x.updatedDiskSize))

  implicit val updateDataprocConfigRequestEncoder: Encoder[UpdateRuntimeConfigRequest.DataprocConfig] =
    Encoder.forProduct5(
      "cloudService",
      "masterMachineType",
      "masterDiskSize",
      "numberOfWorkers",
      "numberOfPreemptibleWorkers"
    )(x =>
      (x.cloudService,
       x.updatedMasterMachineType,
       x.updatedMasterDiskSize,
       x.updatedNumberOfWorkers,
       x.updatedNumberOfPreemptibleWorkers)
    )

  implicit val updateRuntimeConfigRequestEncoder: Encoder[UpdateRuntimeConfigRequest] = Encoder.instance { x =>
    x match {
      case x: UpdateRuntimeConfigRequest.DataprocConfig => x.asJson
      case x: UpdateRuntimeConfigRequest.GceConfig      => x.asJson
    }
  }

  implicit val updateRuntimeRequestEncoder: Encoder[UpdateRuntimeRequest] = Encoder.forProduct4(
    "runtimeConfig",
    "allowStop",
    "autopause",
    "autopauseThreshold"
  )(x =>
    (
      x.updatedRuntimeConfig,
      x.allowStop,
      x.updateAutopauseEnabled,
      x.updateAutopauseThreshold.map(_.toMinutes)
    )
  )

  implicit val updateDiskRequestEncoder: Encoder[UpdateDiskRequest] = Encoder.forProduct2(
    "labels",
    "size"
  )(x =>
    (
      x.labels,
      x.size
    )
  )

  implicit val getClusterResponseDecoder: Decoder[GetRuntimeResponse] = Decoder.instance { x =>
    for {
      id <- x.downField("id").as[Long]
      clusterName <- x.downField("runtimeName").as[RuntimeName]
      googleProject <- x.downField("googleProject").as[GoogleProject]
      serviceAccount <- x.downField("serviceAccount").as[WorkbenchEmail]
      asyncRuntimeFields <- x.downField("asyncRuntimeFields").as[Option[AsyncRuntimeFields]]
      auditInfo <- x.downField("auditInfo").as[AuditInfo]
      kernelFoundBusyDate <- x.downField("kernelFoundBusyDate").as[Option[Instant]]
      runtimeConfig <- x.downField("runtimeConfig").as[RuntimeConfig]
      clusterUrl <- x.downField("proxyUrl").as[URL]
      status <- x.downField("status").as[RuntimeStatus]
      labels <- x.downField("labels").as[LabelMap]
      jupyterUserScriptUri <- x.downField("jupyterUserScriptUri").as[Option[UserScriptPath]]
      jupyterStartUserScriptUri <- x.downField("jupyterStartUserScriptUri").as[Option[UserScriptPath]]
      errors <- x.downField("errors").as[List[RuntimeError]]
      userJupyterExtensionConfig <- x.downField("userJupyterExtensionConfig").as[Option[UserJupyterExtensionConfig]]
      autopauseThreshold <- x.downField("autopauseThreshold").as[Int]
      defaultClientId <- x.downField("defaultClientId").as[Option[String]]
      clusterImages <- x.downField("runtimeImages").as[Set[RuntimeImage]]
      scopes <- x.downField("scopes").as[Set[String]]
    } yield GetRuntimeResponse(
      id,
      RuntimeSamResource(""),
      clusterName,
      googleProject,
      serviceAccount,
      asyncRuntimeFields,
      auditInfo,
      kernelFoundBusyDate,
      runtimeConfig,
      clusterUrl,
      status,
      labels,
      jupyterUserScriptUri,
      jupyterStartUserScriptUri,
      errors,
      Set.empty, // Dataproc instances
      userJupyterExtensionConfig,
      autopauseThreshold,
      defaultClientId,
      false,
      clusterImages,
      scopes,
      true,
      false,
      Map.empty,
      None
    )
  }

  implicit val listClusterResponseDecoder: Decoder[ListRuntimeResponse2] = Decoder.instance { x =>
    for {
      id <- x.downField("id").as[Long]
      clusterName <- x.downField("runtimeName").as[RuntimeName]
      googleProject <- x.downField("googleProject").as[GoogleProject]
      auditInfo <- x.downField("auditInfo").as[AuditInfo]
      machineConfig <- x.downField("runtimeConfig").as[RuntimeConfig]
      clusterUrl <- x.downField("proxyUrl").as[URL]
      status <- x.downField("status").as[RuntimeStatus]
      labels <- x.downField("labels").as[LabelMap]
      patchInProgress <- x.downField("patchInProgress").as[Boolean]
    } yield ListRuntimeResponse2(
      id,
      RuntimeSamResource("fakeId"),
      clusterName,
      googleProject,
      auditInfo,
      machineConfig,
      clusterUrl,
      status,
      labels,
      patchInProgress
    )
  }
}
