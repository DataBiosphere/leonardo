package org.broadinstitute.dsde.workbench.leonardo
package api

import java.net.URL
import java.time.Instant
import java.util.UUID

import cats.syntax.all._
import io.circe.CursorOp.DownField
import io.circe.DecodingFailure
import io.circe.parser.decode
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{auditInfo, _}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec.{
  negativeNumberDecodingFailure,
  oneWorkerSpecifiedDecodingFailure
}
import org.broadinstitute.dsde.workbench.leonardo.http.{DiskConfig, RuntimeConfigRequest}
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutesJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.api.ListRuntimeResponse2
import org.broadinstitute.dsde.workbench.leonardo.http.api.RuntimeRoutes.{
  getRuntimeResponseEncoder,
  listRuntimeResponseEncoder
}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateRuntimeRequest, GetRuntimeResponse}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LeoRoutesJsonCodecSpec extends AnyFlatSpec with Matchers {
  "JsonCodec" should "fail to decode MachineConfig when masterMachineType is empty string" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 0,
        |   "masterMachineType": "",
        |   "masterDiskSize": 500
        |}
        |""".stripMargin

    val decodeResult = for {
      json <- io.circe.parser.parse(inputString)
      r <- json.as[RuntimeConfigRequest.DataprocConfig]
    } yield r
    decodeResult shouldBe Left(
      DecodingFailure("machine type cannot be an empty string", List(DownField("masterMachineType")))
    )
  }

  it should "fail with negativeNumberDecodingFailure when numberOfPreemptibleWorkers is negative" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 10,
        |   "masterMachineType": "n1-standard-8",
        |   "masterDiskSize": 500,
        |   "numberOfPreemptibleWorkers": -1
        |}
        |""".stripMargin

    val decodeResult = for {
      json <- io.circe.parser.parse(inputString)
      r <- json.as[RuntimeConfigRequest.DataprocConfig]
    } yield r
    decodeResult shouldBe Left(negativeNumberDecodingFailure)
  }

  it should "fail with minimumDiskSizeDecodingFailure when masterDiskSize is negative" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 10,
        |   "masterMachineType": "n1-standard-8",
        |   "masterDiskSize": 30
        |}
        |""".stripMargin

    val decodeResult = for {
      json <- io.circe.parser.parse(inputString)
      r <- json.as[RuntimeConfigRequest.DataprocConfig]
    } yield r
    decodeResult.leftMap(_.getMessage) shouldBe Left("Minimum required masterDiskSize is 50GB")
  }

  it should "fail with oneWorkerSpecifiedDecodingFailure when numberOfPreemptibleWorkers is negative" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 1,
        |   "masterMachineType": "n1-standard-8",
        |   "masterDiskSize": 500
        |}
        |""".stripMargin

    val decodeResult = for {
      json <- io.circe.parser.parse(inputString)
      r <- json.as[RuntimeConfigRequest.DataprocConfig]
    } yield r
    decodeResult shouldBe Left(oneWorkerSpecifiedDecodingFailure)
  }

  it should "successfully decode DataprocConfig and ignore worker configs when numberOfWorkers is 0" in {
    val inputString =
      """
        |{
        |      "numberOfWorkers" : 0,
        |      "workerMachineType" : "test-worker-machine-type",
        |      "workerDiskSize" : 100,
        |      "numberOfWorkerLocalSSDs" : 0,
        |      "service" : "DATAPROC",
        |      "numberOfPreemptibleWorkers" : 0,
        |      "masterMachineType" : "test-master-machine-type"
        |}
        |""".stripMargin

    val decodeResult = for {
      json <- io.circe.parser.parse(inputString)
      r <- json.as[RuntimeConfigRequest.DataprocConfig]
    } yield r
    val expectedRuntimeConfig = RuntimeConfigRequest.DataprocConfig(
      Some(0),
      Some(MachineTypeName("test-master-machine-type")),
      None,
      None,
      None,
      None,
      None,
      Map.empty
    )
    decodeResult shouldBe Right(expectedRuntimeConfig)
  }

  it should "successfully decode DataprocConfig" in {
    val inputString =
      """
        |{
        |      "numberOfWorkers" : 3,
        |      "workerMachineType" : "test-worker-machine-type",
        |      "workerDiskSize" : 100,
        |      "numberOfWorkerLocalSSDs" : 0,
        |      "service" : "DATAPROC",
        |      "numberOfPreemptibleWorkers" : 0,
        |      "masterMachineType" : "test-master-machine-type"
        |}
        |""".stripMargin

    val decodeResult = for {
      json <- io.circe.parser.parse(inputString)
      r <- json.as[RuntimeConfigRequest.DataprocConfig]
    } yield r
    val expectedRuntimeConfig = RuntimeConfigRequest.DataprocConfig(
      Some(3),
      Some(MachineTypeName("test-master-machine-type")),
      None,
      Some(MachineTypeName("test-worker-machine-type")),
      Some(DiskSize(100)),
      Some(0),
      Some(0),
      Map.empty
    )
    decodeResult shouldBe Right(expectedRuntimeConfig)
  }

  it should "decode CreateClusterRequest properly" in {
    val inputString =
      """
        |{ "id": 0,
        |  "internalId": "067e2867-5d4a-47f3-a53c-fd711529b287",
        |  "clusterName": "clustername1",
        |  "googleId": "4ba97751-026a-4555-961b-89ae6ce78df4",
        |  "googleProject": "dsp-leo-test",
        |  "serviceAccountInfo": {
        |    "clusterServiceAccount": "testClusterServiceAccount@example.com",
        |    "notebookServiceAccount": "testNotebookServiceAccount@example.com"
        |    },
        |  "machineConfig": {
        |    "numberOfWorkers": 0,
        |    "masterMachineType": "machineType",
        |    "masterDiskSize": 500
        |    },
        |  "clusterUrl": "http://leonardo/proxy/dsp-leo-test/clustername1/jupyter",
        |  "operationName": "operationName1",
        |  "status": "Unknown",
        |  "hostIp": "numbers.and.dots",
        |  "creator": "user1@example.com",
        |  "createdDate": "2018-08-07T10:12:35Z",
        |  "labels": {},
        |  "jupyterExtensionUri": "gs://extension_bucket/extension_path",
        |  "stagingBucket": "stagingbucketname1",
        |  "errors": [],
        |  "instances": [],
        |  "dateAccessed": "2018-08-07T10:12:35Z",
        |  "autopauseThreshold": 30,
        |  "defaultClientId": "defaultClientId",
        |  "stopAfterCreation": true,
        |  "clusterImages": [
        |    { "imageType": "Jupyter",
        |      "imageUrl": "jupyter/jupyter-base:latest",
        |      "timestamp": "2018-08-07T10:12:35Z"
        |      },
        |    { "imageType": "Welder",
        |      "imageUrl": "welder/welder:latest",
        |      "timestamp": "2018-08-07T10:12:35Z"
        |      }
        |    ],
        |  "scopes":["https://www.googleapis.com/auth/userinfo.email","https://www.googleapis.com/auth/userinfo.profile","https://www.googleapis.com/auth/bigquery","https://www.googleapis.com/auth/source.read_only"],
        |  "enableWelder": true
        |}
          """.stripMargin

    val res = decode[CreateRuntimeRequest](inputString)
    val expected = CreateRuntimeRequest(
      Map.empty,
      None,
      None,
      Some(
        RuntimeConfigRequest.DataprocConfig(
          Some(0),
          Some(MachineTypeName("machineType")),
          Some(DiskSize(500)),
          None,
          None,
          None,
          None,
          Map.empty
        )
      ),
      Some(true),
      false,
      Some(
        UserJupyterExtensionConfig(nbExtensions = Map("notebookExtension" -> "gs://extension_bucket/extension_path"))
      ),
      None,
      Some(30),
      Some("defaultClientId"),
      None,
      None,
      None,
      None,
      Set(
        "https://www.googleapis.com/auth/userinfo.email",
        "https://www.googleapis.com/auth/userinfo.profile",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/source.read_only"
      ),
      Some(true),
      Map.empty
    )
    res shouldBe (Right(expected))

  }

  it should "encode ListRuntimeResponse2" in {
    val date = Instant.parse("2020-11-20T17:23:24.650Z")
    val input = ListRuntimeResponse2(
      -1,
      runtimeSamResource,
      name1,
      project,
      auditInfo.copy(createdDate = date, dateAccessed = date),
      defaultGceRuntimeConfig,
      new URL("https://leo.org/proxy"),
      RuntimeStatus.Running,
      Map("foo" -> "bar"),
      true
    )

    val res = input.asJson.spaces2
    res shouldBe
      """{
        |  "id" : -1,
        |  "runtimeName" : "clustername1",
        |  "googleProject" : "dsp-leo-test",
        |  "auditInfo" : {
        |    "creator" : "user1@example.com",
        |    "createdDate" : "2020-11-20T17:23:24.650Z",
        |    "destroyedDate" : null,
        |    "dateAccessed" : "2020-11-20T17:23:24.650Z"
        |  },
        |  "runtimeConfig" : {
        |    "machineType" : "n1-standard-4",
        |    "diskSize" : 500,
        |    "cloudService" : "GCE",
        |    "bootDiskSize" : 50
        |  },
        |  "proxyUrl" : "https://leo.org/proxy",
        |  "status" : "Running",
        |  "labels" : {
        |    "foo" : "bar"
        |  },
        |  "patchInProgress" : true
        |}""".stripMargin
  }

  it should "encode GetRuntimeResponse" in {
    val date = Instant.parse("2020-11-20T17:23:24.650Z")
    val uuid = UUID.fromString("65bc3f6d-a413-4cbe-88f8-b15ed9694543")
    val input = GetRuntimeResponse(
      -1,
      runtimeSamResource,
      name1,
      project,
      serviceAccountEmail,
      Some(makeAsyncRuntimeFields(1).copy(googleId = GoogleId(uuid.toString))),
      auditInfo.copy(createdDate = date, dateAccessed = date),
      Some(date),
      defaultGceRuntimeConfig,
      new URL("https://leo.org/proxy"),
      RuntimeStatus.Running,
      Map("foo" -> "bar"),
      Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket-name"), GcsObjectName("userScript")))),
      Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket-name"), GcsObjectName("startScript")))),
      List.empty,
      Set.empty,
      None,
      30,
      Some("clientId"),
      false,
      Set(jupyterImage, welderImage, proxyImage, cryptoDetectorImage).map(_.copy(timestamp = date)),
      defaultScopes,
      true,
      true,
      Map("ev1" -> "a", "ev2" -> "b"),
      Some(DiskConfig(DiskName("disk"), DiskSize(100), DiskType.Standard, BlockSize(1024)))
    )

    val res = input.asJson.spaces2
    res shouldBe
      """{
        |  "id" : -1,
        |  "runtimeName" : "clustername1",
        |  "googleProject" : "dsp-leo-test",
        |  "serviceAccount" : "pet-1234567890@test-project.iam.gserviceaccount.com",
        |  "asyncRuntimeFields" : {
        |    "googleId" : "65bc3f6d-a413-4cbe-88f8-b15ed9694543",
        |    "operationName" : "operationName1",
        |    "stagingBucket" : "stagingbucketname1",
        |    "hostIp" : "numbers.and.dots"
        |  },
        |  "auditInfo" : {
        |    "creator" : "user1@example.com",
        |    "createdDate" : "2020-11-20T17:23:24.650Z",
        |    "destroyedDate" : null,
        |    "dateAccessed" : "2020-11-20T17:23:24.650Z"
        |  },
        |  "runtimeConfig" : {
        |    "machineType" : "n1-standard-4",
        |    "diskSize" : 500,
        |    "cloudService" : "GCE",
        |    "bootDiskSize" : 50
        |  },
        |  "proxyUrl" : "https://leo.org/proxy",
        |  "status" : "Running",
        |  "labels" : {
        |    "foo" : "bar"
        |  },
        |  "jupyterUserScriptUri" : "gs://bucket-name/userScript",
        |  "jupyterStartUserScriptUri" : "gs://bucket-name/startScript",
        |  "errors" : [
        |  ],
        |  "userJupyterExtensionConfig" : null,
        |  "autopauseThreshold" : 30,
        |  "defaultClientId" : "clientId",
        |  "runtimeImages" : [
        |    {
        |      "imageType" : "Jupyter",
        |      "imageUrl" : "init-resources/jupyter-base:latest",
        |      "timestamp" : "2020-11-20T17:23:24.650Z"
        |    },
        |    {
        |      "imageType" : "Welder",
        |      "imageUrl" : "welder/welder:latest",
        |      "timestamp" : "2020-11-20T17:23:24.650Z"
        |    },
        |    {
        |      "imageType" : "Proxy",
        |      "imageUrl" : "testproxyrepo/test",
        |      "timestamp" : "2020-11-20T17:23:24.650Z"
        |    },
        |    {
        |      "imageType" : "CryptoDetector",
        |      "imageUrl" : "crypto/crypto:0.0.1",
        |      "timestamp" : "2020-11-20T17:23:24.650Z"
        |    }
        |  ],
        |  "scopes" : [
        |    "https://www.googleapis.com/auth/userinfo.email",
        |    "https://www.googleapis.com/auth/userinfo.profile",
        |    "https://www.googleapis.com/auth/bigquery",
        |    "https://www.googleapis.com/auth/source.read_only"
        |  ],
        |  "customEnvironmentVariables" : {
        |    "ev1" : "a",
        |    "ev2" : "b"
        |  },
        |  "diskConfig" : {
        |    "name" : "disk",
        |    "size" : 100,
        |    "diskType" : "pd-standard",
        |    "blockSize" : 1024
        |  },
        |  "patchInProgress" : true
        |}""".stripMargin
  }
}
