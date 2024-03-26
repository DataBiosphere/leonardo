package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import cats.syntax.all._
import io.circe.CursorOp.DownField
import io.circe.DecodingFailure
import io.circe.parser.decode
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec.{
  deleteDefaultLabelsDecodingFailure,
  getRuntimeResponseEncoder,
  negativeNumberDecodingFailure,
  oneWorkerSpecifiedDecodingFailure,
  updateDefaultLabelDecodingFailure,
  upsertEmptyLabelDecodingFailure
}
import org.broadinstitute.dsde.workbench.leonardo.http.api.RuntimeRoutes._
import RuntimeRoutesCodec._
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URL
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration._

class RuntimeRoutesSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite {
  it should "decode RuntimeConfigRequest.DataprocConfig" in {
    val jsonString =
      """
        |{
        |   "cloudService": "dataproc",
        |   "properties": {
        |     "spark:spark.executor.cores": "4"
        |   }
        |}
        |""".stripMargin
    val expectedResult = RuntimeConfigRequest.DataprocConfig(
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      Map("spark:spark.executor.cores" -> "4"),
      None,
      false,
      false
    )
    decode[RuntimeConfigRequest.DataprocConfig](jsonString) shouldBe Right(expectedResult)
  }

  it should "fail to decode MachineConfig when masterMachineType is empty string" in {
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

  it should "successfully decode cluster request with jupyterExtensionUri properly" in {
    val inputJson =
      """
        |{
        |  "jupyterUserScriptUri": "gs://userscript_bucket/userscript.sh",
        |  "jupyterStartUserScriptUri": "gs://startscript_bucket/startscript.sh",
        |  "labels": {"lbl1": "true"},
        |  "scopes": [],
        |  "userJupyterExtensionConfig": {
        |     "nbExtensions": {
        |        "notebookExtension": "gs://extension_bucket/extension_path"
        |     }
        |  }
        |}
      """.stripMargin

    val expectedClusterRequest = defaultCreateRuntimeRequest.copy(
      labels = Map("lbl1" -> "true"),
      scopes = Set.empty,
      runtimeConfig = None,
      userJupyterExtensionConfig = Some(
        UserJupyterExtensionConfig(nbExtensions = Map("notebookExtension" -> "gs://extension_bucket/extension_path"))
      ),
      autopauseThreshold = None,
      autopause = None,
      toolDockerImage = None,
      welderRegistry = None,
      userScriptUri =
        Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("userscript_bucket"), GcsObjectName("userscript.sh")))),
      startUserScriptUri =
        Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("startscript_bucket"), GcsObjectName("startscript.sh"))))
    )

    val decodeResult = for {
      json <- io.circe.parser.parse(inputJson)
      r <- json.as[CreateRuntimeRequest]
    } yield r
    decodeResult shouldBe (Right(expectedClusterRequest))
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
      Map.empty,
      None,
      false,
      false
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
        |      "masterMachineType" : "test-master-machine-type",
        |      "componentGatewayEnabled": true,
        |      "workerPrivateAccess": true
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
      Map.empty,
      None,
      true,
      true
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
        |  "runtimeConfig": {
        |    "cloudService": "dataproc",
        |    "masterMachineType": "n1-standard-4",
        |    "masterDiskSize": 500
        |  },
        |  "userJupyterExtensionConfig": {
        |     "nbExtensions": {
        |        "notebookExtension": "gs://extension_bucket/extension_path"
        |     }
        |  },
        |  "serviceAccountInfo": {
        |    "clusterServiceAccount": "testClusterServiceAccount@example.com",
        |    "notebookServiceAccount": "testNotebookServiceAccount@example.com"
        |    },
        |  "clusterUrl": "http://leonardo/proxy/dsp-leo-test/clustername1/jupyter",
        |  "operationName": "operationName1",
        |  "status": "Unknown",
        |  "hostIp": "numbers.and.dots",
        |  "creator": "user1@example.com",
        |  "createdDate": "2018-08-07T10:12:35Z",
        |  "labels": {},
        |  "stagingBucket": "stagingbucketname1",
        |  "errors": [],
        |  "instances": [],
        |  "dateAccessed": "2018-08-07T10:12:35Z",
        |  "autopauseThreshold": 30,
        |  "defaultClientId": "defaultClientId",
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
          None,
          Some(MachineTypeName("n1-standard-4")),
          Some(DiskSize(500)),
          None,
          None,
          None,
          None,
          Map.empty,
          None,
          false,
          false
        )
      ),
      Some(
        UserJupyterExtensionConfig(nbExtensions = Map("notebookExtension" -> "gs://extension_bucket/extension_path"))
      ),
      None,
      Some(30 minutes),
      Some("defaultClientId"),
      None,
      None,
      Set(
        "https://www.googleapis.com/auth/userinfo.email",
        "https://www.googleapis.com/auth/userinfo.profile",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/source.read_only"
      ),
      Map.empty,
      None
    )
    res shouldBe (Right(expected))
  }

  it should "encode ListRuntimeResponse2" in {
    val date = Instant.parse("2020-11-20T17:23:24.650Z")
    val workspaceId: UUID = UUID.randomUUID()
    val input = ListRuntimeResponse2(
      -1,
      Some(WorkspaceId(workspaceId)),
      runtimeSamResource,
      name1,
      cloudContextGcp,
      auditInfo.copy(createdDate = date, dateAccessed = date),
      gceRuntimeConfigWithGpu,
      new URL("https://leo.org/proxy"),
      RuntimeStatus.Running,
      Map("foo" -> "bar"),
      true
    )

    val res = input.asJson.spaces2
    res shouldBe
      s"""{
         |  "id" : -1,
         |  "workspaceId" : "${workspaceId.toString}",
         |  "runtimeName" : "clustername1",
         |  "googleProject" : "dsp-leo-test",
         |  "cloudContext" : {
         |    "cloudProvider" : "GCP",
         |    "cloudResource" : "dsp-leo-test"
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
         |    "bootDiskSize" : 50,
         |    "zone" : "us-west2-b",
         |    "gpuConfig" : {
         |      "gpuType" : "nvidia-tesla-t4",
         |      "numOfGpus" : 2
         |    },
         |    "configType" : "GceConfig"
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
      cloudContextGcp,
      serviceAccountEmail,
      Some(makeAsyncRuntimeFields(1).copy(proxyHostName = ProxyHostName(uuid.toString))),
      auditInfo.copy(createdDate = date, dateAccessed = date),
      Some(date),
      defaultGceRuntimeConfig,
      new URL("https://leo.org/proxy"),
      RuntimeStatus.Running,
      Map("foo" -> "bar"),
      Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket-name"), GcsObjectName("userScript")))),
      Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("bucket-name"), GcsObjectName("startScript")))),
      List.empty[RuntimeError],
      None,
      30,
      Some("clientId"),
      Set(jupyterImage, welderImage, proxyImage, sfkitImage, cryptoDetectorImage).map(_.copy(timestamp = date)),
      defaultScopes,
      true,
      true,
      Map("ev1" -> "a", "ev2" -> "b"),
      Some(DiskConfig(DiskName("disk"), DiskSize(100), DiskType.Standard, BlockSize(1024)))
    )

    val res = input.asJson.deepDropNullValues.spaces2
    res shouldBe
      """{
        |  "id" : -1,
        |  "runtimeName" : "clustername1",
        |  "googleProject" : "dsp-leo-test",
        |  "cloudContext" : {
        |    "cloudProvider" : "GCP",
        |    "cloudResource" : "dsp-leo-test"
        |  },
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
        |    "dateAccessed" : "2020-11-20T17:23:24.650Z"
        |  },
        |  "runtimeConfig" : {
        |    "machineType" : "n1-standard-4",
        |    "diskSize" : 500,
        |    "cloudService" : "GCE",
        |    "bootDiskSize" : 50,
        |    "zone" : "us-west2-b",
        |    "configType" : "GceConfig"
        |  },
        |  "proxyUrl" : "https://leo.org/proxy",
        |  "status" : "Running",
        |  "labels" : {
        |    "foo" : "bar"
        |  },
        |  "userScriptUri" : "gs://bucket-name/userScript",
        |  "startUserScriptUri" : "gs://bucket-name/startScript",
        |  "jupyterUserScriptUri" : "gs://bucket-name/userScript",
        |  "jupyterStartUserScriptUri" : "gs://bucket-name/startScript",
        |  "errors" : [
        |  ],
        |  "autopauseThreshold" : 30,
        |  "defaultClientId" : "clientId",
        |  "runtimeImages" : [
        |    {
        |      "imageType" : "Jupyter",
        |      "imageUrl" : "init-resources/jupyter-base:latest",
        |      "homeDirectory" : "/home/jupyter",
        |      "timestamp" : "2020-11-20T17:23:24.650Z"
        |    },
        |    {
        |      "imageType" : "sfkit",
        |      "imageUrl" : "us-central1-docker.pkg.dev/dsp-artifact-registry/sfkit/sfkit",
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

  it should "decode RuntimeConfigRequest.GceWithPdConfig correctly" in {
    val jsonString =
      """
        |{
        |  "cloudService": "gce",
        |  "persistentDisk": {
        |    "name": "qi-disk-c1",
        |    "size": 200
        |  }
        |}
        |""".stripMargin
    val expectedResult = RuntimeConfigRequest.GceWithPdConfig(
      None,
      PersistentDiskRequest(DiskName("qi-disk-c1"), Some(DiskSize(200)), None, Map.empty),
      None,
      None
    )
    decode[RuntimeConfigRequest](jsonString) shouldBe Right(expectedResult)
  }

  it should "decode RuntimeConfigRequest.GceConfig correctly" in {
    val jsonString =
      """
        |{
        |  "cloudService": "gce"
        |}
        |""".stripMargin
    val expectedResult = RuntimeConfigRequest.GceConfig(
      None,
      None,
      None,
      None
    )
    decode[RuntimeConfigRequest](jsonString) shouldBe Right(expectedResult)
  }

  it should "decode RuntimeConfigRequest correctly" in {
    val jsonString =
      """
        |{
        |  "cloudService": "gce",
        |  "persistentDisk": {
        |    "name": "qi-disk-c1",
        |    "size": 30
        |  }
        |}
        |""".stripMargin
    val expectedResult = RuntimeConfigRequest.GceWithPdConfig(
      None,
      PersistentDiskRequest(DiskName("qi-disk-c1"), Some(DiskSize(30)), None, Map.empty),
      None,
      None
    )
    decode[RuntimeConfigRequest](jsonString) shouldBe Right(expectedResult)
  }

  it should "decode UpdateRuntimeRequest correctly" in {
    val jsonString =
      """
        |{
        |  "allowStop": true,
        |  "labelsToUpsert": {
        |  "new_label" : "label_val"
        |  }
        |}
        |""".stripMargin
    val expectedResult = UpdateRuntimeRequest(None, true, None, None, Map("new_label" -> "label_val"), Set.empty)
    decode[UpdateRuntimeRequest](jsonString) shouldBe Right(expectedResult)
  }

  it should "decode UpdateRuntimeRequest should fail due to empty label value" in {
    val jsonString =
      """
        |{
        |  "allowStop": true,
        |  "labelsToUpsert": {
        |  "new_label" : "label_val",
        |  "bad_label" : ""
        |  }
        |}
        |""".stripMargin
    decode[UpdateRuntimeRequest](jsonString) shouldBe Left(upsertEmptyLabelDecodingFailure)
  }

  it should "decode UpdateRuntimeRequest should fail due to trying to alter default label" in {
    val jsonString =
      """
        |{
        |  "allowStop": true,
        |  "labelsToUpsert": {
        |  "googleProject" : "label_val"
        |  }
        |}
        |""".stripMargin
    decode[UpdateRuntimeRequest](jsonString) shouldBe Left(updateDefaultLabelDecodingFailure)
  }

  it should "decode UpdateRuntimeRequest should fail due to trying to delete default label" in {
    val jsonString =
      """
        |{
        |  "allowStop": true,
        |  "labelsToUpsert": {
        |  "new_label" : "label_val"
        |  },
        |  "labelsToDelete": ["googleProject"]
        |}
        |""".stripMargin
    decode[UpdateRuntimeRequest](jsonString) shouldBe Left(deleteDefaultLabelsDecodingFailure)
  }

  it should "decode empty CreateRuntime2Request correctly" in {
    val jsonString =
      """
        |{}
        |""".stripMargin
    val expectedResult = CreateRuntimeRequest(
      Map.empty,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      Set.empty,
      Map.empty,
      None
    )
    decode[CreateRuntimeRequest](jsonString) shouldBe Right(expectedResult)
  }

  it should "decode CreateRuntime2Request correctly" in {
    val jsonString =
      """
        |{
        |  "runtimeConfig": {
        |    "cloudService": "gce",
        |    "machineType": "n1-standard-4",
        |    "diskSize": 100,
        |    "zone": "us-central2-b"
        |  }
        |}
        |""".stripMargin
    val expectedResult = CreateRuntimeRequest(
      Map.empty,
      None,
      None,
      Some(
        RuntimeConfigRequest.GceConfig(
          Some(MachineTypeName("n1-standard-4")),
          Some(DiskSize(100)),
          Some(ZoneName("us-central2-b")),
          None
        )
      ),
      None,
      None,
      None,
      None,
      None,
      None,
      Set.empty,
      Map.empty,
      None
    )
    decode[CreateRuntimeRequest](jsonString) shouldBe Right(expectedResult)
  }
}
