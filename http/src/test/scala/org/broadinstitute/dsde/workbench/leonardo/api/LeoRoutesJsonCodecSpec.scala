package org.broadinstitute.dsde.workbench.leonardo
package api

import io.circe.parser.decode
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec.{
  emptyMasterMachineType,
  negativeNumberDecodingFailure,
  oneWorkerSpecifiedDecodingFailure
}
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutesJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateRuntimeRequest, RuntimeConfigRequest}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath}
import org.scalatest.{FlatSpec, Matchers}

class LeoRoutesJsonCodecSpec extends FlatSpec with Matchers {
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
    decodeResult shouldBe Left(emptyMasterMachineType)
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

  it should "fail with negativeNumberDecodingFailure when masterDiskSize is negative" in {
    val inputString =
      """
        |{
        |   "numberOfWorkers": 10,
        |   "masterMachineType": "n1-standard-8",
        |   "masterDiskSize": -1
        |}
        |""".stripMargin

    val decodeResult = for {
      json <- io.circe.parser.parse(inputString)
      r <- json.as[RuntimeConfigRequest.DataprocConfig]
    } yield r
    decodeResult shouldBe Left(negativeNumberDecodingFailure)
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
      None
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
      Some(100),
      Some(0),
      Some(0)
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
      Some(GcsPath(GcsBucketName("extension_bucket"), GcsObjectName("extension_path"))),
      None,
      None,
      Some(
        RuntimeConfigRequest.DataprocConfig(
          Some(0),
          Some(MachineTypeName("machineType")),
          Some(500)
        )
      ),
      Map.empty,
      Some(true),
      false,
      None,
      None,
      Some(30),
      Some("defaultClientId"),
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
}
