package org.broadinstitute.dsde.workbench.leonardo.model

import java.time.Instant
import java.util.UUID._

import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData}
import org.broadinstitute.dsde.workbench.leonardo.db.{TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures

import spray.json._


class LeonardoModelSpec extends TestComponent with FlatSpecLike with Matchers with CommonTestData with ScalaFutures {

  val exampleTime = Instant.parse("2018-08-07T10:12:35Z")

  val cluster = makeCluster(1).copy(
    dataprocInfo = makeDataprocInfo(1).copy(
      googleId = Option(fromString("4ba97751-026a-4555-961b-89ae6ce78df4"))),
    auditInfo = auditInfo.copy(createdDate = exampleTime,
      dateAccessed = exampleTime),
    jupyterExtensionUri = Some(jupyterExtensionUri),
    stopAfterCreation = true,
    clusterImages = Set(jupyterImage.copy(timestamp = exampleTime))
  )


  it should "serialize/deserialize to/from JSON" in isolatedDbTest {

    val expectedJson =
      """
        |{ "id": 0,
        |  "clusterName": "clustername1",
        |  "googleId": "4ba97751-026a-4555-961b-89ae6ce78df4",
        |  "googleProject": "dsp-leo-test",
        |  "serviceAccountInfo": {
        |    "clusterServiceAccount": "testClusterServiceAccount@example.com",
        |    "notebookServiceAccount": "testNotebookServiceAccount@example.com"
        |    },
        |  "machineConfig": {
        |    "numberOfWorkers": 0,
        |    "masterMachineType": "",
        |    "masterDiskSize": 500
        |    },
        |  "clusterUrl": "http://leonardo/dsp-leo-test/clustername1",
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
        |    { "tool": "Jupyter",
        |      "dockerImage": "jupyter/jupyter-base:latest",
        |      "timestamp": "2018-08-07T10:12:35Z"
        |      }
        |    ],
        |  "scopes":["https://www.googleapis.com/auth/userinfo.email","https://www.googleapis.com/auth/userinfo.profile","https://www.googleapis.com/auth/bigquery","https://www.googleapis.com/auth/source.read_only"]
        |}
      """.stripMargin.parseJson

    val missingJson =
      """
        |{ "id": 0,
        |  "googleId": "4ba97751-026a-4555-961b-89ae6ce78df4",
        |  "googleProject": "dsp-leo-test",
        |  "serviceAccountInfo": {
        |    "notebookServiceAccount": "pet-1234567890@test-project.iam.gserviceaccount.com"
        |    },
        |  "machineConfig": {
        |    "numberOfWorkers": 0,
        |    "masterMachineType": "",
        |    "masterDiskSize": 500
        |    },
        |  "clusterUrl": "http://leonardo/dsp-leo-test/name1",
        |  "operationName": "op1",
        |  "status": "Unknown",
        |  "hostIp": "numbers.and.dots",
        |  "creator": "user1@example.com",
        |  "createdDate": "2018-08-07T10:12:35Z",
        |  "labels": {
        |     "bam": "yes",
        |     "vcf": "no"
        |     },
        |  "jupyterExtensionUri": "gs://extension_bucket/extension_path",
        |  "jupyterUserScriptUri": "gs://userscript_bucket/userscript.sh",
        |  "stagingBucket": "testStagingBucket1",
        |  "errors": [],
        |  "instances": [],
        |  "dateAccessed": "2018-08-07T10:12:35Z",
        |  "autopauseThreshold": 0,
        |  "stopAfterCreation": false,
        |  "clusterImages": []
        |}
      """.stripMargin.parseJson

    val testJson = cluster.toJson(ClusterFormat)
    testJson should equal (expectedJson)
    
    val returnedCluster = testJson.convertTo[Cluster]
    returnedCluster shouldBe cluster

    // optional and absent field should deserialize to None
    returnedCluster.userJupyterExtensionConfig shouldBe None

    // optional and present field should deserialize to Some(val)
    returnedCluster.jupyterExtensionUri shouldBe Some(jupyterExtensionUri)

    // required and present field should deserialize to val
    returnedCluster.clusterName shouldBe cluster.clusterName

    // required and absent field should throw a DeserializationException
    val caught = intercept[DeserializationException]{
      missingJson.convertTo[Cluster]
    }
    assert(caught.getMessage == "could not deserialize user object")
  }

  it should "create a map of ClusterInitValues object" in isolatedDbTest {

    val clusterInit = ClusterInitValues(project, name1, initBucketPath, testClusterRequestWithExtensionAndScript, dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, Some(serviceAccountKey), userInfo.userEmail, contentSecurityPolicy, Set(jupyterImage))
    val clusterInitMap = clusterInit.toMap

    clusterInitMap("googleProject") shouldBe project.value
    clusterInitMap("clusterName") shouldBe name1.value
    clusterInitMap("jupyterDockerImage") shouldBe jupyterImage.dockerImage
    clusterInitMap("proxyDockerImage") shouldBe proxyConfig.jupyterProxyDockerImage
    clusterInitMap("defaultClientId") shouldBe testClusterRequestWithExtensionAndScript.defaultClientId.getOrElse("")

    clusterInitMap.size shouldBe 25
  }

}
