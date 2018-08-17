package org.broadinstitute.dsde.workbench.leonardo.model

import java.time.Instant
import java.util.UUID._

import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData}
import org.broadinstitute.dsde.workbench.leonardo.db.{TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures

import spray.json._


class LeonardoModelSpec extends TestComponent with FlatSpecLike with Matchers with CommonTestData with ScalaFutures {

  val exampleTime = Instant.parse("2018-08-07T10:12:35Z")

  val cluster = Cluster(
    clusterName = name1,
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
    dataprocInfo = DataprocInfo(Option(fromString("4ba97751-026a-4555-961b-89ae6ce78df4")), Option(OperationName("op1")), Some(GcsBucketName("testStagingBucket1")), Some(IP("numbers.and.dots"))),
    auditInfo = AuditInfo(userEmail, exampleTime, None, exampleTime),
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
    status = ClusterStatus.Unknown,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = Some(jupyterExtensionUri),
    jupyterUserScriptUri = Some(jupyterUserScriptUri),
    errors = List.empty,
    instances = Set.empty,
    userJupyterExtensionConfig = None,
    autopauseThreshold = 0,
    defaultClientId = None)


  it should "serialize/deserialize to/from JSON" in isolatedDbTest {

    val expectedJson =
      """
        |{ "id": 0,
        |  "clusterName": "name1",
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
        |  "autopauseThreshold": 0
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
        |  "autopauseThreshold": 0
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
    returnedCluster.clusterName shouldBe name1

    // required and absent field should throw a DeserializationException
    val caught = intercept[DeserializationException]{
      missingJson.convertTo[Cluster]
    }
    assert(caught.getMessage == "could not deserialize user object")
  }

  it should "create a map of ClusterInitValues object" in isolatedDbTest {

    val clusterInit = ClusterInitValues(project, name1, initBucketPath, testClusterRequestWithExtensionAndScript, dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, Some(serviceAccountKey), userInfo.userEmail, contentSecurityPolicy)
    val clusterInitMap = clusterInit.toMap

    clusterInitMap("googleProject") shouldBe project.value
    clusterInitMap("clusterName") shouldBe name1.value
    clusterInitMap("jupyterDockerImage") shouldBe dataprocConfig.dataprocDockerImage
    clusterInitMap("proxyDockerImage") shouldBe proxyConfig.jupyterProxyDockerImage
    clusterInitMap("defaultClientId") shouldBe testClusterRequestWithExtensionAndScript.defaultClientId.getOrElse("")

    clusterInitMap.size shouldBe 23
  }

}
