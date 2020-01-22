package org.broadinstitute.dsde.workbench.leonardo
package model

import java.net.{MalformedURLException, URL}
import java.time.Instant
import java.util.UUID._

import org.broadinstitute.dsde.workbench.leonardo.RoutesTestJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpecLike, Matchers}
import spray.json._
import CommonTestData._

class LeonardoModelSpec extends TestComponent with FlatSpecLike with Matchers with ScalaFutures {

  val exampleTime = Instant.parse("2018-08-07T10:12:35Z")

  val cluster = makeCluster(1).copy(
    dataprocInfo = Some(makeDataprocInfo(1).copy(googleId = fromString("4ba97751-026a-4555-961b-89ae6ce78df4"))),
    auditInfo = auditInfo.copy(createdDate = exampleTime, dateAccessed = exampleTime),
    jupyterExtensionUri = Some(jupyterExtensionUri),
    stopAfterCreation = true,
    allowStop = false,
    clusterImages = Set(jupyterImage.copy(timestamp = exampleTime), welderImage.copy(timestamp = exampleTime)),
    welderEnabled = true
  )

  "ClusterRequestFormat" should "successfully decode json" in isolatedDbTest {
    val inputJson =
      """
        |{
        |}
      """.stripMargin.parseJson

    val expectedClusterRequest = ClusterRequest(labels = Map.empty, properties = Map.empty, scopes = Set.empty)

    val decodeResult = inputJson.convertTo[ClusterRequest]
    decodeResult shouldBe (expectedClusterRequest)
  }

  it should "successfully decode cluster request with null values" in isolatedDbTest {
    val inputJson =
      """
        |{
        |  "defaultClientId": null,
        |  "jupyterDockerImage": null,
        |  "jupyterExtensionUri": null,
        |  "jupyterUserScriptUri": null,
        |  "jupyterStartUserScriptUri": null,
        |  "machineConfig": null,
        |  "rstudioDockerImage": null,
        |  "scopes": null,
        |  "stopAfterCreation": null,
        |  "userJupyterExtensionConfig": null
        |}
      """.stripMargin.parseJson

    val expectedClusterRequest = ClusterRequest(labels = Map.empty, properties = Map.empty, scopes = Set.empty)

    val decodeResult = inputJson.convertTo[ClusterRequest]
    decodeResult shouldBe (expectedClusterRequest)
  }

  it should "successfully decode cluster request with jupyterExtensionUri properly" in isolatedDbTest {
    val inputJson =
      """
        |{
        |  "jupyterExtensionUri": "gs://extension_bucket/extension_path",
        |  "jupyterUserScriptUri": "gs://userscript_bucket/userscript.sh",
        |  "jupyterStartUserScriptUri": "gs://startscript_bucket/startscript.sh",
        |  "labels": {},
        |  "properties": {},
        |  "scopes": [],
        |  "userJupyterExtensionConfig": null
        |}
      """.stripMargin.parseJson

    val expectedClusterRequest = ClusterRequest(
      labels = Map.empty,
      properties = Map.empty,
      scopes = Set.empty,
      jupyterExtensionUri = Some(GcsPath(GcsBucketName("extension_bucket"), GcsObjectName("extension_path"))),
      jupyterUserScriptUri =
        Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("userscript_bucket"), GcsObjectName("userscript.sh")))),
      jupyterStartUserScriptUri =
        Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("startscript_bucket"), GcsObjectName("startscript.sh"))))
    )

    val decodeResult = inputJson.convertTo[ClusterRequest]
    decodeResult shouldBe (expectedClusterRequest)
  }

  it should "serialize/deserialize to/from JSON" in isolatedDbTest {

    val expectedJson =
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
        |    "masterMachineType": "",
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
        |  "allowStop": false,
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
        |  "welderEnabled": true
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
        |  "clusterUrl": "http://leonardo/proxy/dsp-leo-test/name1/jupyter",
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
        |  "jupyterStartUserScriptUri": "gs://startscript_bucket/startscript.sh",
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
    testJson should equal(expectedJson)

    val returnedCluster = testJson.convertTo[Cluster]
    returnedCluster shouldBe cluster

    // optional and absent field should deserialize to None
    returnedCluster.userJupyterExtensionConfig shouldBe None

    // optional and present field should deserialize to Some(val)
    returnedCluster.jupyterExtensionUri shouldBe Some(jupyterExtensionUri)

    // required and present field should deserialize to val
    returnedCluster.clusterName shouldBe cluster.clusterName

    // required and absent field should throw a DeserializationException
    assertThrows[DeserializationException] {
      missingJson.convertTo[Cluster]
    }
  }

  it should "create a map of ClusterTemplateValues object" in isolatedDbTest {
    val clusterInit = ClusterTemplateValues(
      cluster,
      Some(initBucketPath),
      Some(stagingBucketName),
      Some(serviceAccountKey),
      dataprocConfig,
      welderConfig,
      proxyConfig,
      clusterFilesConfig,
      clusterResourcesConfig,
      Some(clusterResourceConstraints)
    )
    val clusterInitMap = clusterInit.toMap

    clusterInitMap("googleProject") shouldBe project.value
    clusterInitMap("clusterName") shouldBe name1.value
    clusterInitMap("jupyterDockerImage") shouldBe jupyterImage.imageUrl
    clusterInitMap("proxyDockerImage") shouldBe proxyConfig.jupyterProxyDockerImage
    clusterInitMap("googleClientId") shouldBe cluster.defaultClientId.getOrElse("")
    clusterInitMap("welderDockerImage") shouldBe welderImage.imageUrl
    clusterInitMap("welderEnabled") shouldBe "true"
    clusterInitMap("memLimit") shouldBe clusterResourceConstraints.memoryLimit.bytes.toString + "b"

    clusterInitMap.size shouldBe 36
  }

  it should "create UserScriptPath objects according to provided path" in isolatedDbTest {
    val gcsPath = "gs://userscript_bucket/userscript.sh"
    val httpPath = "https://userscript_path"
    val invalidPath = "invalid_userscript_path"

    UserScriptPath.stringToUserScriptPath(gcsPath) shouldBe Right(
      UserScriptPath.Gcs(GcsPath(GcsBucketName("userscript_bucket"), GcsObjectName("userscript.sh")))
    )
    UserScriptPath.stringToUserScriptPath(httpPath) shouldBe Right(UserScriptPath.Http(new URL(httpPath)))
    UserScriptPath.stringToUserScriptPath(invalidPath).left.get shouldBe a[MalformedURLException]
  }

  "DockerRegistry regex" should "match expected image url format" in {
    ContainerRegistry.GCR.regex.pattern.asPredicate().test("us.gcr.io/google/ubuntu1804:latest") shouldBe (true)
    ContainerRegistry.GCR.regex.pattern.asPredicate().test("us.gcr.io/broad-dsp-gcr-public/ubuntu1804") shouldBe (true)
    ContainerRegistry.GCR.regex.pattern.asPredicate().test("us/broad-dsp-gcr-public/ubuntu1804") shouldBe (false)
    ContainerRegistry.GCR.regex.pattern.asPredicate().test("eu.gcr.io/broad-dsp-gcr-public/ubuntu1804") shouldBe (true)
    ContainerRegistry.GCR.regex.pattern
      .asPredicate()
      .test("asia.gcr.io/broad-dsp-gcr-public/ubuntu1804") shouldBe (true)
    ContainerRegistry.GCR.regex.pattern
      .asPredicate()
      .test("unknown.gcr.io/broad-dsp-gcr-public/ubuntu1804") shouldBe (false)

    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("asd/asdf") shouldBe (true)
    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("asd") shouldBe (false)
    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("asd_sd_as:asdf") shouldBe (false)
    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("asd/as:asdf") shouldBe (true)
    ContainerRegistry.DockerHub.regex.pattern
      .asPredicate()
      .test("asd_as/as:asdf ") shouldBe (false) //trailing white space
    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("asd_as/as: asdf") shouldBe (false) //white space
    ContainerRegistry.DockerHub.regex.pattern
      .asPredicate()
      .test("myrepo/mydocker; mysql -c \"DROP ALL TABLES\"; sudo rm -rf / ") shouldBe (false)
    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("a///////") shouldBe (false)
  }

  "ContainerImage.stringToJupyterDockerImage" should "match GCR first, and then dockerhub" in {
    ContainerImage.stringToJupyterDockerImage("us.gcr.io/broad-dsp-gcr-public/ubuntu1804") shouldBe (Some(
      ContainerImage.GCR("us.gcr.io/broad-dsp-gcr-public/ubuntu1804")
    ))
    ContainerImage.stringToJupyterDockerImage("asd/asdf") shouldBe (Some(ContainerImage.DockerHub("asd/asdf")))
  }

  "Cluster" should "generate a correct cluster URL" in {
    val expectedBase = s"http://leonardo/proxy/$project/$name0/"

    // No images or labels -> default to Jupyter
    Cluster.getClusterUrl(project, name0, Set.empty, Map.empty).toString shouldBe expectedBase + "jupyter"

    // images only
    Cluster.getClusterUrl(project, name0, Set(jupyterImage), Map.empty).toString shouldBe expectedBase + "jupyter"
    Cluster
      .getClusterUrl(project, name0, Set(welderImage, customDataprocImage, jupyterImage), Map.empty)
      .toString shouldBe expectedBase + "jupyter"
    Cluster.getClusterUrl(project, name0, Set(rstudioImage), Map.empty).toString shouldBe expectedBase + "rstudio"
    Cluster
      .getClusterUrl(project, name0, Set(welderImage, customDataprocImage, rstudioImage), Map.empty)
      .toString shouldBe expectedBase + "rstudio"

    // labels only
    Cluster
      .getClusterUrl(project, name0, Set.empty, Map("tool" -> "Jupyter", "foo" -> "bar"))
      .toString shouldBe expectedBase + "jupyter"
    Cluster
      .getClusterUrl(project, name0, Set.empty, Map("tool" -> "RStudio", "foo" -> "bar"))
      .toString shouldBe expectedBase + "rstudio"
    Cluster.getClusterUrl(project, name0, Set.empty, Map("foo" -> "bar")).toString shouldBe expectedBase + "jupyter"

    // images and labels -> images take precedence
    Cluster
      .getClusterUrl(project,
                     name0,
                     Set(welderImage, customDataprocImage, rstudioImage),
                     Map("tool" -> "Jupyter", "foo" -> "bar"))
      .toString shouldBe expectedBase + "rstudio"
  }
}
