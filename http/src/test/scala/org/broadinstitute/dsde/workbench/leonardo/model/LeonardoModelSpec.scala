package org.broadinstitute.dsde.workbench.leonardo
package model

import java.net.{MalformedURLException, URL}
import java.time.Instant
import java.util.UUID._

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutesJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.service.CreateRuntimeRequest
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath}
import org.scalatest.FlatSpecLike

class LeonardoModelSpec extends LeonardoTestSuite with FlatSpecLike {

  val exampleTime = Instant.parse("2018-08-07T10:12:35Z")

  val cluster = makeCluster(1).copy(
    asyncRuntimeFields = Some(makeDataprocInfo(1).copy(googleId = fromString("4ba97751-026a-4555-961b-89ae6ce78df4"))),
    auditInfo = auditInfo.copy(createdDate = exampleTime, dateAccessed = exampleTime),
    jupyterExtensionUri = Some(jupyterExtensionUri),
    stopAfterCreation = true,
    allowStop = false,
    runtimeImages = Set(jupyterImage.copy(timestamp = exampleTime), welderImage.copy(timestamp = exampleTime)),
    welderEnabled = true
  )

  "ClusterRequestFormat" should "successfully decode json" in {
    val inputJson =
      """
        |{
        |}
      """.stripMargin

    val expectedClusterRequest =
      CreateRuntimeRequest(labels = Map.empty, dataprocProperties = Map.empty, scopes = Set.empty)

    val decodeResult = for {
      json <- io.circe.parser.parse(inputJson)
      r <- json.as[CreateRuntimeRequest]
    } yield r
    decodeResult shouldBe (Right(expectedClusterRequest))
  }

  it should "successfully decode cluster request with null values" in {
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
      """.stripMargin

    val expectedClusterRequest =
      CreateRuntimeRequest(labels = Map.empty, dataprocProperties = Map.empty, scopes = Set.empty)

    val decodeResult = for {
      json <- io.circe.parser.parse(inputJson)
      r <- json.as[CreateRuntimeRequest]
    } yield r
    decodeResult shouldBe (Right(expectedClusterRequest))
  }

  it should "successfully decode cluster request with jupyterExtensionUri properly" in {
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
      """.stripMargin

    val expectedClusterRequest = CreateRuntimeRequest(
      labels = Map.empty,
      dataprocProperties = Map.empty,
      scopes = Set.empty,
      jupyterExtensionUri = Some(GcsPath(GcsBucketName("extension_bucket"), GcsObjectName("extension_path"))),
      jupyterUserScriptUri =
        Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("userscript_bucket"), GcsObjectName("userscript.sh")))),
      jupyterStartUserScriptUri =
        Some(UserScriptPath.Gcs(GcsPath(GcsBucketName("startscript_bucket"), GcsObjectName("startscript.sh"))))
    )

    val decodeResult = for {
      json <- io.circe.parser.parse(inputJson)
      r <- json.as[CreateRuntimeRequest]
    } yield r
    decodeResult shouldBe (Right(expectedClusterRequest))
  }

//  it should "serialize/deserialize to/from JSON" in {
//
//    val expectedJson =
//      """
//        |{ "id": 0,
//        |  "internalId": "067e2867-5d4a-47f3-a53c-fd711529b287",
//        |  "clusterName": "clustername1",
//        |  "googleId": "4ba97751-026a-4555-961b-89ae6ce78df4",
//        |  "googleProject": "dsp-leo-test",
//        |  "serviceAccountInfo": {
//        |    "clusterServiceAccount": "testClusterServiceAccount@example.com",
//        |    "notebookServiceAccount": "testNotebookServiceAccount@example.com"
//        |    },
//        |  "machineConfig": {
//        |    "numberOfWorkers": 0,
//        |    "masterMachineType": "",
//        |    "masterDiskSize": 500
//        |    },
//        |  "clusterUrl": "http://leonardo/proxy/dsp-leo-test/clustername1/jupyter",
//        |  "operationName": "operationName1",
//        |  "status": "Unknown",
//        |  "hostIp": "numbers.and.dots",
//        |  "creator": "user1@example.com",
//        |  "createdDate": "2018-08-07T10:12:35Z",
//        |  "labels": {},
//        |  "jupyterExtensionUri": "gs://extension_bucket/extension_path",
//        |  "stagingBucket": "stagingbucketname1",
//        |  "errors": [],
//        |  "instances": [],
//        |  "dateAccessed": "2018-08-07T10:12:35Z",
//        |  "autopauseThreshold": 30,
//        |  "defaultClientId": "defaultClientId",
//        |  "stopAfterCreation": true,
//        |  "clusterImages": [
//        |    { "imageType": "Jupyter",
//        |      "imageUrl": "jupyter/jupyter-base:latest",
//        |      "timestamp": "2018-08-07T10:12:35Z"
//        |      },
//        |    { "imageType": "Welder",
//        |      "imageUrl": "welder/welder:latest",
//        |      "timestamp": "2018-08-07T10:12:35Z"
//        |      }
//        |    ],
//        |  "scopes":["https://www.googleapis.com/auth/userinfo.email","https://www.googleapis.com/auth/userinfo.profile","https://www.googleapis.com/auth/bigquery","https://www.googleapis.com/auth/source.read_only"],
//        |  "welderEnabled": true
//        |}
//      """.stripMargin.parseJson
//
//    val missingJson =
//      """
//        |{ "id": 0,
//        |  "googleId": "4ba97751-026a-4555-961b-89ae6ce78df4",
//        |  "googleProject": "dsp-leo-test",
//        |  "serviceAccountInfo": {
//        |    "notebookServiceAccount": "pet-1234567890@test-project.iam.gserviceaccount.com"
//        |    },
//        |  "machineConfig": {
//        |    "numberOfWorkers": 0,
//        |    "masterMachineType": "",
//        |    "masterDiskSize": 500
//        |    },
//        |  "clusterUrl": "http://leonardo/proxy/dsp-leo-test/name1/jupyter",
//        |  "operationName": "op1",
//        |  "status": "Unknown",
//        |  "hostIp": "numbers.and.dots",
//        |  "creator": "user1@example.com",
//        |  "createdDate": "2018-08-07T10:12:35Z",
//        |  "labels": {
//        |     "bam": "yes",
//        |     "vcf": "no"
//        |     },
//        |  "jupyterExtensionUri": "gs://extension_bucket/extension_path",
//        |  "jupyterUserScriptUri": "gs://userscript_bucket/userscript.sh",
//        |  "jupyterStartUserScriptUri": "gs://startscript_bucket/startscript.sh",
//        |  "stagingBucket": "testStagingBucket1",
//        |  "errors": [],
//        |  "instances": [],
//        |  "dateAccessed": "2018-08-07T10:12:35Z",
//        |  "autopauseThreshold": 0,
//        |  "stopAfterCreation": false,
//        |  "clusterImages": []
//        |}
//      """.stripMargin.parseJson
//
//    val testJson = cluster.toJson(ClusterFormat)
//    testJson should equal(expectedJson)
//
//    val returnedCluster = testJson.convertTo[Cluster]
//    returnedCluster shouldBe cluster
//
//    // optional and absent field should deserialize to None
//    returnedCluster.userJupyterExtensionConfig shouldBe None
//
//    // optional and present field should deserialize to Some(val)
//    returnedCluster.jupyterExtensionUri shouldBe Some(jupyterExtensionUri)
//
//    // required and present field should deserialize to val
//    returnedCluster.clusterName shouldBe cluster.clusterName
//
//    // required and absent field should throw a DeserializationException
//    assertThrows[DeserializationException] {
//      missingJson.convertTo[Cluster]
//    }
//  }

  // TODO make a RuntimeTemplateValuesSpec
//  it should "create a map of ClusterTemplateValues object" in {
//    val clusterInit = ClusterTemplateValues(
//      cluster,
//      Some(initBucketPath),
//      Some(stagingBucketName),
//      Some(serviceAccountKey),
//      dataprocConfig,
//      welderConfig,
//      proxyConfig,
//      clusterFilesConfig,
//      clusterResourcesConfig,
//      Some(clusterResourceConstraints)
//    )
//    val clusterInitMap = clusterInit.toMap
//
//    clusterInitMap("googleProject") shouldBe project.value
//    clusterInitMap("clusterName") shouldBe name1.value
//    clusterInitMap("jupyterDockerImage") shouldBe jupyterImage.imageUrl
//    clusterInitMap("proxyDockerImage") shouldBe proxyConfig.jupyterProxyDockerImage
//    clusterInitMap("googleClientId") shouldBe cluster.defaultClientId.getOrElse("")
//    clusterInitMap("welderDockerImage") shouldBe welderImage.imageUrl
//    clusterInitMap("welderEnabled") shouldBe "true"
//    clusterInitMap("memLimit") shouldBe clusterResourceConstraints.memoryLimit.bytes.toString + "b"
//
//    clusterInitMap.size shouldBe 36
//  }

  it should "create UserScriptPath objects according to provided path" in {
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
      .test("asia.gcr.io/broad-dsp-gcr-public/ubuntu1804") shouldBe true
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
    val expectedBase = s"https://leo/proxy/${project.value}/${name0.asString}/"

    // No images or labels -> default to Jupyter
    Runtime
      .getProxyUrl(proxyUrlBase, project, name0, Set.empty, Map.empty)
      .toString shouldBe expectedBase + "jupyter"

    // images only
    Runtime
      .getProxyUrl(proxyUrlBase, project, name0, Set(jupyterImage), Map.empty)
      .toString shouldBe expectedBase + "jupyter"
    Runtime
      .getProxyUrl(proxyUrlBase, project, name0, Set(welderImage, customDataprocImage, jupyterImage), Map.empty)
      .toString shouldBe expectedBase + "jupyter"
    Runtime
      .getProxyUrl(proxyUrlBase, project, name0, Set(rstudioImage), Map.empty)
      .toString shouldBe expectedBase + "rstudio"
    Runtime
      .getProxyUrl(proxyUrlBase, project, name0, Set(welderImage, customDataprocImage, rstudioImage), Map.empty)
      .toString shouldBe expectedBase + "rstudio"

    // labels only
    Runtime
      .getProxyUrl(proxyUrlBase, project, name0, Set.empty, Map("tool" -> "Jupyter", "foo" -> "bar"))
      .toString shouldBe expectedBase + "jupyter"
    Runtime
      .getProxyUrl(proxyUrlBase, project, name0, Set.empty, Map("tool" -> "RStudio", "foo" -> "bar"))
      .toString shouldBe expectedBase + "rstudio"
    Runtime
      .getProxyUrl(proxyUrlBase, project, name0, Set.empty, Map("foo" -> "bar"))
      .toString shouldBe expectedBase + "jupyter"

    // images and labels -> images take precedence
    Runtime
      .getProxyUrl(proxyUrlBase,
                   project,
                   name0,
                   Set(welderImage, customDataprocImage, rstudioImage),
                   Map("tool" -> "Jupyter", "foo" -> "bar"))
      .toString shouldBe expectedBase + "rstudio"
  }
}
