package org.broadinstitute.dsde.workbench.leonardo
package model

import org.broadinstitute.dsde.workbench.azure.{AzureCloudContext, ManagedResourceGroupName, SubscriptionId, TenantId}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath}
import org.http4s.ParseFailure
import org.scalatest.flatspec.AnyFlatSpecLike

import java.net.{MalformedURLException, URL}
import java.time.Instant

class LeonardoModelSpec extends LeonardoTestSuite with AnyFlatSpecLike {
  val exampleTime = Instant.parse("2018-08-07T10:12:35Z")

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
    val httpPath = "https://www.mytesthttppath.com"
    val invalidPath = "invalid_userscript_path"
    val maliciousPath = "https://url.com|nslookup http://another-url.com"
    val maliciousGcsPath = "gs://userscript_bucket/userscript.sh | nslookup http://another-url.com"

    UserScriptPath.stringToUserScriptPath(gcsPath) shouldBe Right(
      UserScriptPath.Gcs(GcsPath(GcsBucketName("userscript_bucket"), GcsObjectName("userscript.sh")))
    )
    UserScriptPath.stringToUserScriptPath(httpPath) shouldBe Right(UserScriptPath.Http(new URL(httpPath)))
    UserScriptPath.stringToUserScriptPath(invalidPath).swap.toOption.get shouldBe a[MalformedURLException]
    UserScriptPath.stringToUserScriptPath(maliciousPath).swap.toOption.get shouldBe a[ParseFailure]
    // Note: no exception for below assertion.
    // However, when checking the bucket for the file, app creation will fail due to Google validation.
    UserScriptPath.stringToUserScriptPath(maliciousGcsPath) shouldBe Right(
      UserScriptPath.Gcs(
        GcsPath(GcsBucketName("userscript_bucket"), GcsObjectName("userscript.sh | nslookup http://another-url.com"))
      )
    )
  }

  "DockerRegistry regex" should "match expected image url format" in {
    ContainerRegistry.GCR.regex.pattern.asPredicate().test("us.gcr.io/google/ubuntu1804:latest") shouldBe true
    ContainerRegistry.GCR.regex.pattern.asPredicate().test("us.gcr.io/broad-dsp-gcr-public/ubuntu1804") shouldBe true
    ContainerRegistry.GCR.regex.pattern.asPredicate().test("us/broad-dsp-gcr-public/ubuntu1804") shouldBe false
    ContainerRegistry.GCR.regex.pattern.asPredicate().test("eu.gcr.io/broad-dsp-gcr-public/ubuntu1804") shouldBe true
    ContainerRegistry.GCR.regex.pattern
      .asPredicate()
      .test("asia.gcr.io/broad-dsp-gcr-public/ubuntu1804") shouldBe true
    ContainerRegistry.GCR.regex.pattern
      .asPredicate()
      .test("unknown.gcr.io/broad-dsp-gcr-public/ubuntu1804") shouldBe false

    ContainerRegistry.GAR.regex.pattern
      .asPredicate()
      .test("us-docker.pkg.dev/myproject/myrepo/myimage:v1") shouldBe true
    ContainerRegistry.GAR.regex.pattern
      .asPredicate()
      .test("us-docker.pkg.dev/dsp-artifact-registry/myrepo/myimage") shouldBe true
    ContainerRegistry.GAR.regex.pattern
      .asPredicate()
      .test("us-docker.pkg.dev/dsp-artifact-registry/myrepoorimage") shouldBe false
    ContainerRegistry.GAR.regex.pattern.asPredicate().test("us/broad-dsp-gcr-public/ubuntu1804") shouldBe false
    ContainerRegistry.GAR.regex.pattern
      .asPredicate()
      .test("europe-west1-docker.pkg.dev/dsp-artifact-registry/myrepo/myimage") shouldBe true
    ContainerRegistry.GAR.regex.pattern
      .asPredicate()
      .test("unknown-location-docker.pkg.dev/broad-dsp-gcr-public/ubuntu1804") shouldBe false
    ContainerRegistry.GAR.regex.pattern
      .asPredicate()
      .test("unknown1-location-docker.pkg.dev/broad-dsp-gcr-public/ubuntu1804") shouldBe false

    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("asd/asdf") shouldBe true
    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("asd") shouldBe false
    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("asd_sd_as:asdf") shouldBe false
    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("asd/as:asdf") shouldBe true
    ContainerRegistry.DockerHub.regex.pattern
      .asPredicate()
      .test("asd_as/as:asdf ") shouldBe false // trailing white space
    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("asd_as/as: asdf") shouldBe false // white space
    ContainerRegistry.DockerHub.regex.pattern
      .asPredicate()
      .test("myrepo/mydocker; mysql -c \"DROP ALL TABLES\"; sudo rm -rf / ") shouldBe false
    ContainerRegistry.DockerHub.regex.pattern.asPredicate().test("a///////") shouldBe false
    ContainerRegistry.GHCR.regex.pattern.asPredicate().test("ghcr.io/github/super-linter:latest") shouldBe true
    ContainerRegistry.GHCR.regex.pattern
      .asPredicate()
      .test("ghcr.io/lucidtronix/ml4h/ml4h_terra:20201117_123026") shouldBe true
    ContainerRegistry.GHCR.regex.pattern
      .asPredicate()
      .test("ghcr.io/lucidtronix/ml4h/ml4h_terra:latest") shouldBe true
    ContainerRegistry.GHCR.regex.pattern
      .asPredicate()
      .test("us.ghcr.io/lucidtronix/ml4h/ml4h_terra:20201117_123026") shouldBe false
    ContainerRegistry.GHCR.regex.pattern
      .asPredicate()
      .test("ghcr.io/github/super-linter:latest; foo") shouldBe false
    ContainerRegistry.GHCR.regex.pattern
      .asPredicate()
      .test("ghcr.io:foo") shouldBe false
  }

  "ContainerImage.stringToJupyterDockerImage" should "match GCR first, and then dockerhub" in {
    ContainerImage.fromImageUrl("us.gcr.io/broad-dsp-gcr-public/ubuntu1804") shouldBe (Some(
      ContainerImage("us.gcr.io/broad-dsp-gcr-public/ubuntu1804", ContainerRegistry.GCR)
    ))
    ContainerImage.fromImageUrl("asd/asdf") shouldBe (Some(
      ContainerImage("asd/asdf", ContainerRegistry.DockerHub)
    ))
  }

  "Cluster" should "generate a correct cluster URL" in {
    val expectedBase = s"https://leo/proxy/${project.value}/${name0.asString}/"

    // No images or labels -> default to Jupyter
    Runtime
      .getProxyUrl(proxyUrlBase, cloudContextGcp, name0, Set.empty, None, Map.empty)
      .toString shouldBe expectedBase + "jupyter"

    // images only
    Runtime
      .getProxyUrl(proxyUrlBase, cloudContextGcp, name0, Set(jupyterImage), None, Map.empty)
      .toString shouldBe expectedBase + "jupyter"
    Runtime
      .getProxyUrl(proxyUrlBase,
                   cloudContextGcp,
                   name0,
                   Set(welderImage, customDataprocImage, jupyterImage),
                   None,
                   Map.empty
      )
      .toString shouldBe expectedBase + "jupyter"
    Runtime
      .getProxyUrl(proxyUrlBase, cloudContextGcp, name0, Set(rstudioImage), None, Map.empty)
      .toString shouldBe expectedBase + "rstudio"
    Runtime
      .getProxyUrl(proxyUrlBase,
                   cloudContextGcp,
                   name0,
                   Set(welderImage, customDataprocImage, rstudioImage),
                   None,
                   Map.empty
      )
      .toString shouldBe expectedBase + "rstudio"

    // labels only
    Runtime
      .getProxyUrl(proxyUrlBase, cloudContextGcp, name0, Set.empty, None, Map("tool" -> "Jupyter", "foo" -> "bar"))
      .toString shouldBe expectedBase + "jupyter"
    Runtime
      .getProxyUrl(proxyUrlBase, cloudContextGcp, name0, Set.empty, None, Map("tool" -> "RStudio", "foo" -> "bar"))
      .toString shouldBe expectedBase + "rstudio"
    Runtime
      .getProxyUrl(proxyUrlBase, cloudContextGcp, name0, Set.empty, None, Map("foo" -> "bar"))
      .toString shouldBe expectedBase + "jupyter"

    // images and labels -> images take precedence
    Runtime
      .getProxyUrl(proxyUrlBase,
                   cloudContextGcp,
                   name0,
                   Set(welderImage, customDataprocImage, rstudioImage),
                   None,
                   Map("tool" -> "Jupyter", "foo" -> "bar")
      )
      .toString shouldBe expectedBase + "rstudio"

    Runtime
      .getProxyUrl(
        proxyUrlBase,
        CloudContext
          .Azure(AzureCloudContext(TenantId("tenantId"), SubscriptionId("sid"), ManagedResourceGroupName("mrg"))),
        name0,
        Set(welderImage, customDataprocImage, rstudioImage),
        Some(IP("qi-relay.servicebus.windows.net")),
        Map("tool" -> "Jupyter", "foo" -> "bar")
      )
      .toString shouldBe s"https://qi-relay.servicebus.windows.net/${name0.asString}"
  }
}
