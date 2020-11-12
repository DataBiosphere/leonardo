package org.broadinstitute.dsde.workbench.leonardo
package util

import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath}
import org.scalatest.flatspec.AnyFlatSpecLike

class RuntimeTemplateValuesSpec extends LeonardoTestSuite with AnyFlatSpecLike {

  "RuntimeTemplateValues" should "generate correct template values from a Runtime" in {
    val config = RuntimeTemplateValuesConfig.fromRuntime(
      CommonTestData.testCluster,
      Some(CommonTestData.initBucketName),
      None,
      CommonTestData.imageConfig,
      CommonTestData.welderConfig,
      CommonTestData.proxyConfig,
      CommonTestData.clusterFilesConfig,
      CommonTestData.clusterResourcesConfig,
      Some(CommonTestData.clusterResourceConstraints),
      RuntimeOperation.Restarting,
      Some(WelderAction.UpdateWelder),
      false
    )

    val test = for {
      now <- nowInstant
      result = RuntimeTemplateValues(config, Some(now))
    } yield {
      // note: alphabetized
      result.clusterName shouldBe CommonTestData.testCluster.runtimeName.asString
      result.customEnvVarsConfigUri shouldBe GcsPath(CommonTestData.initBucketName,
                                                     GcsObjectName("custom_env_vars.env")).toUri
      result.disableDelocalization shouldBe "false"
      result.googleClientId shouldBe "clientId"
      result.googleProject shouldBe CommonTestData.testCluster.googleProject.value
      result.jupyterCombinedExtensions shouldBe ""
      result.jupyterDockerCompose shouldBe GcsPath(CommonTestData.initBucketName,
                                                   GcsObjectName("test-jupyter-docker-compose.yaml")).toUri
      result.jupyterDockerComposeGce shouldBe GcsPath(CommonTestData.initBucketName,
                                                      GcsObjectName("test-jupyter-docker-compose-gce.yaml")).toUri
      result.jupyterDockerImage shouldBe CommonTestData.jupyterImage.imageUrl
      result.jupyterLabExtensions shouldBe ""
      result.jupyterNbExtensions shouldBe "gs://bucket-name/extension"
      result.jupyterNotebookConfigUri shouldBe GcsPath(CommonTestData.initBucketName,
                                                       GcsObjectName("jupyter_notebook_config.py")).toUri
      result.jupyterNotebookFrontendConfigUri shouldBe GcsPath(CommonTestData.initBucketName,
                                                               GcsObjectName("notebook.json")).toUri
      result.proxyServerCrt shouldBe GcsPath(CommonTestData.initBucketName, GcsObjectName("test-server.crt")).toUri
      result.proxyServerHostName shouldBe "https://leo"
      result.jupyterServerExtensions shouldBe ""
      result.proxyServerKey shouldBe GcsPath(CommonTestData.initBucketName, GcsObjectName("test-server.key")).toUri
      result.jupyterServerName shouldBe "jupyter-server"
      result.jupyterServiceAccountCredentials shouldBe ""
      result.jupyterStartUserScriptOutputUri shouldBe RuntimeTemplateValues
        .jupyterUserStartScriptOutputUriPath(CommonTestData.stagingBucketName, now)
        .toUri
      result.jupyterStartUserScriptUri shouldBe GcsPath(GcsBucketName("bucket-name"), GcsObjectName("startScript")).toUri
      result.jupyterUserScriptOutputUri shouldBe GcsPath(CommonTestData.stagingBucketName,
                                                         GcsObjectName("userscript_output.txt")).toUri
      result.jupyterUserScriptUri shouldBe GcsPath(GcsBucketName("bucket-name"), GcsObjectName("userScript")).toUri
      result.loginHint shouldBe CommonTestData.auditInfo.creator.value
      result.memLimit shouldBe "3758096384b" // 3.5 GB
      result.notebooksDir shouldBe "/home/jupyter-user/notebooks"
      result.proxyDockerCompose shouldBe GcsPath(CommonTestData.initBucketName,
                                                 GcsObjectName("test-proxy-docker-compose.yaml")).toUri
      result.proxyDockerImage shouldBe CommonTestData.proxyImage.imageUrl
      result.proxyServerName shouldBe "proxy-server"
      result.proxySiteConf shouldBe GcsPath(CommonTestData.initBucketName, GcsObjectName("test-site.conf")).toUri
      result.rootCaPem shouldBe GcsPath(CommonTestData.initBucketName, GcsObjectName("test-server.pem")).toUri
      result.rstudioDockerCompose shouldBe GcsPath(CommonTestData.initBucketName,
                                                   GcsObjectName("test-rstudio-docker-compose.yaml")).toUri
      result.rstudioDockerImage shouldBe ""
      result.rstudioLicenseFile shouldBe GcsPath(CommonTestData.initBucketName,
                                                 GcsObjectName("rstudio-license-file.lic")).toUri
      result.rstudioServerName shouldBe "rstudio-server"
      result.runtimeOperation shouldBe RuntimeOperation.Restarting.asString
      result.stagingBucketName shouldBe CommonTestData.stagingBucketName.value
      result.stratumDockerCompose shouldBe GcsPath(CommonTestData.initBucketName,
                                                   GcsObjectName("test-stratum-docker-compose.yaml")).toUri
      result.stratumDockerImage shouldBe CommonTestData.stratumImage.imageUrl
      result.stratumServerName shouldBe "stratum-detector"
      result.welderDockerCompose shouldBe GcsPath(CommonTestData.initBucketName,
                                                  GcsObjectName("test-welder-docker-compose.yaml")).toUri
      result.welderDockerImage shouldBe CommonTestData.welderImage.imageUrl
      result.welderEnabled shouldBe "true"
      result.welderMemLimit shouldBe "805306368b" // 768 MB
      result.welderServerName shouldBe "welder-server"
    }

    test.unsafeRunSync()
  }

}
