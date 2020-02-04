package org.broadinstitute.dsde.workbench.leonardo.util

import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, Proxy, RStudio, Welder}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateCluster
import org.broadinstitute.dsde.workbench.leonardo.{
  AuditInfo,
  ClusterName,
  RuntimeImage,
  RuntimeResourceConstraints,
  UserJupyterExtensionConfig,
  UserScriptPath
}
import org.broadinstitute.dsde.workbench.model.google.{
  GcsBucketName,
  GcsObjectName,
  GcsPath,
  GoogleProject,
  ServiceAccountKey
}

case class RuntimeTemplateValues private (googleProject: String,
                                          clusterName: String,
                                          stagingBucketName: String,
                                          jupyterDockerImage: String,
                                          rstudioDockerImage: String,
                                          proxyDockerImage: String,
                                          welderDockerImage: String,
                                          jupyterServerCrt: String,
                                          jupyterServerKey: String,
                                          rootCaPem: String,
                                          jupyterDockerCompose: String,
                                          rstudioDockerCompose: String,
                                          proxyDockerCompose: String,
                                          welderDockerCompose: String,
                                          proxySiteConf: String,
                                          jupyterServerName: String,
                                          rstudioServerName: String,
                                          welderServerName: String,
                                          proxyServerName: String,
                                          jupyterUserScriptUri: String,
                                          jupyterUserScriptOutputUri: String,
                                          jupyterStartUserScriptUri: String,
                                          jupyterStartUserScriptOutputBaseUri: String,
                                          jupyterServiceAccountCredentials: String,
                                          loginHint: String,
                                          jupyterServerExtensions: String,
                                          jupyterNbExtensions: String,
                                          jupyterCombinedExtensions: String,
                                          jupyterLabExtensions: String,
                                          jupyterNotebookConfigUri: String,
                                          jupyterNotebookFrontendConfigUri: String,
                                          googleClientId: String,
                                          welderEnabled: String,
                                          notebooksDir: String,
                                          customEnvVarsConfigUri: String,
                                          memLimit: String) {

  def toMap: Map[String, String] =
    this.getClass.getDeclaredFields.map(_.getName).zip(this.productIterator.to).toMap.mapValues(_.toString)

}

case class RuntimeTemplateValuesConfig(googleProject: GoogleProject,
                                       clusterName: ClusterName,
                                       stagingBucketName: Option[GcsBucketName],
                                       runtimeImages: Set[RuntimeImage],
                                       initBucketName: Option[GcsBucketName],
                                       jupyterUserScriptUri: Option[UserScriptPath],
                                       jupyterStartUserScriptUri: Option[UserScriptPath],
                                       serviceAccountKey: Option[ServiceAccountKey],
                                       userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                       defaultClientId: Option[String],
                                       welderEnabled: Boolean,
                                       auditInfo: AuditInfo,
                                       imageConfig: ImageConfig,
                                       welderConfig: WelderConfig,
                                       proxyConfig: ProxyConfig,
                                       clusterFilesConfig: ClusterFilesConfig,
                                       clusterResourcesConfig: ClusterResourcesConfig,
                                       clusterResourceConstraints: Option[RuntimeResourceConstraints])

object RuntimeTemplateValues {
  val serviceAccountCredentialsFilename = "service-account-credentials.json"
  val customEnvVarFilename = "custom_env_vars.env"

  def apply(config: RuntimeTemplateValuesConfig): RuntimeTemplateValues =
    RuntimeTemplateValues(
      config.googleProject.value,
      config.clusterName.asString,
      config.stagingBucketName.map(_.value).getOrElse(""),
      config.runtimeImages.find(_.imageType == Jupyter).map(_.imageUrl).getOrElse(""),
      config.runtimeImages.find(_.imageType == RStudio).map(_.imageUrl).getOrElse(""),
      config.runtimeImages.find(_.imageType == Proxy).map(_.imageUrl).getOrElse(""),
      config.runtimeImages.find(_.imageType == Welder).map(_.imageUrl).getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterFilesConfig.jupyterServerCrt.getName)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterFilesConfig.jupyterServerKey.getName)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterFilesConfig.jupyterRootCaPem.getName)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.jupyterDockerCompose.asString)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.rstudioDockerCompose.asString)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.proxyDockerCompose.asString)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.welderDockerCompose.asString)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.proxySiteConf.asString)).toUri)
        .getOrElse(""),
      config.imageConfig.jupyterContainerName,
      config.imageConfig.rstudioContainerName,
      config.imageConfig.welderContainerName,
      config.imageConfig.proxyContainerName,
      config.jupyterUserScriptUri.map(_.asString).getOrElse(""),
      config.stagingBucketName.map(n => GcsPath(n, GcsObjectName("userscript_output.txt")).toUri).getOrElse(""),
      config.jupyterStartUserScriptUri.map(_.asString).getOrElse(""),
      config.stagingBucketName.map(n => GcsPath(n, GcsObjectName("startscript_output.txt")).toUri).getOrElse(""),
      (for {
        _ <- config.serviceAccountKey
        n <- config.initBucketName
      } yield GcsPath(n, GcsObjectName(serviceAccountCredentialsFilename)).toUri).getOrElse(""),
      config.auditInfo.creator.value,
      config.userJupyterExtensionConfig.map(x => x.serverExtensions.values.mkString(" ")).getOrElse(""),
      config.userJupyterExtensionConfig.map(x => x.nbExtensions.values.mkString(" ")).getOrElse(""),
      config.userJupyterExtensionConfig.map(x => x.combinedExtensions.values.mkString(" ")).getOrElse(""),
      config.userJupyterExtensionConfig.map(x => x.labExtensions.values.mkString(" ")).getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.jupyterNotebookConfigUri.asString)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(
          n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.jupyterNotebookFrontendConfigUri.asString)).toUri
        )
        .getOrElse(""),
      config.defaultClientId.getOrElse(""),
      config.welderEnabled.toString, // TODO: remove this and conditional below when welder is rolled out to all clusters
      if (config.welderEnabled) config.welderConfig.welderEnabledNotebooksDir.toString
      else config.welderConfig.welderDisabledNotebooksDir.toString,
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.customEnvVarsConfigUri.asString)).toUri)
        .getOrElse(""),
      config.clusterResourceConstraints.map(_.memoryLimit.toString).getOrElse("")
    )

  // TODO consolidate
  def fromCreateCluster(createCluster: CreateCluster,
                        initBucketName: Option[GcsBucketName],
                        stagingBucketName: Option[GcsBucketName],
                        serviceAccountKey: Option[ServiceAccountKey],
                        dataprocConfig: DataprocConfig,
                        imageConfig: ImageConfig,
                        welderConfig: WelderConfig,
                        proxyConfig: ProxyConfig,
                        clusterFilesConfig: ClusterFilesConfig,
                        clusterResourcesConfig: ClusterResourcesConfig,
                        clusterResourceConstraints: Option[RuntimeResourceConstraints]): RuntimeTemplateValues =
    RuntimeTemplateValues(
      createCluster.clusterProjectAndName.googleProject.value,
      createCluster.clusterProjectAndName.runtimeName.asString,
      stagingBucketName.map(_.value).getOrElse(""),
      createCluster.runtimeImages.find(_.imageType == Jupyter).map(_.imageUrl).getOrElse(""),
      createCluster.runtimeImages.find(_.imageType == RStudio).map(_.imageUrl).getOrElse(""),
      createCluster.runtimeImages.find(_.imageType == Proxy).map(_.imageUrl).getOrElse(""),
      createCluster.runtimeImages.find(_.imageType == Welder).map(_.imageUrl).getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterFilesConfig.jupyterServerCrt.getName)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterFilesConfig.jupyterServerKey.getName)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterFilesConfig.jupyterRootCaPem.getName)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.jupyterDockerCompose.asString)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.rstudioDockerCompose.asString)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.proxyDockerCompose.asString)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.welderDockerCompose.asString)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.proxySiteConf.asString)).toUri)
        .getOrElse(""),
      imageConfig.jupyterContainerName,
      imageConfig.rstudioContainerName,
      imageConfig.welderContainerName,
      imageConfig.proxyContainerName,
      createCluster.jupyterUserScriptUri.map(_.asString).getOrElse(""),
      stagingBucketName.map(n => GcsPath(n, GcsObjectName("userscript_output.txt")).toUri).getOrElse(""),
      createCluster.jupyterStartUserScriptUri.map(_.asString).getOrElse(""),
      stagingBucketName.map(n => GcsPath(n, GcsObjectName("startscript_output.txt")).toUri).getOrElse(""),
      (for {
        _ <- serviceAccountKey
        n <- initBucketName
      } yield GcsPath(n, GcsObjectName(serviceAccountCredentialsFilename)).toUri).getOrElse(""),
      createCluster.auditInfo.creator.value,
      createCluster.userJupyterExtensionConfig.map(x => x.serverExtensions.values.mkString(" ")).getOrElse(""),
      createCluster.userJupyterExtensionConfig.map(x => x.nbExtensions.values.mkString(" ")).getOrElse(""),
      createCluster.userJupyterExtensionConfig.map(x => x.combinedExtensions.values.mkString(" ")).getOrElse(""),
      createCluster.userJupyterExtensionConfig.map(x => x.labExtensions.values.mkString(" ")).getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.jupyterNotebookConfigUri.asString)).toUri)
        .getOrElse(""),
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.jupyterNotebookFrontendConfigUri.asString)).toUri)
        .getOrElse(""),
      createCluster.defaultClientId.getOrElse(""),
      createCluster.welderEnabled.toString, // TODO: remove this and conditional below when welder is rolled out to all clusters
      if (createCluster.welderEnabled) welderConfig.welderEnabledNotebooksDir.toString
      else welderConfig.welderDisabledNotebooksDir.toString,
      initBucketName
        .map(n => GcsPath(n, GcsObjectName(clusterResourcesConfig.customEnvVarsConfigUri.asString)).toUri)
        .getOrElse(""),
      clusterResourceConstraints.map(_.memoryLimit.toString).getOrElse("")
    )
}
