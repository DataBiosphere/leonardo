package org.broadinstitute.dsde.workbench.leonardo.util

import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, Proxy, RStudio, Welder}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateCluster
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, ServiceAccountKey}

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

case class RuntimeTemplateValuesConfig(runtimeProjectAndName: RuntimeProjectAndName,
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
object RuntimeTemplateValuesConfig {
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
                        clusterResourceConstraints: Option[RuntimeResourceConstraints]): RuntimeTemplateValuesConfig =
    RuntimeTemplateValuesConfig(
      createCluster.clusterProjectAndName,
      stagingBucketName,
      createCluster.runtimeImages,
      initBucketName,
      createCluster.jupyterUserScriptUri,
      createCluster.jupyterStartUserScriptUri,
      serviceAccountKey,
      createCluster.userJupyterExtensionConfig,
      createCluster.defaultClientId,
      createCluster.welderEnabled,
      createCluster.auditInfo,
      imageConfig,
      welderConfig,
      proxyConfig,
      clusterFilesConfig,
      clusterResourcesConfig,
      clusterResourceConstraints
    )

  def fromRuntime(runtime: Runtime,
                  initBucketName: Option[GcsBucketName],
                  serviceAccountKey: Option[ServiceAccountKey],
                  dataprocConfig: DataprocConfig,
                  imageConfig: ImageConfig,
                  welderConfig: WelderConfig,
                  proxyConfig: ProxyConfig,
                  clusterFilesConfig: ClusterFilesConfig,
                  clusterResourcesConfig: ClusterResourcesConfig,
                  clusterResourceConstraints: Option[RuntimeResourceConstraints]): RuntimeTemplateValuesConfig =
    RuntimeTemplateValuesConfig(
      RuntimeProjectAndName(runtime.googleProject, runtime.runtimeName),
      runtime.asyncRuntimeFields.map(_.stagingBucket),
      runtime.runtimeImages,
      initBucketName,
      runtime.jupyterUserScriptUri,
      runtime.jupyterStartUserScriptUri,
      serviceAccountKey,
      runtime.userJupyterExtensionConfig,
      runtime.defaultClientId,
      runtime.welderEnabled,
      runtime.auditInfo,
      imageConfig,
      welderConfig,
      proxyConfig,
      clusterFilesConfig,
      clusterResourcesConfig,
      clusterResourceConstraints
    )
}

object RuntimeTemplateValues {
  val serviceAccountCredentialsFilename = "service-account-credentials.json"
  val customEnvVarFilename = "custom_env_vars.env"

  def apply(config: RuntimeTemplateValuesConfig): RuntimeTemplateValues =
    RuntimeTemplateValues(
      config.runtimeProjectAndName.googleProject.value,
      config.runtimeProjectAndName.runtimeName.asString,
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
}
