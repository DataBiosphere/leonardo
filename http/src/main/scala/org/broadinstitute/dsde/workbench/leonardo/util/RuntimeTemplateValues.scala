package org.broadinstitute.dsde.workbench.leonardo.util

import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, Proxy, RStudio, Welder}
import org.broadinstitute.dsde.workbench.leonardo.WelderAction.{DeployWelder, DisableDelocalization, UpdateWelder}
import org.broadinstitute.dsde.workbench.leonardo.config._
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
                                          jupyterDockerComposeGce: String,
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
                                          memLimit: String,
                                          runtimeOperation: String,
                                          deployWelder: String,
                                          updateWelder: String,
                                          disableDelocalization: String) {

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
                                       clusterResourceConstraints: Option[RuntimeResourceConstraints],
                                       runtimeOperation: RuntimeOperation,
                                       welderAction: Option[WelderAction])
object RuntimeTemplateValuesConfig {
  def fromCreateRuntimeParams(
    params: CreateRuntimeParams,
    initBucketName: Option[GcsBucketName],
    stagingBucketName: Option[GcsBucketName],
    serviceAccountKey: Option[ServiceAccountKey],
    imageConfig: ImageConfig,
    welderConfig: WelderConfig,
    proxyConfig: ProxyConfig,
    clusterFilesConfig: ClusterFilesConfig,
    clusterResourcesConfig: ClusterResourcesConfig,
    clusterResourceConstraints: Option[RuntimeResourceConstraints]
  ): RuntimeTemplateValuesConfig =
    RuntimeTemplateValuesConfig(
      params.runtimeProjectAndName,
      stagingBucketName,
      params.runtimeImages,
      initBucketName,
      params.jupyterUserScriptUri,
      params.jupyterStartUserScriptUri,
      serviceAccountKey,
      params.userJupyterExtensionConfig,
      params.defaultClientId,
      params.welderEnabled,
      params.auditInfo,
      imageConfig,
      welderConfig,
      proxyConfig,
      clusterFilesConfig,
      clusterResourcesConfig,
      clusterResourceConstraints,
      RuntimeOperation.Creating,
      None
    )

  def fromRuntime(runtime: Runtime,
                  initBucketName: Option[GcsBucketName],
                  serviceAccountKey: Option[ServiceAccountKey],
                  imageConfig: ImageConfig,
                  welderConfig: WelderConfig,
                  proxyConfig: ProxyConfig,
                  clusterFilesConfig: ClusterFilesConfig,
                  clusterResourcesConfig: ClusterResourcesConfig,
                  clusterResourceConstraints: Option[RuntimeResourceConstraints],
                  runtimeOperation: RuntimeOperation,
                  welderAction: Option[WelderAction]): RuntimeTemplateValuesConfig =
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
      clusterResourceConstraints,
      runtimeOperation,
      welderAction
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
        .map(n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.jupyterDockerComposeGce.asString)).toUri)
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
      config.clusterResourceConstraints.map(_.memoryLimit.toString).getOrElse(""),
      config.runtimeOperation.asString,
      (config.welderAction == Some(DeployWelder)).toString,
      (config.welderAction == Some(UpdateWelder)).toString,
      (config.welderAction == Some(DisableDelocalization)).toString
    )
}
