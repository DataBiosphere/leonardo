package org.broadinstitute.dsde.workbench.leonardo.util

import java.time.format.{DateTimeFormatter, FormatStyle}
import java.time.{Instant, ZoneId}
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{CryptoDetector, Jupyter, Proxy, RStudio, Welder}
import org.broadinstitute.dsde.workbench.leonardo.WelderAction._
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeConfigInCreateRuntimeMessage
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsObjectName, GcsPath, ServiceAccountKey}

case class RuntimeTemplateValues private (googleProject: String,
                                          gpuEnabled: String,
                                          clusterName: String,
                                          initBucketName: String,
                                          stagingBucketName: String,
                                          jupyterDockerImage: String,
                                          rstudioDockerImage: String,
                                          proxyDockerImage: String,
                                          welderDockerImage: String,
                                          cryptoDetectorDockerImage: String,
                                          proxyServerCrt: String,
                                          proxyServerKey: String,
                                          rootCaPem: String,
                                          jupyterDockerCompose: String,
                                          gpuDockerCompose: String,
                                          rstudioDockerCompose: String,
                                          proxyDockerCompose: String,
                                          welderDockerCompose: String,
                                          proxySiteConf: String,
                                          jupyterServerName: String,
                                          rstudioServerName: String,
                                          welderServerName: String,
                                          proxyServerName: String,
                                          cryptoDetectorServerName: String,
                                          userScriptUri: String,
                                          userScriptOutputUri: String,
                                          startUserScriptUri: String,
                                          startUserScriptOutputUri: String,
                                          jupyterServiceAccountCredentials: String,
                                          jupyterHomeDirectory: String,
                                          loginHint: String,
                                          petSaEmail: String,
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
                                          shmSize: String,
                                          welderMemLimit: String,
                                          runtimeOperation: String,
                                          updateWelder: String,
                                          disableDelocalization: String,
                                          proxyServerHostName: String,
                                          isGceFormatted: String,
                                          useGceStartupScript: String,
                                          shouldDeleteJupyterDir: String
) {

  def toMap: Map[String, String] =
    this.productElementNames
      .zip(this.productIterator)
      .map { case (k, v) =>
        (k, v.toString)
      }
      .toMap

}

case class RuntimeTemplateValuesConfig private (runtimeProjectAndName: RuntimeProjectAndName,
                                                gpuEnabled: Boolean,
                                                stagingBucketName: Option[GcsBucketName],
                                                runtimeImages: Set[RuntimeImage],
                                                initBucketName: Option[GcsBucketName],
                                                userScriptUri: Option[UserScriptPath],
                                                startUserScriptUri: Option[UserScriptPath],
                                                serviceAccountKey: Option[ServiceAccountKey],
                                                petSaEmail: WorkbenchEmail,
                                                userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                                defaultClientId: Option[String],
                                                welderEnabled: Boolean,
                                                auditInfo: AuditInfo,
                                                imageConfig: ImageConfig,
                                                welderConfig: WelderConfig,
                                                proxyConfig: ProxyConfig,
                                                clusterFilesConfig: SecurityFilesConfig,
                                                clusterResourcesConfig: ClusterResourcesConfig,
                                                clusterResourceConstraints: Option[RuntimeResourceConstraints],
                                                runtimeOperation: RuntimeOperation,
                                                welderAction: Option[WelderAction],
                                                isGceFormatted: Boolean,
                                                useGceStartupScript: Boolean
)
object RuntimeTemplateValuesConfig {
  def fromCreateRuntimeParams(
    params: CreateRuntimeParams,
    initBucketName: Option[GcsBucketName],
    stagingBucketName: Option[GcsBucketName],
    serviceAccountKey: Option[ServiceAccountKey],
    imageConfig: ImageConfig,
    welderConfig: WelderConfig,
    proxyConfig: ProxyConfig,
    clusterFilesConfig: SecurityFilesConfig,
    clusterResourcesConfig: ClusterResourcesConfig,
    clusterResourceConstraints: Option[RuntimeResourceConstraints],
    isFormatted: Boolean
  ): RuntimeTemplateValuesConfig =
    RuntimeTemplateValuesConfig(
      params.runtimeProjectAndName,
      params.runtimeConfig match {
        case x: RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig => x.gpuConfig.isDefined
        case x: RuntimeConfigInCreateRuntimeMessage.GceConfig       => x.gpuConfig.isDefined
        case _                                                      => false
      },
      stagingBucketName,
      params.runtimeImages,
      initBucketName,
      params.userScriptUri,
      params.startUserScriptUri,
      serviceAccountKey,
      params.serviceAccountInfo,
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
      None,
      isFormatted,
      false
    )

  def fromRuntime(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                  initBucketName: Option[GcsBucketName],
                  serviceAccountKey: Option[ServiceAccountKey],
                  imageConfig: ImageConfig,
                  welderConfig: WelderConfig,
                  proxyConfig: ProxyConfig,
                  clusterFilesConfig: SecurityFilesConfig,
                  clusterResourcesConfig: ClusterResourcesConfig,
                  clusterResourceConstraints: Option[RuntimeResourceConstraints],
                  runtimeOperation: RuntimeOperation,
                  welderAction: Option[WelderAction],
                  useGceStartupScript: Boolean
  ): RuntimeTemplateValuesConfig = {
    val runtime = runtimeAndRuntimeConfig.runtime
    RuntimeTemplateValuesConfig(
      RuntimeProjectAndName(runtime.cloudContext, runtime.runtimeName),
      runtimeAndRuntimeConfig.runtimeConfig match {
        case gce: RuntimeConfig.GceConfig       => gce.gpuConfig.isDefined
        case gce: RuntimeConfig.GceWithPdConfig => gce.gpuConfig.isDefined
        case _: RuntimeConfig.DataprocConfig    => false
        case _: RuntimeConfig.AzureConfig       => false
      },
      runtime.asyncRuntimeFields.map(_.stagingBucket),
      runtime.runtimeImages,
      initBucketName,
      runtime.userScriptUri,
      runtime.startUserScriptUri,
      serviceAccountKey,
      runtime.serviceAccount,
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
      welderAction,
      false,
      useGceStartupScript
    )
  }
}

object RuntimeTemplateValues {
  val serviceAccountCredentialsFilename = "service-account-credentials.json"
  val customEnvVarFilename = "custom_env_vars.env"

  def apply(config: RuntimeTemplateValuesConfig,
            now: Option[Instant],
            shouldDeleteJupyterDir: Boolean
  ): RuntimeTemplateValues = {
    val jupyterUserhome =
      config.runtimeImages
        .find(_.imageType == Jupyter)
        .flatMap(_.homeDirectory.map(_.toString))
        .getOrElse("/home/jupyter")
    RuntimeTemplateValues(
      config.runtimeProjectAndName.cloudContext.asString,
      config.gpuEnabled.toString,
      config.runtimeProjectAndName.runtimeName.asString,
      config.initBucketName.map(_.value).getOrElse(""),
      config.stagingBucketName.map(_.value).getOrElse(""),
      config.runtimeImages.find(_.imageType == Jupyter).map(_.imageUrl).getOrElse(""),
      config.runtimeImages.find(_.imageType == RStudio).map(_.imageUrl).getOrElse(""),
      config.runtimeImages.find(_.imageType == Proxy).map(_.imageUrl).getOrElse(""),
      config.runtimeImages.find(_.imageType == Welder).map(_.imageUrl).getOrElse(""),
      config.runtimeImages.find(_.imageType == CryptoDetector).map(_.imageUrl).getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterFilesConfig.proxyServerCrt.getFileName.toString)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterFilesConfig.proxyServerKey.getFileName.toString)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterFilesConfig.proxyRootCaPem.getFileName.toString)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.jupyterDockerCompose.asString)).toUri)
        .getOrElse(""),
      config.initBucketName
        .flatMap(n =>
          config.clusterResourcesConfig.gpuDockerCompose.map(d => GcsPath(n, GcsObjectName(d.asString)).toUri)
        )
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
      config.imageConfig.cryptoDetectorContainerName,
      config.userScriptUri.map(_.asString).getOrElse(""),
      config.stagingBucketName.map(n => userScriptOutputUriPath(n).toUri).getOrElse(""),
      config.startUserScriptUri.map(_.asString).getOrElse(""),
      config.stagingBucketName
        .map(n => userStartScriptOutputUriPath(n, now.getOrElse(Instant.now)).toUri)
        .getOrElse(""), // TODO: remove this complication
      (for {
        _ <- config.serviceAccountKey
        n <- config.initBucketName
      } yield GcsPath(n, GcsObjectName(serviceAccountCredentialsFilename)).toUri).getOrElse(""),
      jupyterUserhome,
      config.auditInfo.creator.value,
      config.petSaEmail.value,
      config.userJupyterExtensionConfig.map(x => x.serverExtensions.values.mkString(" ")).getOrElse(""),
      config.userJupyterExtensionConfig.map(x => x.nbExtensions.values.mkString(" ")).getOrElse(""),
      config.userJupyterExtensionConfig.map(x => x.combinedExtensions.values.mkString(" ")).getOrElse(""),
      config.userJupyterExtensionConfig.map(x => x.labExtensions.values.mkString(" ")).getOrElse(""),
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.jupyterNotebookConfigUri.asString)).toUri)
        .getOrElse(""),
      config.initBucketName
        .map(n =>
          GcsPath(n, GcsObjectName(config.clusterResourcesConfig.jupyterNotebookFrontendConfigUri.asString)).toUri
        )
        .getOrElse(""),
      config.defaultClientId.getOrElse(""),
      config.welderEnabled.toString, // TODO: remove this and conditional below when welder is rolled out to all clusters
      notebooksDir = s"${jupyterUserhome}",
      config.initBucketName
        .map(n => GcsPath(n, GcsObjectName(config.clusterResourcesConfig.customEnvVarsConfigUri.asString)).toUri)
        .getOrElse(""),
      config.clusterResourceConstraints.map(_.memoryLimit.toString).getOrElse(""),
      config.welderConfig.welderReservedMemory.map(_.toString).getOrElse(""),
      config.runtimeOperation.asString,
      (config.welderAction == Some(UpdateWelder)).toString,
      (config.welderAction == Some(DisableDelocalization)).toString,
      config.proxyConfig.getProxyServerHostName,
      config.isGceFormatted.toString,
      config.useGceStartupScript.toString,
      shouldDeleteJupyterDir.toString
    )
  }

  def userScriptOutputUriPath(stagingBucketName: GcsBucketName): GcsPath =
    GcsPath(stagingBucketName, GcsObjectName("userscript_output.txt"))
  private[util] def userStartScriptOutputUriPath(stagingBucketName: GcsBucketName, now: Instant): GcsPath = {
    val formatter = DateTimeFormatter
      .ofLocalizedDateTime(FormatStyle.SHORT)
      .withZone(ZoneId.systemDefault())
    val formatedNow = formatter
      .format(now)
      .replace(" ", "_")
      .replace("/", "_")
    GcsPath(stagingBucketName, GcsObjectName(s"startscript_output/${formatedNow}.txt"))
  }
}
