package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.net.URL
import java.time.Instant

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.GetAppResult
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.{GcsPathFormat => _}
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}

final case class CreateRuntimeRequest(labels: LabelMap = Map.empty,
                                      jupyterUserScriptUri: Option[UserScriptPath] = None,
                                      jupyterStartUserScriptUri: Option[UserScriptPath] = None,
                                      runtimeConfig: Option[RuntimeConfigRequest] = None,
                                      stopAfterCreation: Option[Boolean] = None,
                                      allowStop: Boolean = false,
                                      userJupyterExtensionConfig: Option[UserJupyterExtensionConfig] = None,
                                      autopause: Option[Boolean] = None,
                                      autopauseThreshold: Option[Int] = None,
                                      defaultClientId: Option[String] = None,
                                      jupyterDockerImage: Option[ContainerImage] = None,
                                      toolDockerImage: Option[ContainerImage] = None,
                                      welderRegistry: Option[ContainerRegistry] = None,
                                      welderDockerImage: Option[ContainerImage] = None,
                                      scopes: Set[String] = Set.empty,
                                      enableWelder: Option[Boolean] = None,
                                      customClusterEnvironmentVariables: Map[String, String] = Map.empty)

object CreateRuntimeRequest {
  def toRuntime(request: CreateRuntimeRequest,
                samResource: RuntimeSamResourceId,
                userEmail: WorkbenchEmail,
                runtimeName: RuntimeName,
                googleProject: GoogleProject,
                serviceAccountInfo: WorkbenchEmail,
                proxyUrlBase: String,
                autopauseThreshold: Int,
                scopes: Set[String],
                runtimeImages: Set[RuntimeImage],
                timestamp: Instant): Runtime =
    Runtime(
      id = -1,
      samResource = samResource,
      runtimeName = runtimeName,
      googleProject = googleProject,
      serviceAccount = serviceAccountInfo,
      asyncRuntimeFields = None,
      auditInfo = AuditInfo(userEmail, timestamp, None, timestamp),
      kernelFoundBusyDate = None,
      proxyUrl = Runtime.getProxyUrl(proxyUrlBase, googleProject, runtimeName, runtimeImages, request.labels),
      status = RuntimeStatus.Creating,
      labels = request.labels,
      jupyterUserScriptUri = request.jupyterUserScriptUri,
      jupyterStartUserScriptUri = request.jupyterStartUserScriptUri,
      errors = List.empty,
      dataprocInstances = Set.empty,
      userJupyterExtensionConfig = request.userJupyterExtensionConfig,
      autopauseThreshold = autopauseThreshold,
      defaultClientId = request.defaultClientId,
      stopAfterCreation = request.stopAfterCreation.getOrElse(false),
      allowStop = request.allowStop,
      runtimeImages = runtimeImages,
      scopes = scopes,
      welderEnabled = request.enableWelder.getOrElse(false),
      customEnvironmentVariables = request.customClusterEnvironmentVariables,
      runtimeConfigId = RuntimeConfigId(-1),
      patchInProgress = false
    )
}

// Currently, CreateRuntimeResponse has exactly the same fields as GetRuntimeResponse, but going forward, when we can,
// we should deprecate and remove some of fields for createRuntime request
final case class CreateRuntimeResponse(id: Long,
                                       samResource: RuntimeSamResourceId,
                                       clusterName: RuntimeName,
                                       googleProject: GoogleProject,
                                       serviceAccountInfo: WorkbenchEmail,
                                       asyncRuntimeFields: Option[AsyncRuntimeFields],
                                       auditInfo: AuditInfo,
                                       kernelFoundBusyDate: Option[Instant],
                                       runtimeConfig: RuntimeConfig,
                                       clusterUrl: URL,
                                       status: RuntimeStatus,
                                       labels: LabelMap,
                                       jupyterExtensionUri: Option[GcsPath],
                                       jupyterUserScriptUri: Option[UserScriptPath],
                                       jupyterStartUserScriptUri: Option[UserScriptPath],
                                       errors: List[RuntimeError],
                                       dataprocInstances: Set[DataprocInstance],
                                       userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                       autopauseThreshold: Int,
                                       defaultClientId: Option[String],
                                       stopAfterCreation: Boolean,
                                       clusterImages: Set[RuntimeImage],
                                       scopes: Set[String],
                                       welderEnabled: Boolean,
                                       patchInProgress: Boolean,
                                       customClusterEnvironmentVariables: Map[String, String])

object CreateRuntimeResponse {
  def fromRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig) = CreateRuntimeResponse(
    runtime.id,
    runtime.samResource,
    runtime.runtimeName,
    runtime.googleProject,
    runtime.serviceAccount,
    runtime.asyncRuntimeFields,
    runtime.auditInfo,
    runtime.kernelFoundBusyDate,
    runtimeConfig,
    runtime.proxyUrl,
    runtime.status,
    runtime.labels,
    None,
    runtime.jupyterUserScriptUri,
    runtime.jupyterStartUserScriptUri,
    runtime.errors,
    runtime.dataprocInstances,
    runtime.userJupyterExtensionConfig,
    runtime.autopauseThreshold,
    runtime.defaultClientId,
    runtime.stopAfterCreation,
    runtime.runtimeImages,
    runtime.scopes,
    runtime.welderEnabled,
    runtime.patchInProgress,
    runtime.customEnvironmentVariables
  )
}

final case class ListRuntimeResponse(id: Long,
                                     samResource: RuntimeSamResourceId,
                                     clusterName: RuntimeName,
                                     googleProject: GoogleProject,
                                     serviceAccountInfo: WorkbenchEmail,
                                     asyncRuntimeFields: Option[AsyncRuntimeFields],
                                     auditInfo: AuditInfo,
                                     kernelFoundBusyDate: Option[Instant],
                                     machineConfig: RuntimeConfig,
                                     clusterUrl: URL,
                                     status: RuntimeStatus,
                                     labels: LabelMap,
                                     jupyterUserScriptUri: Option[UserScriptPath],
                                     dataprocInstances: Set[DataprocInstance],
                                     autopauseThreshold: Int,
                                     defaultClientId: Option[String],
                                     stopAfterCreation: Boolean,
                                     welderEnabled: Boolean,
                                     patchInProgress: Boolean)

object ListRuntimeResponse {
  def fromRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig): ListRuntimeResponse =
    ListRuntimeResponse(
      runtime.id,
      runtime.samResource,
      runtime.runtimeName,
      runtime.googleProject,
      runtime.serviceAccount,
      runtime.asyncRuntimeFields,
      runtime.auditInfo,
      runtime.kernelFoundBusyDate,
      runtimeConfig,
      runtime.proxyUrl,
      runtime.status,
      runtime.labels,
      runtime.jupyterUserScriptUri,
      runtime.dataprocInstances,
      runtime.autopauseThreshold,
      runtime.defaultClientId,
      runtime.stopAfterCreation,
      runtime.welderEnabled,
      runtime.patchInProgress
    )
}

final case class GetRuntimeResponse(id: Long,
                                    samResource: RuntimeSamResourceId,
                                    clusterName: RuntimeName,
                                    googleProject: GoogleProject,
                                    serviceAccountInfo: WorkbenchEmail,
                                    asyncRuntimeFields: Option[AsyncRuntimeFields],
                                    auditInfo: AuditInfo,
                                    kernelFoundBusyDate: Option[Instant],
                                    runtimeConfig: RuntimeConfig,
                                    clusterUrl: URL,
                                    status: RuntimeStatus,
                                    labels: LabelMap,
                                    jupyterUserScriptUri: Option[UserScriptPath],
                                    jupyterStartUserScriptUri: Option[UserScriptPath],
                                    errors: List[RuntimeError],
                                    dataprocInstances: Set[DataprocInstance],
                                    userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                    autopauseThreshold: Int,
                                    defaultClientId: Option[String],
                                    stopAfterCreation: Boolean,
                                    clusterImages: Set[RuntimeImage],
                                    scopes: Set[String],
                                    welderEnabled: Boolean,
                                    patchInProgress: Boolean,
                                    customClusterEnvironmentVariables: Map[String, String],
                                    diskConfig: Option[DiskConfig])

object GetRuntimeResponse {
  def fromRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig, diskConfig: Option[DiskConfig]) = GetRuntimeResponse(
    runtime.id,
    runtime.samResource,
    runtime.runtimeName,
    runtime.googleProject,
    runtime.serviceAccount,
    runtime.asyncRuntimeFields,
    runtime.auditInfo,
    runtime.kernelFoundBusyDate,
    runtimeConfig,
    runtime.proxyUrl,
    runtime.status,
    runtime.labels,
    runtime.jupyterUserScriptUri,
    runtime.jupyterStartUserScriptUri,
    runtime.errors,
    runtime.dataprocInstances,
    runtime.userJupyterExtensionConfig,
    runtime.autopauseThreshold,
    runtime.defaultClientId,
    runtime.stopAfterCreation,
    runtime.runtimeImages,
    runtime.scopes,
    runtime.welderEnabled,
    runtime.patchInProgress,
    runtime.customEnvironmentVariables,
    diskConfig
  )
}

final case class BatchNodepoolCreateRequest(numNodepools: NumNodepools, kubernetesRuntimeConfig: Option[KubernetesRuntimeConfig])

final case class CreateAppRequest(kubernetesRuntimeConfig: Option[KubernetesRuntimeConfig],
                                  appType: AppType,
                                  diskConfig: Option[PersistentDiskRequest],
                                  labels: LabelMap = Map.empty,
                                  customEnvironmentVariables: Map[String, String])

final case class DeleteAppParams(userInfo: UserInfo,
                                 googleProject: GoogleProject,
                                 appName: AppName,
                                 deleteDisk: Boolean)

final case class GetAppResponse(kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                errors: List[AppError],
                                status: AppStatus,
                                proxyUrls: Map[ServiceName, URL],
                                diskName: Option[DiskName])

object GetAppResponse {
  def fromDbResult(appResult: GetAppResult): GetAppResponse = {
    val hasError = appResult.cluster.status == KubernetesClusterStatus.Error ||
      appResult.nodepool.status == NodepoolStatus.Error ||
      appResult.app.status == AppStatus.Error ||
      appResult.app.errors.length > 0
    GetAppResponse(
      KubernetesRuntimeConfig(
        appResult.nodepool.numNodes,
        appResult.nodepool.machineType,
        appResult.nodepool.autoscalingEnabled
      ),
      appResult.app.errors,
      if (hasError) AppStatus.Error else appResult.app.status,
      appResult.app.getProxyUrls(appResult.cluster.googleProject, Config.proxyConfig.proxyUrlBase),
      appResult.app.appResources.disk.map(_.name)
    )
  }
}

final case class ListAppResponse(googleProject: GoogleProject,
                                 kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                 errors: List[AppError],
                                 status: AppStatus,
                                 proxyUrls: Map[ServiceName, URL],
                                 appName: AppName,
                                 diskName: Option[DiskName])

object ListAppResponse {
  def fromCluster(c: KubernetesCluster): List[ListAppResponse] =
    c.nodepools.flatMap(n =>
      n.apps.map { a =>
        val hasError = c.status == KubernetesClusterStatus.Error ||
          n.status == NodepoolStatus.Error ||
          a.status == AppStatus.Error ||
          a.errors.length > 0
        ListAppResponse(
          c.googleProject,
          KubernetesRuntimeConfig(
            n.numNodes,
            n.machineType,
            n.autoscalingEnabled
          ),
          a.errors,
          if (hasError) AppStatus.Error else a.status,
          a.getProxyUrls(c.googleProject, Config.proxyConfig.proxyUrlBase),
          a.appName,
          a.appResources.disk.map(_.name)
        )
      }
    )

}
