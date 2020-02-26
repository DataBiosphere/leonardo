package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.net.URL
import java.time.Instant

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.{GcsPathFormat => _}
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}

/** Runtime configuration in the createRuntime request */
sealed trait RuntimeConfigRequest extends Product with Serializable {
  def cloudService: CloudService
}
object RuntimeConfigRequest {
  final case class GceConfig(
    machineType: Option[MachineTypeName],
    diskSize: Option[Int]
  ) extends RuntimeConfigRequest {
    val cloudService: CloudService = CloudService.GCE
  }

  final case class DataprocConfig(numberOfWorkers: Option[Int],
                                  masterMachineType: Option[MachineTypeName],
                                  masterDiskSize: Option[Int], //min 10
                                  // worker settings are None when numberOfWorkers is 0
                                  workerMachineType: Option[MachineTypeName] = None,
                                  workerDiskSize: Option[Int] = None, //min 10
                                  numberOfWorkerLocalSSDs: Option[Int] = None, //min 0 max 8
                                  numberOfPreemptibleWorkers: Option[Int] = None)
      extends RuntimeConfigRequest {
    val cloudService: CloudService = CloudService.Dataproc

    def toRuntimeConfigDataprocConfig(default: RuntimeConfig.DataprocConfig): RuntimeConfig.DataprocConfig = {
      val minimumDiskSize = 10
      val masterDiskSizeFinal = math.max(minimumDiskSize, masterDiskSize.getOrElse(default.masterDiskSize))
      numberOfWorkers match {
        case None | Some(0) =>
          RuntimeConfig.DataprocConfig(
            0,
            masterMachineType.getOrElse(default.masterMachineType),
            masterDiskSizeFinal
          )
        case Some(numWorkers) =>
          val wds = workerDiskSize.orElse(default.workerDiskSize)
          RuntimeConfig.DataprocConfig(
            numWorkers,
            masterMachineType.getOrElse(default.masterMachineType),
            masterDiskSizeFinal,
            workerMachineType.orElse(default.workerMachineType),
            wds.map(s => math.max(minimumDiskSize, s)),
            numberOfWorkerLocalSSDs.orElse(default.numberOfWorkerLocalSSDs),
            numberOfPreemptibleWorkers.orElse(default.numberOfPreemptibleWorkers)
          )
      }
    }
  }
}

/** The createRuntime request itself */
final case class CreateRuntimeRequest(labels: LabelMap = Map.empty,
                                      jupyterExtensionUri: Option[GcsPath] = None,
                                      jupyterUserScriptUri: Option[UserScriptPath] = None,
                                      jupyterStartUserScriptUri: Option[UserScriptPath] = None,
                                      runtimeConfig: Option[RuntimeConfigRequest] = None,
                                      dataprocProperties: Map[String, String] = Map.empty,
                                      stopAfterCreation: Option[Boolean] = None,
                                      allowStop: Boolean = false,
                                      userJupyterExtensionConfig: Option[UserJupyterExtensionConfig] = None,
                                      autopause: Option[Boolean] = None,
                                      autopauseThreshold: Option[Int] = None,
                                      defaultClientId: Option[String] = None,
                                      jupyterDockerImage: Option[ContainerImage] = None,
                                      toolDockerImage: Option[ContainerImage] = None,
                                      welderDockerImage: Option[ContainerImage] = None,
                                      scopes: Set[String] = Set.empty,
                                      enableWelder: Option[Boolean] = None,
                                      customClusterEnvironmentVariables: Map[String, String] = Map.empty)

object CreateRuntimeRequest {
  def toRuntime(request: CreateRuntimeRequest,
                internalId: RuntimeInternalId,
                userEmail: WorkbenchEmail,
                runtimeName: RuntimeName,
                googleProject: GoogleProject,
                serviceAccountInfo: ServiceAccountInfo,
                machineConfig: RuntimeConfig.DataprocConfig,
                proxyUrlBase: String,
                autopauseThreshold: Int,
                scopes: Set[String],
                runtimeImages: Set[RuntimeImage],
                timestamp: Instant): Runtime =
    Runtime(
      internalId = internalId,
      runtimeName = runtimeName,
      googleProject = googleProject,
      serviceAccountInfo = serviceAccountInfo,
      asyncRuntimeFields = None,
      auditInfo = AuditInfo(userEmail, timestamp, None, timestamp, None),
      dataprocProperties = request.dataprocProperties,
      proxyUrl = Runtime.getProxyUrl(proxyUrlBase, googleProject, runtimeName, runtimeImages, request.labels),
      status = RuntimeStatus.Creating,
      labels = request.labels,
      jupyterExtensionUri = request.jupyterExtensionUri,
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
      scopes = request.scopes,
      welderEnabled = request.enableWelder.getOrElse(false),
      customClusterEnvironmentVariables = request.customClusterEnvironmentVariables,
      runtimeConfigId = RuntimeConfigId(-1)
    )

}

final case class ListRuntimeResponse(id: Long,
                                     internalId: RuntimeInternalId,
                                     clusterName: RuntimeName,
                                     googleProject: GoogleProject,
                                     serviceAccountInfo: ServiceAccountInfo,
                                     asyncRuntimeFields: Option[AsyncRuntimeFields],
                                     auditInfo: AuditInfo,
                                     machineConfig: RuntimeConfig,
                                     clusterUrl: URL,
                                     status: RuntimeStatus,
                                     labels: LabelMap,
                                     jupyterExtensionUri: Option[GcsPath],
                                     jupyterUserScriptUri: Option[UserScriptPath],
                                     dataprocInstances: Set[DataprocInstance],
                                     autopauseThreshold: Int,
                                     defaultClientId: Option[String],
                                     stopAfterCreation: Boolean,
                                     welderEnabled: Boolean)

object ListRuntimeResponse {
  def fromRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig): ListRuntimeResponse =
    ListRuntimeResponse(
      runtime.id,
      runtime.internalId,
      runtime.runtimeName,
      runtime.googleProject,
      runtime.serviceAccountInfo,
      runtime.asyncRuntimeFields,
      runtime.auditInfo,
      runtimeConfig,
      runtime.proxyUrl,
      runtime.status,
      runtime.labels,
      runtime.jupyterExtensionUri,
      runtime.jupyterUserScriptUri,
      runtime.dataprocInstances,
      runtime.autopauseThreshold,
      runtime.defaultClientId,
      runtime.stopAfterCreation,
      runtime.welderEnabled
    )
}

final case class GetRuntimeResponse(id: Long,
                                    internalId: RuntimeInternalId,
                                    clusterName: RuntimeName,
                                    googleProject: GoogleProject,
                                    serviceAccountInfo: ServiceAccountInfo,
                                    asyncRuntimeFields: Option[AsyncRuntimeFields],
                                    auditInfo: AuditInfo,
                                    dataprocProperties: Map[String, String],
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
                                    customClusterEnvironmentVariables: Map[String, String])

object GetRuntimeResponse {
  def fromRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig) = GetRuntimeResponse(
    runtime.id,
    runtime.internalId,
    runtime.runtimeName,
    runtime.googleProject,
    runtime.serviceAccountInfo,
    runtime.asyncRuntimeFields,
    runtime.auditInfo,
    runtime.dataprocProperties,
    runtimeConfig,
    runtime.proxyUrl,
    runtime.status,
    runtime.labels,
    runtime.jupyterExtensionUri,
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
    runtime.customClusterEnvironmentVariables
  )
}

// Currently, CreateRuntimeAPIResponse has exactly the same fields as GetRuntimeResponse, but going forward, when we can,
// we should deprecate and remove some of fields for createRuntime request
final case class CreateRuntimeAPIResponse(id: Long,
                                          internalId: RuntimeInternalId,
                                          clusterName: RuntimeName,
                                          googleProject: GoogleProject,
                                          serviceAccountInfo: ServiceAccountInfo,
                                          asyncRuntimeFields: Option[AsyncRuntimeFields],
                                          auditInfo: AuditInfo,
                                          dataprocProperties: Map[String, String],
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
                                          customClusterEnvironmentVariables: Map[String, String])

object CreateRuntimeAPIResponse {
  def fromRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig) = CreateRuntimeAPIResponse(
    runtime.id,
    runtime.internalId,
    runtime.runtimeName,
    runtime.googleProject,
    runtime.serviceAccountInfo,
    runtime.asyncRuntimeFields,
    runtime.auditInfo,
    runtime.dataprocProperties,
    runtimeConfig,
    runtime.proxyUrl,
    runtime.status,
    runtime.labels,
    runtime.jupyterExtensionUri,
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
    runtime.customClusterEnvironmentVariables
  )
}

// Currently, UpdateRuntimeResponse has exactly the same fields as GetRuntimeResponse, but going forward, when we can,
// we should deprecate and remove some of fields for updateRuntime request
final case class UpdateRuntimeResponse(id: Long,
                                       internalId: RuntimeInternalId,
                                       clusterName: RuntimeName,
                                       googleProject: GoogleProject,
                                       serviceAccountInfo: ServiceAccountInfo,
                                       asyncRuntimeFields: Option[AsyncRuntimeFields],
                                       auditInfo: AuditInfo,
                                       dataprocProperties: Map[String, String],
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
                                       customClusterEnvironmentVariables: Map[String, String])

object UpdateRuntimeResponse {
  def fromCluster(runtime: Runtime, runtimeConfig: RuntimeConfig) = UpdateRuntimeResponse(
    runtime.id,
    runtime.internalId,
    runtime.runtimeName,
    runtime.googleProject,
    runtime.serviceAccountInfo,
    runtime.asyncRuntimeFields,
    runtime.auditInfo,
    runtime.dataprocProperties,
    runtimeConfig,
    runtime.proxyUrl,
    runtime.status,
    runtime.labels,
    runtime.jupyterExtensionUri,
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
    runtime.customClusterEnvironmentVariables
  )
}
