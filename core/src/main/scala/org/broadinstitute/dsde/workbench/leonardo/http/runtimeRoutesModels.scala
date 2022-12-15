package org.broadinstitute.dsde.workbench.leonardo
package http

import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}

import java.net.URL
import java.time.Instant
import JsonCodec._
import io.circe.Encoder

import scala.concurrent.duration.FiniteDuration

sealed trait RuntimeConfigRequest extends Product with Serializable {
  def cloudService: CloudService
}
object RuntimeConfigRequest {
  final case class GceConfig(
    machineType: Option[MachineTypeName],
    diskSize: Option[DiskSize],
    zone: Option[ZoneName],
    gpuConfig: Option[GpuConfig]
  ) extends RuntimeConfigRequest {
    val cloudService: CloudService = CloudService.GCE
  }

  final case class GceWithPdConfig(
    machineType: Option[MachineTypeName],
    persistentDisk: PersistentDiskRequest,
    zone: Option[ZoneName],
    gpuConfig: Option[GpuConfig]
  ) extends RuntimeConfigRequest {
    val cloudService: CloudService = CloudService.GCE
  }

  final case class DataprocConfig(numberOfWorkers: Option[Int],
                                  masterMachineType: Option[MachineTypeName],
                                  masterDiskSize: Option[DiskSize], // min 10
                                  // worker settings are None when numberOfWorkers is 0
                                  workerMachineType: Option[MachineTypeName] = None,
                                  workerDiskSize: Option[DiskSize] = None, // min 10
                                  numberOfWorkerLocalSSDs: Option[Int] = None, // min 0 max 8
                                  numberOfPreemptibleWorkers: Option[Int] = None,
                                  properties: Map[String, String],
                                  region: Option[RegionName],
                                  componentGatewayEnabled: Boolean,
                                  workerPrivateAccess: Boolean
  ) extends RuntimeConfigRequest {
    val cloudService: CloudService = CloudService.Dataproc
  }
}

final case class CreateRuntimeRequest(
  labels: LabelMap,
  userScriptUri: Option[UserScriptPath],
  startUserScriptUri: Option[UserScriptPath],
  runtimeConfig: Option[RuntimeConfigRequest],
  userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
  autopause: Option[Boolean],
  autopauseThreshold: Option[FiniteDuration],
  defaultClientId: Option[String],
  toolDockerImage: Option[ContainerImage],
  welderRegistry: Option[ContainerRegistry],
  scopes: Set[String],
  customEnvironmentVariables: Map[String, String],
  checkToolsInterruptAfter: Option[FiniteDuration]
)

final case class CreateRuntimeResponse(traceId: TraceId)

sealed trait UpdateRuntimeConfigRequest extends Product with Serializable {
  def cloudService: CloudService
}
object UpdateRuntimeConfigRequest {
  final case class GceConfig(updatedMachineType: Option[MachineTypeName], updatedDiskSize: Option[DiskSize])
      extends UpdateRuntimeConfigRequest {
    val cloudService: CloudService = CloudService.GCE
  }
  final case class DataprocConfig(updatedMasterMachineType: Option[MachineTypeName],
                                  updatedMasterDiskSize: Option[DiskSize],
                                  updatedNumberOfWorkers: Option[Int],
                                  updatedNumberOfPreemptibleWorkers: Option[Int]
  ) extends UpdateRuntimeConfigRequest {
    val cloudService: CloudService = CloudService.Dataproc
  }
}

final case class UpdateRuntimeRequest(updatedRuntimeConfig: Option[UpdateRuntimeConfigRequest],
                                      allowStop: Boolean,
                                      updateAutopauseEnabled: Option[Boolean],
                                      updateAutopauseThreshold: Option[FiniteDuration],
                                      labelsToUpsert: LabelMap,
                                      labelsToDelete: Set[String]
)

final case class GetRuntimeResponse(id: Long,
                                    samResource: RuntimeSamResourceId,
                                    clusterName: RuntimeName,
                                    cloudContext: CloudContext,
                                    serviceAccountInfo: WorkbenchEmail,
                                    asyncRuntimeFields: Option[AsyncRuntimeFields],
                                    auditInfo: AuditInfo,
                                    kernelFoundBusyDate: Option[Instant],
                                    runtimeConfig: RuntimeConfig,
                                    clusterUrl: URL,
                                    status: RuntimeStatus,
                                    labels: LabelMap,
                                    userScriptUri: Option[UserScriptPath],
                                    startUserScriptUri: Option[UserScriptPath],
                                    errors: List[RuntimeError],
                                    userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                    autopauseThreshold: Int,
                                    defaultClientId: Option[String],
                                    clusterImages: Set[RuntimeImage],
                                    scopes: Set[String],
                                    welderEnabled: Boolean,
                                    patchInProgress: Boolean,
                                    customClusterEnvironmentVariables: Map[String, String],
                                    diskConfig: Option[DiskConfig]
)

object GetRuntimeResponse {
  def fromRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig, diskConfig: Option[DiskConfig]) = GetRuntimeResponse(
    runtime.id,
    runtime.samResource,
    runtime.runtimeName,
    runtime.cloudContext,
    runtime.serviceAccount,
    runtime.asyncRuntimeFields,
    runtime.auditInfo,
    runtime.kernelFoundBusyDate,
    runtimeConfig,
    runtime.proxyUrl,
    runtime.status,
    runtime.labels,
    runtime.userScriptUri,
    runtime.startUserScriptUri,
    runtime.errors,
    runtime.userJupyterExtensionConfig,
    runtime.autopauseThreshold,
    runtime.defaultClientId,
    runtime.runtimeImages,
    runtime.scopes,
    runtime.welderEnabled,
    runtime.patchInProgress,
    runtime.customEnvironmentVariables,
    diskConfig
  )
}

final case class ListRuntimeResponse2(id: Long,
                                      workspaceId: Option[WorkspaceId],
                                      samResource: RuntimeSamResourceId,
                                      clusterName: RuntimeName,
                                      cloudContext: CloudContext,
                                      auditInfo: AuditInfo,
                                      runtimeConfig: RuntimeConfig,
                                      proxyUrl: URL,
                                      status: RuntimeStatus,
                                      labels: LabelMap,
                                      patchInProgress: Boolean
)

object RuntimeRoutesCodec {
  implicit val listRuntimeResponseEncoder: Encoder[ListRuntimeResponse2] = Encoder.forProduct11(
    "id",
    "workspaceId",
    "runtimeName",
    "googleProject",
    "cloudContext",
    "auditInfo",
    "runtimeConfig",
    "proxyUrl",
    "status",
    "labels",
    "patchInProgress"
  )(x =>
    (
      x.id,
      x.workspaceId,
      x.clusterName,
      x.cloudContext.asString,
      x.cloudContext,
      x.auditInfo,
      x.runtimeConfig,
      x.proxyUrl,
      x.status,
      x.labels,
      x.patchInProgress
    )
  )
}
