package org.broadinstitute.dsde.workbench.leonardo
package monitor

import ca.mrvisser.sealerate
import cats.syntax.all._
import com.google.cloud.compute.v1.Disk
import enumeratum.{Enum, EnumEntry}
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.JsonCodec.{traceIdDecoder, traceIdEncoder}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.NamespaceName
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.config.GalaxyDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.StorageContainerResponse
import org.broadinstitute.dsde.workbench.leonardo.http.{
  dataprocInCreateRuntimeMsgToDataprocRuntime,
  RuntimeConfigRequest
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterNodepoolAction.{
  CreateCluster,
  CreateClusterAndNodepool,
  CreateNodepool
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail, WorkbenchException}

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

sealed trait RuntimeConfigInCreateRuntimeMessage extends Product with Serializable {
  def cloudService: CloudService
  def machineType: MachineTypeName
}
object RuntimeConfigInCreateRuntimeMessage {
  final case class GceConfig(
    machineType: MachineTypeName,
    diskSize: DiskSize,
    bootDiskSize: DiskSize,
    zone: ZoneName,
    gpuConfig: Option[GpuConfig]
  ) extends RuntimeConfigInCreateRuntimeMessage {
    val cloudService: CloudService = CloudService.GCE
  }

  final case class GceWithPdConfig(machineType: MachineTypeName,
                                   persistentDiskId: DiskId,
                                   bootDiskSize: DiskSize,
                                   zone: ZoneName,
                                   gpuConfig: Option[GpuConfig]
  ) extends RuntimeConfigInCreateRuntimeMessage {
    val cloudService: CloudService = CloudService.GCE
  }

  final case class DataprocConfig(numberOfWorkers: Int,
                                  masterMachineType: MachineTypeName,
                                  masterDiskSize: DiskSize, // min 10
                                  // worker settings are None when numberOfWorkers is 0
                                  workerMachineType: Option[MachineTypeName] = None,
                                  workerDiskSize: Option[DiskSize] = None, // min 10
                                  numberOfWorkerLocalSSDs: Option[Int] = None, // min 0 max 8
                                  numberOfPreemptibleWorkers: Option[Int] = None,
                                  properties: Map[String, String],
                                  region: RegionName,
                                  componentGatewayEnabled: Boolean,
                                  workerPrivateAccess: Boolean
  ) extends RuntimeConfigInCreateRuntimeMessage {
    val cloudService: CloudService = CloudService.Dataproc
    val machineType: MachineTypeName = masterMachineType
    val diskSize: DiskSize = masterDiskSize
  }

  def fromDataprocInRuntimeConfigRequest(
    dataproc: RuntimeConfigRequest.DataprocConfig,
    default: RuntimeConfig.DataprocConfig
  ): RuntimeConfigInCreateRuntimeMessage.DataprocConfig = {
    val minimumDiskSize = 10
    val masterDiskSizeFinal = math.max(minimumDiskSize, dataproc.masterDiskSize.getOrElse(default.masterDiskSize).gb)
    dataproc.numberOfWorkers match {
      case None | Some(0) =>
        RuntimeConfigInCreateRuntimeMessage.DataprocConfig(
          0,
          dataproc.masterMachineType.getOrElse(default.masterMachineType),
          DiskSize(masterDiskSizeFinal),
          None,
          None,
          None,
          None,
          dataproc.properties,
          dataproc.region.getOrElse(default.region),
          dataproc.componentGatewayEnabled,
          dataproc.workerPrivateAccess
        )
      case Some(numWorkers) =>
        val wds = dataproc.workerDiskSize.orElse(default.workerDiskSize)
        RuntimeConfigInCreateRuntimeMessage.DataprocConfig(
          numWorkers,
          dataproc.masterMachineType.getOrElse(default.masterMachineType),
          DiskSize(masterDiskSizeFinal),
          dataproc.workerMachineType.orElse(default.workerMachineType),
          wds.map(s => DiskSize(math.max(minimumDiskSize, s.gb))),
          dataproc.numberOfWorkerLocalSSDs.orElse(default.numberOfWorkerLocalSSDs),
          dataproc.numberOfPreemptibleWorkers.orElse(default.numberOfPreemptibleWorkers),
          dataproc.properties,
          dataproc.region.getOrElse(default.region),
          dataproc.componentGatewayEnabled,
          dataproc.workerPrivateAccess
        )
    }
  }
}

sealed trait LeoPubsubMessageType extends EnumEntry with Serializable with Product {
  def asString: String
  override def toString = asString
}
object LeoPubsubMessageType extends Enum[LeoPubsubMessageType] {
  val values = findValues

  final case object CreateRuntime extends LeoPubsubMessageType {
    val asString = "createRuntime"
  }
  final case object DeleteRuntime extends LeoPubsubMessageType {
    val asString = "deleteRuntime"
  }
  final case object StopRuntime extends LeoPubsubMessageType {
    val asString = "stopRuntime"
  }
  final case object StartRuntime extends LeoPubsubMessageType {
    val asString = "startRuntime"
  }
  final case object UpdateRuntime extends LeoPubsubMessageType {
    val asString = "updateRuntime"
  }

  final case object CreateDisk extends LeoPubsubMessageType {
    val asString = "createDisk"
  }
  final case object UpdateDisk extends LeoPubsubMessageType {
    val asString = "updateDisk"
  }
  final case object DeleteDisk extends LeoPubsubMessageType {
    val asString = "deleteDisk"
  }

  final case object CreateApp extends LeoPubsubMessageType {
    val asString = "createApp"
  }
  final case object DeleteApp extends LeoPubsubMessageType {
    val asString = "deleteApp"
  }

  final case object StopApp extends LeoPubsubMessageType {
    val asString = "stopApp"
  }
  final case object StartApp extends LeoPubsubMessageType {
    val asString = "startApp"
  }
  final case object UpdateApp extends LeoPubsubMessageType {
    val asString = "updateApp"
  }
  final case object CreateAzureRuntime extends LeoPubsubMessageType {
    val asString = "createAzureRuntime"
  }
  final case object DeleteAzureRuntime extends LeoPubsubMessageType {
    val asString = "deleteAzureRuntime"
  }

  final case object DeleteDiskV2 extends LeoPubsubMessageType {
    val asString = "deleteDiskV2"
  }

  final case object CreateAppV2 extends LeoPubsubMessageType {
    val asString = "createAppV2"
  }

  final case object DeleteAppV2 extends LeoPubsubMessageType {
    val asString = "deleteAppV2"
  }
}

sealed trait LeoPubsubMessage {
  def traceId: Option[TraceId]
  def messageType: LeoPubsubMessageType
}

object LeoPubsubMessage {
  final case class CreateRuntimeMessage(runtimeId: Long,
                                        runtimeProjectAndName: RuntimeProjectAndName,
                                        serviceAccountInfo: WorkbenchEmail,
                                        asyncRuntimeFields: Option[AsyncRuntimeFields],
                                        auditInfo: AuditInfo,
                                        userScriptUri: Option[UserScriptPath],
                                        startUserScriptUri: Option[UserScriptPath],
                                        userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                        defaultClientId: Option[String],
                                        runtimeImages: Set[RuntimeImage],
                                        scopes: Set[String],
                                        welderEnabled: Boolean,
                                        customEnvironmentVariables: Map[String, String],
                                        runtimeConfig: RuntimeConfigInCreateRuntimeMessage,
                                        traceId: Option[TraceId],
                                        checkToolsInterruptAfter: Option[Int]
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.CreateRuntime
  }

  object CreateRuntimeMessage {
    def fromRuntime(runtime: Runtime,
                    runtimeConfig: RuntimeConfigInCreateRuntimeMessage,
                    traceId: Option[TraceId],
                    checkToolsInterruptAfter: Option[FiniteDuration]
    ): CreateRuntimeMessage =
      CreateRuntimeMessage(
        runtime.id,
        RuntimeProjectAndName(runtime.cloudContext, runtime.runtimeName),
        runtime.serviceAccount,
        runtime.asyncRuntimeFields,
        runtime.auditInfo,
        runtime.userScriptUri,
        runtime.startUserScriptUri,
        runtime.userJupyterExtensionConfig,
        runtime.defaultClientId,
        runtime.runtimeImages,
        runtime.scopes,
        runtime.welderEnabled,
        runtime.customEnvironmentVariables,
        runtimeConfig,
        traceId,
        checkToolsInterruptAfter.map(x => x.toMinutes.toInt)
      )
  }

  final case class CreateDiskMessage(diskId: DiskId,
                                     googleProject: GoogleProject,
                                     name: DiskName,
                                     zone: ZoneName,
                                     size: DiskSize,
                                     diskType: DiskType,
                                     blockSize: BlockSize,
                                     traceId: Option[TraceId],
                                     sourceDisk: Option[DiskLink]
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.CreateDisk
  }

  object CreateDiskMessage {
    def fromDisk(disk: PersistentDisk, traceId: Option[TraceId]): CreateDiskMessage =
      CreateDiskMessage(
        disk.id,
        GoogleProject(
          disk.cloudContext.asString
        ), // TODO: we might think about use cloudContext in CreateDiskMessage to support Azure
        disk.name,
        disk.zone,
        disk.size,
        disk.diskType,
        disk.blockSize,
        traceId,
        disk.sourceDisk
      )
  }

  final case class CreateAppMessage(
    project: GoogleProject,
    clusterNodepoolAction: Option[ClusterNodepoolAction],
    appId: AppId,
    appName: AppName,
    createDisk: Option[DiskId],
    customEnvironmentVariables: Map[String, String],
    appType: AppType,
    namespaceName: NamespaceName,
    machineType: Option[
      AppMachineType
    ], // Currently only galaxy is using this info, but potentially other apps might take advantage of this info too
    traceId: Option[TraceId],
    enableIntraNodeVisibility: Boolean
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.CreateApp
  }

  final case class CreateAppV2Message(
    appId: AppId,
    appName: AppName,
    workspaceId: WorkspaceId,
    cloudContext: CloudContext,
    billingProfileId: BillingProfileId,
    traceId: Option[TraceId]
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.CreateAppV2
  }

  final case class DeleteAppMessage(appId: AppId,
                                    appName: AppName,
                                    project: GoogleProject,
                                    diskId: Option[DiskId],
                                    traceId: Option[TraceId]
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.DeleteApp
  }

  final case class DeleteAppV2Message(appId: AppId,
                                      appName: AppName,
                                      workspaceId: WorkspaceId,
                                      cloudContext: CloudContext,
                                      diskId: Option[DiskId],
                                      billingProfileId: BillingProfileId,
                                      traceId: Option[TraceId]
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.DeleteAppV2
  }

  final case class StopAppMessage(appId: AppId, appName: AppName, project: GoogleProject, traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.StopApp
  }

  final case class StartAppMessage(appId: AppId, appName: AppName, project: GoogleProject, traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.StartApp
  }

  final case class DeleteRuntimeMessage(runtimeId: Long,
                                        persistentDiskToDelete: Option[DiskId],
                                        traceId: Option[TraceId]
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.DeleteRuntime
  }

  final case class DeleteDiskMessage(diskId: DiskId, traceId: Option[TraceId]) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.DeleteDisk
  }

  final case class StopRuntimeMessage(runtimeId: Long, traceId: Option[TraceId]) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.StopRuntime
  }

  final case class StartRuntimeMessage(runtimeId: Long, traceId: Option[TraceId]) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.StartRuntime
  }

  final case class UpdateRuntimeMessage(runtimeId: Long,
                                        newMachineType: Option[MachineTypeName],
                                        // if true, the runtime will be stopped and undergo a followup transition
                                        stopToUpdateMachineType: Boolean,
                                        diskUpdate: Option[DiskUpdate],
                                        newNumWorkers: Option[Int],
                                        newNumPreemptibles: Option[Int],
                                        traceId: Option[TraceId]
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.UpdateRuntime
  }

  final case class UpdateDiskMessage(diskId: DiskId, newSize: DiskSize, traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.UpdateDisk
  }

  final case class UpdateAppMessage(appId: AppId,
                                    appName: AppName,
                                    cloudContext: CloudContext,
                                    workspaceId: Option[WorkspaceId],
                                    googleProject: Option[GoogleProject],
                                    traceId: Option[TraceId]
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.UpdateApp
  }

  final case class CreateAzureRuntimeMessage(
    runtimeId: Long,
    workspaceId: WorkspaceId,
    useExistingDisk: Boolean, // if using existing disk, will attach pd to new runtime
    traceId: Option[TraceId],
    workspaceName: String,
    billingProfileId: BillingProfileId
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.CreateAzureRuntime
  }

  final case class DeleteAzureRuntimeMessage(runtimeId: Long,
                                             diskIdToDelete: Option[DiskId],
                                             workspaceId: WorkspaceId,
                                             wsmResourceId: Option[WsmControlledResourceId],
                                             billingProfileId: BillingProfileId,
                                             traceId: Option[TraceId]
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.DeleteAzureRuntime
  }

  final case class DeleteDiskV2Message(diskId: DiskId,
                                       workspaceId: WorkspaceId,
                                       cloudContext: CloudContext,
                                       wsmResourceId: Option[WsmControlledResourceId],
                                       traceId: Option[TraceId]
  ) extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.DeleteDiskV2
  }

}

sealed trait ClusterNodepoolActionType extends Product with Serializable {
  def asString: String
}
object ClusterNodepoolActionType {
  final case object CreateClusterAndNodepool extends ClusterNodepoolActionType {
    val asString: String = "createClusterAndNodepool"
  }
  final case object CreateNodepool extends ClusterNodepoolActionType {
    val asString: String = "createNodepool"
  }
  final case object CreateCluster extends ClusterNodepoolActionType {
    val asString: String = "createCluster"
  }

  def values: Set[ClusterNodepoolActionType] = sealerate.values[ClusterNodepoolActionType]
  def stringToObject: Map[String, ClusterNodepoolActionType] = values.map(v => v.asString -> v).toMap
}

sealed trait ClusterNodepoolAction extends Product with Serializable {
  def actionType: ClusterNodepoolActionType
}
object ClusterNodepoolAction {
  final case class CreateClusterAndNodepool(clusterId: KubernetesClusterLeoId,
                                            defaultNodepoolId: NodepoolLeoId,
                                            nodepoolId: NodepoolLeoId
  ) extends ClusterNodepoolAction {
    val actionType: ClusterNodepoolActionType = ClusterNodepoolActionType.CreateClusterAndNodepool

  }
  final case class CreateNodepool(nodepoolId: NodepoolLeoId) extends ClusterNodepoolAction {
    val actionType: ClusterNodepoolActionType = ClusterNodepoolActionType.CreateNodepool
  }
  final case class CreateCluster(clusterId: KubernetesClusterLeoId) extends ClusterNodepoolAction {
    val actionType: ClusterNodepoolActionType = ClusterNodepoolActionType.CreateCluster
  }
}

sealed trait DiskUpdate extends Product with Serializable {
  def newDiskSize: DiskSize
}
object DiskUpdate {
  final case class NoPdSizeUpdate(override val newDiskSize: DiskSize) extends DiskUpdate
  final case class PdSizeUpdate(diskId: DiskId, diskName: DiskName, override val newDiskSize: DiskSize)
      extends DiskUpdate
  final case class Dataproc(override val newDiskSize: DiskSize, masterInstance: DataprocInstance) extends DiskUpdate
}

final case class RuntimePatchDetails(runtimeId: Long, runtimeStatus: RuntimeStatus) extends Product with Serializable

final case class PubsubException(message: String) extends WorkbenchException(message)

object LeoPubsubCodec {
  implicit val runtimePatchDetailsDecoder: Decoder[RuntimePatchDetails] =
    Decoder.forProduct2("clusterId", "clusterStatus")(RuntimePatchDetails.apply)

  implicit val deleteRuntimeMessageDecoder: Decoder[DeleteRuntimeMessage] =
    Decoder.forProduct3("runtimeId", "persistentDiskToDelete", "traceId")(DeleteRuntimeMessage.apply)

  implicit val stopRuntimeDecoder: Decoder[StopRuntimeMessage] =
    Decoder.forProduct2("runtimeId", "traceId")(StopRuntimeMessage.apply)

  implicit val startRuntimeDecoder: Decoder[StartRuntimeMessage] =
    Decoder.forProduct2("runtimeId", "traceId")(StartRuntimeMessage.apply)

  implicit val diskUpdateDecoder: Decoder[DiskUpdate] = Decoder.instance { c =>
    for {
      diskId <- c.downField("diskId").as[Option[DiskId]]
      diskSize <- c.downField("newDiskSize").as[DiskSize]
      res <- diskId.fold(for {
        masterInstance <- c.downField("masterInstance").as[Option[DataprocInstance]]
      } yield masterInstance match {
        case Some(instance) =>
          DiskUpdate.Dataproc(diskSize, instance): DiskUpdate
        case None =>
          DiskUpdate.NoPdSizeUpdate(diskSize): DiskUpdate
      })(diskId =>
        for {
          diskName <- c.downField("diskName").as[DiskName]
        } yield DiskUpdate.PdSizeUpdate(diskId, diskName, diskSize): DiskUpdate
      )
    } yield res
  }

  implicit val updateRuntimeDecoder: Decoder[UpdateRuntimeMessage] =
    Decoder.forProduct7("runtimeId",
                        "newMachineType",
                        "stopToUpdateMachineType",
                        "diskUpdate",
                        "newNumWorkers",
                        "newNumPreemptibles",
                        "traceId"
    )(
      UpdateRuntimeMessage.apply
    )

  implicit val createDiskDecoder: Decoder[CreateDiskMessage] =
    Decoder.forProduct9("diskId",
                        "googleProject",
                        "name",
                        "zone",
                        "size",
                        "diskType",
                        "blockSize",
                        "traceId",
                        "sourceDisk"
    )(
      CreateDiskMessage.apply
    )

  implicit val updateDiskDecoder: Decoder[UpdateDiskMessage] =
    Decoder.forProduct3("diskId", "newSize", "traceId")(UpdateDiskMessage.apply)

  implicit val deleteDiskDecoder: Decoder[DeleteDiskMessage] =
    Decoder.forProduct2("diskId", "traceId")(DeleteDiskMessage.apply)

  implicit val appIdDecoder: Decoder[AppId] = Decoder.decodeLong.map(AppId)

  implicit val clusterNodepoolActionTypeDecoder: Decoder[ClusterNodepoolActionType] =
    Decoder.decodeString.emap(x =>
      ClusterNodepoolActionType.stringToObject.get(x).toRight(s"Invalid cluster nodepool action type: $x")
    )

  implicit val createClusterAndNodepoolDecoder: Decoder[CreateClusterAndNodepool] =
    Decoder.forProduct3("clusterId", "defaultNodepoolId", "nodepoolId")(CreateClusterAndNodepool.apply)

  implicit val createNodepoolDecoder: Decoder[CreateNodepool] =
    Decoder.forProduct1("nodepoolId")(CreateNodepool.apply)

  implicit val createClusterDecoder: Decoder[CreateCluster] =
    Decoder.forProduct1("clusterId")(CreateCluster.apply)

  implicit val clusterNodepoolActionDecoder: Decoder[ClusterNodepoolAction] = Decoder.instance { message =>
    for {
      actionType <- message.downField("actionType").as[ClusterNodepoolActionType]
      value <- actionType match {
        case ClusterNodepoolActionType.CreateClusterAndNodepool => message.as[CreateClusterAndNodepool]
        case ClusterNodepoolActionType.CreateNodepool           => message.as[CreateNodepool]
        case ClusterNodepoolActionType.CreateCluster            => message.as[CreateCluster]
      }
    } yield value
  }

  implicit val createAppMessageDecoder: Decoder[CreateAppMessage] =
    Decoder.forProduct11(
      "project",
      "clusterNodepoolAction",
      "appId",
      "appName",
      "createDisk",
      "customEnvironmentVariables",
      "appType",
      "namespaceName",
      "machineType",
      "traceId",
      "enableIntraNodeVisibility"
    )(CreateAppMessage.apply)

  implicit val deleteAppDecoder: Decoder[DeleteAppMessage] =
    Decoder.forProduct5("appId", "appName", "project", "diskId", "traceId")(DeleteAppMessage.apply)

  implicit val stopAppDecoder: Decoder[StopAppMessage] =
    Decoder.forProduct4("appId", "appName", "project", "traceId")(StopAppMessage.apply)

  implicit val startAppDecoder: Decoder[StartAppMessage] =
    Decoder.forProduct4("appId", "appName", "project", "traceId")(StartAppMessage.apply)

  implicit val updateAppDecoder: Decoder[UpdateAppMessage] =
    Decoder.forProduct6("appId", "appName", "cloudContext", "workspaceId", "googleProject", "traceId")(
      UpdateAppMessage.apply
    )

  implicit val createAzureRuntimeMessageDecoder: Decoder[CreateAzureRuntimeMessage] =
    Decoder.forProduct6(
      "runtimeId",
      "workspaceId",
      "useExistingDisk",
      "traceId",
      "workspaceName",
      "billingProfileId"
    )(
      CreateAzureRuntimeMessage.apply
    )

  implicit val deleteAzureRuntimeDecoder: Decoder[DeleteAzureRuntimeMessage] =
    Decoder.forProduct6("runtimeId", "diskId", "workspaceId", "wsmResourceId", "billingProfileId", "traceId")(
      DeleteAzureRuntimeMessage.apply
    )

  implicit val leoPubsubMessageTypeDecoder: Decoder[LeoPubsubMessageType] = Decoder.decodeString.emap { x =>
    Either.catchNonFatal(LeoPubsubMessageType.withName(x)).leftMap(_.getMessage)
  }

  implicit val storageContainerResponseDecoder: Decoder[StorageContainerResponse] =
    Decoder.forProduct2("name", "resourceId")(StorageContainerResponse.apply)

  implicit val createAppV2Decoder: Decoder[CreateAppV2Message] =
    Decoder.forProduct6("appId", "appName", "workspaceId", "cloudContext", "billingProfileId", "traceId")(
      CreateAppV2Message.apply
    )

  implicit val deleteAppV2Decoder: Decoder[DeleteAppV2Message] =
    Decoder.forProduct7("appId", "appName", "workspaceId", "cloudContext", "diskId", "billingProfileId", "traceId")(
      DeleteAppV2Message.apply
    )

  implicit val deleteDiskV2Decoder: Decoder[DeleteDiskV2Message] =
    Decoder.forProduct5("diskId", "workspaceId", "cloudContext", "wsmResourceId", "traceId")(
      DeleteDiskV2Message.apply
    )

  implicit val leoPubsubMessageDecoder: Decoder[LeoPubsubMessage] = Decoder.instance { message =>
    for {
      messageType <- message.downField("messageType").as[LeoPubsubMessageType]
      value <- messageType match {
        case LeoPubsubMessageType.CreateDisk         => message.as[CreateDiskMessage]
        case LeoPubsubMessageType.UpdateDisk         => message.as[UpdateDiskMessage]
        case LeoPubsubMessageType.DeleteDisk         => message.as[DeleteDiskMessage]
        case LeoPubsubMessageType.CreateRuntime      => message.as[CreateRuntimeMessage]
        case LeoPubsubMessageType.DeleteRuntime      => message.as[DeleteRuntimeMessage]
        case LeoPubsubMessageType.StopRuntime        => message.as[StopRuntimeMessage]
        case LeoPubsubMessageType.StartRuntime       => message.as[StartRuntimeMessage]
        case LeoPubsubMessageType.UpdateRuntime      => message.as[UpdateRuntimeMessage]
        case LeoPubsubMessageType.CreateApp          => message.as[CreateAppMessage]
        case LeoPubsubMessageType.DeleteApp          => message.as[DeleteAppMessage]
        case LeoPubsubMessageType.StopApp            => message.as[StopAppMessage]
        case LeoPubsubMessageType.StartApp           => message.as[StartAppMessage]
        case LeoPubsubMessageType.UpdateApp          => message.as[UpdateAppMessage]
        case LeoPubsubMessageType.CreateAzureRuntime => message.as[CreateAzureRuntimeMessage]
        case LeoPubsubMessageType.DeleteAzureRuntime => message.as[DeleteAzureRuntimeMessage]
        case LeoPubsubMessageType.CreateAppV2        => message.as[CreateAppV2Message]
        case LeoPubsubMessageType.DeleteAppV2        => message.as[DeleteAppV2Message]
        case LeoPubsubMessageType.DeleteDiskV2       => message.as[DeleteDiskV2Message]

      }
    } yield value
  }

  implicit val leoPubsubMessageTypeEncoder: Encoder[LeoPubsubMessageType] = Encoder.encodeString.contramap(_.asString)

  implicit val runtimeStatusEncoder: Encoder[RuntimeStatus] = Encoder.encodeString.contramap(_.toString)

  implicit val runtimePatchDetailsEncoder: Encoder[RuntimePatchDetails] =
    Encoder.forProduct2("clusterId", "clusterStatus")(x => (x.runtimeId, x.runtimeStatus))

  implicit val dataprocConfigInRequestEncoder: Encoder[RuntimeConfigInCreateRuntimeMessage.DataprocConfig] =
    dataprocConfigEncoder.contramap(x => dataprocInCreateRuntimeMsgToDataprocRuntime(x))

  implicit val gceRuntimeConfigInCreateRuntimeMessageEncoder: Encoder[RuntimeConfigInCreateRuntimeMessage.GceConfig] =
    Encoder.forProduct6(
      "machineType",
      "diskSize",
      "cloudService",
      "bootDiskSize",
      "zone",
      "gpuConfig"
    )(x => (x.machineType, x.diskSize, x.cloudService, x.bootDiskSize, x.zone, x.gpuConfig))

  implicit val gceWithPdConfigInCreateRuntimeMessageEncoder
    : Encoder[RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig] =
    Encoder.forProduct6(
      "machineType",
      "persistentDiskId",
      "cloudService",
      "bootDiskSize",
      "zone",
      "gpuConfig"
    )(x => (x.machineType, x.persistentDiskId, x.cloudService, x.bootDiskSize, x.zone, x.gpuConfig))

  implicit val runtimeConfigEncoder: Encoder[RuntimeConfigInCreateRuntimeMessage] = Encoder.instance {
    case x: RuntimeConfigInCreateRuntimeMessage.DataprocConfig  => x.asJson
    case x: RuntimeConfigInCreateRuntimeMessage.GceConfig       => x.asJson
    case x: RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig => x.asJson
  }

  implicit val createRuntimeMessageEncoder: Encoder[CreateRuntimeMessage] =
    Encoder.forProduct17(
      "messageType",
      "id",
      "clusterProjectAndName",
      "serviceAccountInfo",
      "dataprocInfo",
      "auditInfo",
      "userScriptUri",
      "startUserScriptUri",
      "userJupyterExtensionConfig",
      "defaultClientId",
      "clusterImages",
      "scopes",
      "welderEnabled",
      "customClusterEnvironmentVariables",
      "runtimeConfig",
      "checkToolsInterruptAfter",
      "traceId"
    )(x =>
      (x.messageType,
       x.runtimeId,
       x.runtimeProjectAndName,
       x.serviceAccountInfo,
       x.asyncRuntimeFields,
       x.auditInfo,
       x.userScriptUri,
       x.startUserScriptUri,
       x.userJupyterExtensionConfig,
       x.defaultClientId,
       x.runtimeImages,
       x.scopes,
       x.welderEnabled,
       x.customEnvironmentVariables,
       x.runtimeConfig,
       x.checkToolsInterruptAfter,
       x.traceId
      )
    )

  implicit val rtDataprocConfigInCreateRuntimeMessageDecoder
    : Decoder[RuntimeConfigInCreateRuntimeMessage.DataprocConfig] = Decoder.instance { c =>
    for {
      numberOfWorkers <- c.downField("numberOfWorkers").as[Int]
      masterMachineType <- c.downField("masterMachineType").as[MachineTypeName]
      masterDiskSize <- c.downField("masterDiskSize").as[DiskSize]
      workerMachineType <- c.downField("workerMachineType").as[Option[MachineTypeName]]
      workerDiskSize <- c.downField("workerDiskSize").as[Option[DiskSize]]
      numberOfWorkerLocalSSDs <- c.downField("numberOfWorkerLocalSSDs").as[Option[Int]]
      numberOfPreemptibleWorkers <- c.downField("numberOfPreemptibleWorkers").as[Option[Int]]
      propertiesOpt <- c.downField("properties").as[Option[LabelMap]]
      properties = propertiesOpt.getOrElse(Map.empty)
      region <- c.downField("region").as[RegionName]
      componentGatewayEnabled <- c.downField("componentGatewayEnabled").as[Boolean]
      workerPrivateAccess <- c.downField("workerPrivateAccess").as[Boolean]
    } yield RuntimeConfigInCreateRuntimeMessage.DataprocConfig(
      numberOfWorkers,
      masterMachineType,
      masterDiskSize,
      workerMachineType,
      workerDiskSize,
      numberOfWorkerLocalSSDs,
      numberOfPreemptibleWorkers,
      properties,
      region,
      componentGatewayEnabled,
      workerPrivateAccess
    )
  }

  implicit val gceConfigInCreateRuntimeMessageDecoder: Decoder[RuntimeConfigInCreateRuntimeMessage.GceConfig] =
    Decoder.forProduct5(
      "machineType",
      "diskSize",
      "bootDiskSize",
      "zone",
      "gpuConfig"
    )(RuntimeConfigInCreateRuntimeMessage.GceConfig.apply)

  implicit val gceWithPdConfigInCreateRuntimeMessageDecoder
    : Decoder[RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig] = Decoder.forProduct5(
    "machineType",
    "persistentDiskId",
    "bootDiskSize",
    "zone",
    "gpuConfig"
  )(RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig.apply)

  implicit val runtimeConfigInCreateRuntimeMessageDecoder: Decoder[RuntimeConfigInCreateRuntimeMessage] =
    Decoder.instance { x =>
      for {
        cloudService <- x.downField("cloudService").as[CloudService]
        r <- cloudService match {
          case CloudService.Dataproc =>
            x.as[RuntimeConfigInCreateRuntimeMessage.DataprocConfig]
          case CloudService.GCE =>
            x.as[RuntimeConfigInCreateRuntimeMessage.GceConfig] orElse x
              .as[RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig]
          case CloudService.AzureVm =>
            throw new AzureUnimplementedException("Azure should not be used with existing create runtime message")
        }
      } yield r
    }

  implicit val createRuntimeMessageDecoder: Decoder[CreateRuntimeMessage] =
    Decoder.forProduct16(
      "id",
      "clusterProjectAndName",
      "serviceAccountInfo",
      "dataprocInfo",
      "auditInfo",
      "userScriptUri",
      "startUserScriptUri",
      "userJupyterExtensionConfig",
      "defaultClientId",
      "clusterImages",
      "scopes",
      "welderEnabled",
      "customClusterEnvironmentVariables",
      "runtimeConfig",
      "traceId",
      "checkToolsInterruptAfter"
    )(CreateRuntimeMessage.apply)

  implicit val deleteRuntimeMessageEncoder: Encoder[DeleteRuntimeMessage] =
    Encoder.forProduct4("messageType", "runtimeId", "persistentDiskToDelete", "traceId")(x =>
      (x.messageType, x.runtimeId, x.persistentDiskToDelete, x.traceId)
    )

  implicit val stopRuntimeMessageEncoder: Encoder[StopRuntimeMessage] =
    Encoder.forProduct3("messageType", "runtimeId", "traceId")(x => (x.messageType, x.runtimeId, x.traceId))

  implicit val startRuntimeMessageEncoder: Encoder[StartRuntimeMessage] =
    Encoder.forProduct3("messageType", "runtimeId", "traceId")(x => (x.messageType, x.runtimeId, x.traceId))

  implicit val noPdSizeUpdateEncoder: Encoder[DiskUpdate.NoPdSizeUpdate] =
    Encoder.forProduct1("newDiskSize")(x => DiskUpdate.NoPdSizeUpdate.unapply(x).get)
  implicit val pdSizeUpdateEncoder: Encoder[DiskUpdate.PdSizeUpdate] =
    Encoder.forProduct3("diskId", "diskName", "newDiskSize")(x => DiskUpdate.PdSizeUpdate.unapply(x).get)
  implicit val dataprocDiskUpdateEncoder: Encoder[DiskUpdate.Dataproc] =
    Encoder.forProduct2("newDiskSize", "masterInstance")(x => DiskUpdate.Dataproc.unapply(x).get)

  implicit val diskUpdateEncoder: Encoder[DiskUpdate] = Encoder.instance { a =>
    a match {
      case x: DiskUpdate.NoPdSizeUpdate => x.asJson
      case x: DiskUpdate.PdSizeUpdate   => x.asJson
      case x: DiskUpdate.Dataproc       => x.asJson
    }
  }

  implicit val updateRuntimeMessageEncoder: Encoder[UpdateRuntimeMessage] =
    Encoder.forProduct8("messageType",
                        "runtimeId",
                        "newMachineType",
                        "stopToUpdateMachineType",
                        "diskUpdate",
                        "newNumWorkers",
                        "newNumPreemptibles",
                        "traceId"
    )(x =>
      (x.messageType,
       x.runtimeId,
       x.newMachineType,
       x.stopToUpdateMachineType,
       x.diskUpdate,
       x.newNumWorkers,
       x.newNumPreemptibles,
       x.traceId
      )
    )

  implicit val createDiskMessageEncoder: Encoder[CreateDiskMessage] =
    Encoder.forProduct10("messageType",
                         "diskId",
                         "googleProject",
                         "name",
                         "zone",
                         "size",
                         "diskType",
                         "blockSize",
                         "traceId",
                         "sourceDisk"
    )(x =>
      (
        x.messageType,
        x.diskId,
        x.googleProject,
        x.name,
        x.zone,
        x.size,
        x.diskType,
        x.blockSize,
        x.traceId,
        x.sourceDisk
      )
    )

  implicit val updateDiskMessageEncoder: Encoder[UpdateDiskMessage] =
    Encoder.forProduct4("messageType", "diskId", "newSize", "traceId")(x =>
      (x.messageType, x.diskId, x.newSize, x.traceId)
    )

  implicit val deleteDiskMessageEncoder: Encoder[DeleteDiskMessage] =
    Encoder.forProduct3("messageType", "diskId", "traceId")(x => (x.messageType, x.diskId, x.traceId))

  implicit val appIdEncoder: Encoder[AppId] = Encoder.encodeLong.contramap(_.id)

  implicit val clusterNodepoolActionTypeEncoder: Encoder[ClusterNodepoolActionType] =
    Encoder.encodeString.contramap(_.asString)

  implicit val createClusterAndNodepoolEncoder: Encoder[CreateClusterAndNodepool] =
    Encoder.forProduct4("actionType", "clusterId", "defaultNodepoolId", "nodepoolId")(x =>
      (x.actionType, x.clusterId, x.defaultNodepoolId, x.nodepoolId)
    )

  implicit val createNodepoolEncoder: Encoder[CreateNodepool] =
    Encoder.forProduct2("actionType", "nodepoolId")(x => (x.actionType, x.nodepoolId))

  implicit val createClusterEncoder: Encoder[CreateCluster] =
    Encoder.forProduct2("actionType", "clusterId")(x => (x.actionType, x.clusterId))

  implicit val clusterNodepoolActionEncoder: Encoder[ClusterNodepoolAction] =
    Encoder.instance { message =>
      message match {
        case m: CreateClusterAndNodepool => m.asJson
        case m: CreateNodepool           => m.asJson
        case m: CreateCluster            => m.asJson
      }
    }

  implicit val createAppMessageEncoder: Encoder[CreateAppMessage] =
    Encoder.forProduct12(
      "messageType",
      "project",
      "clusterNodepoolAction",
      "appId",
      "appName",
      "createDisk",
      "customEnvironmentVariables",
      "appType",
      "namespaceName",
      "machineType",
      "traceId",
      "enableIntraNodeVisibility"
    )(x =>
      (x.messageType,
       x.project,
       x.clusterNodepoolAction,
       x.appId,
       x.appName,
       x.createDisk,
       x.customEnvironmentVariables,
       x.appType,
       x.namespaceName,
       x.machineType,
       x.traceId,
       x.enableIntraNodeVisibility
      )
    )

  implicit val deleteAppMessageEncoder: Encoder[DeleteAppMessage] =
    Encoder.forProduct6("messageType", "appId", "appName", "project", "diskId", "traceId")(x =>
      (x.messageType, x.appId, x.appName, x.project, x.diskId, x.traceId)
    )

  implicit val stopAppMessageEncoder: Encoder[StopAppMessage] =
    Encoder.forProduct5("messageType", "appId", "appName", "project", "traceId")(x =>
      (x.messageType, x.appId, x.appName, x.project, x.traceId)
    )

  implicit val startAppMessageEncoder: Encoder[StartAppMessage] =
    Encoder.forProduct5("messageType", "appId", "appName", "project", "traceId")(x =>
      (x.messageType, x.appId, x.appName, x.project, x.traceId)
    )

  implicit val updateAppMessageEncoder: Encoder[UpdateAppMessage] =
    Encoder.forProduct7("messageType", "appId", "appName", "cloudContext", "workspaceId", "googleProject", "traceId")(
      x => (x.messageType, x.appId, x.appName, x.cloudContext, x.workspaceId, x.googleProject, x.traceId)
    )

  implicit val createAzureRuntimeMessageEncoder: Encoder[CreateAzureRuntimeMessage] =
    Encoder.forProduct7(
      "messageType",
      "runtimeId",
      "workspaceId",
      "useExistingDisk",
      "traceId",
      "workspaceName",
      "billingProfileId"
    )(x =>
      (x.messageType, x.runtimeId, x.workspaceId, x.useExistingDisk, x.traceId, x.workspaceName, x.billingProfileId)
    )

  implicit val deleteAzureMessageEncoder: Encoder[DeleteAzureRuntimeMessage] =
    Encoder.forProduct7("messageType",
                        "runtimeId",
                        "diskId",
                        "workspaceId",
                        "wsmResourceId",
                        "billingProfileId",
                        "traceId"
    )(x =>
      (x.messageType, x.runtimeId, x.diskIdToDelete, x.workspaceId, x.wsmResourceId, x.billingProfileId, x.traceId)
    )

  implicit val createAppV2MessageEncoder: Encoder[CreateAppV2Message] =
    Encoder.forProduct7(
      "messageType",
      "appId",
      "appName",
      "workspaceId",
      "cloudContext",
      "billingProfileId",
      "traceId"
    )(x => (x.messageType, x.appId, x.appName, x.workspaceId, x.cloudContext, x.billingProfileId, x.traceId))

  implicit val deleteAppV2MessageEncoder: Encoder[DeleteAppV2Message] =
    Encoder.forProduct8("messageType",
                        "appId",
                        "appName",
                        "workspaceId",
                        "cloudContext",
                        "diskId",
                        "billingProfileId",
                        "traceId"
    )(x => (x.messageType, x.appId, x.appName, x.workspaceId, x.cloudContext, x.diskId, x.billingProfileId, x.traceId))

  implicit val deleteDiskV2MessageEncoder: Encoder[DeleteDiskV2Message] =
    Encoder.forProduct6("messageType", "diskId", "workspaceId", "cloudContext", "wsmResourceId", "traceId")(x =>
      (x.messageType, x.diskId, x.workspaceId, x.cloudContext, x.wsmResourceId, x.traceId)
    )

  implicit val leoPubsubMessageEncoder: Encoder[LeoPubsubMessage] = Encoder.instance {
    case m: CreateDiskMessage         => m.asJson
    case m: UpdateDiskMessage         => m.asJson
    case m: DeleteDiskMessage         => m.asJson
    case m: CreateRuntimeMessage      => m.asJson
    case m: DeleteRuntimeMessage      => m.asJson
    case m: StopRuntimeMessage        => m.asJson
    case m: StartRuntimeMessage       => m.asJson
    case m: UpdateRuntimeMessage      => m.asJson
    case m: CreateAppMessage          => m.asJson
    case m: DeleteAppMessage          => m.asJson
    case m: StopAppMessage            => m.asJson
    case m: StartAppMessage           => m.asJson
    case m: UpdateAppMessage          => m.asJson
    case m: CreateAzureRuntimeMessage => m.asJson
    case m: DeleteAzureRuntimeMessage => m.asJson
    case m: CreateAppV2Message        => m.asJson
    case m: DeleteAppV2Message        => m.asJson
    case m: DeleteDiskV2Message       => m.asJson
  }
}

sealed trait PubsubHandleMessageError extends NoStackTrace {
  def isRetryable: Boolean
}
object PubsubHandleMessageError {
  final case class PubsubKubernetesError(dbError: AppError,
                                         appId: Option[AppId],
                                         isRetryable: Boolean,
                                         nodepoolId: Option[NodepoolLeoId],
                                         diskId: Option[DiskId],
                                         clusterId: Option[KubernetesClusterLeoId]
  ) extends PubsubHandleMessageError {
    // You cannot update this log wording without updating the corresponding prod and staging alerts here
    //     https://console.cloud.google.com/monitoring/alerting/policies/8184448493858086363?project=broad-dsde-prod
    //     https://console.cloud.google.com/monitoring/alerting/policies/16136890211426349187?project=broad-dsde-staging
    override def getMessage: String =
      s"An error occurred with a kubernetes operation from source ${dbError.source} during action ${dbError.action}. \nOriginal message: ${dbError.errorMessage}"
  }

  final case class ClusterError(clusterId: Long, traceId: TraceId, msg: String) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"${clusterId}: ${msg}"

    override def isRetryable: Boolean = true
  }

  final case class ClusterNotFound(clusterId: Long, message: LeoPubsubMessage) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"Unable to process transition finished message ${message} for cluster ${clusterId} because it was not found in the database"
    val isRetryable: Boolean = false
  }

  final case class ClusterNotStopped(clusterId: Long,
                                     projectName: String,
                                     clusterStatus: RuntimeStatus,
                                     message: LeoPubsubMessage
  ) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"Unable to process message ${message} for cluster ${clusterId}/${projectName} in status ${clusterStatus.toString}, when the monitor signalled it stopped as it is not stopped."
    val isRetryable: Boolean = false
  }

  final case class ClusterInvalidState(clusterId: Long,
                                       projectName: String,
                                       cluster: Runtime,
                                       message: LeoPubsubMessage
  ) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"${clusterId}, ${projectName}, ${message} | This is likely due to a mismatch in state between the db and the message, or an improperly formatted machineConfig in the message. Cluster details: ${cluster}"
    val isRetryable: Boolean = false
  }

  final case class DiskNotFound(diskId: DiskId) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"Unable to process message for disk ${diskId.value} because it was not found in the database"
    val isRetryable: Boolean = false
  }

  final case class DiskDeletionError(diskId: DiskId, workspaceId: WorkspaceId, errorMsg: String)
      extends PubsubHandleMessageError {
    override def getMessage: String =
      s"\n\tdisk ${diskId.value} in workspace ${workspaceId.value}, \n\tmsg: ${errorMsg})"

    val isRetryable: Boolean = false
  }

  final case class AzureDiskDeletionError(diskId: DiskId,
                                          wsmControlledResourceId: WsmControlledResourceId,
                                          workspaceId: WorkspaceId,
                                          errorMsg: String
  ) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"\n\tdisk ${diskId.value} with resource id: ${wsmControlledResourceId.value}, \n\tmsg: ${errorMsg})"

    val isRetryable: Boolean = false
  }

  final case class AzureDiskResourceDeletionError(id: Either[Long, WsmControlledResourceId],
                                                  workspaceId: WorkspaceId,
                                                  errorMsg: String
  ) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"\n\tAssociated disk resource: ${id} in workspace ${workspaceId.value}, \n\tmsg: ${errorMsg})"

    val isRetryable: Boolean = false
  }

  final case class AzureRuntimeCreationError(runtimeId: Long,
                                             workspaceId: WorkspaceId,
                                             errorMsg: String,
                                             useExistingDisk: Boolean
  ) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"\n\truntimeId: ${runtimeId}, \n\tmsg: ${errorMsg})"
    val isRetryable: Boolean = false
  }

  final case class AzureRuntimeDeletionError(runtimeId: Long, workspaceId: WorkspaceId, errorMsg: String)
      extends PubsubHandleMessageError {
    override def getMessage: String =
      s"\n\truntimeId: ${runtimeId}, \n\tmsg: ${errorMsg})"
    val isRetryable: Boolean = false
  }

  final case class AzureRuntimeStartingError(runtimeId: Long, errorMsg: String, traceId: TraceId)
      extends PubsubHandleMessageError {
    override def getMessage: String =
      s"\n\truntimeId: ${runtimeId}, \n\tmsg: ${errorMsg}, traceId: ${traceId.asString}"
    val isRetryable: Boolean = false
  }

  final case class AzureRuntimeStoppingError(runtimeId: Long, errorMsg: String, traceId: TraceId)
      extends PubsubHandleMessageError {
    override def getMessage: String =
      s"\n\truntimeId: ${runtimeId}, \n\tmsg: ${errorMsg}, traceId: ${traceId.asString}"
    val isRetryable: Boolean = false
  }

  final case class AppNotFound(appId: Long, message: LeoPubsubMessage) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"Unable to process transition finished message ${message} for app ${appId} because it was not found in the database"

    val isRetryable: Boolean = false
  }

  final case class AppIsAlreadyUpdatingException(message: UpdateAppMessage) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"Unable to process update for app ${message.appId} because it is already in updating status. \n\tPubsub message:${message} "

    val isRetryable: Boolean = false
  }
}

final case class PersistentDiskMonitor(maxAttempts: Int, interval: FiniteDuration)

final case class PollMonitorConfig(initialDelay: FiniteDuration, maxAttempts: Int, interval: FiniteDuration) {
  def totalDuration: FiniteDuration = initialDelay + interval * maxAttempts
}

final case class InterruptablePollMonitorConfig(maxAttempts: Int,
                                                interval: FiniteDuration,
                                                interruptAfter: FiniteDuration
)

final case class CreateDiskTimeout(defaultInMinutes: Int, sourceDiskCopyInMinutes: Int)
final case class PersistentDiskMonitorConfig(create: CreateDiskTimeout,
                                             delete: PollMonitorConfig,
                                             update: PollMonitorConfig
)

final case class LeoPubsubMessageSubscriberConfig(concurrency: Int,
                                                  timeout: FiniteDuration,
                                                  persistentDiskMonitorConfig: PersistentDiskMonitorConfig,
                                                  galaxyDiskConfig: GalaxyDiskConfig
)

final case class DiskDetachStatus(disk: Option[Disk], originalDetachTimestampOpt: Option[String])
