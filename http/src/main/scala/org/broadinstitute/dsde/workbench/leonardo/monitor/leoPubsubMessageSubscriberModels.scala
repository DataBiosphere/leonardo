package org.broadinstitute.dsde.workbench.leonardo
package monitor

import ca.mrvisser.sealerate
import cats.implicits._
import enumeratum.{Enum, EnumEntry}
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.JsonCodec.{traceIdDecoder, traceIdEncoder}
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.{
  dataprocInCreateRuntimeMsgToDataprocRuntime,
  RuntimeConfigRequest
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterNodepoolAction.{
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
    bootDiskSize: DiskSize
  ) extends RuntimeConfigInCreateRuntimeMessage {
    val cloudService: CloudService = CloudService.GCE
  }

  final case class GceWithPdConfig(machineType: MachineTypeName, persistentDiskId: DiskId, bootDiskSize: DiskSize)
      extends RuntimeConfigInCreateRuntimeMessage {
    val cloudService: CloudService = CloudService.GCE
  }

  final case class DataprocConfig(numberOfWorkers: Int,
                                  masterMachineType: MachineTypeName,
                                  masterDiskSize: DiskSize, //min 10
                                  // worker settings are None when numberOfWorkers is 0
                                  workerMachineType: Option[MachineTypeName] = None,
                                  workerDiskSize: Option[DiskSize] = None, //min 10
                                  numberOfWorkerLocalSSDs: Option[Int] = None, //min 0 max 8
                                  numberOfPreemptibleWorkers: Option[Int] = None,
                                  properties: Map[String, String])
      extends RuntimeConfigInCreateRuntimeMessage {
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
          dataproc.properties
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
          dataproc.properties
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

  final case object DeleteKubernetesCluster extends LeoPubsubMessageType {
    val asString = "deleteKubernetesCluster"
  }

  final case object BatchNodepoolCreate extends LeoPubsubMessageType {
    val asString = "batchNodepoolCreate"
  }

  final case object StopApp extends LeoPubsubMessageType {
    val asString = "stopApp"
  }
  final case object StartApp extends LeoPubsubMessageType {
    val asString = "startApp"
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
                                        jupyterUserScriptUri: Option[UserScriptPath],
                                        jupyterStartUserScriptUri: Option[UserScriptPath],
                                        userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                        defaultClientId: Option[String],
                                        runtimeImages: Set[RuntimeImage],
                                        scopes: Set[String],
                                        welderEnabled: Boolean,
                                        customEnvironmentVariables: Map[String, String],
                                        runtimeConfig: RuntimeConfigInCreateRuntimeMessage,
                                        stopAfterCreation: Boolean = false,
                                        traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.CreateRuntime
  }

  object CreateRuntimeMessage {
    def fromRuntime(runtime: Runtime,
                    runtimeConfig: RuntimeConfigInCreateRuntimeMessage,
                    traceId: Option[TraceId]): CreateRuntimeMessage =
      CreateRuntimeMessage(
        runtime.id,
        RuntimeProjectAndName(runtime.googleProject, runtime.runtimeName),
        runtime.serviceAccount,
        runtime.asyncRuntimeFields,
        runtime.auditInfo,
        runtime.jupyterUserScriptUri,
        runtime.jupyterStartUserScriptUri,
        runtime.userJupyterExtensionConfig,
        runtime.defaultClientId,
        runtime.runtimeImages,
        runtime.scopes,
        runtime.welderEnabled,
        runtime.customEnvironmentVariables,
        runtimeConfig,
        runtime.stopAfterCreation,
        traceId
      )
  }

  final case class CreateDiskMessage(diskId: DiskId,
                                     googleProject: GoogleProject,
                                     name: DiskName,
                                     zone: ZoneName,
                                     size: DiskSize,
                                     diskType: DiskType,
                                     blockSize: BlockSize,
                                     traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.CreateDisk
  }

  object CreateDiskMessage {
    def fromDisk(disk: PersistentDisk, traceId: Option[TraceId]): CreateDiskMessage =
      CreateDiskMessage(
        disk.id,
        disk.googleProject,
        disk.name,
        disk.zone,
        disk.size,
        disk.diskType,
        disk.blockSize,
        traceId
      )
  }

  final case class CreateAppMessage(project: GoogleProject,
                                    clusterNodepoolAction: Option[ClusterNodepoolAction],
                                    appId: AppId,
                                    appName: AppName,
                                    createDisk: Option[DiskId],
                                    customEnvironmentVariables: Map[String, String],
                                    appType: AppType,
                                    traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.CreateApp
  }

  final case class DeleteAppMessage(appId: AppId,
                                    appName: AppName,
                                    nodepoolId: NodepoolLeoId,
                                    project: GoogleProject,
                                    diskId: Option[DiskId],
                                    traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.DeleteApp
  }

  final case class DeleteKubernetesClusterMessage(clusterId: KubernetesClusterLeoId,
                                                  project: GoogleProject,
                                                  traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.DeleteKubernetesCluster
  }

  final case class BatchNodepoolCreateMessage(clusterId: KubernetesClusterLeoId,
                                              nodepools: List[NodepoolLeoId],
                                              project: GoogleProject,
                                              traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.BatchNodepoolCreate
  }

  final case class StopAppMessage(appId: AppId,
                                  nodepoolId: NodepoolLeoId,
                                  project: GoogleProject,
                                  traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.StopApp
  }

  final case class StartAppMessage(appId: AppId,
                                   nodepoolId: NodepoolLeoId,
                                   project: GoogleProject,
                                   traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.StartApp
  }

  final case class DeleteRuntimeMessage(runtimeId: Long,
                                        persistentDiskToDelete: Option[DiskId],
                                        traceId: Option[TraceId])
      extends LeoPubsubMessage {
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
                                        traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.UpdateRuntime
  }

  final case class UpdateDiskMessage(diskId: DiskId, newSize: DiskSize, traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.UpdateDisk
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
  def values: Set[ClusterNodepoolActionType] = sealerate.values[ClusterNodepoolActionType]
  def stringToObject: Map[String, ClusterNodepoolActionType] = values.map(v => v.asString -> v).toMap
}

sealed trait ClusterNodepoolAction extends Product with Serializable {
  def actionType: ClusterNodepoolActionType
}
object ClusterNodepoolAction {
  final case class CreateClusterAndNodepool(clusterId: KubernetesClusterLeoId,
                                            defaultNodepoolId: NodepoolLeoId,
                                            nodepoolId: NodepoolLeoId)
      extends ClusterNodepoolAction {
    val actionType: ClusterNodepoolActionType = ClusterNodepoolActionType.CreateClusterAndNodepool

  }
  final case class CreateNodepool(nodepoolId: NodepoolLeoId) extends ClusterNodepoolAction {
    val actionType: ClusterNodepoolActionType = ClusterNodepoolActionType.CreateNodepool
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
        } yield (DiskUpdate.PdSizeUpdate(diskId, diskName, diskSize): DiskUpdate)
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
                        "traceId")(
      UpdateRuntimeMessage.apply
    )

  implicit val createDiskDecoder: Decoder[CreateDiskMessage] =
    Decoder.forProduct8("diskId", "googleProject", "name", "zone", "size", "diskType", "blockSize", "traceId")(
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

  implicit val clusterNodepoolActionDecoder: Decoder[ClusterNodepoolAction] = Decoder.instance { message =>
    for {
      actionType <- message.downField("actionType").as[ClusterNodepoolActionType]
      value <- actionType match {
        case ClusterNodepoolActionType.CreateClusterAndNodepool => message.as[CreateClusterAndNodepool]
        case ClusterNodepoolActionType.CreateNodepool           => message.as[CreateNodepool]
      }
    } yield value
  }

  implicit val createAppDecoder: Decoder[CreateAppMessage] =
    Decoder.forProduct8("project",
                        "clusterNodepoolAction",
                        "appId",
                        "appName",
                        "createDisk",
                        "customEnvironmentVariables",
                        "appType",
                        "traceId")(CreateAppMessage.apply)

  implicit val deleteAppDecoder: Decoder[DeleteAppMessage] =
    Decoder.forProduct6("appId", "appName", "nodepoolId", "project", "diskId", "traceId")(DeleteAppMessage.apply)

  implicit val deleteKubernetesClusterDecoder: Decoder[DeleteKubernetesClusterMessage] =
    Decoder.forProduct3("clusterId", "project", "traceId")(DeleteKubernetesClusterMessage.apply)

  implicit val batchNodepoolCreateDecoder: Decoder[BatchNodepoolCreateMessage] =
    Decoder.forProduct4("clusterId", "nodepools", "project", "traceId")(BatchNodepoolCreateMessage.apply)

  implicit val stopAppDecoder: Decoder[StopAppMessage] =
    Decoder.forProduct4("appId", "nodepoolId", "project", "traceId")(StopAppMessage.apply)

  implicit val startAppDecoder: Decoder[StartAppMessage] =
    Decoder.forProduct4("appId", "nodepoolId", "project", "traceId")(StartAppMessage.apply)

  implicit val leoPubsubMessageTypeDecoder: Decoder[LeoPubsubMessageType] = Decoder.decodeString.emap { x =>
    Either.catchNonFatal(LeoPubsubMessageType.withName(x)).leftMap(_.getMessage)
  }

  implicit val leoPubsubMessageDecoder: Decoder[LeoPubsubMessage] = Decoder.instance { message =>
    for {
      messageType <- message.downField("messageType").as[LeoPubsubMessageType]
      value <- messageType match {
        case LeoPubsubMessageType.CreateDisk              => message.as[CreateDiskMessage]
        case LeoPubsubMessageType.UpdateDisk              => message.as[UpdateDiskMessage]
        case LeoPubsubMessageType.DeleteDisk              => message.as[DeleteDiskMessage]
        case LeoPubsubMessageType.CreateRuntime           => message.as[CreateRuntimeMessage]
        case LeoPubsubMessageType.DeleteRuntime           => message.as[DeleteRuntimeMessage]
        case LeoPubsubMessageType.StopRuntime             => message.as[StopRuntimeMessage]
        case LeoPubsubMessageType.StartRuntime            => message.as[StartRuntimeMessage]
        case LeoPubsubMessageType.UpdateRuntime           => message.as[UpdateRuntimeMessage]
        case LeoPubsubMessageType.CreateApp               => message.as[CreateAppMessage]
        case LeoPubsubMessageType.DeleteApp               => message.as[DeleteAppMessage]
        case LeoPubsubMessageType.DeleteKubernetesCluster => message.as[DeleteKubernetesClusterMessage]
        case LeoPubsubMessageType.BatchNodepoolCreate     => message.as[BatchNodepoolCreateMessage]
        case LeoPubsubMessageType.StopApp                 => message.as[StopAppMessage]
        case LeoPubsubMessageType.StartApp                => message.as[StartAppMessage]
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
    Encoder.forProduct4(
      "machineType",
      "diskSize",
      "cloudService",
      "bootDiskSize"
    )(x => (x.machineType, x.diskSize, x.cloudService, x.bootDiskSize))

  implicit val gceWithPdConfigInCreateRuntimeMessageEncoder
    : Encoder[RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig] =
    Encoder.forProduct4(
      "machineType",
      "persistentDiskId",
      "cloudService",
      "bootDiskSize"
    )(x => (x.machineType, x.persistentDiskId, x.cloudService, x.bootDiskSize))

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
      "jupyterUserScriptUri",
      "jupyterStartUserScriptUri",
      "userJupyterExtensionConfig",
      "defaultClientId",
      "clusterImages",
      "scopes",
      "welderEnabled",
      "customClusterEnvironmentVariables",
      "runtimeConfig",
      "stopAfterCreation",
      "traceId"
    )(x =>
      (x.messageType,
       x.runtimeId,
       x.runtimeProjectAndName,
       x.serviceAccountInfo,
       x.asyncRuntimeFields,
       x.auditInfo,
       x.jupyterUserScriptUri,
       x.jupyterStartUserScriptUri,
       x.userJupyterExtensionConfig,
       x.defaultClientId,
       x.runtimeImages,
       x.scopes,
       x.welderEnabled,
       x.customEnvironmentVariables,
       x.runtimeConfig,
       x.stopAfterCreation,
       x.traceId)
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
    } yield RuntimeConfigInCreateRuntimeMessage.DataprocConfig(numberOfWorkers,
                                                               masterMachineType,
                                                               masterDiskSize,
                                                               workerMachineType,
                                                               workerDiskSize,
                                                               numberOfWorkerLocalSSDs,
                                                               numberOfPreemptibleWorkers,
                                                               properties)
  }

  implicit val gceConfigInCreateRuntimeMessageDecoder: Decoder[RuntimeConfigInCreateRuntimeMessage.GceConfig] =
    Decoder.forProduct3(
      "machineType",
      "diskSize",
      "bootDiskSize"
    )(RuntimeConfigInCreateRuntimeMessage.GceConfig.apply)

  implicit val gceWithPdConfigInCreateRuntimeMessageDecoder
    : Decoder[RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig] = Decoder.forProduct3(
    "machineType",
    "persistentDiskId",
    "bootDiskSize"
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
      "jupyterUserScriptUri",
      "jupyterStartUserScriptUri",
      "userJupyterExtensionConfig",
      "defaultClientId",
      "clusterImages",
      "scopes",
      "welderEnabled",
      "customClusterEnvironmentVariables",
      "runtimeConfig",
      "stopAfterCreation",
      "traceId"
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
                        "traceId")(x =>
      (x.messageType,
       x.runtimeId,
       x.newMachineType,
       x.stopToUpdateMachineType,
       x.diskUpdate,
       x.newNumWorkers,
       x.newNumPreemptibles,
       x.traceId)
    )

  implicit val createDiskMessageEncoder: Encoder[CreateDiskMessage] =
    Encoder.forProduct9("messageType",
                        "diskId",
                        "googleProject",
                        "name",
                        "zone",
                        "size",
                        "diskType",
                        "blockSize",
                        "traceId")(x =>
      (
        x.messageType,
        x.diskId,
        x.googleProject,
        x.name,
        x.zone,
        x.size,
        x.diskType,
        x.blockSize,
        x.traceId
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

  implicit val clusterNodepoolActionEncoder: Encoder[ClusterNodepoolAction] =
    Encoder.instance { message =>
      message match {
        case m: CreateClusterAndNodepool => m.asJson
        case m: CreateNodepool           => m.asJson
      }
    }

  implicit val createAppMessageEncoder: Encoder[CreateAppMessage] =
    Encoder.forProduct9(
      "messageType",
      "project",
      "clusterNodepoolAction",
      "appId",
      "appName",
      "createDisk",
      "customEnvironmentVariables",
      "appType",
      "traceId"
    )(x =>
      (x.messageType,
       x.project,
       x.clusterNodepoolAction,
       x.appId,
       x.appName,
       x.createDisk,
       x.customEnvironmentVariables,
       x.appType,
       x.traceId)
    )

  implicit val batchNodepoolCreateMessageEncoder: Encoder[BatchNodepoolCreateMessage] =
    Encoder.forProduct5("messageType", "clusterId", "nodepools", "project", "traceId")(x =>
      (x.messageType, x.clusterId, x.nodepools, x.project, x.traceId)
    )

  implicit val deleteAppMessageEncoder: Encoder[DeleteAppMessage] =
    Encoder.forProduct7("messageType", "appId", "appName", "nodepoolId", "project", "diskId", "traceId")(x =>
      (x.messageType, x.appId, x.appName, x.nodepoolId, x.project, x.diskId, x.traceId)
    )

  implicit val deleteKubernetesClusterMessageEncoder: Encoder[DeleteKubernetesClusterMessage] =
    Encoder.forProduct4("messageType", "clusterId", "project", "traceId")(x =>
      (x.messageType, x.clusterId, x.project, x.traceId)
    )

  implicit val stopAppMessageEncoder: Encoder[StopAppMessage] =
    Encoder.forProduct5("messageType", "appId", "nodepoolId", "project", "traceId")(x =>
      (x.messageType, x.appId, x.nodepoolId, x.project, x.traceId)
    )

  implicit val startAppMessageEncoder: Encoder[StartAppMessage] =
    Encoder.forProduct5("messageType", "appId", "nodepoolId", "project", "traceId")(x =>
      (x.messageType, x.appId, x.nodepoolId, x.project, x.traceId)
    )

  implicit val leoPubsubMessageEncoder: Encoder[LeoPubsubMessage] = Encoder.instance {
    case m: CreateDiskMessage              => m.asJson
    case m: UpdateDiskMessage              => m.asJson
    case m: DeleteDiskMessage              => m.asJson
    case m: CreateRuntimeMessage           => m.asJson
    case m: DeleteRuntimeMessage           => m.asJson
    case m: StopRuntimeMessage             => m.asJson
    case m: StartRuntimeMessage            => m.asJson
    case m: UpdateRuntimeMessage           => m.asJson
    case m: CreateAppMessage               => m.asJson
    case m: DeleteAppMessage               => m.asJson
    case m: DeleteKubernetesClusterMessage => m.asJson
    case m: BatchNodepoolCreateMessage     => m.asJson
    case m: StopAppMessage                 => m.asJson
    case m: StartAppMessage                => m.asJson
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
                                         clusterId: Option[KubernetesClusterLeoId])
      extends PubsubHandleMessageError {
    override def getMessage: String =
      s"An error occurred with a kubernetes operation from source ${dbError.source} during action ${dbError.action}. \nOriginal message: ${dbError.errorMessage}"
  }

  final case class ClusterNotFound(clusterId: Long, message: LeoPubsubMessage) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"Unable to process transition finished message ${message} for cluster ${clusterId} because it was not found in the database"
    val isRetryable: Boolean = false
  }

  final case class ClusterNotStopped(clusterId: Long,
                                     projectName: String,
                                     clusterStatus: RuntimeStatus,
                                     message: LeoPubsubMessage)
      extends PubsubHandleMessageError {
    override def getMessage: String =
      s"Unable to process message ${message} for cluster ${clusterId}/${projectName} in status ${clusterStatus.toString}, when the monitor signalled it stopped as it is not stopped."
    val isRetryable: Boolean = false
  }

  final case class ClusterInvalidState(clusterId: Long,
                                       projectName: String,
                                       cluster: Runtime,
                                       message: LeoPubsubMessage)
      extends PubsubHandleMessageError {
    override def getMessage: String =
      s"${clusterId}, ${projectName}, ${message} | This is likely due to a mismatch in state between the db and the message, or an improperly formatted machineConfig in the message. Cluster details: ${cluster}"
    val isRetryable: Boolean = false
  }

  final case class DiskNotFound(diskId: DiskId) extends PubsubHandleMessageError {
    override def getMessage: String =
      s"Unable to process message for disk ${diskId.value} because it was not found in the database"
    val isRetryable: Boolean = false
  }

  final case class DiskInvalidState(diskId: DiskId, projectName: String, disk: PersistentDisk)
      extends PubsubHandleMessageError {
    override def getMessage: String =
      s"${diskId}, ${projectName} | Unable to process disk because not in correct state. Disk details: ${disk}"
    val isRetryable: Boolean = false
  }
}

final case class PersistentDiskMonitor(maxAttempts: Int, interval: FiniteDuration)

final case class PollMonitorConfig(maxAttempts: Int, interval: FiniteDuration) {
  def totalDuration: FiniteDuration = interval * maxAttempts
}

final case class PersistentDiskMonitorConfig(create: PollMonitorConfig,
                                             delete: PollMonitorConfig,
                                             update: PollMonitorConfig)

final case class LeoPubsubMessageSubscriberConfig(concurrency: Int,
                                                  timeout: FiniteDuration,
                                                  persistentDiskMonitorConfig: PersistentDiskMonitorConfig)
