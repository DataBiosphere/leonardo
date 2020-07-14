package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.implicits._
import enumeratum.{Enum, EnumEntry}
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.google2.JsonCodec.{traceIdDecoder, traceIdEncoder}
import org.broadinstitute.dsde.workbench.google2.{DiskName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.RuntimeConfigRequest
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail, WorkbenchException}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.http.dataprocInCreateRuntimeMsgToDataprocRuntime

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

  final case class CreateAppMessage(cluster: Option[CreateCluster],
                                    appId: AppId,
                                    nodepoolId: NodepoolLeoId,
                                    project: GoogleProject,
                                    createDisk: Boolean,
                                    traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.CreateApp
  }

  final case class DeleteAppMessage(appId: AppId,
                                    nodepoolId: NodepoolLeoId,
                                    project: GoogleProject,
                                    deleteDisk: Boolean,
                                    traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType: LeoPubsubMessageType = LeoPubsubMessageType.DeleteApp
  }

  final case class DeleteRuntimeMessage(runtimeId: Long, deleteDisk: Boolean, traceId: Option[TraceId])
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
                                        newDiskSize: Option[DiskSize],
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

final case class RuntimePatchDetails(runtimeId: Long, runtimeStatus: RuntimeStatus) extends Product with Serializable

final case class PubsubException(message: String) extends WorkbenchException(message)

object LeoPubsubCodec {
  implicit val runtimePatchDetailsDecoder: Decoder[RuntimePatchDetails] =
    Decoder.forProduct2("clusterId", "clusterStatus")(RuntimePatchDetails.apply)

  implicit val deleteRuntimeDecoder: Decoder[DeleteRuntimeMessage] =
    Decoder.forProduct3("runtimeId", "deleteDisk", "traceId")(DeleteRuntimeMessage.apply)

  implicit val stopRuntimeDecoder: Decoder[StopRuntimeMessage] =
    Decoder.forProduct2("runtimeId", "traceId")(StopRuntimeMessage.apply)

  implicit val startRuntimeDecoder: Decoder[StartRuntimeMessage] =
    Decoder.forProduct2("runtimeId", "traceId")(StartRuntimeMessage.apply)

  implicit val updateRuntimeDecoder: Decoder[UpdateRuntimeMessage] =
    Decoder.forProduct7("runtimeId",
                        "newMachineType",
                        "stopToUpdateMachineType",
                        "newDiskSize",
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
  implicit val createAppDecoder: Decoder[CreateAppMessage] =
    Decoder.forProduct6("cluster", "appId", "nodepoolId", "project", "createDisk", "traceId")(CreateAppMessage.apply)

  implicit val deleteAppDecoder: Decoder[DeleteAppMessage] =
    Decoder.forProduct5("appId", "nodepoolId", "project", "deleteDisk", "traceId")(DeleteAppMessage.apply)

  implicit val leoPubsubMessageTypeDecoder: Decoder[LeoPubsubMessageType] = Decoder.decodeString.emap { x =>
    Either.catchNonFatal(LeoPubsubMessageType.withName(x)).leftMap(_.getMessage)
  }

  implicit val leoPubsubMessageDecoder: Decoder[LeoPubsubMessage] = Decoder.instance { message =>
    for {
      messageType <- message.downField("messageType").as[LeoPubsubMessageType]
      value <- messageType match {
        case LeoPubsubMessageType.CreateDisk    => message.as[CreateDiskMessage]
        case LeoPubsubMessageType.UpdateDisk    => message.as[UpdateDiskMessage]
        case LeoPubsubMessageType.DeleteDisk    => message.as[DeleteDiskMessage]
        case LeoPubsubMessageType.CreateRuntime => message.as[CreateRuntimeMessage]
        case LeoPubsubMessageType.DeleteRuntime => message.as[DeleteRuntimeMessage]
        case LeoPubsubMessageType.StopRuntime   => message.as[StopRuntimeMessage]
        case LeoPubsubMessageType.StartRuntime  => message.as[StartRuntimeMessage]
        case LeoPubsubMessageType.UpdateRuntime => message.as[UpdateRuntimeMessage]
        case LeoPubsubMessageType.CreateApp     => message.as[CreateAppMessage]
        case LeoPubsubMessageType.DeleteApp     => message.as[DeleteAppMessage]
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

  implicit val runtimeConfigEncoder: Encoder[RuntimeConfigInCreateRuntimeMessage] = Encoder.instance(x =>
    x match {
      case x: RuntimeConfigInCreateRuntimeMessage.DataprocConfig  => x.asJson
      case x: RuntimeConfigInCreateRuntimeMessage.GceConfig       => x.asJson
      case x: RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig => x.asJson
    }
  )

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
    Encoder.forProduct4("messageType", "runtimeId", "deleteDisk", "traceId")(x =>
      (x.messageType, x.runtimeId, x.deleteDisk, x.traceId)
    )

  implicit val stopRuntimeMessageEncoder: Encoder[StopRuntimeMessage] =
    Encoder.forProduct3("messageType", "runtimeId", "traceId")(x => (x.messageType, x.runtimeId, x.traceId))

  implicit val startRuntimeMessageEncoder: Encoder[StartRuntimeMessage] =
    Encoder.forProduct3("messageType", "runtimeId", "traceId")(x => (x.messageType, x.runtimeId, x.traceId))

  implicit val updateRuntimeMessageEncoder: Encoder[UpdateRuntimeMessage] =
    Encoder.forProduct8("messageType",
                        "runtimeId",
                        "newMachineType",
                        "stopToUpdateMachineType",
                        "newDiskSize",
                        "newNumWorkers",
                        "newNumPreemptibles",
                        "traceId")(x =>
      (x.messageType,
       x.runtimeId,
       x.newMachineType,
       x.stopToUpdateMachineType,
       x.newDiskSize,
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
  implicit val createAppMessageEncoder: Encoder[CreateAppMessage] =
    Encoder.forProduct5("cluster", "appId", "project", "createDisk", "traceId")(x =>
      (x.cluster, x.appId, x.project, x.createDisk, x.traceId)
    )
  implicit val deleteAppMessageEncoder: Encoder[DeleteAppMessage] =
    Encoder.forProduct5("appId", "nodepoolId", "project", "deleteDisk", "traceId")(x =>
      (x.appId, x.nodepoolId, x.project, x.deleteDisk, x.traceId)
    )

  implicit val leoPubsubMessageEncoder: Encoder[LeoPubsubMessage] = Encoder.instance { message =>
    message match {
      case m: CreateDiskMessage    => m.asJson
      case m: UpdateDiskMessage    => m.asJson
      case m: DeleteDiskMessage    => m.asJson
      case m: CreateRuntimeMessage => m.asJson
      case m: DeleteRuntimeMessage => m.asJson
      case m: StopRuntimeMessage   => m.asJson
      case m: StartRuntimeMessage  => m.asJson
      case m: UpdateRuntimeMessage => m.asJson
      case m: CreateAppMessage     => m.asJson
      case m: DeleteAppMessage     => m.asJson
    }
  }
}
