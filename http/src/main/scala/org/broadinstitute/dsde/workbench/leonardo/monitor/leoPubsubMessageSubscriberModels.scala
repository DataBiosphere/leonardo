package org.broadinstitute.dsde.workbench.leonardo
package monitor

import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder}
import org.broadinstitute.dsde.workbench.google2.JsonCodec.{traceIdDecoder, traceIdEncoder}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsPath

sealed trait LeoPubsubMessage {
  def traceId: Option[TraceId]
  def messageType: String
}

object LeoPubsubMessage {
  final case class StopUpdateMessage(updatedMachineConfig: RuntimeConfig, runtimeId: Long, traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType = "stopUpdate"
  }

  final case class RuntimeTransitionMessage(runtimeFollowupDetails: RuntimeFollowupDetails, traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType = "transitionFinished"
  }

  final case class CreateRuntimeMessage(id: Long,
                                        runtimeProjectAndName: RuntimeProjectAndName,
                                        serviceAccountInfo: ServiceAccountInfo,
                                        asyncRuntimeFields: Option[AsyncRuntimeFields],
                                        auditInfo: AuditInfo,
                                        jupyterExtensionUri: Option[GcsPath],
                                        jupyterUserScriptUri: Option[UserScriptPath],
                                        jupyterStartUserScriptUri: Option[UserScriptPath],
                                        userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                        defaultClientId: Option[String],
                                        runtimeImages: Set[RuntimeImage],
                                        scopes: Set[String],
                                        welderEnabled: Boolean,
                                        customEnvironmentVariables: Map[String, String],
                                        runtimeConfig: RuntimeConfig,
                                        traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType = "createRuntime"
  }

  object CreateRuntimeMessage {
    def fromRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig, traceId: Option[TraceId]): CreateRuntimeMessage =
      CreateRuntimeMessage(
        runtime.id,
        RuntimeProjectAndName(runtime.googleProject, runtime.runtimeName),
        runtime.serviceAccountInfo,
        runtime.asyncRuntimeFields,
        runtime.auditInfo,
        runtime.jupyterExtensionUri,
        runtime.jupyterUserScriptUri,
        runtime.jupyterStartUserScriptUri,
        runtime.userJupyterExtensionConfig,
        runtime.defaultClientId,
        runtime.runtimeImages,
        runtime.scopes,
        runtime.welderEnabled,
        runtime.customEnvironmentVariables,
        runtimeConfig,
        traceId
      )
  }

  final case class DeleteRuntimeMessage(runtimeId: Long, traceId: Option[TraceId]) extends LeoPubsubMessage {
    val messageType = "deleteRuntime"
  }

  final case class StopRuntimeMessage(runtimeId: Long, traceId: Option[TraceId]) extends LeoPubsubMessage {
    val messageType = "stopRuntime"
  }

  final case class StartRuntimeMessage(runtimeId: Long, traceId: Option[TraceId]) extends LeoPubsubMessage {
    val messageType = "startRuntime"
  }

  final case class RuntimeFollowupDetails(runtimeId: Long, runtimeStatus: RuntimeStatus)
      extends Product
      with Serializable
}

final case class PubsubException(message: String) extends Exception

object LeoPubsubCodec {
  implicit val stopUpdateMessageDecoder: Decoder[StopUpdateMessage] =
    Decoder.forProduct3("updatedMachineConfig", "clusterId", "traceId")(StopUpdateMessage.apply)

  implicit val runtimeFollowupDetailsDecoder: Decoder[RuntimeFollowupDetails] =
    Decoder.forProduct2("clusterId", "clusterStatus")(RuntimeFollowupDetails.apply)

  implicit val clusterTransitionFinishedDecoder: Decoder[RuntimeTransitionMessage] =
    Decoder.forProduct2("clusterFollowupDetails", "traceId")(RuntimeTransitionMessage.apply)

  implicit val createClusterDecoder: Decoder[CreateRuntimeMessage] =
    Decoder.forProduct16(
      "id",
      "clusterProjectAndName",
      "serviceAccountInfo",
      "dataprocInfo",
      "auditInfo",
      "jupyterExtensionUri",
      "jupyterUserScriptUri",
      "jupyterStartUserScriptUri",
      "userJupyterExtensionConfig",
      "defaultClientId",
      "clusterImages",
      "scopes",
      "welderEnabled",
      "customClusterEnvironmentVariables",
      "runtimeConfig",
      "traceId"
    )(CreateRuntimeMessage.apply)

  implicit val deleteRuntimeDecoder: Decoder[DeleteRuntimeMessage] =
    Decoder.forProduct2("runtimeId", "traceId")(DeleteRuntimeMessage.apply)

  implicit val stopRuntimeDecoder: Decoder[StopRuntimeMessage] =
    Decoder.forProduct2("runtimeId", "traceId")(StopRuntimeMessage.apply)

  implicit val startRuntimeDecoder: Decoder[StartRuntimeMessage] =
    Decoder.forProduct2("runtimeId", "traceId")(StartRuntimeMessage.apply)

  implicit val leoPubsubMessageDecoder: Decoder[LeoPubsubMessage] = Decoder.instance { message =>
    for {
      messageType <- message.downField("messageType").as[String]
      value <- messageType match {
        case "stopUpdate"         => message.as[StopUpdateMessage]
        case "transitionFinished" => message.as[RuntimeTransitionMessage]
        case "createRuntime"      => message.as[CreateRuntimeMessage]
        case "deleteRuntime"      => message.as[DeleteRuntimeMessage]
        case "stopRuntime"        => message.as[StopRuntimeMessage]
        case "startRuntime"       => message.as[StartRuntimeMessage]
        case other                => Left(DecodingFailure(s"invalid message type: $other", List.empty))
      }
    } yield value
  }

  implicit val stopUpdateMessageEncoder: Encoder[StopUpdateMessage] =
    Encoder.forProduct3("messageType", "updatedMachineConfig", "clusterId")(
      x => (x.messageType, x.updatedMachineConfig, x.runtimeId)
    )

  implicit val runtimeFollowupDetailsEncoder: Encoder[RuntimeFollowupDetails] =
    Encoder.forProduct2("clusterId", "clusterStatus")(x => (x.runtimeId, x.runtimeStatus))

  implicit val runtimeTransitionFinishedEncoder: Encoder[RuntimeTransitionMessage] =
    Encoder.forProduct2("messageType", "clusterFollowupDetails")(x => (x.messageType, x.runtimeFollowupDetails))

  implicit val createRuntimeMessageEncoder: Encoder[CreateRuntimeMessage] =
    Encoder.forProduct17(
      "messageType",
      "id",
      "clusterProjectAndName",
      "serviceAccountInfo",
      "dataprocInfo",
      "auditInfo",
      "jupyterExtensionUri",
      "jupyterUserScriptUri",
      "jupyterStartUserScriptUri",
      "userJupyterExtensionConfig",
      "defaultClientId",
      "clusterImages",
      "scopes",
      "welderEnabled",
      "customClusterEnvironmentVariables",
      "runtimeConfig",
      "traceId"
    )(
      x =>
        (x.messageType,
         x.id,
         x.runtimeProjectAndName,
         x.serviceAccountInfo,
         x.asyncRuntimeFields,
         x.auditInfo,
         x.jupyterExtensionUri,
         x.jupyterUserScriptUri,
         x.jupyterStartUserScriptUri,
         x.userJupyterExtensionConfig,
         x.defaultClientId,
         x.runtimeImages,
         x.scopes,
         x.welderEnabled,
         x.customEnvironmentVariables,
         x.runtimeConfig,
         x.traceId)
    )

  implicit val deleteRuntimeMessageEncoder: Encoder[DeleteRuntimeMessage] =
    Encoder.forProduct3("messageType", "runtimeId", "traceId")(x => (x.messageType, x.runtimeId, x.traceId))

  implicit val stopRuntimeMessageEncoder: Encoder[StopRuntimeMessage] =
    Encoder.forProduct3("messageType", "runtimeId", "traceId")(x => (x.messageType, x.runtimeId, x.traceId))

  implicit val startRuntimeMessageEncoder: Encoder[StartRuntimeMessage] =
    Encoder.forProduct3("messageType", "runtimeId", "traceId")(x => (x.messageType, x.runtimeId, x.traceId))

  implicit val leoPubsubMessageEncoder: Encoder[LeoPubsubMessage] = Encoder.instance { message =>
    message match {
      case m: StopUpdateMessage        => m.asJson
      case m: RuntimeTransitionMessage => m.asJson
      case m: CreateRuntimeMessage     => m.asJson
      case m: DeleteRuntimeMessage     => m.asJson
      case m: StopRuntimeMessage       => m.asJson
      case m: StartRuntimeMessage      => m.asJson
    }
  }
}
