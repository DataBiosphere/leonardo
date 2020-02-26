package org.broadinstitute.dsde.workbench.leonardo
package monitor

import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder}
import org.broadinstitute.dsde.workbench.google2.JsonCodec.traceIdDecoder
import org.broadinstitute.dsde.workbench.google2.JsonCodec.traceIdEncoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GcsPath

sealed trait LeoPubsubMessage {
  def traceId: Option[TraceId]
  def messageType: String
}

object LeoPubsubMessage {
  final case class StopUpdate(updatedMachineConfig: RuntimeConfig, clusterId: Long, traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType = "stopUpdate"
  }

  case class ClusterTransition(clusterFollowupDetails: ClusterFollowupDetails, traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType = "transitionFinished"
  }

  case class CreateCluster(id: Long,
                           clusterProjectAndName: RuntimeProjectAndName,
                           serviceAccountInfo: ServiceAccountInfo,
                           asyncRuntimeFields: Option[AsyncRuntimeFields],
                           auditInfo: AuditInfo,
                           properties: Map[String, String],
                           jupyterExtensionUri: Option[GcsPath],
                           jupyterUserScriptUri: Option[UserScriptPath],
                           jupyterStartUserScriptUri: Option[UserScriptPath],
                           userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                           defaultClientId: Option[String],
                           runtimeImages: Set[RuntimeImage],
                           scopes: Set[String],
                           welderEnabled: Boolean,
                           customClusterEnvironmentVariables: Map[String, String],
                           runtimeConfig: RuntimeConfig,
                           traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType = "createCluster"
  }

  object CreateCluster {
    def fromRuntime(runtime: Runtime, runtimeConfig: RuntimeConfig, traceId: Option[TraceId]): CreateCluster =
      CreateCluster(
        runtime.id,
        RuntimeProjectAndName(runtime.googleProject, runtime.runtimeName),
        runtime.serviceAccountInfo,
        runtime.asyncRuntimeFields,
        runtime.auditInfo,
        runtime.dataprocProperties,
        runtime.jupyterExtensionUri,
        runtime.jupyterUserScriptUri,
        runtime.jupyterStartUserScriptUri,
        runtime.userJupyterExtensionConfig,
        runtime.defaultClientId,
        runtime.runtimeImages,
        runtime.scopes,
        runtime.welderEnabled,
        runtime.customClusterEnvironmentVariables,
        runtimeConfig,
        traceId
      )
  }

  final case class ClusterFollowupDetails(clusterId: Long, runtimeStatus: RuntimeStatus)
      extends Product
      with Serializable
}

final case class PubsubException(message: String) extends Exception

object LeoPubsubCodec {
  implicit val stopUpdateMessageDecoder: Decoder[StopUpdate] =
    Decoder.forProduct3("updatedMachineConfig", "clusterId", "traceId")(StopUpdate.apply)

  implicit val clusterFollowupDetailsDecoder: Decoder[ClusterFollowupDetails] =
    Decoder.forProduct2("clusterId", "clusterStatus")(ClusterFollowupDetails.apply)

  implicit val clusterTransitionFinishedDecoder: Decoder[ClusterTransition] =
    Decoder.forProduct2("clusterFollowupDetails", "traceId")(ClusterTransition.apply)

  implicit val createClusterDecoder: Decoder[CreateCluster] =
    Decoder.forProduct17(
      "id",
      "clusterProjectAndName",
      "serviceAccountInfo",
      "dataprocInfo",
      "auditInfo",
      "properties",
      "jupyterExtensionUri",
      "jupyterUserScriptUri",
      "jupyterStartUserScriptUri",
      "userJupyterExtensionConfig",
      "defaultClientId",
      "runtimeImages",
      "scopes",
      "welderEnabled",
      "customClusterEnvironmentVariables",
      "runtimeConfig",
      "traceId"
    )(CreateCluster.apply)

  implicit val leoPubsubMessageDecoder: Decoder[LeoPubsubMessage] = Decoder.instance { message =>
    for {
      messageType <- message.downField("messageType").as[String]
      value <- messageType match {
        case "stopUpdate"         => message.as[StopUpdate]
        case "transitionFinished" => message.as[ClusterTransition]
        case "createCluster"      => message.as[CreateCluster]
        case other                => Left(DecodingFailure(s"invalid message type: ${other}", List.empty))
      }
    } yield value
  }

  implicit val stopUpdateMessageEncoder: Encoder[StopUpdate] =
    Encoder.forProduct3("messageType", "updatedMachineConfig", "clusterId")(
      x => (x.messageType, x.updatedMachineConfig, x.clusterId)
    )

  implicit val clusterFollowupDetailsEncoder: Encoder[ClusterFollowupDetails] =
    Encoder.forProduct2("clusterId", "clusterStatus")(x => (x.clusterId, x.runtimeStatus))

  implicit val clusterTransitionFinishedEncoder: Encoder[ClusterTransition] =
    Encoder.forProduct2("messageType", "clusterFollowupDetails")(x => (x.messageType, x.clusterFollowupDetails))

  implicit val createClusterEncoder: Encoder[CreateCluster] =
    Encoder.forProduct18(
      "messageType",
      "id",
      "clusterProjectAndName",
      "serviceAccountInfo",
      "dataprocInfo",
      "auditInfo",
      "properties",
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
         x.clusterProjectAndName,
         x.serviceAccountInfo,
         x.asyncRuntimeFields,
         x.auditInfo,
         x.properties,
         x.jupyterExtensionUri,
         x.jupyterUserScriptUri,
         x.jupyterStartUserScriptUri,
         x.userJupyterExtensionConfig,
         x.defaultClientId,
         x.runtimeImages,
         x.scopes,
         x.welderEnabled,
         x.customClusterEnvironmentVariables,
         x.runtimeConfig,
         x.traceId)
    )

  implicit val leoPubsubMessageEncoder: Encoder[LeoPubsubMessage] = Encoder.instance { message =>
    message match {
      case m: StopUpdate        => m.asJson
      case m: ClusterTransition => m.asJson
      case m: CreateCluster     => m.asJson
    }
  }
}
