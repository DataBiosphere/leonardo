package org.broadinstitute.dsde.workbench.leonardo
package monitor

import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder}
import org.broadinstitute.dsde.workbench.google2.JsonCodec.traceIdDecoder
import org.broadinstitute.dsde.workbench.google2.JsonCodec.traceIdEncoder
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, ClusterStatus, IP, OperationName}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterProjectAndName, DataprocInfo}
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
                           clusterProjectAndName: ClusterProjectAndName,
                           serviceAccountInfo: ServiceAccountInfo,
                           dataprocInfo: Option[DataprocInfo],
                           auditInfo: AuditInfo,
                           properties: Map[String, String],
                           jupyterExtensionUri: Option[GcsPath],
                           jupyterUserScriptUri: Option[UserScriptPath],
                           jupyterStartUserScriptUri: Option[UserScriptPath],
                           userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                           defaultClientId: Option[String],
                           clusterImages: Set[ClusterImage],
                           scopes: Set[String],
                           welderEnabled: Boolean,
                           customClusterEnvironmentVariables: Map[String, String],
                           runtimeConfig: RuntimeConfig,
                           traceId: Option[TraceId])
      extends LeoPubsubMessage {
    val messageType = "createCluster"
  }

  final case class ClusterFollowupDetails(clusterId: Long, clusterStatus: ClusterStatus)
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

  implicit val clusterNameDecoder: Decoder[ClusterName] = Decoder.decodeString.map(ClusterName)
  implicit val operationNameDecoder: Decoder[OperationName] = Decoder.decodeString.map(OperationName)
  implicit val ipDecoder: Decoder[IP] = Decoder.decodeString.map(IP)

  implicit val clusterProjectAndNameDecoder: Decoder[ClusterProjectAndName] =
    Decoder.forProduct2("googleProject", "clusterName")(ClusterProjectAndName.apply)

  implicit val dataprocInfoDecoder: Decoder[DataprocInfo] =
    Decoder.forProduct4("googleId", "operationName", "stagingBucket", "hostIp")(DataprocInfo.apply)

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
      "clusterImages",
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
    Encoder.forProduct2("clusterId", "clusterStatus")(x => (x.clusterId, x.clusterStatus))

  implicit val clusterTransitionFinishedEncoder: Encoder[ClusterTransition] =
    Encoder.forProduct2("messageType", "clusterFollowupDetails")(x => (x.messageType, x.clusterFollowupDetails))

  //TODO: These google specific models json codec should be moved to a better place once Rob's GCE PR is in
  implicit val clusterNameEncoder: Encoder[ClusterName] = Encoder.encodeString.contramap(_.value)
  implicit val operationNameEncoder: Encoder[OperationName] = Encoder.encodeString.contramap(_.value)
  implicit val ipEncoder: Encoder[IP] = Encoder.encodeString.contramap(_.value)
  implicit val dataprocInfoEncoder: Encoder[DataprocInfo] =
    Encoder.forProduct4("googleId", "operationName", "stagingBucket", "hostIp")(x => DataprocInfo.unapply(x).get)

  implicit val clusterProjectAndNameEncoder: Encoder[ClusterProjectAndName] = Encoder.forProduct2(
    "googleProject",
    "clusterName"
  )(x => ClusterProjectAndName.unapply(x).get)

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
         x.dataprocInfo,
         x.auditInfo,
         x.properties,
         x.jupyterExtensionUri,
         x.jupyterUserScriptUri,
         x.jupyterStartUserScriptUri,
         x.userJupyterExtensionConfig,
         x.defaultClientId,
         x.clusterImages,
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
