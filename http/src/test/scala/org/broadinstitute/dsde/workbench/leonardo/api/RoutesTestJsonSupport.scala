package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.net.URL
import java.time.Instant
import java.util.UUID

import cats.implicits._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  CreateRuntimeRequest,
  ListRuntimeResponse,
  RuntimeConfigRequest
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.{GcsPathFormat => _}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsPath, GoogleProject}
import spray.json.DefaultJsonProtocol

object RoutesTestJsonSupport extends DefaultJsonProtocol {
  implicit val listClusterResponseDecoder: Decoder[ListRuntimeResponse] = Decoder.instance { x =>
    for {
      id <- x.downField("id").as[Long]
      internalId <- x.downField("internalId").as[ClusterInternalId]
      clusterName <- x.downField("clusterName").as[ClusterName]
      googleProject <- x.downField("googleProject").as[GoogleProject]
      serviceAccountInfo <- x.downField("serviceAccountInfo").as[ServiceAccountInfo]
      dataprocInfo <- for {
        googleId <- x.downField("googleId").as[Option[UUID]]
        operationName <- x.downField("operationName").as[Option[OperationName]]
        stagingBucket <- x.downField("stagingBucket").as[Option[GcsBucketName]]
        hostIp <- x.downField("hostIp").as[Option[IP]]
      } yield {
        (googleId, operationName, stagingBucket).mapN((x, y, z) => AsyncRuntimeFields(x, y, z, hostIp))
      }
      machineConfig <- x.downField("machineConfig").as[RuntimeConfig]
      clusterUrl <- x.downField("clusterUrl").as[URL]
//      status <- x.downField("status").as[ClusterStatus]
      creator <- x.downField("creator").as[WorkbenchEmail]
      createdDate <- x.downField("createdDate").as[Instant]
      destroyedDate <- x.downField("destroyedDate").as[Option[Instant]]
      kernelFoundBusyDate <- x.downField("kernelFoundBusyDate").as[Option[Instant]]
      labels <- x.downField("labels").as[LabelMap]
      jupyterExtensionUri <- x.downField("jupyterExtensionUri").as[Option[GcsPath]]
      jupyterUserScriptUri <- x.downField("jupyterUserScriptUri").as[Option[UserScriptPath]]
      dateAccessed <- x.downField("dateAccessed").as[Instant]
      autopauseThreshold <- x.downField("autopauseThreshold").as[Int]
      defaultClientId <- x.downField("defaultClientId").as[Option[String]]
      stopAfterCreation <- x.downField("stopAfterCreation").as[Boolean]
      welderEnabled <- x.downField("welderEnabled").as[Boolean]
    } yield ListRuntimeResponse(
      id,
      internalId,
      clusterName,
      googleProject,
      serviceAccountInfo,
      dataprocInfo,
      AuditInfo(creator, createdDate, destroyedDate, dateAccessed, kernelFoundBusyDate),
      machineConfig,
      clusterUrl,
      RuntimeStatus.Running, //TODO: fill real value when this field is needed in test
      labels,
      jupyterExtensionUri,
      jupyterUserScriptUri,
      Set.empty, //TODO: do this when this field is needed
      autopauseThreshold,
      defaultClientId,
      stopAfterCreation,
      welderEnabled
    )
  }

  implicit val dataprocConfigEncoder: Encoder[RuntimeConfigRequest.DataprocConfig] = Encoder.forProduct7(
    "numberOfWorkers",
    "masterMachineType",
    "masterDiskSize",
    // worker settings are None when numberOfWorkers is 0
    "workerMachineType",
    "workerDiskSize",
    "numberOfWorkerLocalSSDs",
    "numberOfPreemptibleWorkers"
  )(
    x =>
      (x.numberOfWorkers,
       x.masterMachineType,
       x.masterDiskSize,
       x.workerMachineType,
       x.workerDiskSize,
       x.numberOfWorkerLocalSSDs,
       x.numberOfPreemptibleWorkers)
  )
  implicit val gceRuntimConfigEncoder: Encoder[RuntimeConfigRequest.GceConfig] = Encoder.forProduct2(
    "machineType",
    "diskSize"
  )(x => (x.machineType, x.diskSize))

  implicit val runtimeConfigRequestEncoder: Encoder[RuntimeConfigRequest] = Encoder.instance { x =>
    x match {
      case x: RuntimeConfigRequest.DataprocConfig => x.asJson
      case x: RuntimeConfigRequest.GceConfig      => x.asJson
    }
  }
  implicit val clusterRequestEncoder: Encoder[CreateRuntimeRequest] = Encoder.forProduct18(
    "labels",
    "jupyterExtensionUri",
    "jupyterUserScriptUri",
    "jupyterStartUserScriptUri",
    "runtimeConfig",
    "properties",
    "stopAfterCreation",
    "allowStop",
    "userJupyterExtensionConfig",
    "autopause",
    "autopauseThreshold",
    "defaultClientId",
    "jupyterDockerImage",
    "toolDockerImage",
    "welderDockerImage",
    "scopes",
    "enableWelder",
    "customClusterEnvironmentVariables"
  )(x => CreateRuntimeRequest.unapply(x).get)

  implicit val getClusterResponseTestDecoder: Decoder[GetClusterResponseTest] = Decoder.forProduct4(
    "id",
    "clusterName",
    "serviceAccountInfo",
    "jupyterExtensionUri"
  )(GetClusterResponseTest.apply)
}
