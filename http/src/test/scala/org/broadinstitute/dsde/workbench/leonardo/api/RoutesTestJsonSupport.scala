package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.net.URL
import java.time.Instant

import cats.implicits._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.RuntimeSamResource
import org.broadinstitute.dsde.workbench.leonardo.http.RuntimeRoutesTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  CreateRuntimeRequest,
  GetRuntimeResponse,
  ListRuntimeResponse
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import spray.json.DefaultJsonProtocol

object RoutesTestJsonSupport extends DefaultJsonProtocol {
  implicit val listClusterResponseDecoder: Decoder[ListRuntimeResponse] = Decoder.instance { x =>
    for {
      id <- x.downField("id").as[Long]
      internalId <- x.downField("internalId").as[RuntimeSamResource]
      clusterName <- x.downField("clusterName").as[RuntimeName]
      googleProject <- x.downField("googleProject").as[GoogleProject]
      serviceAccountInfo <- x.downField("googleServiceAccount").as[WorkbenchEmail]
      dataprocInfo <- for {
        googleId <- x.downField("googleId").as[Option[GoogleId]]
        operationName <- x.downField("operationName").as[Option[OperationName]]
        stagingBucket <- x.downField("stagingBucket").as[Option[GcsBucketName]]
        hostIp <- x.downField("hostIp").as[Option[IP]]
      } yield {
        (googleId, operationName, stagingBucket).mapN((x, y, z) => AsyncRuntimeFields(x, y, z, hostIp))
      }
      machineConfig <- x.downField("machineConfig").as[RuntimeConfig]
      clusterUrl <- x.downField("clusterUrl").as[URL]
      status <- x.downField("status").as[RuntimeStatus]
      creator <- x.downField("creator").as[WorkbenchEmail]
      createdDate <- x.downField("createdDate").as[Instant]
      destroyedDate <- x.downField("destroyedDate").as[Option[Instant]]
      kernelFoundBusyDate <- x.downField("kernelFoundBusyDate").as[Option[Instant]]
      labels <- x.downField("labels").as[LabelMap]
      jupyterUserScriptUri <- x.downField("jupyterUserScriptUri").as[Option[UserScriptPath]]
      dateAccessed <- x.downField("dateAccessed").as[Instant]
      autopauseThreshold <- x.downField("autopauseThreshold").as[Int]
      defaultClientId <- x.downField("defaultClientId").as[Option[String]]
      stopAfterCreation <- x.downField("stopAfterCreation").as[Boolean]
      welderEnabled <- x.downField("welderEnabled").as[Boolean]
      patchInProgress <- x.downField("patchInProgress").as[Boolean]
    } yield ListRuntimeResponse(
      id,
      internalId,
      clusterName,
      googleProject,
      serviceAccountInfo,
      dataprocInfo,
      AuditInfo(creator, createdDate, destroyedDate, dateAccessed),
      kernelFoundBusyDate,
      machineConfig,
      clusterUrl,
      status,
      labels,
      jupyterUserScriptUri,
      Set.empty, //TODO: do this when this field is needed
      autopauseThreshold,
      defaultClientId,
      stopAfterCreation,
      welderEnabled,
      patchInProgress
    )
  }

  implicit val getClusterResponseDecoder: Decoder[GetRuntimeResponse] = Decoder.instance { x =>
    for {
      id <- x.downField("id").as[Long]
      internalId <- x.downField("internalId").as[RuntimeSamResource]
      clusterName <- x.downField("clusterName").as[RuntimeName]
      googleProject <- x.downField("googleProject").as[GoogleProject]
      serviceAccountInfo <- x.downField("serviceAccountInfo").as[WorkbenchEmail]
      dataprocInfo <- for {
        googleId <- x.downField("googleId").as[Option[GoogleId]]
        operationName <- x.downField("operationName").as[Option[OperationName]]
        stagingBucket <- x.downField("stagingBucket").as[Option[GcsBucketName]]
        hostIp <- x.downField("hostIp").as[Option[IP]]
      } yield {
        (googleId, operationName, stagingBucket).mapN((x, y, z) => AsyncRuntimeFields(x, y, z, hostIp))
      }
      machineConfig <- x.downField("machineConfig").as[RuntimeConfig]
      clusterUrl <- x.downField("clusterUrl").as[URL]
      status <- x.downField("status").as[RuntimeStatus]
      creator <- x.downField("creator").as[WorkbenchEmail]
      createdDate <- x.downField("createdDate").as[Instant]
      destroyedDate <- x.downField("destroyedDate").as[Option[Instant]]
      kernelFoundBusyDate <- x.downField("kernelFoundBusyDate").as[Option[Instant]]
      labels <- x.downField("labels").as[LabelMap]
      jupyterUserScriptUri <- x.downField("jupyterUserScriptUri").as[Option[UserScriptPath]]
      jupyterStartUserScriptUri <- x.downField("jupyterStartUserScriptUri").as[Option[UserScriptPath]]
      errors <- x.downField("errors").as[List[RuntimeError]]
      userJupyterExtensionConfig <- x.downField("userJupyterExtensionConfig").as[Option[UserJupyterExtensionConfig]]
      dateAccessed <- x.downField("dateAccessed").as[Instant]
      autopauseThreshold <- x.downField("autopauseThreshold").as[Int]
      defaultClientId <- x.downField("defaultClientId").as[Option[String]]
      stopAfterCreation <- x.downField("stopAfterCreation").as[Boolean]
      clusterImages <- x.downField("clusterImages").as[Set[RuntimeImage]]
      scopes <- x.downField("scopes").as[Set[String]]
      welderEnabled <- x.downField("welderEnabled").as[Boolean]
      patchInProgress <- x.downField("patchInProgress").as[Boolean]
    } yield GetRuntimeResponse(
      id,
      internalId,
      clusterName,
      googleProject,
      serviceAccountInfo,
      dataprocInfo,
      AuditInfo(creator, createdDate, destroyedDate, dateAccessed),
      kernelFoundBusyDate,
      machineConfig,
      clusterUrl,
      status,
      labels,
      jupyterUserScriptUri,
      jupyterStartUserScriptUri,
      errors,
      Set.empty, // Dataproc instances
      userJupyterExtensionConfig,
      autopauseThreshold,
      defaultClientId,
      stopAfterCreation,
      clusterImages,
      scopes,
      welderEnabled,
      patchInProgress,
      Map.empty, // custom cluster env vars
      None // disk config
    )
  }

  implicit val clusterRequestEncoder: Encoder[CreateRuntimeRequest] = Encoder.forProduct16(
    "labels",
    "jupyterUserScriptUri",
    "jupyterStartUserScriptUri",
    "runtimeConfig",
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

  implicit val getClusterResponseTestDecoder: Decoder[GetClusterResponseTest] = Decoder.forProduct3(
    "id",
    "clusterName",
    "googleServiceAccount"
  )(GetClusterResponseTest.apply)
}
