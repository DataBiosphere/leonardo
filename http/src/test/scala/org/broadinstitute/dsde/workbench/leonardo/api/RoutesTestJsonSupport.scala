package org.broadinstitute.dsde.workbench.leonardo

import java.net.URL
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.Cluster.LabelMap
import org.broadinstitute.dsde.workbench.leonardo.model.{
  AuditInfo,
  Cluster,
  ClusterImage,
  ClusterInternalId,
  DataprocInfo,
  UserJupyterExtensionConfig,
  UserScriptPath
}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, ClusterStatus, IP, Instance, OperationName}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsPath, GoogleProject}
import cats.implicits._
import spray.json._
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.{GcsPathFormat => _, _}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.google.GoogleJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.service.ListClusterResponse
import spray.json.DefaultJsonProtocol

object RoutesTestJsonSupport extends DefaultJsonProtocol {
  implicit val clusterReaderJsonReader: RootJsonReader[Cluster] = (json: JsValue) =>
    json match {
      case JsObject(fields: Map[String, JsValue]) =>
        Cluster(
          fields.getOrElse("id", JsNull).convertTo[Long],
          fields.getOrElse("internalId", JsNull).convertTo[ClusterInternalId],
          fields.getOrElse("clusterName", JsNull).convertTo[ClusterName],
          fields.getOrElse("googleProject", JsNull).convertTo[GoogleProject],
          fields.getOrElse("serviceAccountInfo", JsNull).convertTo[ServiceAccountInfo],
          (fields.get("googleId"), fields.get("operationName"), fields.get("stagingBucket")).mapN {
            (googleId, operationName, stagingBucket) =>
              DataprocInfo(
                googleId.convertTo[UUID],
                operationName.convertTo[OperationName],
                stagingBucket.convertTo[GcsBucketName],
                fields.getOrElse("hostIp", JsNull).convertTo[Option[IP]]
              )
          },
          AuditInfo(
            fields.getOrElse("creator", JsNull).convertTo[WorkbenchEmail],
            fields.getOrElse("createdDate", JsNull).convertTo[Instant],
            fields.getOrElse("destroyedDate", JsNull).convertTo[Option[Instant]],
            fields.getOrElse("dateAccessed", JsNull).convertTo[Instant],
            fields.getOrElse("kernelFoundBusyDate", JsNull).convertTo[Option[Instant]]
          ),
          fields.getOrElse("machineConfig", JsNull).convertTo[MachineConfig],
          fields.getOrElse("properties", JsNull).convertTo[Option[Map[String, String]]].getOrElse(Map.empty),
          fields.getOrElse("clusterUrl", JsNull).convertTo[URL],
          fields.getOrElse("status", JsNull).convertTo[ClusterStatus],
          fields.getOrElse("labels", JsNull).convertTo[LabelMap],
          fields.getOrElse("jupyterExtensionUri", JsNull).convertTo[Option[GcsPath]],
          fields.getOrElse("jupyterUserScriptUri", JsNull).convertTo[Option[UserScriptPath]],
          fields.getOrElse("jupyterStartUserScriptUri", JsNull).convertTo[Option[UserScriptPath]],
          fields.getOrElse("errors", JsNull).convertTo[List[ClusterError]],
          fields.getOrElse("instances", JsNull).convertTo[Set[Instance]],
          fields.getOrElse("userJupyterExtensionConfig", JsNull).convertTo[Option[UserJupyterExtensionConfig]],
          fields.getOrElse("autopauseThreshold", JsNull).convertTo[Int],
          fields.getOrElse("defaultClientId", JsNull).convertTo[Option[String]],
          fields.getOrElse("stopAfterCreation", JsNull).convertTo[Boolean],
          fields.getOrElse("allowStop", JsNull).convertTo[Boolean],
          fields.getOrElse("clusterImages", JsNull).convertTo[Set[ClusterImage]],
          fields.getOrElse("scopes", JsNull).convertTo[Set[String]],
          fields.getOrElse("welderEnabled", JsNull).convertTo[Boolean],
          fields
            .getOrElse("customClusterEnvironmentVariables", JsNull)
            .convertTo[Option[Map[String, String]]]
            .getOrElse(Map.empty)
        )
      case _ => deserializationError("Cluster expected as a JsObject")
    }

  implicit val listClusterResponseReaderJsonReader: RootJsonReader[ListClusterResponse] = (json: JsValue) =>
    json match {
      case JsObject(fields: Map[String, JsValue]) =>
        ListClusterResponse(
          fields.getOrElse("id", JsNull).convertTo[Long],
          fields.getOrElse("internalId", JsNull).convertTo[ClusterInternalId],
          fields.getOrElse("clusterName", JsNull).convertTo[ClusterName],
          fields.getOrElse("googleProject", JsNull).convertTo[GoogleProject],
          fields.getOrElse("serviceAccountInfo", JsNull).convertTo[ServiceAccountInfo],
          (fields.get("googleId"), fields.get("operationName"), fields.get("stagingBucket")).mapN {
            (googleId, operationName, stagingBucket) =>
              DataprocInfo(
                googleId.convertTo[UUID],
                operationName.convertTo[OperationName],
                stagingBucket.convertTo[GcsBucketName],
                fields.getOrElse("hostIp", JsNull).convertTo[Option[IP]]
              )
          },
          AuditInfo(
            fields.getOrElse("creator", JsNull).convertTo[WorkbenchEmail],
            fields.getOrElse("createdDate", JsNull).convertTo[Instant],
            fields.getOrElse("destroyedDate", JsNull).convertTo[Option[Instant]],
            fields.getOrElse("dateAccessed", JsNull).convertTo[Instant],
            fields.getOrElse("kernelFoundBusyDate", JsNull).convertTo[Option[Instant]]
          ),
          fields.getOrElse("machineConfig", JsNull).convertTo[MachineConfig],
          fields.getOrElse("clusterUrl", JsNull).convertTo[URL],
          fields.getOrElse("status", JsNull).convertTo[ClusterStatus],
          fields.getOrElse("labels", JsNull).convertTo[LabelMap],
          fields.getOrElse("jupyterExtensionUri", JsNull).convertTo[Option[GcsPath]],
          fields.getOrElse("jupyterUserScriptUri", JsNull).convertTo[Option[UserScriptPath]],
          fields.getOrElse("instances", JsNull).convertTo[Set[Instance]],
          fields.getOrElse("autopauseThreshold", JsNull).convertTo[Int],
          fields.getOrElse("defaultClientId", JsNull).convertTo[Option[String]],
          fields.getOrElse("stopAfterCreation", JsNull).convertTo[Boolean],
          fields.getOrElse("welderEnabled", JsNull).convertTo[Boolean]
        )
      case _ => deserializationError("Cluster expected as a JsObject")
    }
}
