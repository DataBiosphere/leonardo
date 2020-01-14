package org.broadinstitute.dsde.workbench.leonardo.service

import java.net.URL

import org.broadinstitute.dsde.workbench.leonardo.model.Cluster.LabelMap
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.SecondaryWorker
import org.broadinstitute.dsde.workbench.leonardo.model.google.GoogleJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, ClusterStatus, Instance}
import org.broadinstitute.dsde.workbench.leonardo.{MachineConfig, ServiceAccountInfo}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.{GcsPathFormat => _, _}
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}
import spray.json.{JsObject, _}

final case class ListClusterResponse(id: Long,
                                     internalId: ClusterInternalId,
                                     clusterName: ClusterName,
                                     googleProject: GoogleProject,
                                     serviceAccountInfo: ServiceAccountInfo,
                                     dataprocInfo: Option[DataprocInfo],
                                     auditInfo: AuditInfo,
                                     machineConfig: MachineConfig,
                                     clusterUrl: URL,
                                     status: ClusterStatus,
                                     labels: LabelMap,
                                     jupyterExtensionUri: Option[GcsPath],
                                     jupyterUserScriptUri: Option[UserScriptPath],
                                     instances: Set[Instance],
                                     autopauseThreshold: Int,
                                     defaultClientId: Option[String],
                                     stopAfterCreation: Boolean,
                                     welderEnabled: Boolean) {
  def projectNameString: String = s"${googleProject.value}/${clusterName.value}"
  def nonPreemptibleInstances: Set[Instance] = instances.filterNot(_.dataprocRole.contains(SecondaryWorker))
}

object LeonardoServiceJsonCodec extends DefaultJsonProtocol {
  implicit val listClusterResponseWriter: RootJsonWriter[ListClusterResponse] = (obj: ListClusterResponse) => {
    val allFields = Map(
      "id" -> obj.id.toJson,
      "internalId" -> obj.internalId.value.toJson,
      "clusterName" -> obj.clusterName.toJson,
      "googleId" -> obj.dataprocInfo.map(_.googleId.toJson).getOrElse(JsNull),
      "googleProject" -> obj.googleProject.toJson,
      "serviceAccountInfo" -> obj.serviceAccountInfo.toJson,
      "machineConfig" -> obj.machineConfig.toJson,
      "clusterUrl" -> obj.clusterUrl.toJson,
      "operationName" -> obj.dataprocInfo.map(_.operationName.toJson).getOrElse(JsNull),
      "status" -> obj.status.toJson,
      "hostIp" -> obj.dataprocInfo.map(_.hostIp.toJson).getOrElse(JsNull),
      "creator" -> obj.auditInfo.creator.toJson,
      "createdDate" -> obj.auditInfo.createdDate.toJson,
      "destroyedDate" -> obj.auditInfo.destroyedDate.toJson,
      "kernelFoundBusyDate" -> obj.auditInfo.kernelFoundBusyDate.toJson,
      "labels" -> obj.labels.toJson,
      "jupyterExtensionUri" -> obj.jupyterExtensionUri.toJson,
      "jupyterUserScriptUri" -> obj.jupyterUserScriptUri.toJson,
      "stagingBucket" -> obj.dataprocInfo.map(_.stagingBucket.toJson).getOrElse(JsNull),
      "instances" -> obj.instances.toJson,
      "dateAccessed" -> obj.auditInfo.dateAccessed.toJson,
      "autopauseThreshold" -> obj.autopauseThreshold.toJson,
      "defaultClientId" -> obj.defaultClientId.toJson,
      "stopAfterCreation" -> obj.stopAfterCreation.toJson,
      "welderEnabled" -> obj.welderEnabled.toJson,
      "scopes" -> List
        .empty[String]
        .toJson //TODO: stubbing this out temporarily until AOU move to new swagger generated client
    )

    val presentFields = allFields.filter(_._2 != JsNull)

    JsObject(presentFields)
  }
}
