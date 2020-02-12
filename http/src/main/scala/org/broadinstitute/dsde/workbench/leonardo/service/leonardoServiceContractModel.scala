package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.net.URL

import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.SecondaryWorker
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, ClusterStatus, Instance}
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport.{GcsPathFormat => _}
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}

final case class ListClusterResponse(id: Long,
                                     internalId: ClusterInternalId,
                                     clusterName: ClusterName,
                                     googleProject: GoogleProject,
                                     serviceAccountInfo: ServiceAccountInfo,
                                     dataprocInfo: Option[DataprocInfo],
                                     auditInfo: AuditInfo,
                                     machineConfig: RuntimeConfig,
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

final case class GetClusterResponse(id: Long,
                                    internalId: ClusterInternalId,
                                    clusterName: ClusterName,
                                    googleProject: GoogleProject,
                                    serviceAccountInfo: ServiceAccountInfo,
                                    dataprocInfo: Option[DataprocInfo],
                                    auditInfo: AuditInfo,
                                    properties: Map[String, String],
                                    runtimeConfig: RuntimeConfig,
                                    clusterUrl: URL,
                                    status: ClusterStatus,
                                    labels: LabelMap,
                                    jupyterExtensionUri: Option[GcsPath],
                                    jupyterUserScriptUri: Option[UserScriptPath],
                                    jupyterStartUserScriptUri: Option[UserScriptPath],
                                    errors: List[ClusterError],
                                    instances: Set[Instance],
                                    userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                    autopauseThreshold: Int,
                                    defaultClientId: Option[String],
                                    stopAfterCreation: Boolean,
                                    clusterImages: Set[ClusterImage],
                                    scopes: Set[String],
                                    welderEnabled: Boolean,
                                    customClusterEnvironmentVariables: Map[String, String])

object GetClusterResponse {
  def fromCluster(cluster: Cluster, runtimeConfig: RuntimeConfig) = GetClusterResponse(
    cluster.id,
    cluster.internalId,
    cluster.clusterName,
    cluster.googleProject,
    cluster.serviceAccountInfo,
    cluster.dataprocInfo,
    cluster.auditInfo,
    cluster.properties,
    runtimeConfig,
    cluster.clusterUrl,
    cluster.status,
    cluster.labels,
    cluster.jupyterExtensionUri,
    cluster.jupyterUserScriptUri,
    cluster.jupyterStartUserScriptUri,
    cluster.errors,
    cluster.instances,
    cluster.userJupyterExtensionConfig,
    cluster.autopauseThreshold,
    cluster.defaultClientId,
    cluster.stopAfterCreation,
    cluster.clusterImages,
    cluster.scopes,
    cluster.welderEnabled,
    cluster.customClusterEnvironmentVariables
  )
}

// Currently, CreateClusterResponse has exactly the same fields as GetClusterResponse, but going forward, when we can,
// we should deprecate and remove some of fields for createCluster request
final case class CreateClusterAPIResponse(id: Long,
                                          internalId: ClusterInternalId,
                                          clusterName: ClusterName,
                                          googleProject: GoogleProject,
                                          serviceAccountInfo: ServiceAccountInfo,
                                          dataprocInfo: Option[DataprocInfo],
                                          auditInfo: AuditInfo,
                                          properties: Map[String, String],
                                          runtimeConfig: RuntimeConfig,
                                          clusterUrl: URL,
                                          status: ClusterStatus,
                                          labels: LabelMap,
                                          jupyterExtensionUri: Option[GcsPath],
                                          jupyterUserScriptUri: Option[UserScriptPath],
                                          jupyterStartUserScriptUri: Option[UserScriptPath],
                                          errors: List[ClusterError],
                                          instances: Set[Instance],
                                          userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                          autopauseThreshold: Int,
                                          defaultClientId: Option[String],
                                          stopAfterCreation: Boolean,
                                          clusterImages: Set[ClusterImage],
                                          scopes: Set[String],
                                          welderEnabled: Boolean,
                                          customClusterEnvironmentVariables: Map[String, String])

object CreateClusterAPIResponse {
  def fromCluster(cluster: Cluster, runtimeConfig: RuntimeConfig) = CreateClusterAPIResponse(
    cluster.id,
    cluster.internalId,
    cluster.clusterName,
    cluster.googleProject,
    cluster.serviceAccountInfo,
    cluster.dataprocInfo,
    cluster.auditInfo,
    cluster.properties,
    runtimeConfig,
    cluster.clusterUrl,
    cluster.status,
    cluster.labels,
    cluster.jupyterExtensionUri,
    cluster.jupyterUserScriptUri,
    cluster.jupyterStartUserScriptUri,
    cluster.errors,
    cluster.instances,
    cluster.userJupyterExtensionConfig,
    cluster.autopauseThreshold,
    cluster.defaultClientId,
    cluster.stopAfterCreation,
    cluster.clusterImages,
    cluster.scopes,
    cluster.welderEnabled,
    cluster.customClusterEnvironmentVariables
  )
}

// Currently, CreateClusterResponse has exactly the same fields as GetClusterResponse, but going forward, when we can,
// we should deprecate and remove some of fields for createCluster request
final case class UpdateClusterResponse(id: Long,
                                       internalId: ClusterInternalId,
                                       clusterName: ClusterName,
                                       googleProject: GoogleProject,
                                       serviceAccountInfo: ServiceAccountInfo,
                                       dataprocInfo: Option[DataprocInfo],
                                       auditInfo: AuditInfo,
                                       properties: Map[String, String],
                                       runtimeConfig: RuntimeConfig,
                                       clusterUrl: URL,
                                       status: ClusterStatus,
                                       labels: LabelMap,
                                       jupyterExtensionUri: Option[GcsPath],
                                       jupyterUserScriptUri: Option[UserScriptPath],
                                       jupyterStartUserScriptUri: Option[UserScriptPath],
                                       errors: List[ClusterError],
                                       instances: Set[Instance],
                                       userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                       autopauseThreshold: Int,
                                       defaultClientId: Option[String],
                                       stopAfterCreation: Boolean,
                                       clusterImages: Set[ClusterImage],
                                       scopes: Set[String],
                                       welderEnabled: Boolean,
                                       customClusterEnvironmentVariables: Map[String, String])

object UpdateClusterResponse {
  def fromCluster(cluster: Cluster, runtimeConfig: RuntimeConfig) = UpdateClusterResponse(
    cluster.id,
    cluster.internalId,
    cluster.clusterName,
    cluster.googleProject,
    cluster.serviceAccountInfo,
    cluster.dataprocInfo,
    cluster.auditInfo,
    cluster.properties,
    runtimeConfig,
    cluster.clusterUrl,
    cluster.status,
    cluster.labels,
    cluster.jupyterExtensionUri,
    cluster.jupyterUserScriptUri,
    cluster.jupyterStartUserScriptUri,
    cluster.errors,
    cluster.instances,
    cluster.userJupyterExtensionConfig,
    cluster.autopauseThreshold,
    cluster.defaultClientId,
    cluster.stopAfterCreation,
    cluster.clusterImages,
    cluster.scopes,
    cluster.welderEnabled,
    cluster.customClusterEnvironmentVariables
  )
}
