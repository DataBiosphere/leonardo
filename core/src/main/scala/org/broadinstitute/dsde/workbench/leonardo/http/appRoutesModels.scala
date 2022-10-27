package org.broadinstitute.dsde.workbench.leonardo.http

import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.{
  App,
  AppError,
  AppName,
  AppStatus,
  AppType,
  AuditInfo,
  CloudContext,
  CloudProvider,
  KubernetesCluster,
  KubernetesRuntimeConfig,
  LabelMap,
  Nodepool,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.http4s.Uri

import java.net.URL

final case class CreateAppRequest(kubernetesRuntimeConfig: Option[KubernetesRuntimeConfig],
                                  appType: AppType,
                                  diskConfig: Option[PersistentDiskRequest],
                                  labels: LabelMap = Map.empty,
                                  customEnvironmentVariables: Map[String, String],
                                  descriptorPath: Option[Uri],
                                  extraArgs: List[String]
)

final case class DeleteAppRequest(userInfo: UserInfo, cloudContext: CloudContext, appName: AppName, deleteDisk: Boolean)

final case class GetAppResponse(appName: AppName,
                                cloudContext: CloudContext,
                                kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                errors: List[AppError],
                                status: AppStatus, // TODO: do we need some sort of aggregate status?
                                proxyUrls: Map[ServiceName, URL],
                                diskName: Option[DiskName],
                                customEnvironmentVariables: Map[String, String],
                                auditInfo: AuditInfo,
                                appType: AppType,
                                labels: LabelMap
)

final case class ListAppResponse(cloudProvider: CloudProvider,
                                 workspaceId: Option[WorkspaceId],
                                 cloudContext: CloudContext,
                                 kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                 errors: List[AppError],
                                 status: AppStatus, // TODO: do we need some sort of aggregate status?
                                 proxyUrls: Map[ServiceName, URL],
                                 appName: AppName,
                                 appType: AppType,
                                 diskName: Option[DiskName],
                                 auditInfo: AuditInfo,
                                 labels: LabelMap
)

final case class GetAppResult(cluster: KubernetesCluster, nodepool: Nodepool, app: App)

object ListAppResponse {
  def fromCluster(c: KubernetesCluster,
                  proxyUrlBase: String,
                  labelsToReturn: List[String],
                  apiVersion: String
  ): List[ListAppResponse] =
    c.nodepools.flatMap(n =>
      n.apps.map { a =>
        ListAppResponse(
          c.cloudContext.cloudProvider,
          c.workspaceId,
          c.cloudContext,
          KubernetesRuntimeConfig(
            n.numNodes,
            n.machineType,
            n.autoscalingEnabled
          ),
          a.errors,
          a.status,
          a.getProxyUrls(c.cloudContext, c.workspaceId, proxyUrlBase, apiVersion),
          a.appName,
          a.appType,
          a.appResources.disk.map(_.name),
          a.auditInfo,
          a.labels.filter(l => labelsToReturn.contains(l._1))
        )
      }
    )
}

object GetAppResponse {
  def fromDbResult(appResult: GetAppResult, proxyUrlBase: String, apiVersion: String): GetAppResponse =
    GetAppResponse(
      appResult.app.appName,
      appResult.cluster.cloudContext,
      KubernetesRuntimeConfig(
        appResult.nodepool.numNodes,
        appResult.nodepool.machineType,
        appResult.nodepool.autoscalingEnabled
      ),
      appResult.app.errors,
      appResult.app.status,
      appResult.app.getProxyUrls(appResult.cluster.cloudContext,
                                 appResult.cluster.workspaceId,
                                 proxyUrlBase,
                                 apiVersion
      ),
      appResult.app.appResources.disk.map(_.name),
      appResult.app.customEnvironmentVariables,
      appResult.app.auditInfo,
      appResult.app.appType,
      appResult.app.labels
    )
}
