package org.broadinstitute.dsde.workbench.leonardo.http

import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.{
  AllowedChartName,
  App,
  AppAccessScope,
  AppError,
  AppName,
  AppStatus,
  AppType,
  AuditInfo,
  AutodeleteThreshold,
  CloudContext,
  KubernetesCluster,
  KubernetesRuntimeConfig,
  LabelMap,
  Nodepool,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.google2.RegionName
import org.broadinstitute.dsp.ChartName
import org.http4s.Uri

import java.net.URL

final case class CreateAppRequest(kubernetesRuntimeConfig: Option[KubernetesRuntimeConfig],
                                  appType: AppType,
                                  allowedChartName: Option[AllowedChartName],
                                  accessScope: Option[AppAccessScope],
                                  diskConfig: Option[PersistentDiskRequest],
                                  labels: LabelMap = Map.empty,
                                  customEnvironmentVariables: Map[String, String],
                                  descriptorPath: Option[Uri],
                                  extraArgs: List[String],
                                  workspaceId: Option[WorkspaceId],
                                  sourceWorkspaceId: Option[WorkspaceId],
                                  autodeleteEnabled: Option[Boolean],
                                  autodeleteThreshold: Option[AutodeleteThreshold]
)

final case class UpdateAppRequest(autodeleteEnabled: Option[Boolean], autodeleteThreshold: Option[AutodeleteThreshold])

final case class GetAppResponse(
  workspaceId: Option[WorkspaceId],
  appName: AppName,
  cloudContext: CloudContext,
  region: RegionName,
  kubernetesRuntimeConfig: KubernetesRuntimeConfig,
  errors: List[AppError],
  status: AppStatus, // TODO: do we need some sort of aggregate status?
  proxyUrls: Map[ServiceName, URL],
  diskName: Option[DiskName],
  customEnvironmentVariables: Map[String, String],
  auditInfo: AuditInfo,
  appType: AppType,
  chartName: ChartName,
  accessScope: Option[AppAccessScope],
  labels: LabelMap,
  autodeleteEnabled: Boolean,
  autodeleteThreshold: Option[AutodeleteThreshold]
)

final case class ListAppResponse(workspaceId: Option[WorkspaceId],
                                 cloudContext: CloudContext,
                                 region: RegionName,
                                 kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                 errors: List[AppError],
                                 status: AppStatus, // TODO: do we need some sort of aggregate status?
                                 proxyUrls: Map[ServiceName, URL],
                                 appName: AppName,
                                 appType: AppType,
                                 chartName: ChartName,
                                 diskName: Option[DiskName],
                                 auditInfo: AuditInfo,
                                 accessScope: Option[AppAccessScope],
                                 labels: LabelMap,
                                 autodeleteEnabled: Boolean,
                                 autodeleteThreshold: Option[AutodeleteThreshold]
)

final case class GetAppResult(cluster: KubernetesCluster, nodepool: Nodepool, app: App)

object ListAppResponse {
  def fromCluster(c: KubernetesCluster, proxyUrlBase: String, labelsToReturn: List[String]): List[ListAppResponse] =
    c.nodepools.flatMap(n =>
      n.apps.map { a =>
        ListAppResponse(
          a.workspaceId,
          c.cloudContext,
          c.region,
          KubernetesRuntimeConfig(
            n.numNodes,
            n.machineType,
            n.autoscalingEnabled
          ),
          a.errors,
          a.status,
          a.getProxyUrls(c, proxyUrlBase),
          a.appName,
          a.appType,
          a.chart.name,
          a.appResources.disk.map(_.name),
          a.auditInfo,
          a.appAccessScope,
          a.labels.filter(l => labelsToReturn.contains(l._1)),
          a.autodeleteEnabled,
          a.autodeleteThreshold
        )
      }
    )
}

object GetAppResponse {
  def fromDbResult(appResult: GetAppResult, proxyUrlBase: String): GetAppResponse =
    GetAppResponse(
      appResult.app.workspaceId,
      appResult.app.appName,
      appResult.cluster.cloudContext,
      appResult.cluster.region,
      KubernetesRuntimeConfig(
        appResult.nodepool.numNodes,
        appResult.nodepool.machineType,
        appResult.nodepool.autoscalingEnabled
      ),
      appResult.app.errors,
      appResult.app.status,
      appResult.app.getProxyUrls(appResult.cluster, proxyUrlBase),
      appResult.app.appResources.disk.map(_.name),
      appResult.app.customEnvironmentVariables,
      appResult.app.auditInfo,
      appResult.app.appType,
      appResult.app.chart.name,
      appResult.app.appAccessScope,
      appResult.app.labels,
      appResult.app.autodeleteEnabled,
      appResult.app.autodeleteThreshold
    )
}
