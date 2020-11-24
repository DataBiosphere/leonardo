package org.broadinstitute.dsde.workbench.leonardo.http

import java.net.URL

import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterName
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.{
  App,
  AppConfig,
  AppError,
  AppName,
  AppStatus,
  AppType,
  AuditInfo,
  DiskId,
  KubernetesCluster,
  KubernetesClusterStatus,
  KubernetesRuntimeConfig,
  LabelMap,
  Nodepool,
  NodepoolStatus,
  NumNodepools
}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class CreateAppRequest(kubernetesRuntimeConfig: Option[KubernetesRuntimeConfig],
                                  appType: AppType,
                                  diskConfig: Option[PersistentDiskRequest],
                                  labels: LabelMap = Map.empty,
                                  customEnvironmentVariables: Map[String, String])

final case class CreateAppRequest2(kubernetesRuntimeConfig: Option[KubernetesRuntimeConfig],
                                   appConfig: AppConfigRequest,
                                   labels: LabelMap = Map.empty,
                                   customEnvironmentVariables: Map[String, String])

sealed trait AppConfigRequest extends Product with Serializable {
  def appType: AppType
  def diskId: DiskId
}
object AppConfigRequest {
  final case class GalaxyConfig(diskId: DiskId, postgresDiskId: DiskId) extends AppConfigRequest {
    val appType = AppType.Galaxy
  }
  final case class Custom(descriptorPath: String, diskId: DiskId) extends AppConfigRequest {
    val appType = AppType.Custom
  }
}

final case class DeleteAppRequest(userInfo: UserInfo,
                                  googleProject: GoogleProject,
                                  appName: AppName,
                                  deleteDisk: Boolean)

final case class GetAppResponse(kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                errors: List[AppError],
                                status: AppStatus, //TODO: do we need some sort of aggregate status?
                                proxyUrls: Map[ServiceName, URL],
                                diskName: Option[DiskName],
                                customEnvironmentVariables: Map[String, String],
                                auditInfo: AuditInfo)

final case class GetAppResponse2(kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                 errors: List[AppError],
                                 status: AppStatus, //TODO: do we need some sort of aggregate status?
                                 proxyUrls: Map[ServiceName, URL],
                                 appConfig: AppConfig,
                                 customEnvironmentVariables: Map[String, String],
                                 auditInfo: AuditInfo)

final case class ListAppResponse(googleProject: GoogleProject,
                                 kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                 errors: List[AppError],
                                 status: AppStatus, //TODO: do we need some sort of aggregate status?
                                 proxyUrls: Map[ServiceName, URL],
                                 appName: AppName,
                                 diskName: Option[DiskName],
                                 auditInfo: AuditInfo)
object ListAppResponse {
  def fromCluster(c: KubernetesCluster, proxyUrlBase: String): List[ListAppResponse] =
    c.nodepools.flatMap(n =>
      n.apps.map { a =>
        val hasError = c.status == KubernetesClusterStatus.Error ||
          n.status == NodepoolStatus.Error ||
          a.status == AppStatus.Error ||
          a.errors.length > 0
        ListAppResponse(
          c.googleProject,
          KubernetesRuntimeConfig(
            n.numNodes,
            n.machineType,
            n.autoscalingEnabled
          ),
          a.errors,
          if (hasError) AppStatus.Error else a.status,
          a.getProxyUrls(c.googleProject, proxyUrlBase),
          a.appName,
          Some(DiskName("todo")),
          a.auditInfo
        )
      }
    )

}

final case class ListAppResponse2(googleProject: GoogleProject,
                                  kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                  errors: List[AppError],
                                  status: AppStatus, //TODO: do we need some sort of aggregate status?
                                  proxyUrls: Map[ServiceName, URL],
                                  appName: AppName,
                                  appConfig: AppConfig,
                                  auditInfo: AuditInfo)
object ListAppResponse2 {
  def fromCluster(c: KubernetesCluster, proxyUrlBase: String): List[ListAppResponse2] =
    c.nodepools.flatMap(n =>
      n.apps.map { a =>
        val hasError = c.status == KubernetesClusterStatus.Error ||
          n.status == NodepoolStatus.Error ||
          a.status == AppStatus.Error ||
          a.errors.length > 0
        ListAppResponse2(
          c.googleProject,
          KubernetesRuntimeConfig(
            n.numNodes,
            n.machineType,
            n.autoscalingEnabled
          ),
          a.errors,
          if (hasError) AppStatus.Error else a.status,
          a.getProxyUrls(c.googleProject, proxyUrlBase),
          a.appName,
          AppConfig
          a.auditInfo
        )
      }
    )
}

final case class BatchNodepoolCreateRequest(numNodepools: NumNodepools,
                                            kubernetesRuntimeConfig: Option[KubernetesRuntimeConfig],
                                            clusterName: Option[KubernetesClusterName])

final case class GetAppResult(cluster: KubernetesCluster, nodepool: Nodepool, app: App)



object GetAppResponse {
  def fromDbResult(appResult: GetAppResult, proxyUrlBase: String): GetAppResponse = {
    val hasError = appResult.cluster.status == KubernetesClusterStatus.Error ||
      appResult.nodepool.status == NodepoolStatus.Error ||
      appResult.app.status == AppStatus.Error ||
      appResult.app.errors.length > 0
    GetAppResponse(
      KubernetesRuntimeConfig(
        appResult.nodepool.numNodes,
        appResult.nodepool.machineType,
        appResult.nodepool.autoscalingEnabled
      ),
      appResult.app.errors,
      if (hasError) AppStatus.Error else appResult.app.status,
      appResult.app.getProxyUrls(appResult.cluster.googleProject, proxyUrlBase),
      appResult.app.appResources.disk.map(_.name),
      appResult.app.customEnvironmentVariables,
      appResult.app.auditInfo
    )
  }
}
