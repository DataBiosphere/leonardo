package org.broadinstitute.dsde.workbench.leonardo.http

import java.net.URL

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.{
  AppError,
  AppName,
  AppStatus,
  AppType,
  KubernetesCluster,
  KubernetesClusterStatus,
  KubernetesRuntimeConfig,
  LabelMap,
  NodepoolStatus
}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

final case class CreateAppRequest(kubernetesRuntimeConfig: Option[KubernetesRuntimeConfig],
                                  appType: AppType,
                                  diskConfig: Option[PersistentDiskRequest],
                                  labels: LabelMap = Map.empty,
                                  customEnvironmentVariables: Map[String, String])

final case class DeleteAppParams(userInfo: UserInfo,
                                 googleProject: GoogleProject,
                                 appName: AppName,
                                 deleteDisk: Boolean)

final case class GetAppResponse(kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                errors: List[AppError],
                                status: AppStatus, //TODO: do we need some sort of aggregate status?
                                proxyUrls: Map[ServiceName, URL],
                                diskName: Option[DiskName])

final case class ListAppResponse(googleProject: GoogleProject,
                                 kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                 errors: List[AppError],
                                 status: AppStatus, //TODO: do we need some sort of aggregate status?
                                 proxyUrls: Map[ServiceName, URL],
                                 appName: AppName,
                                 diskName: Option[DiskName])

object ListAppResponse {
  def fromCluster(c: KubernetesCluster): List[ListAppResponse] =
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
          a.getProxyUrls(c.googleProject, "https://leo/proxy/"),
          a.appName,
          a.appResources.disk.map(_.name)
        )
      }
    )

}
