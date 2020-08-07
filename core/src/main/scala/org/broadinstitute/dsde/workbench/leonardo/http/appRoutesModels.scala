package org.broadinstitute.dsde.workbench.leonardo.http

import java.net.URL

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.{
  AppName,
  AppStatus,
  AppType,
  KubernetesCluster,
  KubernetesError,
  KubernetesRuntimeConfig,
  LabelMap
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
                                errors: List[KubernetesError],
                                status: AppStatus, //TODO: do we need some sort of aggregate status?
                                proxyUrls: Map[ServiceName, URL],
                                diskName: Option[DiskName])

final case class ListAppResponse(googleProject: GoogleProject,
                                 kubernetesRuntimeConfig: KubernetesRuntimeConfig,
                                 errors: List[KubernetesError],
                                 status: AppStatus, //TODO: do we need some sort of aggregate status?
                                 proxyUrls: Map[ServiceName, URL],
                                 appName: AppName,
                                 diskName: Option[DiskName])

object ListAppResponse {
  def fromCluster(c: KubernetesCluster): List[ListAppResponse] =
    c.nodepools.flatMap(n =>
      n.apps.map { a =>
        val errors = c.errors ++ n.errors ++ a.errors
        ListAppResponse(
          c.googleProject,
          KubernetesRuntimeConfig(
            n.numNodes,
            n.machineType,
            n.autoscalingEnabled
          ),
          errors,
          if (errors.isEmpty) a.status else AppStatus.Error, //TODO: aggregate?
          Map.empty, //TODO: change this when proxy is implemented
          a.appName,
          a.appResources.disk.map(_.name)
        )
      }
    )

}
