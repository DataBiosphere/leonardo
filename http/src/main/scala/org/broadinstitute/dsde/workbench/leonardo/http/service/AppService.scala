package org.broadinstitute.dsde.workbench.leonardo.http
package service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoAppServiceInterp.LeoKubernetesConfig
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppName, CloudContext, WorkspaceId}
import org.broadinstitute.dsde.workbench.model.UserInfo

trait AppService[F[_]] {

  def createApp(
    userInfo: UserInfo,
    cloudContext: CloudContext.Gcp,
    appName: AppName,
    req: CreateAppRequest,
    workspaceId: Option[WorkspaceId] = None
  )(implicit as: Ask[F, AppContext]): F[Unit]

  def getApp(
    userInfo: UserInfo,
    cloudContext: CloudContext.Gcp,
    appName: AppName
  )(implicit as: Ask[F, AppContext]): F[GetAppResponse]

  def listApp(
    userInfo: UserInfo,
    cloudContext: Option[CloudContext.Gcp],
    params: Map[String, String]
  )(implicit as: Ask[F, AppContext]): F[Vector[ListAppResponse]]

  def updateApp(
                 userInfo: UserInfo,
                 cloudContext: CloudContext.Gcp,
                 appName: AppName,
                 req: UpdateAppRequest,
               )(implicit as: Ask[F, AppContext]): F[Unit]

  def deleteApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def deleteAllApps(userInfo: UserInfo, cloudContext: CloudContext.Gcp, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Vector[DiskName]]

  def deleteAppRecords(userInfo: UserInfo, cloudContext: CloudContext, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def deleteAllAppsRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def stopApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def startApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def createAppV2(
    userInfo: UserInfo,
    workspaceId: WorkspaceId,
    appName: AppName,
    req: CreateAppRequest
  )(implicit as: Ask[F, AppContext]): F[Unit]

  def getAppV2(
    userInfo: UserInfo,
    workspaceId: WorkspaceId,
    appName: AppName
  )(implicit as: Ask[F, AppContext]): F[GetAppResponse]

  def listAppV2(
    userInfo: UserInfo,
    workspaceId: WorkspaceId,
    params: Map[String, String]
  )(implicit as: Ask[F, AppContext]): F[Vector[ListAppResponse]]

  def deleteAppV2(
    userInfo: UserInfo,
    workspaceId: WorkspaceId,
    appName: AppName,
    deleteDisk: Boolean
  )(implicit as: Ask[F, AppContext]): F[Unit]

  def deleteAllAppsV2(userInfo: UserInfo, workspaceId: WorkspaceId, deleteDisk: Boolean)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]
}

final case class AppServiceConfig(enableCustomAppCheck: Boolean,
                                  enableSasApp: Boolean,
                                  leoKubernetesConfig: LeoKubernetesConfig
)
