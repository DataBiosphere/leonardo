package org.broadinstitute.dsde.workbench.leonardo.http
package service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoAppServiceInterp.LeoKubernetesConfig
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppName, CloudContext, WorkspaceId}
import org.broadinstitute.dsde.workbench.model.UserInfo

trait AppService[F[_]] {
  def createApp(
    userInfo: UserInfo,
    cloudContext: CloudContext,
    appName: AppName,
    req: CreateAppRequest
  )(implicit as: Ask[F, AppContext]): F[Unit]

  def getApp(
    userInfo: UserInfo,
    cloudContext: CloudContext,
    appName: AppName
  )(implicit as: Ask[F, AppContext]): F[GetAppResponse]

  def listApp(
    userInfo: UserInfo,
    cloudContext: Option[CloudContext],
    params: Map[String, String]
  )(implicit as: Ask[F, AppContext]): F[Vector[ListAppResponse]]

  def deleteApp(request: DeleteAppRequest)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def stopApp(userInfo: UserInfo, cloudContext: CloudContext, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def startApp(userInfo: UserInfo, cloudContext: CloudContext, appName: AppName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def listAppV2(
    userInfo: UserInfo,
    workspaceId: WorkspaceId,
    params: Map[String, String]
  )(implicit as: Ask[F, AppContext]): F[Vector[ListAppResponse]]
}

final case class AppServiceConfig(enableCustomAppCheck: Boolean, leoKubernetesConfig: LeoKubernetesConfig)
