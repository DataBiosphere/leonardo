package org.broadinstitute.dsde.workbench.leonardo.http
package service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoAppServiceInterp.LeoKubernetesConfig
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, AppName, CloudContext}
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
}

final case class AppServiceConfig(enableCustomAppGroupPermissionCheck: Boolean,
                                  leoKubernetesConfig: LeoKubernetesConfig
)
