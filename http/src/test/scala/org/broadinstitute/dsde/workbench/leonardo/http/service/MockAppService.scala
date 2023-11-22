package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, GoogleResourceService}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.model.UserInfo

class MockAppService extends AppService[IO] {
  override def createApp(userInfo: UserInfo,
                         cloudContext: CloudContext.Gcp,
                         appName: AppName,
                         req: CreateAppRequest,
                         workspaceId: Option[WorkspaceId] = None
  )(implicit
    as: Ask[IO, AppContext],
    googleResourceService: GoogleResourceService[IO],
    computeService: GoogleComputeService[IO]
  ): IO[Unit] =
    IO.unit

  override def getApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[IO, AppContext]
  ): IO[GetAppResponse] =
    IO.pure(getAppResponse)

  override def listApp(userInfo: UserInfo, cloudContext: Option[CloudContext.Gcp], params: Map[String, String])(implicit
    as: Ask[IO, AppContext]
  ): IO[Vector[ListAppResponse]] =
    IO.pure(listAppResponse)

  override def deleteApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName, deleteDisk: Boolean)(
    implicit as: Ask[IO, AppContext]
  ): IO[Unit] =
    IO.unit

  override def stopApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

  override def startApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

  override def getAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, appName: AppName)(implicit
    as: Ask[IO, AppContext]
  ): IO[GetAppResponse] = IO.pure(getAppResponse)

  override def listAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, params: Map[String, String])(implicit
    as: Ask[IO, AppContext]
  ): IO[Vector[ListAppResponse]] = IO.pure(listAppResponse)

  override def createAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, appName: AppName, req: CreateAppRequest)(
    implicit as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

  override def deleteAppV2(userInfo: UserInfo, workspaceId: WorkspaceId, appName: AppName, deleteDisk: Boolean)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] =
    IO.unit

  override def deleteAllAppsV2(userInfo: UserInfo, workspaceId: WorkspaceId, deleteDisk: Boolean)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] =
    IO.unit

}

object MockAppService extends MockAppService
