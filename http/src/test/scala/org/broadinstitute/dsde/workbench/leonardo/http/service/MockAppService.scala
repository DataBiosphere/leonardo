package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.model.UserInfo

class MockAppService extends AppService[IO] {

  val diskNames = Vector(DiskName("disk-name-1"), DiskName("disk-name-2"))
  override def createApp(userInfo: UserInfo,
                         cloudContext: CloudContext.Gcp,
                         appName: AppName,
                         req: CreateAppRequest,
                         workspaceId: Option[WorkspaceId] = None
  )(implicit
    as: Ask[IO, AppContext]
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

  override def deleteAllApps(userInfo: UserInfo, cloudContext: CloudContext.Gcp, deleteDisk: Boolean)(implicit
    as: Ask[IO, AppContext]
  ): IO[Vector[DiskName]] = IO.pure(diskNames)

  override def deleteAppRecords(userInfo: UserInfo, cloudContext: CloudContext, appName: AppName)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] =
    IO.unit

  override def deleteAllAppsRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] =
    IO.unit

  override def stopApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

  override def startApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

  override def updateApp(userInfo: UserInfo, cloudContext: CloudContext.Gcp, appName: AppName, req: UpdateAppRequest)(
    implicit as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit
}

object MockAppService extends MockAppService
