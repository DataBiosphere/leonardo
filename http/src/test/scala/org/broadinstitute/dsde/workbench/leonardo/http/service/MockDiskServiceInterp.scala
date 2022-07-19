package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

object MockDiskServiceInterp extends DiskService[IO] {
  override def createDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: CreateDiskRequest)(
    implicit as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

  def getDisk(userInfo: UserInfo, cloudContext: CloudContext, diskName: DiskName)(implicit
    as: Ask[IO, AppContext]
  ): IO[GetPersistentDiskResponse] =
    IO.pure(
      GetPersistentDiskResponse(
        DiskId(-1),
        CommonTestData.cloudContext,
        CommonTestData.zone,
        CommonTestData.diskName,
        CommonTestData.serviceAccount,
        CommonTestData.diskSamResource,
        DiskStatus.Ready,
        CommonTestData.auditInfo,
        CommonTestData.diskSize,
        CommonTestData.diskType,
        CommonTestData.blockSize,
        Map.empty
      )
    )

  override def lookupSourceDiskLink(userInfo: UserInfo, ctx: AppContext)(sourceDiskReq: SourceDiskRequest)(implicit
    as: Ask[IO, AppContext]
  ): IO[DiskLink] =
    IO.pure(
      DiskLink(s"projects/${sourceDiskReq.googleProject}/zones/${CommonTestData.zone}/disks/${sourceDiskReq.name}")
    )

  def listDisks(userInfo: UserInfo, cloudContext: Option[CloudContext], params: Map[String, String])(implicit
    as: Ask[IO, AppContext]
  ): IO[Vector[ListPersistentDiskResponse]] =
    IO.pure(
      Vector(
        ListPersistentDiskResponse(
          DiskId(-1),
          CommonTestData.cloudContext,
          CommonTestData.zone,
          CommonTestData.diskName,
          DiskStatus.Ready,
          CommonTestData.auditInfo,
          CommonTestData.diskSize,
          CommonTestData.diskType,
          CommonTestData.blockSize,
          Map.empty
        )
      )
    )

  def deleteDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit

  def updateDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: UpdateDiskRequest)(implicit
    as: Ask[IO, AppContext]
  ): IO[Unit] = IO.unit
}
