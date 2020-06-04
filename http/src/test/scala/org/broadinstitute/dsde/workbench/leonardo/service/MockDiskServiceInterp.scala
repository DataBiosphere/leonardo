package org.broadinstitute.dsde.workbench.leonardo
package service

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.http.CreateDiskRequest
import org.broadinstitute.dsde.workbench.leonardo.http.api.{
  GetPersistentDiskResponse,
  ListPersistentDiskResponse,
  UpdateDiskRequest
}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

object MockDiskServiceInterp extends DiskService[IO] {
  override def createDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: CreateDiskRequest)(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[Unit] = IO.unit

  def getDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[GetPersistentDiskResponse] =
    IO.pure(
      GetPersistentDiskResponse(
        DiskId(-1),
        CommonTestData.project,
        CommonTestData.zone,
        CommonTestData.diskName,
        Some(CommonTestData.googleId),
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

  def listDisks(userInfo: UserInfo, googleProject: Option[GoogleProject], params: Map[String, String])(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[Vector[ListPersistentDiskResponse]] =
    IO.pure(
      Vector(
        ListPersistentDiskResponse(
          DiskId(-1),
          CommonTestData.project,
          CommonTestData.zone,
          CommonTestData.diskName,
          DiskStatus.Ready,
          CommonTestData.auditInfo,
          CommonTestData.diskSize,
          CommonTestData.diskType,
          CommonTestData.blockSize
        )
      )
    )

  def deleteDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[Unit] = IO.unit

  def updateDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: UpdateDiskRequest)(
    implicit as: ApplicativeAsk[IO, AppContext]
  ): IO[Unit] = IO.unit
}
