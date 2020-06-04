package org.broadinstitute.dsde.workbench.leonardo

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

trait DiskService[F[_]] {
  def createDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: CreateDiskRequest)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit]

  def getDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[GetPersistentDiskResponse]

  def listDisks(userInfo: UserInfo, googleProject: Option[GoogleProject], params: Map[String, String])(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Vector[ListPersistentDiskResponse]]

  def deleteDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit]

  def updateDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: UpdateDiskRequest)(
    implicit as: ApplicativeAsk[F, AppContext]
  ): F[Unit]
}
