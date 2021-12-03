package org.broadinstitute.dsde.workbench.leonardo.http
package service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, CloudContext}
import org.broadinstitute.dsde.workbench.leonardo.http.api.UpdateDiskRequest
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait DiskService[F[_]] {
  def createDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: CreateDiskRequest)(
    implicit as: Ask[F, AppContext]
  ): F[Unit]

  def getDisk(userInfo: UserInfo, cloudContext: CloudContext, diskName: DiskName)(
    implicit as: Ask[F, AppContext]
  ): F[GetPersistentDiskResponse]

  def listDisks(userInfo: UserInfo, cloudContext: Option[CloudContext], params: Map[String, String])(
    implicit as: Ask[F, AppContext]
  ): F[Vector[ListPersistentDiskResponse]]

  def deleteDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(
    implicit as: Ask[F, AppContext]
  ): F[Unit]

  def updateDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: UpdateDiskRequest)(
    implicit as: Ask[F, AppContext]
  ): F[Unit]
}
