package org.broadinstitute.dsde.workbench.leonardo.http
package service

import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, CloudContext, DiskId}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

trait DiskService[F[_]] {
  def createDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: CreateDiskRequest)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def getDisk(userInfo: UserInfo, cloudContext: CloudContext, diskName: DiskName)(implicit
    as: Ask[F, AppContext]
  ): F[GetPersistentDiskResponse]

  def listDisks(userInfo: UserInfo, cloudContext: Option[CloudContext], params: Map[String, String])(implicit
    as: Ask[F, AppContext]
  ): F[Vector[ListPersistentDiskResponse]]

  def deleteDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def deleteAllOrphanedDisks(userInfo: UserInfo,
                             cloudContext: CloudContext.Gcp,
                             runtimeDiskIds: Vector[DiskId],
                             appDisksNames: Vector[DiskName]
  )(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def deleteDiskRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp, disk: ListPersistentDiskResponse)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def deleteAllDisksRecords(userInfo: UserInfo, cloudContext: CloudContext.Gcp)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]

  def updateDisk(userInfo: UserInfo, googleProject: GoogleProject, diskName: DiskName, req: UpdateDiskRequest)(implicit
    as: Ask[F, AppContext]
  ): F[Unit]
}
