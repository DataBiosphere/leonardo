package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import cats.implicits._
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.{dummyDate, unmarshalDestroyedDate}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

final case class PersistentDiskRecord(id: Long,
                                      googleProject: GoogleProject,
                                      zone: ZoneName,
                                      name: DiskName,
                                      googleId: Option[GoogleId],
                                      samResourceId: DiskSamResourceId,
                                      status: DiskStatus,
                                      creator: WorkbenchEmail,
                                      createdDate: Instant,
                                      destroyedDate: Instant,
                                      dateAccessed: Instant,
                                      size: DiskSize,
                                      diskType: DiskType,
                                      blockSize: BlockSize)

class PersistentDiskTable(tag: Tag) extends Table[PersistentDiskRecord](tag, "PERSISTENT_DISK") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def googleProject = column[GoogleProject]("googleProject", O.Length(255))
  def zone = column[ZoneName]("zone", O.Length(255))
  def name = column[DiskName]("name", O.Length(255))
  def googleId = column[Option[GoogleId]]("googleId", O.Length(255))
  def samResourceId = column[DiskSamResourceId]("samResourceId", O.Length(255))
  def status = column[DiskStatus]("status", O.Length(255))
  def creator = column[WorkbenchEmail]("creator", O.Length(255))
  def createdDate = column[Instant]("createdDate", O.SqlType("TIMESTAMP(6)"))
  def destroyedDate = column[Instant]("destroyedDate", O.SqlType("TIMESTAMP(6)"))
  def dateAccessed = column[Instant]("dateAccessed", O.SqlType("TIMESTAMP(6)"))
  def size = column[DiskSize]("sizeGb")
  def diskType = column[DiskType]("type", O.Length(255))
  def blockSize = column[BlockSize]("blockSize")

  override def * =
    (id,
     googleProject,
     zone,
     name,
     googleId,
     samResourceId,
     status,
     creator,
     createdDate,
     destroyedDate,
     dateAccessed,
     size,
     diskType,
     blockSize) <> (PersistentDiskRecord.tupled, PersistentDiskRecord.unapply)
}

object persistentDiskQuery extends TableQuery(new PersistentDiskTable(_)) {
  private[db] def findByIdQuery(id: Long) = persistentDiskQuery.filter(_.id === id)

  private[db] def findActiveByNameQuery(googleProject: GoogleProject, name: DiskName) =
    persistentDiskQuery.filter(_.googleProject === googleProject).filter(_.name === name)

  private[db] def joinLabelQuery(baseQuery: Query[PersistentDiskTable, PersistentDiskRecord, Seq]) =
    baseQuery joinLeft persistentDiskLabelQuery on (_.id === _.diskId)

  def save(disk: PersistentDisk) =
    persistentDiskQuery += marshalPersistentDisk(disk)

  def getById(id: Long)(implicit ec: ExecutionContext): DBIO[Option[PersistentDisk]] =
    joinLabelQuery(findByIdQuery(id)).result.map(aggregateLabels).map(_.headOption)

  def getActiveByName(googleProject: GoogleProject,
                      name: DiskName)(implicit ec: ExecutionContext): DBIO[Option[PersistentDisk]] =
    joinLabelQuery(findActiveByNameQuery(googleProject, name)).result.map(aggregateLabels).map(_.headOption)

  def updateStatus(id: Long, newStatus: DiskStatus, dateAccessed: Instant) =
    findByIdQuery(id).map(d => (d.status, d.dateAccessed)).update((newStatus, dateAccessed))

  def delete(id: Long, destroyedDate: Instant) =
    findByIdQuery(id)
      .map(d => (d.status, d.destroyedDate, d.dateAccessed))
      .update((DiskStatus.Deleted, destroyedDate, destroyedDate))

  // TODO add other queries as needed

  private[db] def marshalPersistentDisk(disk: PersistentDisk): PersistentDiskRecord =
    PersistentDiskRecord(
      disk.id,
      disk.googleProject,
      disk.zone,
      disk.name,
      disk.googleId,
      disk.samResourceId,
      disk.status,
      disk.auditInfo.creator,
      disk.auditInfo.createdDate,
      disk.auditInfo.destroyedDate.getOrElse(dummyDate),
      disk.auditInfo.dateAccessed,
      disk.size,
      disk.diskType,
      disk.blockSize
    )

  private[db] def aggregateLabels(
    recs: Seq[(PersistentDiskRecord, Option[PersistentDiskLabelRecord])]
  ): Seq[PersistentDisk] = {
    val pdLabelMap: Map[PersistentDiskRecord, Map[String, String]] =
      recs.toList.foldMap {
        case (rec, labelRecOpt) =>
          val labelMap = labelRecOpt.map(lblRec => Map(lblRec.key -> lblRec.value)).getOrElse(Map.empty)
          Map(rec -> labelMap)
      }

    pdLabelMap.toList.map {
      case (rec, labels) =>
        unmarshalPersistentDisk(rec, labels)
    }
  }

  private[db] def unmarshalPersistentDisk(rec: PersistentDiskRecord, labels: LabelMap): PersistentDisk =
    PersistentDisk(
      rec.id,
      rec.googleProject,
      rec.zone,
      rec.name,
      rec.googleId,
      rec.samResourceId,
      rec.status,
      DiskAuditInfo(
        rec.creator,
        rec.createdDate,
        unmarshalDestroyedDate(rec.destroyedDate),
        rec.dateAccessed
      ),
      rec.size,
      rec.diskType,
      rec.blockSize,
      labels
    )
}
