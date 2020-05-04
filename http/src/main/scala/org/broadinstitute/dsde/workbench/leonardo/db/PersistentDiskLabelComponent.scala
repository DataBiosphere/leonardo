package org.broadinstitute.dsde.workbench.leonardo.db

import LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.{DiskId, LabelMap}

import scala.concurrent.ExecutionContext

case class PersistentDiskLabelRecord(diskId: DiskId, key: String, value: String)

class PersistentDiskLabelTable(tag: Tag) extends Table[PersistentDiskLabelRecord](tag, "PERSISTENT_DISK_LABEL") {
  def diskId = column[DiskId]("persistentDiskId")
  def key = column[String]("key", O.Length(254))
  def value = column[String]("value", O.Length(254))

  def diskFk = foreignKey("FK_PERSISTENT_DISK_ID", diskId, persistentDiskQuery)(_.id)
  def uniqueKey = index("IDX_PERSISTENT_DISK_LABEL_UNIQUE", (diskId, key), unique = true)

  def * = (diskId, key, value) <> (PersistentDiskLabelRecord.tupled, PersistentDiskLabelRecord.unapply)
}

object persistentDiskLabelQuery extends TableQuery(new PersistentDiskLabelTable(_)) {

  def save(diskId: DiskId, key: String, value: String): DBIO[Int] =
    persistentDiskLabelQuery += PersistentDiskLabelRecord(diskId, key, value)

  // ++= does not actually produce a useful return value
  def saveAllForDisk(diskId: DiskId, m: LabelMap): DBIO[Option[Int]] =
    persistentDiskLabelQuery ++= m.map { case (key, value) => PersistentDiskLabelRecord(diskId, key, value) }

  def getAllForDisk(diskId: DiskId)(implicit ec: ExecutionContext): DBIO[LabelMap] =
    persistentDiskLabelQuery.filter(_.diskId === diskId).result map { recs =>
      val tuples = recs.map(rec => rec.key -> rec.value)
      tuples.toMap
    }

  def deleteAllForDisk(diskId: DiskId): DBIO[Int] =
    persistentDiskLabelQuery.filter(_.diskId === diskId).delete
}
