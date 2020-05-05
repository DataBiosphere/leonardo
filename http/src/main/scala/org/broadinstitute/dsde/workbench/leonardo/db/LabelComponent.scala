package org.broadinstitute.dsde.workbench.leonardo
package db

import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import ca.mrvisser.sealerate

import scala.concurrent.ExecutionContext

// TODO IA-1893 is open to add better types for ResourceId, ResourceType that can be more widely used
case class LabelRecord(resourceId: Long, resourceType: LabelResourceType, key: String, value: String)

class LabelTable(tag: Tag) extends Table[LabelRecord](tag, "LABEL") {
  def resourceId = column[Long]("resourceId")
  def resourceType = column[LabelResourceType]("resourceType", O.Length(254))
  def key = column[String]("key", O.Length(254))
  def value = column[String]("value", O.Length(254))

  def uniqueKey = index("IDX_LABEL_UNIQUE", (resourceId, resourceType, key), unique = true)

  def * = (resourceId, resourceType, key, value) <> (LabelRecord.tupled, LabelRecord.unapply)
}

object labelQuery extends TableQuery(new LabelTable(_)) {
//  val runtimeLabels = labelQuery.filter(_.resourceType === LabelResourceType.runtime)
//  val diskLabels = labelQuery.filter(_.resourceType === LabelResourceType.persistentDisk)

  def save(resourceId: Long, resourceType: LabelResourceType, key: String, value: String): DBIO[Int] =
    labelQuery += LabelRecord(resourceId, resourceType, key, value)

  // ++= does not actually produce a useful return value
  def saveAllForResource(resourceId: Long, resourceType: LabelResourceType, m: LabelMap): DBIO[Option[Int]] =
    labelQuery ++= m map { case (key, value) => LabelRecord(resourceId, resourceType, key, value) }

  def getAllForResource(resourceId: Long,
                        resourceType: LabelResourceType)(implicit ec: ExecutionContext): DBIO[LabelMap] =
    labelQuery.filter(_.resourceId === resourceId).filter(_.resourceType === resourceType).result map { recs =>
      val tuples = recs map { rec => rec.key -> rec.value }
      tuples.toMap
    }

  def deleteAllForResource(resourceId: Long, resourceType: LabelResourceType): DBIO[Int] =
    labelQuery.filter(_.resourceId === resourceId).filter(_.resourceType === resourceType).delete
}

sealed trait LabelResourceType extends Product with Serializable {
  def asString: String
}
object LabelResourceType {
  final case object Runtime extends LabelResourceType {
    val asString = "runtime"
  }
  final case object PersistentDisk extends LabelResourceType {
    val asString = "persistentDisk"
  }
  val stringToLabelResourceType: Map[String, LabelResourceType] =
    sealerate.collect[LabelResourceType].map(p => (p.asString, p)).toMap

  val runtime: LabelResourceType = Runtime
  val persistentDisk: LabelResourceType = PersistentDisk
}
