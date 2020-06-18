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

  def pk = primaryKey("PK_LABEL", (resourceId, resourceType, key))

  def * = (resourceId, resourceType, key, value) <> (LabelRecord.tupled, LabelRecord.unapply)
}

object labelQuery extends TableQuery(new LabelTable(_)) {

  def save(resourceId: Long, resourceType: LabelResourceType, key: String, value: String): DBIO[Int] =
    labelQuery.insertOrUpdate(LabelRecord(resourceId, resourceType, key, value))

  def saveAllForResource(resourceId: Long, resourceType: LabelResourceType, m: LabelMap)(
    implicit ec: ExecutionContext
  ): DBIO[Int] =
    DBIO.fold(m.toSeq map { case (key, value) => save(resourceId, resourceType, key, value) }, 0)(_ + _)

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

  final case object App extends LabelResourceType {
    val asString = "app"
  }

  val stringToLabelResourceType: Map[String, LabelResourceType] =
    sealerate.collect[LabelResourceType].map(p => (p.asString, p)).toMap

  val runtime: LabelResourceType = Runtime
  val persistentDisk: LabelResourceType = PersistentDisk
  val app: LabelResourceType = App
}
