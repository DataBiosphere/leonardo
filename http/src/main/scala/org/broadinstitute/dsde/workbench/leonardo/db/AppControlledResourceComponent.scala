package org.broadinstitute.dsde.workbench.leonardo.db

import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.WsmControlledResourceId
import ca.mrvisser.sealerate

import scala.concurrent.ExecutionContext

sealed abstract class AppControlledResourceStatus
object AppControlledResourceStatus {
  case object Created extends AppControlledResourceStatus {
    override def toString: String = "CREATED"
  }
  case object Deleted extends AppControlledResourceStatus {
    override def toString: String = "DELETED"
  }

  def values: Set[AppControlledResourceStatus] = sealerate.values[AppControlledResourceStatus]

  def stringToObject: Map[String, AppControlledResourceStatus] = values.map(v => v.toString -> v).toMap
}
case class AppControlledResourceRecord(appId: Long,
                                       resourceId: WsmControlledResourceId,
                                       resourceType: WsmResourceType,
                                       status: AppControlledResourceStatus
)

class AppControlledResourceTable(tag: Tag) extends Table[AppControlledResourceRecord](tag, "APP_CONTROLLED_RESOURCE") {
  def appId = column[Long]("appId")
  def resourceId = column[WsmControlledResourceId]("resourceId")
  def resourceType = column[WsmResourceType]("resourceType")
  def status = column[AppControlledResourceStatus]("status", O.Length(254))

  def * =
    (appId,
     resourceId,
     resourceType,
     status
    ) <> (AppControlledResourceRecord.tupled, AppControlledResourceRecord.unapply)
}

object appControlledResourceQuery extends TableQuery(new AppControlledResourceTable(_)) {
  def save(appId: Long,
           resourceId: WsmControlledResourceId,
           resourceType: WsmResourceType,
           status: AppControlledResourceStatus
  ): DBIO[Int] =
    appControlledResourceQuery += AppControlledResourceRecord(appId, resourceId, resourceType, status)

  def delete(appId: Long, resourceId: WsmControlledResourceId): DBIO[Int] =
    appControlledResourceQuery
      .filter(_.resourceId === resourceId)
      .filter(_.resourceId === resourceId)
      .map(_.status)
      .update(AppControlledResourceStatus.Deleted)

  def getAllForApp(appId: Long, status: AppControlledResourceStatus)(implicit
    ec: ExecutionContext
  ): DBIO[List[AppControlledResourceRecord]] =
    appControlledResourceQuery
      .filter(_.appId === appId)
      .filter(_.status === status)
      .result
      .map(_.toList)

  def getWsmRecordForApp(appId: Long, resourceType: WsmResourceType): DBIO[Option[AppControlledResourceRecord]] =
    appControlledResourceQuery
      .filter(_.appId === appId)
      .filter(_.resourceType === resourceType)
      .result
      .headOption
}
