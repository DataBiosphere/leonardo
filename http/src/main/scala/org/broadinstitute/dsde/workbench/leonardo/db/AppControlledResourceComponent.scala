package org.broadinstitute.dsde.workbench.leonardo.db

import ca.mrvisser.sealerate
import org.broadinstitute.dsde.workbench.leonardo.{AppId, WsmControlledResourceId}
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._

import scala.concurrent.ExecutionContext

sealed abstract class AppControlledResourceStatus
object AppControlledResourceStatus {
  case object Creating extends AppControlledResourceStatus {
    override def toString: String = "CREATING"
  }
  case object Created extends AppControlledResourceStatus {
    override def toString: String = "CREATED"
  }
  case object Deleting extends AppControlledResourceStatus {
    override def toString: String = "DELETING"
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
  def insert(appId: Long,
             resourceId: WsmControlledResourceId,
             resourceType: WsmResourceType,
             status: AppControlledResourceStatus
  ): DBIO[Int] =
    appControlledResourceQuery += AppControlledResourceRecord(appId, resourceId, resourceType, status)

  def updateStatus(resourceId: WsmControlledResourceId, status: AppControlledResourceStatus)(implicit
    ec: ExecutionContext
  ): DBIO[Int] =
    appControlledResourceQuery.filter(_.resourceId === resourceId).map(_.status).update(status).flatMap { updateCount =>
      if (updateCount == 0) {
        DBIO.failed(new Exception(s"Failed to update status for resource $resourceId"))
      } else {
        DBIO.successful(updateCount)
      }
    }

  def delete(resourceId: WsmControlledResourceId): DBIO[Int] =
    appControlledResourceQuery
      .filter(_.resourceId === resourceId)
      .map(_.status)
      .update(AppControlledResourceStatus.Deleted)

  def getAllForAppByStatus(appId: Long, statuses: AppControlledResourceStatus*)(implicit
    ec: ExecutionContext
  ): DBIO[List[AppControlledResourceRecord]] =
    appControlledResourceQuery
      .filter(_.appId === appId)
      .filter(_.status inSet statuses)
      .result
      .map(_.toList)

  def getAllForApp(appId: AppId)(implicit
    ec: ExecutionContext
  ): DBIO[List[AppControlledResourceRecord]] =
    appControlledResourceQuery
      .filter(_.appId === appId.id)
      .result
      .map(_.toList)

  def getAllForAppByType(appId: Long, resourceType: WsmResourceType)(implicit
    ec: ExecutionContext
  ): DBIO[List[AppControlledResourceRecord]] =
    appControlledResourceQuery
      .filter(_.appId === appId)
      .filter(_.resourceType === resourceType)
      .result
      .map(_.toList)

}
