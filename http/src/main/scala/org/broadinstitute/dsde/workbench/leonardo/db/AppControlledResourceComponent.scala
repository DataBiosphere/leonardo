package org.broadinstitute.dsde.workbench.leonardo.db

import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.WsmControlledResourceId

import scala.concurrent.ExecutionContext

case class AppControlledResourceRecord(appId: Long, resourceId: WsmControlledResourceId, resourceType: WsmResourceType)

class AppControlledResourceTable(tag: Tag) extends Table[AppControlledResourceRecord](tag, "APP_CONTROLLED_RESOURCE") {
  def appId = column[Long]("appId")
  def resourceId = column[WsmControlledResourceId]("resourceId")
  def resourceType = column[WsmResourceType]("resourceType")

  def * =
    (appId, resourceId, resourceType) <> (AppControlledResourceRecord.tupled, AppControlledResourceRecord.unapply)
}

object appControlledResourceQuery extends TableQuery(new AppControlledResourceTable(_)) {
  def save(appId: Long, resourceId: WsmControlledResourceId, resourceType: WsmResourceType): DBIO[Int] =
    appControlledResourceQuery += AppControlledResourceRecord(appId, resourceId, resourceType)

  def getWsmRecordForApp(appId: Long, resourceType: WsmResourceType): DBIO[Option[AppControlledResourceRecord]] =
    appControlledResourceQuery
      .filter(_.appId === appId)
      .filter(_.resourceType === resourceType)
      .result
      .headOption

  def getWsmRecordFromResourceId(resourceId: WsmControlledResourceId,
                                 resourceType: WsmResourceType
  ): DBIO[Option[AppControlledResourceRecord]] =
    appControlledResourceQuery
      .filter(_.resourceId === resourceId)
      .filter(_.resourceType === resourceType)
      .result
      .headOption // db entry is updated with new appId, so there will only ever be 1 entry per resourceId

  def getAllForApp(appId: Long)(implicit ec: ExecutionContext): DBIO[List[AppControlledResourceRecord]] =
    appControlledResourceQuery
      .filter(_.appId === appId)
      .result
      .map(_.toList)

  def updateApp(resourceId: WsmControlledResourceId, resourceType: WsmResourceType, newAppId: Long): DBIO[Int] =
    appControlledResourceQuery
      .filter(_.resourceId === resourceId)
      .filter(_.resourceType === resourceType)
      .map(_.appId)
      .update(newAppId)
}
