package org.broadinstitute.dsde.workbench.leonardo
package db

import ca.mrvisser.sealerate
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._

import scala.concurrent.ExecutionContext

case class RuntimeControlledResourceRecord(runtimeId: Long,
                                           resourceId: WsmControlledResourceId,
                                           resourceType: WsmResourceType
)

class RuntimeControlledResourceTable(tag: Tag)
    extends Table[RuntimeControlledResourceRecord](tag, "RUNTIME_CONTROLLED_RESOURCE") {
  def runtimeId = column[Long]("runtimeId")
  def resourceId = column[WsmControlledResourceId]("resourceId")
  def resourceType = column[WsmResourceType]("resourceType")

  def * =
    (runtimeId,
     resourceId,
     resourceType
    ) <> (RuntimeControlledResourceRecord.tupled, RuntimeControlledResourceRecord.unapply)
}

object controlledResourceQuery extends TableQuery(new RuntimeControlledResourceTable(_)) {
  def save(runtimeId: Long, resourceId: WsmControlledResourceId, resourceType: WsmResourceType): DBIO[Int] =
    controlledResourceQuery += RuntimeControlledResourceRecord(runtimeId, resourceId, resourceType)

  def getWsmRecordForRuntime(runtimeId: Long,
                             resourceType: WsmResourceType
  ): DBIO[Option[RuntimeControlledResourceRecord]] =
    controlledResourceQuery
      .filter(_.runtimeId === runtimeId)
      .filter(_.resourceType === resourceType)
      .result
      .headOption

  def getWsmRecordFromResourceId(resourceId: WsmControlledResourceId,
                                 resourceType: WsmResourceType
  ): DBIO[Option[RuntimeControlledResourceRecord]] =
    controlledResourceQuery
      .filter(_.resourceId === resourceId)
      .filter(_.resourceType === resourceType)
      .result
      .headOption // db entry is updated with new runtimeId, so there will only ever be 1 entry per resourceId

  def getAllForRuntime(runtimeId: Long)(implicit ec: ExecutionContext): DBIO[List[RuntimeControlledResourceRecord]] =
    controlledResourceQuery
      .filter(_.runtimeId === runtimeId)
      .result
      .map(_.toList)

  def updateRuntime(resourceId: WsmControlledResourceId, resourceType: WsmResourceType, newRuntimeId: Long): DBIO[Int] =
    controlledResourceQuery
      .filter(_.resourceId === resourceId)
      .filter(_.resourceType === resourceType)
      .map(_.runtimeId)
      .update(newRuntimeId)

}

sealed abstract class WsmResourceType

object WsmResourceType {
  case object AzureDisk extends WsmResourceType {
    override def toString: String = "AZURE_DISK"
  }

  case object AzureStorageContainer extends WsmResourceType {
    override def toString: String = "AZURE_STORAGE_CONTAINER"
  }

  case object AzureManagedIdentity extends WsmResourceType {
    override def toString: String = "AZURE_MANAGED_IDENTITY"
  }

  case object AzureDatabase extends WsmResourceType {
    override def toString: String = "AZURE_DATABASE"
  }

  case object AzureVm extends WsmResourceType {
    override def toString: String = "AZURE_VM"
  }

  def values: Set[WsmResourceType] = sealerate.values[WsmResourceType]

  def stringToObject: Map[String, WsmResourceType] = values.map(v => v.toString -> v).toMap
}
