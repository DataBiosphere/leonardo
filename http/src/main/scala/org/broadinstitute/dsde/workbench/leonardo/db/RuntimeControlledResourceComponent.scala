package org.broadinstitute.dsde.workbench.leonardo
package db

import ca.mrvisser.sealerate
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.dao.WsmControlledResourceId

case class RuntimeControlledResourceRecord(runtimeId: Long,
                                           resourceId: WsmControlledResourceId,
                                           resourceType: WsmResourceType)

class RuntimeControlledResourceTable(tag: Tag)
    extends Table[RuntimeControlledResourceRecord](tag, "RUNTIME_CONTROLLED_RESOURCE") {
  def runtimeId = column[Long]("runtimeId")
  def resourceId = column[WsmControlledResourceId]("resourceId")
  def resourceType = column[WsmResourceType]("resourceType")

  def * =
    (runtimeId, resourceId, resourceType) <> (RuntimeControlledResourceRecord.tupled, (runtimeId,
                                                                                       resourceId,
                                                                                       resourceType) =>
      RuntimeControlledResourceRecord.unapply())
}

object controlledResourceQuery extends TableQuery(new RuntimeControlledResourceTable(_)) {

  def save(runtimeId: Long, resourceId: WsmControlledResourceId, resourceType: WsmResourceType): DBIO[Int] =
    controlledResourceQuery += RuntimeControlledResourceRecord(runtimeId, resourceId, resourceType)

  def getResourceTypeForRuntime(runtimeId: Long,
                                resourceType: WsmResourceType): DBIO[Option[RuntimeControlledResourceRecord]] =
    controlledResourceQuery
      .filter(_.runtimeId === runtimeId)
      .filter(_.resourceType === resourceType)
      .result
      .headOption
}

sealed abstract class WsmResourceType

object WsmResourceType {
  case object AzureVm extends WsmResourceType {
    override def toString: String = "AZURE_VM"
  }

  case object AzureIp extends WsmResourceType {
    override def toString: String = "AZURE_IP"
  }

  case object AzureNetwork extends WsmResourceType {
    override def toString: String = "AZURE_NETWORK"
  }

  case object AzureDisk extends WsmResourceType {
    override def toString: String = "AZURE_DISK"
  }

  def values: Set[WsmResourceType] = sealerate.values[WsmResourceType]

  def stringToObject: Map[String, WsmResourceType] = values.map(v => v.toString -> v).toMap
}
