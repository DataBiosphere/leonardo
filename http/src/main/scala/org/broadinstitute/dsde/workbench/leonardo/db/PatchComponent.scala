package org.broadinstitute.dsde.workbench.leonardo
package db

import scala.concurrent.ExecutionContext
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimePatchDetails

case class PatchRecord(clusterId: Long,
                       status: RuntimeStatus,
                       masterMachineType: Option[MachineTypeName],
                       inProgress: Boolean
)

class PatchTable(tag: Tag) extends Table[PatchRecord](tag, "CLUSTER_PATCH") {
  def clusterId = column[Long]("clusterId", O.PrimaryKey)
  def status = column[RuntimeStatus]("status", O.Length(254))
  def masterMachineType = column[Option[MachineTypeName]]("masterMachineType", O.Length(254))
  def inProgress = column[Boolean]("inProgress")

  def cluster = foreignKey("FK_CLUSTER_PATCH_CLUSTER_ID", clusterId, clusterQuery)(_.id)

  def * =
    (clusterId, status, masterMachineType, inProgress) <> (PatchRecord.tupled, PatchRecord.unapply)
}

object patchQuery extends TableQuery(new PatchTable(_)) {
  def save(patchDetails: RuntimePatchDetails, masterMachineType: Option[MachineTypeName]): DBIO[Int] =
    patchQuery.insertOrUpdate(
      PatchRecord(patchDetails.runtimeId, patchDetails.runtimeStatus, masterMachineType, inProgress = true)
    )

  def delete(runtimeId: Long): DBIO[Int] =
    basePatchQuery(runtimeId).delete

  // retrieve additional patch actions need to be done
  def getPatchAction(
    runtimeId: Long
  )(implicit ec: ExecutionContext): DBIO[Option[MachineTypeName]] =
    patchQuery
      .filter(_.clusterId === runtimeId)
      .filter(_.inProgress === true)
      .result
      .headOption
      .map { recordOpt =>
        recordOpt match {
          case Some(record) => record.masterMachineType
          case None         => None
        }
      }

  def updatePatchAsComplete(clusterId: Long): DBIO[Int] =
    patchQuery
      .filter(_.clusterId === clusterId)
      .map(_.inProgress)
      .update(false)

  def isInprogress(runtimeId: Long)(implicit ec: ExecutionContext): DBIO[Boolean] =
    patchQuery
      .filter(_.clusterId === runtimeId)
      .result
      .headOption
      .map { recOpt =>
        recOpt match {
          case Some(r) => r.inProgress
          case None    => false
        }
      }

  private def basePatchQuery(runtimeId: Long) =
    patchQuery
      .filter(_.clusterId === runtimeId)
}
