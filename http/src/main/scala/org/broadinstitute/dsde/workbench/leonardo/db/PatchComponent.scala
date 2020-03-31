package org.broadinstitute.dsde.workbench.leonardo
package db

import scala.concurrent.ExecutionContext
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimePatchDetails

case class PatchRecord(clusterId: Long, status: RuntimeStatus, masterMachineType: Option[MachineTypeName], inProgress: Boolean)

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

  def delete(patchDetails: RuntimePatchDetails): DBIO[Int] =
    basePatchQuery(patchDetails).delete

  def getPatchAction(
    patchDetails: RuntimePatchDetails
  )(implicit ec: ExecutionContext): DBIO[Option[MachineTypeName]] =
    basePatchQuery(patchDetails).result.headOption
      .map { recordOpt =>
        recordOpt match {
          case Some(record) => record.masterMachineType
          case None         => None
        }
      }

  def updatePatchAsComplete(clusterId: Long): DBIO[Int] =
    patchQuery
      .filter { _.clusterId === clusterId }
      .map(_.inProgress)
      .update(false)

  private def basePatchQuery(patchDetails: RuntimePatchDetails) =
    patchQuery
      .filter(_.clusterId === patchDetails.runtimeId)
}
