package org.broadinstitute.dsde.workbench.leonardo
package db

import scala.concurrent.ExecutionContext
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.ClusterFollowupDetails

case class FollowupRecord(clusterId: Long, status: RuntimeStatus, masterMachineType: Option[MachineTypeName])

class FollowupTable(tag: Tag) extends Table[FollowupRecord](tag, "CLUSTER_FOLLOWUP") {
  def clusterId = column[Long]("clusterId", O.PrimaryKey)
  def status = column[RuntimeStatus]("status", O.Length(254))
  def masterMachineType = column[Option[MachineTypeName]]("masterMachineType", O.Length(254))

  def cluster = foreignKey("FK_CLUSTER_FOLLOWUP_CLUSTER_ID", clusterId, clusterQuery)(_.id)

  def * =
    (clusterId, status, masterMachineType) <> (FollowupRecord.tupled, FollowupRecord.unapply)
}

object followupQuery extends TableQuery(new FollowupTable(_)) {
  def save(followupDetails: ClusterFollowupDetails, masterMachineType: Option[MachineTypeName]): DBIO[Int] =
    followupQuery.insertOrUpdate(
      FollowupRecord(followupDetails.clusterId, followupDetails.runtimeStatus, masterMachineType)
    )

  def delete(followupDetails: ClusterFollowupDetails): DBIO[Int] =
    baseFollowupQuery(followupDetails).delete

  def getFollowupAction(
    followupDetails: ClusterFollowupDetails
  )(implicit ec: ExecutionContext): DBIO[Option[MachineTypeName]] =
    baseFollowupQuery(followupDetails).result.headOption
      .map { recordOpt =>
        recordOpt match {
          case Some(record) => record.masterMachineType
          case None         => None
        }
      }

  private def baseFollowupQuery(followupDetails: ClusterFollowupDetails) =
    followupQuery
      .filter(_.clusterId === followupDetails.clusterId)
}
