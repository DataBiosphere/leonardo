package org.broadinstitute.dsde.workbench.leonardo
package db

import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterFollowupDetails

import scala.concurrent.ExecutionContext
import LeoProfile.api._

case class FollowupRecord(clusterId: Long, status: String, masterMachineType: Option[String])

class FollowupTable(tag: Tag) extends Table[FollowupRecord](tag, "CLUSTER_FOLLOWUP") {
  def clusterId = column[Long]("clusterId", O.PrimaryKey)
  def status = column[String]("status", O.Length(254))
  def masterMachineType = column[Option[String]]("masterMachineType", O.Length(254))

  def cluster = foreignKey("FK_CLUSTER_FOLLOWUP_CLUSTER_ID", clusterId, clusterQuery)(_.id)

  def * =
    (clusterId, status, masterMachineType) <> (FollowupRecord.tupled, FollowupRecord.unapply)
}

object followupQuery extends TableQuery(new FollowupTable(_)) {
  def save(followupDetails: ClusterFollowupDetails, masterMachineType: Option[String]): DBIO[Int] =
    followupQuery += FollowupRecord(followupDetails.clusterId,
                                    followupDetails.clusterStatus.entryName,
                                    masterMachineType)

  def delete(followupDetails: ClusterFollowupDetails): DBIO[Int] =
    baseFollowupQuery(followupDetails).delete

  //TODO: should this differentiate itself between no cluster with that ID found, and no machine config found?
  def getFollowupAction(followupDetails: ClusterFollowupDetails)(implicit ec: ExecutionContext): DBIO[Option[String]] =
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
