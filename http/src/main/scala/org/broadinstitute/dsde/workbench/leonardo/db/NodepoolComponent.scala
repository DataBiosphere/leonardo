package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant

import org.broadinstitute.dsde.workbench.google2.GKEModels.NodePoolName
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.{
  AutoScalingMax,
  AutoScalingMin,
  KubernetesAuditInfo,
  KubernetesClusterLeoId,
  Nodepool,
  NodepoolAutoscaling,
  NodepoolLeoId,
  NodepoolStatus,
  NumNodes
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.{dummyDate, unmarshalDestroyedDate}

import scala.concurrent.ExecutionContext

case class NodepoolRecord(
  id: NodepoolLeoId,
  clusterId: KubernetesClusterLeoId,
  nodepoolName: NodePoolName,
  status: NodepoolStatus,
  creator: WorkbenchEmail,
  createdDate: Instant,
  destroyedDate: Instant,
  machineType: MachineTypeName,
  numNodes: NumNodes,
  autoScalingEnabled: Boolean,
  autoScalingMin: Option[AutoScalingMin],
  autoScalingMax: Option[AutoScalingMax]
)

class NodepoolTable(tag: Tag) extends Table[NodepoolRecord](tag, "NODEPOOL") {
  def id = column[NodepoolLeoId]("id", O.PrimaryKey, O.AutoInc)
  def clusterId = column[KubernetesClusterLeoId]("clusterId")
  def nodepoolName = column[NodePoolName]("nodepoolName", O.Length(254))
  def status = column[NodepoolStatus]("status", O.Length(254))
  def creator = column[WorkbenchEmail]("creator", O.Length(254))
  def createdDate = column[Instant]("createdDate", O.SqlType("TIMESTAMP(6)"))
  def destroyedDate = column[Instant]("destroyedDate", O.SqlType("TIMESTAMP(6)"))
  def machineType = column[MachineTypeName]("machineType", O.Length(254))
  def numNodes = column[NumNodes]("numNodes", O.Length(254))
  def autoScalingEnabled = column[Boolean]("autoScalingEnabled")
  def autoScalingMin = column[Option[AutoScalingMin]]("autoScalingMin")
  def autoScalingMax = column[Option[AutoScalingMax]]("autoScalingMax")

  def cluster = foreignKey("FK_NODEPOOL_CLUSTER_ID", clusterId, kubernetesClusterQuery)(_.id)
  def * =
    (id,
     clusterId,
     nodepoolName,
     status,
     creator,
     createdDate,
     destroyedDate,
     machineType,
     numNodes,
     autoScalingEnabled,
     autoScalingMin,
     autoScalingMax) <>
      (NodepoolRecord.tupled, NodepoolRecord.unapply)

}

object nodepoolQuery extends TableQuery(new NodepoolTable(_)) {
  private def findByClusterIdQuery(clusterId: KubernetesClusterLeoId) =
    nodepoolQuery
      .filter(_.clusterId === clusterId)

  private def findByNodePoolIdQuery(id: NodepoolLeoId) =
    nodepoolQuery
      .filter(_.id === id)

  def getById(id: NodepoolLeoId)(implicit ec: ExecutionContext): DBIO[Option[Nodepool]] =
    findByNodePoolIdQuery(id).result.headOption
      .map(recOpt => recOpt.map(rec => unmarshalNodePool(rec)))

  def getAllForCluster(clusterId: KubernetesClusterLeoId)(implicit ec: ExecutionContext): DBIO[Set[Nodepool]] =
    findByClusterIdQuery(clusterId).result
      .map(rowOpt => rowOpt.map(row => unmarshalNodePool(row)).toSet)

  def saveForCluster(n: Nodepool)(implicit ec: ExecutionContext): DBIO[Nodepool] =
    for {
      nodepoolId <- nodepoolQuery returning nodepoolQuery.map(_.id) +=
        NodepoolRecord(
          NodepoolLeoId(0),
          n.clusterId,
          n.nodepoolName,
          n.status,
          n.auditInfo.creator,
          n.auditInfo.createdDate,
          dummyDate,
          n.machineType,
          n.numNodes,
          n.autoScalingEnabled,
          n.autoscalingConfig.map(_.autoScalingMin),
          n.autoscalingConfig.map(_.autoScalingMax)
        )
    } yield n.copy(id = nodepoolId)

  def updateStatus(id: NodepoolLeoId, status: NodepoolStatus): DBIO[Int] =
    findByNodePoolIdQuery(id)
      .map(_.status)
      .update(status)

  def updateDestroyedDate(id: NodepoolLeoId, destroyedDate: Instant): DBIO[Int] =
    findByNodePoolIdQuery(id)
      .map(_.destroyedDate)
      .update(destroyedDate)

  def deleteAllForCluster(id: KubernetesClusterLeoId): DBIO[Int] =
    findByClusterIdQuery(id).delete

  def delete(id: NodepoolLeoId): DBIO[Int] =
    findByNodePoolIdQuery(id).delete

  private def unmarshalNodePool(rec: NodepoolRecord): Nodepool =
    Nodepool(
      rec.id,
      rec.clusterId,
      rec.nodepoolName,
      rec.status,
      KubernetesAuditInfo(
        rec.creator,
        rec.createdDate,
        unmarshalDestroyedDate(rec.destroyedDate)
      ),
      rec.machineType,
      rec.numNodes,
      rec.autoScalingEnabled,
      (rec.autoScalingMin, rec.autoScalingMax) match {
        case (Some(autoScalingMin), Some(autoScalingMax)) => Some(NodepoolAutoscaling(autoScalingMin, autoScalingMax))
        case _                                            => None
      }
    )
}
