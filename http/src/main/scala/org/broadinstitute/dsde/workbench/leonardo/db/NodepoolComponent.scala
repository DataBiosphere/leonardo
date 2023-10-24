package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import org.broadinstitute.dsde.workbench.google2.GKEModels.NodepoolName
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.{
  App,
  AuditInfo,
  AutoscalingConfig,
  AutoscalingMax,
  AutoscalingMin,
  CloudContext,
  KubernetesClusterLeoId,
  KubernetesRuntimeConfig,
  Nodepool,
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
  nodepoolName: NodepoolName,
  status: NodepoolStatus,
  creator: WorkbenchEmail,
  createdDate: Instant,
  destroyedDate: Instant,
  machineType: MachineTypeName,
  numNodes: NumNodes,
  autoscalingEnabled: Boolean,
  autoscalingMin: Option[AutoscalingMin],
  autoscalingMax: Option[AutoscalingMax],
  isDefault: Boolean
)

class NodepoolTable(tag: Tag) extends Table[NodepoolRecord](tag, "NODEPOOL") {
  def id = column[NodepoolLeoId]("id", O.PrimaryKey, O.AutoInc)
  def clusterId = column[KubernetesClusterLeoId]("clusterId")
  def nodepoolName = column[NodepoolName]("nodepoolName", O.Length(254))
  def status = column[NodepoolStatus]("status", O.Length(254))
  def creator = column[WorkbenchEmail]("creator", O.Length(254))
  def createdDate = column[Instant]("createdDate", O.SqlType("TIMESTAMP(6)"))
  def destroyedDate = column[Instant]("destroyedDate", O.SqlType("TIMESTAMP(6)"))
  def machineType = column[MachineTypeName]("machineType", O.Length(254))
  def numNodes = column[NumNodes]("numNodes", O.Length(254))
  def autoscalingEnabled = column[Boolean]("autoscalingEnabled")
  def autoscalingMin = column[Option[AutoscalingMin]]("autoscalingMin")
  def autoscalingMax = column[Option[AutoscalingMax]]("autoscalingMax")
  def isDefault = column[Boolean]("isDefault")

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
     autoscalingEnabled,
     autoscalingMin,
     autoscalingMax,
     isDefault
    ) <>
      (NodepoolRecord.tupled, NodepoolRecord.unapply)

}

object nodepoolQuery extends TableQuery(new NodepoolTable(_)) {
  private[db] def findActiveByClusterIdQuery(
    clusterId: KubernetesClusterLeoId
  ): Query[NodepoolTable, NodepoolRecord, Seq] =
    nodepoolQuery
      .filter(_.clusterId === clusterId)
      .filter(_.destroyedDate === dummyDate)

  private def findByNodepoolIdQuery(id: NodepoolLeoId): Query[NodepoolTable, NodepoolRecord, Seq] =
    nodepoolQuery
      .filter(_.id === id)

  def saveAllForCluster(nodepools: List[Nodepool])(implicit ec: ExecutionContext): DBIO[Unit] =
    for {
      _ <- nodepoolQuery ++=
        nodepools.map(fromNodepool(_))
    } yield ()

  def saveForCluster(n: Nodepool)(implicit ec: ExecutionContext): DBIO[Nodepool] =
    for {
      nodepoolId <- nodepoolQuery returning nodepoolQuery.map(_.id) +=
        fromNodepool(n)
    } yield n.copy(id = nodepoolId)

  private def fromNodepool(n: Nodepool) = NodepoolRecord(
    NodepoolLeoId(0),
    n.clusterId,
    n.nodepoolName,
    n.status,
    n.auditInfo.creator,
    n.auditInfo.createdDate,
    n.auditInfo.destroyedDate.getOrElse(dummyDate),
    n.machineType,
    n.numNodes,
    n.autoscalingEnabled,
    n.autoscalingConfig.map(_.autoscalingMin),
    n.autoscalingConfig.map(_.autoscalingMax),
    n.isDefault
  )

  def updateStatus(id: NodepoolLeoId, status: NodepoolStatus): DBIO[Int] =
    findByNodepoolIdQuery(id)
      .map(_.status)
      .update(status)

  def updateStatusAndCreator(id: NodepoolLeoId, status: NodepoolStatus, creator: WorkbenchEmail): DBIO[Int] =
    findByNodepoolIdQuery(id)
      .map(n => (n.status, n.creator))
      .update((status, creator))

  def updateStatuses(ids: List[NodepoolLeoId], status: NodepoolStatus)(implicit ec: ExecutionContext): DBIO[Unit] =
    for {
      _ <- nodepoolQuery
        .filter(_.id inSetBind ids.toSet)
        .map(_.status)
        .update(status)
    } yield ()

  def markActiveAsDeletedForCluster(clusterId: KubernetesClusterLeoId, destroyedDate: Instant): DBIO[Int] =
    deleteFromQuery(findActiveByClusterIdQuery(clusterId), destroyedDate)

  def markAsDeleted(id: NodepoolLeoId, destroyedDate: Instant): DBIO[Int] =
    deleteFromQuery(findByNodepoolIdQuery(id), destroyedDate)

  private def deleteFromQuery(baseQuery: Query[NodepoolTable, NodepoolRecord, Seq], destroyedDate: Instant): DBIO[Int] =
    baseQuery
      .map(n => (n.destroyedDate, n.status))
      .update((destroyedDate, NodepoolStatus.Deleted))

  def markPendingDeletionForCluster(clusterId: KubernetesClusterLeoId): DBIO[Int] =
    pendingDeletionFromQuery(findActiveByClusterIdQuery(clusterId))

  def markPendingDeletion(id: NodepoolLeoId): DBIO[Int] =
    pendingDeletionFromQuery(findByNodepoolIdQuery(id))

  // will not return any apps associated with the nodepool
  def getMinimalById(id: NodepoolLeoId)(implicit ec: ExecutionContext): DBIO[Option[Nodepool]] =
    for {
      nodepools <- findByNodepoolIdQuery(id).result
    } yield nodepools.map(rec => unmarshalNodepool(rec, List())).headOption

  def getMinimalByUserAndConfig(creator: WorkbenchEmail, cloudContext: CloudContext, config: KubernetesRuntimeConfig)(
    implicit ec: ExecutionContext
  ): DBIO[Option[Nodepool]] =
    for {
      clusters <- kubernetesClusterQuery.joinMinimalClusterAndUnmarshal(
        kubernetesClusterQuery.findActiveByCloudContextQuery(cloudContext),
        nodepoolQuery
          .filter(_.destroyedDate === dummyDate)
          .filterNot(_.isDefault)
          .filter(_.creator === creator)
          .filter(_.machineType === config.machineType)
          .filter(_.numNodes === config.numNodes)
          .filter(_.autoscalingEnabled === config.autoscalingEnabled)
      )
    } yield clusters.flatMap(_.nodepools).headOption

  def getDefaultNodepoolForCluster(
    clusterId: KubernetesClusterLeoId
  )(implicit ec: ExecutionContext): DBIO[Option[Nodepool]] =
    nodepoolQuery
      .filter(_.clusterId === clusterId)
      .filter(_.isDefault === true)
      .result
      .map(ns => ns.map(n => unmarshalNodepool(n, List())).headOption)

  private[db] def pendingDeletionFromQuery(baseQuery: Query[NodepoolTable, NodepoolRecord, Seq]): DBIO[Int] =
    baseQuery
      .map(_.status)
      .update(NodepoolStatus.Deleting)

  private[db] def unmarshalNodepool(rec: NodepoolRecord, apps: List[App]): Nodepool =
    Nodepool(
      rec.id,
      rec.clusterId,
      rec.nodepoolName,
      rec.status,
      AuditInfo(
        rec.creator,
        rec.createdDate,
        unmarshalDestroyedDate(rec.destroyedDate),
        dummyDate
      ),
      rec.machineType,
      rec.numNodes,
      rec.autoscalingEnabled,
      (rec.autoscalingMin, rec.autoscalingMax) match {
        case (Some(autoscalingMin), Some(autoscalingMax)) => Some(AutoscalingConfig(autoscalingMin, autoscalingMax))
        case _                                            => None
      },
      apps,
      rec.isDefault
    )
}
