package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterName
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.KubernetesApiServerIp
import org.broadinstitute.dsde.workbench.google2.{Location, NetworkName, SubnetworkName}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import slick.lifted.Tag
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import cats.implicits._
import nodepoolQuery.unmarshalNodepool
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.{dummyDate, unmarshalDestroyedDate}

import scala.concurrent.ExecutionContext

final case class KubernetesClusterRecord(id: KubernetesClusterLeoId,
                                         googleProject: GoogleProject,
                                         clusterName: KubernetesClusterName,
                                         location: Location,
                                         status: KubernetesClusterStatus,
                                         serviceAccount: WorkbenchEmail,
                                         creator: WorkbenchEmail,
                                         createdDate: Instant,
                                         destroyedDate: Instant,
                                         dateAccessed: Instant,
                                         apiServerIp: Option[KubernetesApiServerIp],
                                         networkName: Option[NetworkName],
                                         subNetworkName: Option[SubnetworkName],
                                         subNetworkIpRange: Option[IpRange])

case class KubernetesClusterTable(tag: Tag) extends Table[KubernetesClusterRecord](tag, "KUBERNETES_CLUSTER") {
  def id = column[KubernetesClusterLeoId]("id", O.PrimaryKey, O.AutoInc)
  def googleProject = column[GoogleProject]("googleProject", O.Length(254))
  def clusterName = column[KubernetesClusterName]("clusterName", O.Length(254))
  def location = column[Location]("location", O.Length(254))
  def status = column[KubernetesClusterStatus]("status", O.Length(254))
  def serviceAccount = column[WorkbenchEmail]("serviceAccount", O.Length(254))
  def creator = column[WorkbenchEmail]("creator", O.Length(254))
  def createdDate = column[Instant]("createdDate", O.SqlType("TIMESTAMP(6)"))
  def destroyedDate = column[Instant]("destroyedDate", O.SqlType("TIMESTAMP(6)"))
  def dateAccessed = column[Instant]("dateAccessed", O.SqlType("TIMESTAMP(6)"))
  def apiServerIp = column[Option[KubernetesApiServerIp]]("apiServerIp", O.Length(254))
  def networkName = column[Option[NetworkName]]("networkName", O.Length(254))
  def subNetworkName = column[Option[SubnetworkName]]("subNetworkName", O.Length(254))
  def subNetworkIpRange = column[Option[IpRange]]("subNetworkIpRange", O.Length(254))

  def uniqueKey = index("IDX_KUBERNETES_CLUSTER_UNIQUE", (googleProject, clusterName, destroyedDate), unique = true)

  def * =
    (id,
     googleProject,
     clusterName,
     location,
     status,
     serviceAccount,
     creator,
     createdDate,
     destroyedDate,
     dateAccessed,
     apiServerIp,
     networkName,
     subNetworkName,
     subNetworkIpRange) <> (KubernetesClusterRecord.tupled, KubernetesClusterRecord.unapply)
}

object kubernetesClusterQuery extends TableQuery(new KubernetesClusterTable(_)) {

  //this retrieves the nodepool and namespaces associated with a cluster
  def getMinimalClusterById(id: KubernetesClusterLeoId, includeDeletedNodepool: Boolean = false)(
    implicit ec: ExecutionContext
  ): DBIO[Option[KubernetesCluster]] =
    joinMinimalClusterAndUnmarshal(
      findByIdQuery(id),
      includeDeletedNodepool match {
        case false => nodepoolQuery.filter(_.destroyedDate === dummyDate)
        case true  => nodepoolQuery
      }
    ).map(_.headOption)

  //this retrieves the nodepool and namespaces associated with a cluster
  def getMinimalActiveClusterByName(
    googleProject: GoogleProject
  )(implicit ec: ExecutionContext): DBIO[Option[KubernetesCluster]] =
    joinMinimalClusterAndUnmarshal(
      findActiveByNameQuery(googleProject),
      nodepoolQuery.filter(_.destroyedDate === dummyDate)
    ).map(_.headOption)

  private[db] def save(saveCluster: SaveKubernetesCluster)(implicit ec: ExecutionContext): DBIO[KubernetesCluster] = {
    val clusterRecord = saveCluster.toClusterRecord()
    for {
      clusterId <- kubernetesClusterQuery returning kubernetesClusterQuery.map(_.id) += clusterRecord
      nodepool <- nodepoolQuery.saveForCluster(saveCluster.defaultNodepool.copy(clusterId = clusterId))
    } yield unmarshalKubernetesCluster(clusterRecord.copy(id = clusterId), List(nodepool), List())
  }

  def updateStatus(id: KubernetesClusterLeoId, status: KubernetesClusterStatus): DBIO[Int] =
    findByIdQuery(id)
      .map(_.status)
      .update(status)

  def updateAsyncFields(id: KubernetesClusterLeoId, asyncFields: KubernetesClusterAsyncFields): DBIO[Int] =
    findByIdQuery(id)
      .map(c => (c.apiServerIp, c.networkName, c.subNetworkName, c.subNetworkIpRange))
      .update(
        (Some(asyncFields.apiServerIp),
         Some(asyncFields.networkInfo.networkName),
         Some(asyncFields.networkInfo.subNetworkName),
         Some(asyncFields.networkInfo.subNetworkIpRange))
      )

  def markPendingDeletion(id: KubernetesClusterLeoId)(implicit ec: ExecutionContext): DBIO[Int] =
    for {
      nodepool <- nodepoolQuery.markPendingDeletionForCluster(id)
      cluster <- findByIdQuery(id)
        .map(_.status)
        .update(KubernetesClusterStatus.Deleting)
    } yield nodepool + cluster

  def markAsDeleted(id: KubernetesClusterLeoId, destroyedDate: Instant)(implicit ec: ExecutionContext): DBIO[Int] =
    for {
      nodepool <- nodepoolQuery.markActiveAsDeletedForCluster(id, destroyedDate)
      cluster <- findByIdQuery(id)
        .map(c => (c.destroyedDate, c.status))
        .update((destroyedDate, KubernetesClusterStatus.Deleted))
    } yield nodepool + cluster

  private[db] def joinMinimalClusterAndUnmarshal(
    clusterQuery: Query[KubernetesClusterTable, KubernetesClusterRecord, Seq],
    nodepoolQuery: Query[NodepoolTable, NodepoolRecord, Seq]
  )(implicit ec: ExecutionContext): DBIO[List[KubernetesCluster]] =
    joinMinimalCluster(clusterQuery, nodepoolQuery).result
      .map(recs => aggregateJoinedCluster(recs).toList)

  private[db] def joinMinimalCluster(clusterQuery: Query[KubernetesClusterTable, KubernetesClusterRecord, Seq],
                                     nodepoolQuery: Query[NodepoolTable, NodepoolRecord, Seq]) =
    for {
      ((cluster, nodepoolOpt), namespaceOpt) <- clusterQuery joinLeft
        nodepoolQuery on (_.id === _.clusterId) joinLeft
        namespaceQuery on (_._1.id === _.clusterId)
    } yield (cluster, nodepoolOpt, namespaceOpt)

  private[db] def aggregateJoinedCluster(
    records: Seq[(KubernetesClusterRecord, Option[NodepoolRecord], Option[NamespaceRecord])]
  ): Seq[KubernetesCluster] = {
    val map = records.toList.foldMap {
      case (clusterRecord, nodepoolRecordOpt, clusterNamespaceRecordOpt) =>
        Map(clusterRecord -> (nodepoolRecordOpt.toList, clusterNamespaceRecordOpt.toList))
    }

    map.map {
      case (clusterRec, (nodepools, clusterNamespaces)) =>
        unmarshalKubernetesCluster(
          clusterRec,
          nodepools.toSet.map(rec => unmarshalNodepool(rec, List.empty)).toList,
          clusterNamespaces.toSet[NamespaceRecord].map(rec => Namespace(rec.id, rec.namespaceName)).toList
        )
    }.toSeq
  }

  private[db] def unmarshalKubernetesCluster(cr: KubernetesClusterRecord,
                                             nodepools: List[Nodepool],
                                             namespaces: List[Namespace]): KubernetesCluster =
    KubernetesCluster(
      cr.id,
      cr.googleProject,
      cr.clusterName,
      cr.location,
      cr.status,
      cr.serviceAccount,
      AuditInfo(
        cr.creator,
        cr.createdDate,
        unmarshalDestroyedDate(cr.destroyedDate),
        cr.dateAccessed
      ),
      (cr.apiServerIp, unmarshalNetwork(cr)) match {
        case (Some(apiServerIp), Some(networkFields)) => Some(KubernetesClusterAsyncFields(apiServerIp, networkFields))
        case _                                        => None
      },
      namespaces,
      nodepools,
      List()
    )

  private[db] def findByIdQuery(
    id: KubernetesClusterLeoId
  ): Query[KubernetesClusterTable, KubernetesClusterRecord, Seq] =
    kubernetesClusterQuery
      .filter(_.id === id)

  private[db] def findActiveByNameQuery(
    googleProject: GoogleProject
  ): Query[KubernetesClusterTable, KubernetesClusterRecord, Seq] =
    kubernetesClusterQuery
      .filter(_.googleProject === googleProject)
      .filter(_.destroyedDate === dummyDate)

  //all network fields should be set at the same time. We unmarshal the entire record as None if any fields are unset
  private def unmarshalNetwork(cr: KubernetesClusterRecord): Option[NetworkFields] =
    (cr.networkName, cr.subNetworkName, cr.subNetworkIpRange) match {
      case (Some(networkName), Some(subNetworkName), Some(subNetworkIpRange)) =>
        Some(NetworkFields(networkName, subNetworkName, subNetworkIpRange))
      case _ => None
    }

}

case class KubernetesAppCreationException(message: String) extends Exception

case class SaveKubernetesCluster(googleProject: GoogleProject,
                                 clusterName: KubernetesClusterName,
                                 location: Location,
                                 status: KubernetesClusterStatus,
                                 serviceAccount: WorkbenchEmail,
                                 auditInfo: AuditInfo,
                                 defaultNodepool: Nodepool) {
  def toClusterRecord(): KubernetesClusterRecord =
    KubernetesClusterRecord(
      KubernetesClusterLeoId(0),
      googleProject,
      clusterName,
      location,
      status,
      serviceAccount,
      auditInfo.creator,
      auditInfo.createdDate,
      dummyDate,
      auditInfo.dateAccessed,
      None,
      None,
      None,
      None
    )
}
