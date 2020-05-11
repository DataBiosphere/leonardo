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
import cats.data.Chain
import cats.implicits._
import nodepoolQuery.unmarshalNodepool
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.KubernetesNamespaceName
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.{dummyDate, unmarshalDestroyedDate}

import scala.concurrent.ExecutionContext

case class KubernetesClusterRecord(id: KubernetesClusterLeoId,
                                   googleProject: GoogleProject,
                                   clusterName: KubernetesClusterName,
                                   location: Location,
                                   status: KubernetesClusterStatus,
                                   serviceAccount: WorkbenchEmail,
                                   samResourceId: KubernetesClusterSamResourceId,
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
  def samResourceId = column[KubernetesClusterSamResourceId]("samResourceId", O.Length(254))
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
     samResourceId,
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

  def getActiveFullClusterByName(googleProject: GoogleProject, clusterName: KubernetesClusterName)(
    implicit ec: ExecutionContext
  ): DBIO[Option[KubernetesCluster]] =
    joinFullClusterAndUnmarshal(findActiveByNameQuery(googleProject, clusterName))
      .map(_.headOption)

  def getFullClusterById(id: KubernetesClusterLeoId)(implicit ec: ExecutionContext): DBIO[Option[KubernetesCluster]] =
    joinFullClusterAndUnmarshal(findByIdQuery(id))
      .map(_.headOption)

  def listFullClusters(googleProject: GoogleProject,
                       includeDeleted: Boolean = false)(implicit ec: ExecutionContext): DBIO[List[KubernetesCluster]] =
    joinFullClusterAndUnmarshal(listByProject(googleProject, includeDeleted))

  def save(saveCluster: SaveKubernetesCluster)(implicit ec: ExecutionContext): DBIO[KubernetesCluster] = {
    val clusterRecord = KubernetesClusterRecord(
      KubernetesClusterLeoId(0),
      saveCluster.googleProject,
      saveCluster.clusterName,
      saveCluster.location,
      saveCluster.status,
      saveCluster.serviceAccount,
      saveCluster.samResourceId,
      saveCluster.auditInfo.creator,
      saveCluster.auditInfo.createdDate,
      dummyDate,
      saveCluster.auditInfo.dateAccessed,
      None,
      None,
      None,
      None
    )
    for {
      clusterId <- kubernetesClusterQuery returning kubernetesClusterQuery.map(_.id) += clusterRecord
      nodepool <- nodepoolQuery.saveForCluster(saveCluster.initialNodepool.copy(clusterId = clusterId))
      _ <- labelQuery.saveAllForResource(clusterId.id, LabelResourceType.KubernetesCluster, saveCluster.labels)
    } yield unmarshalKubernetesCluster(clusterRecord.copy(id = clusterId), Set(nodepool), Set(), saveCluster.labels)
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

  def markAsDeleted(id: KubernetesClusterLeoId, destroyedDate: Instant)(implicit ec: ExecutionContext): DBIO[Int] =
    for {
      nodepool <- nodepoolQuery.markActiveAsDeletedForCluster(id, destroyedDate)
      namespace <- namespaceQuery.deleteAllForCluster(id)
      label <- labelQuery.deleteAllForResource(id.id, LabelResourceType.KubernetesCluster)
      cluster <- findByIdQuery(id)
        .map(c => (c.destroyedDate, c.status))
        .update((destroyedDate, KubernetesClusterStatus.Deleted))
    } yield nodepool + namespace + label + cluster

  private[db] def joinFullClusterAndUnmarshal(
    baseQuery: Query[KubernetesClusterTable, KubernetesClusterRecord, Seq]
  )(implicit ec: ExecutionContext): DBIO[List[KubernetesCluster]] =
    joinFullCluster(baseQuery).result
      .map(rec => aggregateJoinedRecords(rec).toList)

  private[db] def joinFullCluster(baseQuery: Query[KubernetesClusterTable, KubernetesClusterRecord, Seq]) =
    for {
      (((clusters, nodepool), namespace), label) <- baseQuery joinLeft
        nodepoolQuery on (_.id === _.clusterId) joinLeft
        namespaceQuery on (_._1.id === _.clusterId) joinLeft
        labelQuery on {
        case (c, lbl) =>
          lbl.resourceId
            .mapTo[KubernetesClusterLeoId] === c._1._1.id && lbl.resourceType === LabelResourceType.kubernetesCluster
      }
    } yield (clusters, nodepool, namespace, label)

  private def aggregateJoinedRecords(
    records: Seq[(KubernetesClusterRecord, Option[NodepoolRecord], Option[NamespaceRecord], Option[LabelRecord])]
  ): Seq[KubernetesCluster] = {
    val clusterMap
      : Map[KubernetesClusterRecord, (List[NodepoolRecord], List[NamespaceRecord], Map[String, Chain[String]])] =
      records.toList.foldMap {
        case (clusterRecord, nodepoolRecordOpt, namespaceRecordOpt, labelRecordOpt) =>
          val labelMap = labelRecordOpt.map(labelRecord => labelRecord.key -> Chain(labelRecord.value)).toMap
          val nodepoolList = nodepoolRecordOpt.toList
          val namespaceList = namespaceRecordOpt.toList
          Map(clusterRecord -> (nodepoolList, namespaceList, labelMap))
      }

    clusterMap.map {
      case (clusterRec, (nodepoolList, namespaceList, labelMap)) =>
        unmarshalKubernetesCluster(
          clusterRec,
          nodepoolList.map(rec => unmarshalNodepool(rec)).toSet,
          namespaceList.map(rec => rec.namespaceName).toSet,
          labelMap.mapValues(_.toList.toSet.head)
        )
    }.toSeq
  }

  private[db] def findByIdQuery(
    id: KubernetesClusterLeoId
  ): Query[KubernetesClusterTable, KubernetesClusterRecord, Seq] =
    kubernetesClusterQuery
      .filter(_.id === id.value)

  private[db] def findActiveByNameQuery(
    googleProject: GoogleProject,
    clusterName: KubernetesClusterName
  ): Query[KubernetesClusterTable, KubernetesClusterRecord, Seq] =
    kubernetesClusterQuery
      .filter(_.googleProject === googleProject)
      .filter(_.clusterName === clusterName)
      .filter(_.destroyedDate === dummyDate)

  private[db] def listByProject(
    googleProject: GoogleProject,
    includeDeleted: Boolean
  ): Query[KubernetesClusterTable, KubernetesClusterRecord, Seq] = {
    val initialQuery = kubernetesClusterQuery
      .filter(_.googleProject === googleProject)

    includeDeleted match {
      case false => initialQuery.filterNot(_.status === KubernetesClusterStatus.deleted)
      case true  => initialQuery
    }
  }

  private def unmarshalKubernetesCluster(cr: KubernetesClusterRecord,
                                         nodepools: Set[Nodepool],
                                         namespaces: Set[KubernetesNamespaceName],
                                         labels: LabelMap): KubernetesCluster =
    KubernetesCluster(
      cr.id,
      cr.googleProject,
      cr.clusterName,
      cr.location,
      cr.status,
      cr.serviceAccount,
      cr.samResourceId,
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
      labels,
      nodepools,
      List()
    )

  //all network fields should be set at the same time. We unmarshal the entire record as None if any fields are unset
  private def unmarshalNetwork(cr: KubernetesClusterRecord): Option[NetworkFields] =
    (cr.networkName, cr.subNetworkName, cr.subNetworkIpRange) match {
      case (Some(networkName), Some(subNetworkName), Some(subNetworkIpRange)) =>
        Some(NetworkFields(networkName, subNetworkName, subNetworkIpRange))
      case _ => None
    }

}

case class SaveKubernetesCluster(
  googleProject: GoogleProject,
  clusterName: KubernetesClusterName,
  location: Location,
  status: KubernetesClusterStatus,
  serviceAccount: WorkbenchEmail,
  samResourceId: KubernetesClusterSamResourceId,
  auditInfo: AuditInfo,
  labels: LabelMap,
  initialNodepool: Nodepool
) //the clusterId specified here isn't used, and will be replaced by the id of cluster saved beforehand
