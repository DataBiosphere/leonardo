package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterName
import org.broadinstitute.dsde.workbench.google2.{Location, NetworkName, RegionName, SubnetworkName}
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.{dummyDate, unmarshalDestroyedDate}
import org.broadinstitute.dsde.workbench.leonardo.db.nodepoolQuery.unmarshalNodepool
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import slick.lifted.Tag

import java.sql.SQLDataException
import scala.concurrent.ExecutionContext

final case class KubernetesClusterRecord(id: KubernetesClusterLeoId,
                                         cloudContext: CloudContext,
                                         clusterName: KubernetesClusterName,
                                         location: Location,
                                         region: RegionName,
                                         status: KubernetesClusterStatus,
                                         creator: WorkbenchEmail,
                                         createdDate: Instant,
                                         destroyedDate: Instant,
                                         dateAccessed: Instant,
                                         ingressChart: Chart,
                                         loadBalancerIp: Option[IP],
                                         apiServerIp: Option[IP],
                                         networkName: Option[NetworkName],
                                         subNetworkName: Option[SubnetworkName],
                                         subNetworkIpRange: Option[IpRange]
)

case class KubernetesClusterTable(tag: Tag) extends Table[KubernetesClusterRecord](tag, "KUBERNETES_CLUSTER") {
  def id = column[KubernetesClusterLeoId]("id", O.PrimaryKey, O.AutoInc)
  def cloudContextDb = column[CloudContextDb]("cloudContext", O.Length(254))
  def cloudProvider = column[CloudProvider]("cloudProvider", O.Length(50))
  def clusterName = column[KubernetesClusterName]("clusterName", O.Length(254))
  def location = column[Location]("location", O.Length(254))
  def region = column[RegionName]("region", O.Length(254))
  def status = column[KubernetesClusterStatus]("status", O.Length(254))
  def creator = column[WorkbenchEmail]("creator", O.Length(254))
  def createdDate = column[Instant]("createdDate", O.SqlType("TIMESTAMP(6)"))
  def destroyedDate = column[Instant]("destroyedDate", O.SqlType("TIMESTAMP(6)"))
  def dateAccessed = column[Instant]("dateAccessed", O.SqlType("TIMESTAMP(6)"))
  def ingressChart = column[Chart]("ingressChart", O.Length(254))
  def loadBalancerIp = column[Option[IP]]("loadBalancerIp", O.Length(254))
  def apiServerIp = column[Option[IP]]("apiServerIp", O.Length(254))
  def networkName = column[Option[NetworkName]]("networkName", O.Length(254))
  def subNetworkName = column[Option[SubnetworkName]]("subNetworkName", O.Length(254))
  def subNetworkIpRange = column[Option[IpRange]]("subNetworkIpRange", O.Length(254))

  def * =
    (id,
     (cloudProvider, cloudContextDb),
     clusterName,
     location,
     region,
     status,
     creator,
     createdDate,
     destroyedDate,
     dateAccessed,
     ingressChart,
     loadBalancerIp,
     apiServerIp,
     networkName,
     subNetworkName,
     subNetworkIpRange
    ).shaped <> ({
      case (id,
            (cloudProvider, cloudContextDb),
            clusterName,
            location,
            region,
            status,
            creator,
            createdDate,
            destroyedDate,
            dateAccessed,
            ingressChart,
            loadBalancerIp,
            apiServerIp,
            networkName,
            subNetworkName,
            subNetworkIpRange
          ) =>
        KubernetesClusterRecord(
          id,
          cloudProvider match {
            case CloudProvider.Gcp =>
              CloudContext.Gcp(GoogleProject(cloudContextDb.value)): CloudContext
            case CloudProvider.Azure =>
              val context =
                AzureCloudContext.fromString(cloudContextDb.value).fold(s => throw new SQLDataException(s), identity)
              CloudContext.Azure(context): CloudContext
          },
          clusterName,
          location,
          region,
          status,
          creator,
          createdDate,
          destroyedDate,
          dateAccessed,
          ingressChart,
          loadBalancerIp,
          apiServerIp,
          networkName,
          subNetworkName,
          subNetworkIpRange
        )
    }, { r: KubernetesClusterRecord =>
      Some(
        (r.id,
         r.cloudContext match {
           case CloudContext.Gcp(value) =>
             (CloudProvider.Gcp, CloudContextDb(value.value))
           case CloudContext.Azure(value) =>
             (CloudProvider.Azure, CloudContextDb(value.asString))
         },
         r.clusterName,
         r.location,
         r.region,
         r.status,
         r.creator,
         r.createdDate,
         r.destroyedDate,
         r.dateAccessed,
         r.ingressChart,
         r.loadBalancerIp,
         r.apiServerIp,
         r.networkName,
         r.subNetworkName,
         r.subNetworkIpRange
        )
      )
    })
}

object kubernetesClusterQuery extends TableQuery(KubernetesClusterTable(_)) {

  // this retrieves the nodepool and namespaces associated with a cluster
  def getMinimalClusterById(id: KubernetesClusterLeoId, includeDeletedNodepool: Boolean = false)(implicit
    ec: ExecutionContext
  ): DBIO[Option[KubernetesCluster]] =
    joinMinimalClusterAndUnmarshal(
      findByIdQuery(id),
      includeDeletedNodepool match {
        case false => nodepoolQuery.filter(_.destroyedDate === dummyDate)
        case true  => nodepoolQuery
      }
    ).map(_.headOption)

  // this retrieves the nodepool and namespaces associated with a cluster
  def getMinimalActiveClusterByCloudContext(
    cloudContext: CloudContext
  )(implicit ec: ExecutionContext): DBIO[Option[KubernetesCluster]] =
    joinMinimalClusterAndUnmarshal(
      findActiveByCloudContextQuery(cloudContext),
      nodepoolQuery.filter(_.destroyedDate === dummyDate)
    ).map(_.headOption)

  private[db] def save(saveCluster: SaveKubernetesCluster)(implicit ec: ExecutionContext): DBIO[KubernetesCluster] = {
    val clusterRecord = saveCluster.toClusterRecord
    for {
      clusterId <- kubernetesClusterQuery returning kubernetesClusterQuery.map(_.id) += clusterRecord
      nodepool <- nodepoolQuery
        .saveForCluster(saveCluster.defaultNodepool.copy(clusterId = clusterId).toNodepool())
    } yield unmarshalKubernetesCluster(clusterRecord.copy(id = clusterId), List(nodepool))
  }

  private[db] def save(
    kubernetesClusterRecord: KubernetesClusterRecord
  )(implicit ec: ExecutionContext): DBIO[KubernetesCluster] =
    for {
      clusterId <- kubernetesClusterQuery returning kubernetesClusterQuery.map(_.id) += kubernetesClusterRecord
    } yield unmarshalKubernetesCluster(kubernetesClusterRecord.copy(id = clusterId), List())

  def updateStatus(id: KubernetesClusterLeoId, status: KubernetesClusterStatus): DBIO[Int] =
    findByIdQuery(id)
      .map(_.status)
      .update(status)

  def updateAsyncFields(id: KubernetesClusterLeoId, asyncFields: KubernetesClusterAsyncFields): DBIO[Int] =
    findByIdQuery(id)
      .map(c => (c.loadBalancerIp, c.apiServerIp, c.networkName, c.subNetworkName, c.subNetworkIpRange))
      .update(
        (Some(asyncFields.loadBalancerIp),
         Some(asyncFields.apiServerIp),
         Some(asyncFields.networkInfo.networkName),
         Some(asyncFields.networkInfo.subNetworkName),
         Some(asyncFields.networkInfo.subNetworkIpRange)
        )
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

  def updateRegion(id: KubernetesClusterLeoId, regionName: RegionName): DBIO[Int] =
    findByIdQuery(id)
      .map(c => c.region)
      .update(regionName)

  private[db] def joinMinimalClusterAndUnmarshal(
    clusterQuery: Query[KubernetesClusterTable, KubernetesClusterRecord, Seq],
    nodepoolQuery: Query[NodepoolTable, NodepoolRecord, Seq]
  )(implicit ec: ExecutionContext): DBIO[List[KubernetesCluster]] =
    joinMinimalCluster(clusterQuery, nodepoolQuery).result.map(recs => aggregateJoinedCluster(recs).toList)

  private[db] def joinMinimalCluster(clusterQuery: Query[KubernetesClusterTable, KubernetesClusterRecord, Seq],
                                     nodepoolQuery: Query[NodepoolTable, NodepoolRecord, Seq]
  ) =
    for {
      ((cluster, nodepoolOpt)) <- clusterQuery joinLeft
        nodepoolQuery on (_.id === _.clusterId)
    } yield (cluster, nodepoolOpt)

  private[db] def aggregateJoinedCluster(
    records: Seq[(KubernetesClusterRecord, Option[NodepoolRecord])]
  ): Seq[KubernetesCluster] = {
    val map = records.toList.foldMap { case (clusterRecord, nodepoolRecordOpt) =>
      Map(clusterRecord -> (nodepoolRecordOpt.toList))
    }

    map.map { case (clusterRec, nodepools) =>
      unmarshalKubernetesCluster(
        clusterRec,
        nodepools.toSet.map(rec => unmarshalNodepool(rec, List.empty)).toList
      )
    }.toSeq
  }

  private[db] def unmarshalKubernetesCluster(cr: KubernetesClusterRecord,
                                             nodepools: List[Nodepool]
  ): KubernetesCluster =
    KubernetesCluster(
      cr.id,
      cr.cloudContext,
      cr.clusterName,
      cr.location,
      cr.region,
      cr.status,
      cr.ingressChart,
      AuditInfo(
        cr.creator,
        cr.createdDate,
        unmarshalDestroyedDate(cr.destroyedDate),
        cr.dateAccessed
      ),
      (cr.loadBalancerIp, cr.apiServerIp, unmarshalNetwork(cr)) match {
        case (Some(externalIp), Some(apiServerIp), Some(networkFields)) =>
          Some(KubernetesClusterAsyncFields(externalIp, apiServerIp, networkFields))
        case _ => None
      },
      nodepools
    )

  private[db] def findByIdQuery(
    id: KubernetesClusterLeoId
  ): Query[KubernetesClusterTable, KubernetesClusterRecord, Seq] =
    kubernetesClusterQuery
      .filter(_.id === id)

  private[db] def findActiveByCloudContextQuery(
    cloudContext: CloudContext
  ): Query[KubernetesClusterTable, KubernetesClusterRecord, Seq] =
    kubernetesClusterQuery
      .filter(_.cloudProvider === cloudContext.cloudProvider)
      .filter(_.cloudContextDb === cloudContext.asCloudContextDb)
      .filter(_.destroyedDate === dummyDate)

  // all network fields should be set at the same time. We unmarshal the entire record as None if any fields are unset
  private def unmarshalNetwork(cr: KubernetesClusterRecord): Option[NetworkFields] =
    (cr.networkName, cr.subNetworkName, cr.subNetworkIpRange) match {
      case (Some(networkName), Some(subNetworkName), Some(subNetworkIpRange)) =>
        Some(NetworkFields(networkName, subNetworkName, subNetworkIpRange))
      case _ => None
    }

}

case class SaveKubernetesCluster(cloudContext: CloudContext,
                                 clusterName: KubernetesClusterName,
                                 location: Location,
                                 region: RegionName,
                                 status: KubernetesClusterStatus,
                                 ingressChart: Chart,
                                 auditInfo: AuditInfo,
                                 defaultNodepool: DefaultNodepool, // Question: does this have to be `DefaultNodepool`?
                                 autopilot: Boolean
) {
  def toClusterRecord: KubernetesClusterRecord =
    KubernetesClusterRecord(
      KubernetesClusterLeoId(0),
      cloudContext,
      clusterName,
      location,
      region,
      status,
      auditInfo.creator,
      auditInfo.createdDate,
      auditInfo.destroyedDate.getOrElse(dummyDate),
      auditInfo.dateAccessed,
      ingressChart,
      None,
      None,
      None,
      None,
      None
    )
}
