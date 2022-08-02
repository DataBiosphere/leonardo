package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant
import akka.http.scaladsl.model.StatusCodes
import cats.data.Chain
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.kubernetesClusterQuery.unmarshalKubernetesCluster
import org.broadinstitute.dsde.workbench.leonardo.db.nodepoolQuery.unmarshalNodepool
import org.broadinstitute.dsde.workbench.leonardo.http.GetAppResult
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import DBIOInstances._
import org.broadinstitute.dsde.workbench.google2.KubernetesClusterNotFoundException
import org.broadinstitute.dsde.workbench.leonardo.db.appQuery.nonDeletedAppQuery
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.concurrent.ExecutionContext

object KubernetesServiceDbQueries {
  // the 'full' here means that all joins possible are done, meaning all fields in the cluster, nodepool, and app will be present
  // if you just need the cluster, nodepools, and cluster-wide namespaces, see the minimal join in the KubernetesClusterComponent
  def listFullApps(cloudContext: Option[CloudContext], labelFilter: LabelMap = Map(), includeDeleted: Boolean = false)(
    implicit ec: ExecutionContext
  ): DBIO[List[KubernetesCluster]] =
    joinFullAppAndUnmarshal(
      listClustersByCloudContext(cloudContext),
      nodepoolQuery,
      if (includeDeleted) appQuery else nonDeletedAppQuery,
      labelFilter
    )

  // Called by MonitorAtBoot to determine which apps need monitoring
  def listMonitoredApps(implicit ec: ExecutionContext): DBIO[List[KubernetesCluster]] =
    // note we only use AppStatus to trigger monitoring; not cluster status or nodepool status
    joinFullAppAndUnmarshal(
      kubernetesClusterQuery,
      nodepoolQuery,
      appQuery.filter(_.status inSetBind AppStatus.monitoredStatuses)
    )

  // this is intended to be called first by any kubernetes /app endpoints to enforce one cluster per project
  // an error is thrown if the cluster is creating, due to the fact that we do not allow apps to be created while the cluster for that project is creating, since that means another app is already queued and waiting on this cluster
  // if the cluster already exists, this is a no-op
  // if the cluster does not exist, this also saves a dummy nodepool with the cluster.
  // For more info on dummy nodepool, See: https://broadworkbench.atlassian.net/wiki/spaces/IA/pages/492175377/2020-05-29+Dummy+Nodepool+for+Galaxy
  def saveOrGetClusterForApp(
    saveKubernetesCluster: SaveKubernetesCluster
  )(implicit ec: ExecutionContext): DBIO[SaveClusterResult] =
    for {
      clusterOpt <- kubernetesClusterQuery.getMinimalActiveClusterByName(saveKubernetesCluster.cloudContext)

      eitherClusterOrError <- clusterOpt match {
        case Some(cluster) =>
          cluster.status match {
            case s if KubernetesClusterStatus.creatingStatuses contains s =>
              DBIO.failed(
                KubernetesAppCreationException(
                  s"You cannot create an app while a cluster is in ${KubernetesClusterStatus.creatingStatuses}. Status: ${s}",
                  None
                )
              )
            case _ => DBIO.successful(ClusterExists(cluster))
          }
        case _ =>
          kubernetesClusterQuery
            .save(saveKubernetesCluster)
            .map(c => ClusterDoesNotExist(c, DefaultNodepool.fromNodepool(c.nodepools.head)))
      }
    } yield eitherClusterOrError

  def getActiveFullAppByName(cloudContext: CloudContext, appName: AppName, labelFilter: LabelMap = Map())(implicit
    ec: ExecutionContext
  ): DBIO[Option[GetAppResult]] =
    getActiveFullApp(listClustersByCloudContext(Some(cloudContext)),
                     nodepoolQuery,
                     appQuery.findActiveByNameQuery(appName),
                     labelFilter
    )

  def getFullAppByName(cloudContext: CloudContext, appId: AppId, labelFilter: LabelMap = Map())(implicit
    ec: ExecutionContext
  ): DBIO[Option[GetAppResult]] =
    getActiveFullApp(listClustersByCloudContext(Some(cloudContext)),
                     nodepoolQuery,
                     appQuery.getByIdQuery(appId),
                     labelFilter
    )

  def hasClusterOperationInProgress(clusterId: KubernetesClusterLeoId)(implicit
    ec: ExecutionContext
  ): DBIO[Boolean] =
    for {
      clusterOpt <- kubernetesClusterQuery.getMinimalClusterById(clusterId)
      cluster <- clusterOpt match {
        case None =>
          DBIO.failed(
            KubernetesClusterNotFoundException(
              s"did not find cluster with id $clusterId while checking for concurrent cluster operations"
            )
          )
        case Some(c) => DBIO.successful(c)
      }
      hasOperationInProgress = cluster.nodepools.exists(nodepool =>
        NodepoolStatus.nonParellizableStatuses.contains(nodepool.status)
      )
    } yield hasOperationInProgress

  private[db] def getActiveFullApp(clusterQuery: Query[KubernetesClusterTable, KubernetesClusterRecord, Seq],
                                   nodepoolQuery: Query[NodepoolTable, NodepoolRecord, Seq],
                                   appQuery: Query[AppTable, AppRecord, Seq],
                                   labelFilter: LabelMap = Map()
  )(implicit
    ec: ExecutionContext
  ): DBIO[Option[GetAppResult]] =
    for {
      clusters <- joinFullAppAndUnmarshal(clusterQuery, nodepoolQuery, appQuery, labelFilter)
      _ <-
        if (clusters.length > 1)
          DBIO.failed(
            GetAppAssertion(
              "(GoogleProject, AppName) uniqueness has been violated, clusters returned > 1 by getActiveFullApp"
            )
          )
        else DBIO.successful(())
      validatedApp <- clusters.headOption.fold[DBIO[Option[GetAppResult]]](
        DBIO.successful(None)
      ) { cluster =>
        for {
          _ <-
            if (cluster.nodepools.length != 1)
              DBIO.failed(
                GetAppAssertion(
                  "(GoogleProject, AppName) uniqueness has been violated, num nodepools returned != 1 by getActiveFullApp"
                )
              )
            else DBIO.successful(())
          appResult <- DBIO.successful(
            Some(
              GetAppResult(
                // we are guaranteed exactly one app and nodepool here if the query doesn't return None due to the way the query JOINs and database unique enforcement
                // we explicitly enforce this above as well
                cluster,
                cluster.nodepools.head,
                cluster.nodepools.head.apps.head
              )
            )
          )
        } yield appResult
      }
    } yield validatedApp

  // if there are no apps associated with this cluster, None will be returned even if the cluster exists.
  // see the `Minimal` methods if you wish to get the cluster itself
  def getFullClusterById(id: KubernetesClusterLeoId)(implicit ec: ExecutionContext): DBIO[Option[KubernetesCluster]] =
    joinFullAppAndUnmarshal(kubernetesClusterQuery.findByIdQuery(id), nodepoolQuery, appQuery)
      .map(_.headOption)

  def getAllNodepoolsForCluster(
    clusterId: KubernetesClusterLeoId
  )(implicit ec: ExecutionContext): DBIO[List[Nodepool]] =
    for {
      nodepools <- nodepoolQuery
        .findActiveByClusterIdQuery(clusterId)
        .result
    } yield nodepools.map(rec => unmarshalNodepool(rec, List.empty)).toList

  def markPendingCreating(appId: AppId,
                          clusterId: Option[KubernetesClusterLeoId],
                          defaultNodepoolId: Option[NodepoolLeoId],
                          nodepoolId: Option[NodepoolLeoId]
  )(implicit
    ec: ExecutionContext
  ): DBIO[Unit] =
    for {
      _ <- clusterId.traverse(id => kubernetesClusterQuery.updateStatus(id, KubernetesClusterStatus.Provisioning))
      _ <- defaultNodepoolId.traverse(id => nodepoolQuery.updateStatus(id, NodepoolStatus.Provisioning))
      _ <- nodepoolId.traverse(id => nodepoolQuery.updateStatus(id, NodepoolStatus.Provisioning))
      _ <- appQuery.updateStatus(appId, AppStatus.Provisioning)
    } yield ()

  def markPendingBatchCreating(clusterId: KubernetesClusterLeoId, nodepoolIds: List[NodepoolLeoId])(implicit
    ec: ExecutionContext
  ): DBIO[Unit] =
    for {
      _ <- kubernetesClusterQuery.updateStatus(clusterId, KubernetesClusterStatus.Provisioning)
      _ <- nodepoolQuery
        .filter(_.id.inSet(nodepoolIds.toSet))
        .map(_.status)
        .update(NodepoolStatus.Provisioning)
    } yield ()

  def markPreDeleting(nodepoolId: NodepoolLeoId, appId: AppId)(implicit ec: ExecutionContext): DBIO[Unit] =
    for {
      _ <- appQuery.updateStatus(appId, AppStatus.Predeleting)
    } yield ()

  def markPendingAppDeletion(appId: AppId, diskId: Option[DiskId], now: Instant)(implicit
    ec: ExecutionContext
  ): DBIO[Unit] =
    for {
      _ <- appQuery.markPendingDeletion(appId)
      _ <- diskId.fold[DBIO[Int]](DBIO.successful(0))(diskId => persistentDiskQuery.markPendingDeletion(diskId, now))
    } yield ()

  private[db] def listClustersByCloudContext(
    cloudContext: Option[CloudContext]
  ): Query[KubernetesClusterTable, KubernetesClusterRecord, Seq] =
    cloudContext match {
      case Some(context) =>
        kubernetesClusterQuery
          .filter(_.cloudProvider === context.cloudProvider)
          .filter(_.cloudContextDb === context.asCloudContextDb)
      case None => kubernetesClusterQuery
    }

  private[db] def joinFullAppAndUnmarshal(
    baseQuery: Query[KubernetesClusterTable, KubernetesClusterRecord, Seq],
    nodepoolQuery: Query[NodepoolTable, NodepoolRecord, Seq],
    appQuery: Query[AppTable, AppRecord, Seq],
    labelFilter: LabelMap = Map()
  )(implicit ec: ExecutionContext): DBIO[List[KubernetesCluster]] =
    joinFullApp(baseQuery, nodepoolQuery, appQuery, labelFilter).result
      .map(rec => aggregateJoinedApp(rec).toList)

  private[db] def joinFullApp(baseClusterQuery: Query[KubernetesClusterTable, KubernetesClusterRecord, Seq],
                              baseNodepoolQuery: Query[NodepoolTable, NodepoolRecord, Seq],
                              baseAppQuery: Query[AppTable, AppRecord, Seq],
                              labelFilter: LabelMap
  ) = {
    val query = for {
      (((((((((app, nodepool), cluster), label), clusterNamespaces), appNamespace), services), disks), diskLabels),
       appError
      ) <- baseAppQuery join
        baseNodepoolQuery on (_.nodepoolId === _.id) join
        baseClusterQuery on (_._2.clusterId === _.id) joinLeft
        labelQuery on { case (c, lbl) =>
          val resourceTypeFilter = lbl.resourceId
            .mapTo[AppId] === c._1._1.id && lbl.resourceType === LabelResourceType.app
          resourceTypeFilter
        } joinLeft
        namespaceQuery on (_._1._2.id === _.clusterId) join
        namespaceQuery on (_._1._1._1._1.namespaceId === _.id) joinLeft
        serviceQuery on (_._1._1._1._1._1.id === _.appId) joinLeft
        persistentDiskQuery.tableQuery on (_._1._1._1._1._1._1.diskId === _.id) joinLeft
        labelQuery on { case (c, lbl) =>
          lbl.resourceId
            .mapTo[DiskId] === c._1._1._1._1._1._1._1.diskId && lbl.resourceType === LabelResourceType.persistentDisk
        } joinLeft
        appErrorQuery on (_._1._1._1._1._1._1._1._1.id === _.appId)
    } yield (cluster, nodepool, app, label, clusterNamespaces, appNamespace, services, disks, diskLabels, appError)

    // apply label filter
    query.filter { case (_, _, app, _, _, _, _, _, _, _) =>
      labelQuery
        .filter(lbl => lbl.resourceId.mapTo[AppId] === app.id && lbl.resourceType === LabelResourceType.app)
        .filter(lbl =>
          labelFilter
            .map { case (k, v) => lbl.key === k && lbl.value === v }
            .fold[Rep[Boolean]](false)(_ || _)
        )
        .length === labelFilter.size
    }
  }

  private[db] def aggregateJoinedApp(
    records: Seq[
      (KubernetesClusterRecord,
       NodepoolRecord,
       AppRecord,
       Option[LabelRecord],
       Option[NamespaceRecord],
       NamespaceRecord,
       Option[ServiceRecord],
       Option[PersistentDiskRecord],
       Option[LabelRecord],
       Option[AppErrorRecord]
      )
    ]
  ): Seq[KubernetesCluster] = {

    val map = records.toList.foldMap {
      case (clusterRecord,
            nodepoolRecord,
            appRecord,
            labelRecordOpt,
            clusterNamespaceRecordOpt,
            appNamespaceRecord,
            serviceRecord,
            diskRecordOpt,
            diskLabelOpt,
            appErrorOpt
          ) =>
        val labelMap = labelRecordOpt.map(labelRecord => labelRecord.key -> Chain(labelRecord.value)).toMap
        val diskLabelMap = diskLabelOpt.map(labelRecord => labelRecord.key -> Chain(labelRecord.value)).toMap
        val diskMap = diskRecordOpt.fold[Map[PersistentDiskRecord, Map[String, Chain[String]]]](Map())(disk =>
          Map(disk -> diskLabelMap)
        )
        val appMap =
          Map(
            appRecord -> (Chain.fromSeq(serviceRecord.toList), Chain(appNamespaceRecord), labelMap, diskMap, Chain
              .fromSeq(appErrorOpt.toList))
          )
        val nodepoolMap = Map(nodepoolRecord -> appMap)

        Map(clusterRecord -> (nodepoolMap, clusterNamespaceRecordOpt.toList))
    }

    map.map { case (clusterRec, (nodepoolMap, clusterNamespaces)) =>
      unmarshalKubernetesCluster(
        clusterRec,
        unmarshalNodepoolMap(nodepoolMap),
        clusterNamespaces.toSet[NamespaceRecord].map(rec => Namespace(rec.id, rec.namespaceName)).toList
      )
    }.toSeq
  }

  private def unmarshalNodepoolMap(
    nodepools: Map[NodepoolRecord, Map[AppRecord,
                                       (Chain[ServiceRecord],
                                        Chain[NamespaceRecord],
                                        Map[String, Chain[String]],
                                        Map[PersistentDiskRecord, Map[String, Chain[String]]],
                                        Chain[AppErrorRecord]
                                       )
    ]]
  ): List[Nodepool] =
    nodepools
      .map { case (nodepoolRec, appMap) =>
        unmarshalNodepool(nodepoolRec, unmarshalAppMap(appMap))
      }
      .toSet
      .toList

  private def unmarshalAppMap(
    apps: Map[AppRecord,
              (Chain[ServiceRecord],
               Chain[NamespaceRecord],
               Map[String, Chain[String]],
               Map[PersistentDiskRecord, Map[String, Chain[String]]],
               Chain[AppErrorRecord]
              )
    ]
  ): List[App] =
    apps
      .map { case (appRec, (services, namespaces, labelMap, diskMap, errors)) =>
        appQuery.unmarshalApp(
          appRec,
          services.map(serviceQuery.unmarshalService).toList.toSet.toList,
          labelMap.view.mapValues(_.toList.toSet.head).toMap,
          // the database ensures we always have a single namespace here
          namespaceQuery.unmarshalNamespace(namespaces.headOption.get),
          unmarshalDiskMap(diskMap),
          errors.map(rec => appErrorQuery.unmarshallAppErrorRecord(rec)).toList.toSet.toList
        )
      }
      .toSet
      .toList

  private def unmarshalDiskMap(disks: Map[PersistentDiskRecord, Map[String, Chain[String]]]): Option[PersistentDisk] =
    disks.map { case (disk, labels) =>
      persistentDiskQuery.unmarshalPersistentDisk(disk, labels.view.mapValues(_.toList.toSet.head).toMap)
    }.headOption
}

//minimal cluster has the nodepools, but no namespaces or apps
sealed trait SaveClusterResult {
  def minimalCluster: KubernetesCluster
}
final case class ClusterDoesNotExist(minimalCluster: KubernetesCluster, defaultNodepool: DefaultNodepool)
    extends SaveClusterResult
final case class ClusterExists(minimalCluster: KubernetesCluster) extends SaveClusterResult
final case class GetAppAssertion(msg: String) extends LeoException(msg, StatusCodes.InternalServerError, traceId = None)
final case class KubernetesAppCreationException(msg: String, traceId: Option[TraceId])
    extends LeoException(msg, StatusCodes.Conflict, traceId = traceId)
