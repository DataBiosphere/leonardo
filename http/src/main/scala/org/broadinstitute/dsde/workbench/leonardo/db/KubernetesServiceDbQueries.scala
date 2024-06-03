package org.broadinstitute.dsde.workbench.leonardo
package db

import akka.http.scaladsl.model.StatusCodes
import cats.data.Chain
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.KubernetesClusterNotFoundException
import org.broadinstitute.dsde.workbench.leonardo.db.DBIOInstances._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.appQuery.{filterByWorkspaceIdAndCreator, nonDeletedAppQuery}
import org.broadinstitute.dsde.workbench.leonardo.db.kubernetesClusterQuery.unmarshalKubernetesCluster
import org.broadinstitute.dsde.workbench.leonardo.db.nodepoolQuery.unmarshalNodepool
import org.broadinstitute.dsde.workbench.leonardo.http.GetAppResult
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}

import java.time.Instant
import scala.concurrent.ExecutionContext

object KubernetesServiceDbQueries {

  /**
    * List all apps in the given CloudContext, with optional label filter.
    * This method should be used by v1 app routes. v2 apps should use `listFullAppsByWorkspaceId`.
    *
    * Note: the 'full' here means that all joins possible are done, meaning all fields in the cluster, nodepool,
    * and app will be present. If you just need the cluster, nodepools, and cluster-wide namespaces, see the minimal
    * join in the KubernetesClusterComponent.
    */
  def listFullApps(cloudContext: Option[CloudContext],
                   labelFilter: LabelMap = Map(),
                   includeDeleted: Boolean = false,
                   creatorOnly: Option[WorkbenchEmail] = None
  )(implicit
    ec: ExecutionContext
  ): DBIO[List[KubernetesCluster]] =
    joinFullAppAndUnmarshal(
      listClustersByCloudContext(cloudContext),
      nodepoolQuery,
      filterByWorkspaceIdAndCreator(query = if (includeDeleted) appQuery else nonDeletedAppQuery,
                                    workspaceId = None,
                                    creatorOnly = creatorOnly
      ),
      labelFilter
    )

  /**
    * List all apps in the given workspace, with optional label filter.
    * This method should be used by v2 app routes.
    */
  def listFullAppsByWorkspaceId(workspaceId: Option[WorkspaceId],
                                labelFilter: LabelMap = Map(),
                                includeDeleted: Boolean = false,
                                creatorOnly: Option[WorkbenchEmail] = None
  )(implicit
    ec: ExecutionContext
  ): DBIO[List[KubernetesCluster]] =
    joinFullAppAndUnmarshal(
      kubernetesClusterQuery,
      nodepoolQuery,
      filterByWorkspaceIdAndCreator(query = if (includeDeleted) appQuery else nonDeletedAppQuery,
                                    workspaceId = workspaceId,
                                    creatorOnly = creatorOnly
      ),
      labelFilter
    )

  /**
    * List all RUNNING apps that are not on the given target chart version. Used to decide which apps to update.
    */
  def listAppsForUpdate(targetVersion: Chart,
                        appType: AppType,
                        cloudProvider: CloudProvider,
                        chartVersionsToInclude: List[Chart] = List(),
                        chartVersionsToExclude: List[Chart] = List(),
                        googleProject: Option[GoogleProject] = None,
                        workspaceId: Option[WorkspaceId] = None,
                        appNames: List[AppName] = List()
  )(implicit
    ec: ExecutionContext
  ): DBIO[List[KubernetesCluster]] =
    joinFullAppAndUnmarshal(
      kubernetesClusterQuery
        .filter(_.cloudProvider === cloudProvider)
        .filterOpt(googleProject) { case (clusterTable, gp) =>
          clusterTable.cloudContextDb === CloudContextDb(gp.value)
        },
      nodepoolQuery,
      appQuery
        .filter(_.status === (AppStatus.Running: AppStatus))
        .filter(_.appType === appType)
        .filter(_.chart =!= targetVersion)
        .filterIf(chartVersionsToInclude.nonEmpty)(_.chart inSetBind chartVersionsToInclude)
        .filterIf(chartVersionsToExclude.nonEmpty)(t => !(t.chart inSetBind chartVersionsToExclude))
        .filterOpt(workspaceId) { case (appTable, wId) => appTable.workspaceId === wId }
        .filterIf(appNames.nonEmpty)(_.appName inSetBind appNames)
    )

  /**
    * List all apps that need monitoring. Called by MonitorAtBoot.
    */
  def listMonitoredApps(implicit ec: ExecutionContext): DBIO[List[KubernetesCluster]] =
    // note we only use AppStatus to trigger monitoring; not cluster status or nodepool status
    joinFullAppAndUnmarshal(
      kubernetesClusterQuery,
      nodepoolQuery,
      appQuery.filter(_.status inSetBind AppStatus.monitoredStatuses)
    )

  /**
    * List all apps for metrics. Called by AppHealthMonitor.
    */
  def listAppsForMetrics(implicit ec: ExecutionContext): DBIO[List[KubernetesCluster]] =
    joinFullAppAndUnmarshal(
      kubernetesClusterQuery,
      nodepoolQuery,
      nonDeletedAppQuery
    )

  /**
    * Looks up or persists a KubernetesCluster, and returns it.
    * Throws an error if the cluster is in creating status.
    */
  def saveOrGetClusterForApp(
    saveKubernetesCluster: SaveKubernetesCluster,
    traceId: TraceId
  )(implicit ec: ExecutionContext): DBIO[SaveClusterResult] =
    for {
      clusterOpt <- kubernetesClusterQuery.getMinimalActiveClusterByCloudContext(saveKubernetesCluster.cloudContext)

      eitherClusterOrError <- clusterOpt match {
        case Some(cluster) =>
          cluster.status match {
            case s if KubernetesClusterStatus.creatingStatuses contains s =>
              DBIO.failed(
                KubernetesAppCreationException(
                  s"You cannot create an app while a cluster is in ${KubernetesClusterStatus.creatingStatuses}. Status: ${s}",
                  Some(traceId)
                )
              )
            case _ => DBIO.successful(ClusterExists(cluster, DefaultNodepool.fromNodepool(cluster.nodepools.head)))
          }
        case _ =>
          kubernetesClusterQuery
            .save(saveKubernetesCluster)
            .map(c => ClusterDoesNotExist(c, DefaultNodepool.fromNodepool(c.nodepools.head)))
      }
    } yield eitherClusterOrError

  /**
    * Gets an active app by name and cloud context.
    * This method should be used by v1 app routes. v2 apps should use `getActiveFullAppByWorkspaceIdAndAppName`.
    */
  def getActiveFullAppByName(cloudContext: CloudContext, appName: AppName, labelFilter: LabelMap = Map())(implicit
    ec: ExecutionContext
  ): DBIO[Option[GetAppResult]] =
    getActiveFullApp(listClustersByCloudContext(Some(cloudContext)),
                     nodepoolQuery,
                     appQuery.findActiveByNameQuery(appName),
                     labelFilter
    )

  /**
    * Gets an active app by name and workspace.
    * This method should be used by v2 app routes.
    */
  def getActiveFullAppByWorkspaceIdAndAppName(workspaceId: WorkspaceId,
                                              appName: AppName,
                                              labelFilter: LabelMap = Map()
  )(implicit
    ec: ExecutionContext
  ): DBIO[Option[GetAppResult]] =
    getActiveFullApp(
      kubernetesClusterQuery,
      nodepoolQuery,
      filterByWorkspaceIdAndCreator(query = appQuery.findActiveByNameQuery(appName),
                                    workspaceId = Some(workspaceId),
                                    creatorOnly = None
      ),
      labelFilter
    )

  /**
    * Gets an app by ID. This method is safe to use for both v1 and v2 routes.
    */
  def getFullAppById(cloudContext: CloudContext, appId: AppId, labelFilter: LabelMap = Map())(implicit
    ec: ExecutionContext
  ): DBIO[Option[GetAppResult]] =
    getActiveFullApp(listClustersByCloudContext(Some(cloudContext)),
                     nodepoolQuery,
                     appQuery.getByIdQuery(appId),
                     labelFilter
    )

  /**
    * Queries a cluster by ID, and returns true if the cluster has a nodepool operation (creation, deletion)
    * in progress.
    */
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

  def markPreDeleting(appId: AppId)(implicit ec: ExecutionContext): DBIO[Unit] =
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
      ((((((((app, nodepool), cluster), label)), services), disks), diskLabels), appError) <- baseAppQuery join
        baseNodepoolQuery on (_.nodepoolId === _.id) join
        baseClusterQuery on (_._2.clusterId === _.id) joinLeft
        labelQuery on { case (c, lbl) =>
          val resourceTypeFilter = lbl.resourceId
            .mapTo[AppId] === c._1._1.id && lbl.resourceType === LabelResourceType.app
          resourceTypeFilter
        } joinLeft
        serviceQuery on (_._1._1._1.id === _.appId) joinLeft
        persistentDiskQuery.tableQuery on (_._1._1._1._1.diskId === _.id) joinLeft
        labelQuery on { case (c, lbl) =>
          lbl.resourceId
            .mapTo[DiskId] === c._1._1._1._1._1.diskId && lbl.resourceType === LabelResourceType.persistentDisk
        } joinLeft
        appErrorQuery on (_._1._1._1._1._1._1.id === _.appId)
    } yield (cluster, nodepool, app, label, services, disks, diskLabels, appError)

    // apply label filter
    query.filter { case (_, _, app, _, _, _, _, _) =>
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
            appRecord -> (Chain.fromSeq(serviceRecord.toList), labelMap, diskMap, Chain
              .fromSeq(appErrorOpt.toList))
          )
        val nodepoolMap = Map(nodepoolRecord -> appMap)

        Map(clusterRecord -> nodepoolMap)
    }

    map.map { case (clusterRec, nodepoolMap) =>
      unmarshalKubernetesCluster(
        clusterRec,
        unmarshalNodepoolMap(nodepoolMap)
      )
    }.toSeq
  }

  private def unmarshalNodepoolMap(
    nodepools: Map[
      NodepoolRecord,
      Map[
        AppRecord,
        (Chain[ServiceRecord],
         Map[String, Chain[String]],
         Map[PersistentDiskRecord, Map[String, Chain[String]]],
         Chain[AppErrorRecord]
        )
      ]
    ]
  ): List[Nodepool] =
    nodepools
      .map { case (nodepoolRec, appMap) =>
        unmarshalNodepool(nodepoolRec, unmarshalAppMap(appMap))
      }
      .toSet
      .toList

  private def unmarshalAppMap(
    apps: Map[
      AppRecord,
      (Chain[ServiceRecord],
       Map[String, Chain[String]],
       Map[PersistentDiskRecord, Map[String, Chain[String]]],
       Chain[AppErrorRecord]
      )
    ]
  ): List[App] =
    apps
      .map { case (appRec, (services, labelMap, diskMap, errors)) =>
        appQuery.unmarshalApp(
          appRec,
          services.map(serviceQuery.unmarshalService).toList.toSet.toList,
          labelMap.view.mapValues(_.toList.toSet.head).toMap,
          // the database ensures we always have a single namespace here
          appRec.namespaceName,
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
  def defaultNodepool: DefaultNodepool
}

final case class ClusterDoesNotExist(minimalCluster: KubernetesCluster, defaultNodepool: DefaultNodepool)
    extends SaveClusterResult

final case class ClusterExists(minimalCluster: KubernetesCluster, defaultNodepool: DefaultNodepool)
    extends SaveClusterResult

final case class GetAppAssertion(msg: String) extends LeoException(msg, StatusCodes.InternalServerError, traceId = None)

final case class KubernetesAppCreationException(msg: String, traceId: Option[TraceId])
    extends LeoException(msg, StatusCodes.Conflict, traceId = traceId)
