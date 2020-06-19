package org.broadinstitute.dsde.workbench.leonardo
package db

import org.broadinstitute.dsde.workbench.leonardo.db.kubernetesClusterQuery.unmarshalKubernetesCluster
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import cats.data.Chain
import cats.implicits._
import LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.leonardo.db.nodepoolQuery.unmarshalNodepool

object KubernetesServiceDbQueries {
  //the 'full' here means that all joins possible are done, meaning all fields in the cluster, nodepool, and app will be present
  //if you just need the cluster, nodepools, and cluster-wide namespaces, see the minimal join in the KubernetesClusterComponent
  def listFullApps(googleProject: Option[GoogleProject],
                   labelFilter: LabelMap = Map(),
                   includeDeleted: Boolean = false)(implicit ec: ExecutionContext): DBIO[List[KubernetesCluster]] =
    joinFullAppAndUnmarshal(
      listByProject(googleProject, includeDeleted),
      includeDeleted match {
        case true  => nodepoolQuery
        case false => nodepoolQuery.filter(_.destroyedDate === dummyDate)
      },
      includeDeleted match {
        case true  => appQuery
        case false => appQuery.filter(_.destroyedDate === dummyDate)
      },
      labelFilter
    )

  // this is intended to be called first by any kubernetes /app endpoints to enforce one cluster per project
  // an error is thrown if the cluster is creating, due to the fact that we do not allow apps to be created while the cluster for that project is creating, since that means another app is already queued and waiting on this cluster
  // if the cluster already exists, this is a no-op
  // if the cluster does not exist, this also saves a dummy nodepool with the cluster.
  // For more info on dummy nodepool, See: https://broadworkbench.atlassian.net/wiki/spaces/IA/pages/492175377/2020-05-29+Dummy+Nodepool+for+Galaxy
  def saveOrGetForApp(
    saveKubernetesCluster: SaveKubernetesCluster
  )(implicit ec: ExecutionContext): DBIO[SaveClusterResult] =
    for {
      clusterOpt <- kubernetesClusterQuery.getMinimalActiveClusterByName(saveKubernetesCluster.googleProject)

      failure = DBIO.failed(
        KubernetesAppCreationException(
          s"You cannot create an app while a cluster is in ${KubernetesClusterStatus.Precreating} or ${KubernetesClusterStatus.Provisioning}"
        )
      )
      eitherClusterOrError <- clusterOpt match {
        case Some(cluster) =>
          cluster.status match {
            case KubernetesClusterStatus.Precreating  => failure
            case KubernetesClusterStatus.Provisioning => failure
            case _                                    => DBIO.successful(SaveClusterResult(cluster, true))
          }
        case _ => kubernetesClusterQuery.save(saveKubernetesCluster).map(c => SaveClusterResult(c, false))
      }
    } yield eitherClusterOrError

  def getActiveFullAppByName(googleProject: GoogleProject, appName: AppName, labelFilter: LabelMap = Map())(
    implicit ec: ExecutionContext
  ): DBIO[Option[GetAppResult]] =
    for {
      clusters <- joinFullAppAndUnmarshal(listByProject(Some(googleProject)),
                                          nodepoolQuery.filter(_.destroyedDate === dummyDate),
                                          appQuery.findActiveByNameQuery(appName),
                                          labelFilter)
      validateClusters <- if (clusters.length > 1)
        DBIO.failed(
          GetAppAssertion(
            "(GoogleProject, AppName) uniqueness has been violated, clusters returned > 1 by getActiveFullApp"
          )
        )
      else DBIO.successful(0)
      validatedApp <- clusters.headOption.fold[DBIO[Option[GetAppResult]]](
        DBIO.successful(None)
      ) { cluster =>
        for {
          _ <- if (cluster.nodepools.length != 1)
            DBIO.failed(
              GetAppAssertion(
                "(GoogleProject, AppName) uniqueness has been violated, num nodepools returned != 1 by getActiveFullApp"
              )
            )
          else DBIO.successful(0)
          _ <- if (cluster.nodepools.head.apps.length != 1)
            DBIO.failed(
              GetAppAssertion(
                "(GoogleProject, AppName) uniqueness has been violated, num apps returned != 1 by getActiveFullApp"
              )
            )
          else DBIO.successful(0)
          appResult <- DBIO.successful(
            Some(
              GetAppResult(
                //we are guaranteed exactly one app and nodepool here if the query doesn't return None due to the way the query JOINs and database unique enforcement
                //we explicitly enforce this above as well
                cluster,
                cluster.nodepools.head,
                cluster.nodepools.head.apps.head
              )
            )
          )
        } yield appResult
      }
    } yield validatedApp

  //if there are no apps associated with this cluster, None will be returned even if the cluster exists.
  //see the `Minimal` methods if you wish to get the cluster itself
  def getFullClusterById(id: KubernetesClusterLeoId)(implicit ec: ExecutionContext): DBIO[Option[KubernetesCluster]] =
    joinFullAppAndUnmarshal(kubernetesClusterQuery.findByIdQuery(id), nodepoolQuery, appQuery)
      .map(_.headOption)

  def isDiskAttached(disk: PersistentDisk)(implicit ec: ExecutionContext): DBIO[Boolean] =
    appQuery.filter(x => x.diskId.isDefined && x.diskId === disk.id).length.result.map(_ > 0)

  private[db] def listByProject(
    googleProject: Option[GoogleProject],
    includeDeleted: Boolean = false
  ): Query[KubernetesClusterTable, KubernetesClusterRecord, Seq] = {
    val initialQuery =
      googleProject match {
        case Some(project) => kubernetesClusterQuery.filter(_.googleProject === project)
        case None          => kubernetesClusterQuery
      }

    includeDeleted match {
      case false => initialQuery.filter(_.destroyedDate === dummyDate)
      case true  => initialQuery
    }
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
                              labelFilter: LabelMap) = {
    val query = for {
      (((((((((app, nodepool), cluster), label), clusterNamespaces), appNamespace), services), ports), disks),
       diskLabels) <- baseAppQuery join
        baseNodepoolQuery on (_.nodepoolId === _.id) join
        baseClusterQuery on (_._2.clusterId === _.id) joinLeft
        labelQuery on {
        case (c, lbl) =>
          val resourceTypeFilter = lbl.resourceId
            .mapTo[AppId] === c._1._1.id && lbl.resourceType === LabelResourceType.app
          resourceTypeFilter
      } joinLeft
        namespaceQuery on (_._1._2.id === _.clusterId) join
        namespaceQuery on (_._1._1._1._1.namespaceId === _.id) joinLeft
        serviceQuery on (_._1._1._1._1._1.id === _.appId) joinLeft
        portQuery on (_._2.map(_.id) === _.serviceId) joinLeft
        persistentDiskQuery on (_._1._1._1._1._1._1._1.diskId === _.id) joinLeft
        labelQuery on {
        case (c, lbl) =>
          lbl.resourceId
            .mapTo[DiskId] === c._1._1._1._1._1._1._1._1.diskId && lbl.resourceType === LabelResourceType.persistentDisk
      }
    } yield (cluster, nodepool, app, label, clusterNamespaces, appNamespace, services, ports, disks, diskLabels)

    query.filter {
      case (_, _, app, _, _, _, _, _, _, _) =>
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
       Option[PortRecord],
       Option[PersistentDiskRecord],
       Option[LabelRecord])
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
            portRecord,
            diskRecordOpt,
            diskLabelOpt) =>
        val serviceMap = serviceRecord.map(s => Map(s -> Chain.fromSeq(portRecord.toList))).getOrElse(Map.empty)
        val labelMap = labelRecordOpt.map(labelRecord => labelRecord.key -> Chain(labelRecord.value)).toMap
        val diskLabelMap = diskLabelOpt.map(labelRecord => labelRecord.key -> Chain(labelRecord.value)).toMap
        val diskMap = diskRecordOpt.fold[Map[PersistentDiskRecord, Map[String, Chain[String]]]](Map())(disk =>
          Map(disk -> diskLabelMap)
        )
        val appMap = Map(appRecord -> (serviceMap, Chain(appNamespaceRecord), labelMap, diskMap))
        val nodepoolMap = Map(nodepoolRecord -> appMap)

        Map(clusterRecord -> (nodepoolMap, clusterNamespaceRecordOpt.toList))
    }

    map.map {
      case (clusterRec, (nodepoolMap, clusterNamespaces)) =>
        unmarshalKubernetesCluster(
          clusterRec,
          unmarshalNodepoolMap(nodepoolMap),
          clusterNamespaces.toSet[NamespaceRecord].map(rec => Namespace(rec.id, rec.namespaceName)).toList
        )
    }.toSeq
  }

  private def unmarshalNodepoolMap(
    nodepools: Map[NodepoolRecord, Map[AppRecord,
                                       (Map[ServiceRecord, Chain[PortRecord]],
                                        Chain[NamespaceRecord],
                                        Map[String, Chain[String]],
                                        Map[PersistentDiskRecord, Map[String, Chain[String]]])]]
  ): List[Nodepool] =
    nodepools
      .map {
        case (nodepoolRec, appMap) =>
          unmarshalNodepool(nodepoolRec, unmarshalAppMap(appMap))
      }
      .toSet
      .toList

  private def unmarshalAppMap(
    apps: Map[AppRecord,
              (Map[ServiceRecord, Chain[PortRecord]],
               Chain[NamespaceRecord],
               Map[String, Chain[String]],
               Map[PersistentDiskRecord, Map[String, Chain[String]]])]
  ): List[App] =
    apps
      .map {
        case (appRec, (serviceMap, namespaces, labelMap, diskMap)) =>
          appQuery.unmarshalApp(
            appRec,
            unmarshalServiceMap(serviceMap),
            labelMap.mapValues(_.toList.toSet.head),
            //the database ensures we always have a single namespace here
            namespaceQuery.unmarshalNamespace(namespaces.headOption.get),
            unmarshalDiskMap(diskMap)
          )
      }
      .toSet
      .toList

  private def unmarshalDiskMap(disks: Map[PersistentDiskRecord, Map[String, Chain[String]]]): Option[PersistentDisk] =
    disks.map {
      case (disk, labels) =>
        persistentDiskQuery.unmarshalPersistentDisk(disk, labels.mapValues(_.toList.toSet.head))
    }.headOption

  private def unmarshalServiceMap(services: Map[ServiceRecord, Chain[PortRecord]]): List[KubernetesService] =
    services
      .map {
        case (serviceRec, portRecs) =>
          serviceQuery.unmarshalService(serviceRec, portRecs.toList.toSet.toList)
      }
      .toSet
      .toList
}

//minimal cluster has the nodepools, but no namespaces or apps
case class SaveClusterResult(minimalCluster: KubernetesCluster, doesActiveClusterExist: Boolean)
case class GetAppResult(cluster: KubernetesCluster, nodepool: Nodepool, app: App)
case class GetAppAssertion(message: String) extends Exception
