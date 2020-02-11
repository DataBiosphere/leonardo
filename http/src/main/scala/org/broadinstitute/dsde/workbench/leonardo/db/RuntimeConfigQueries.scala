package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant

import cats.data.Chain
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.http.service.ListClusterResponse
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, DataprocInfo}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterStatus, IP, OperationName}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import cats.implicits._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent.ExecutionContext

object RuntimeConfigQueries {
  type ClusterJoinLabelJoinRuntimeConfig = Query[((ClusterTable, Rep[Option[LabelTable]]), Rep[Option[RuntimeConfigTable]]), ((ClusterRecord, Option[LabelRecord]), Option[RuntimeConfigRecord]), Seq]

  val runtimeConfigs = TableQuery[RuntimeConfigTable]

  val clusterLabelRuntimeConfigQuery: ClusterJoinLabelJoinRuntimeConfig = {
    for {
      (cluster, label) <- clusterQuery
        .joinLeft(labelQuery).on(_.id === _.clusterId)
        .joinLeft(runtimeConfigs).on(_._1.runtimeConfigId === _.id)
    } yield (cluster, label)
  }

  /**
    * return DB generated id
   */
  def insertRuntime(runtimeConfig: RuntimeConfig, dateAccessed: Instant): DBIO[RuntimeConfigId] =  {
    runtimeConfigs.returning(runtimeConfigs.map(_.id)) += RuntimeConfigRecord(RuntimeConfigId(0), runtimeConfig, dateAccessed)
  }

  def getRuntime(id: RuntimeConfigId)(implicit ec: ExecutionContext): DBIO[RuntimeConfig] =  {
    runtimeConfigs.filter(x => x.id === id).result.flatMap {
      x =>
        val res = x.headOption.map {
          x =>
            x.runtimeConfig
        }
        res.fold[DBIO[RuntimeConfig]](DBIO.failed(new Exception(s"no runtimeConfig found for ${id}")))(x => DBIO.successful(x))
    }
  }

  def getClustersByLabelsWithRuntimeConfig(labelMap: LabelMap, includeDeleted: Boolean, googleProjectOpt: Option[GoogleProject] = None)(implicit ec: ExecutionContext): DBIO[List[ListClusterResponse]] = {
    val clusterStatusQuery =
      if (includeDeleted) clusterLabelRuntimeConfigQuery else {
        val clusterTableQuery = clusterQuery.filterNot(_.status === "Deleted")
        clusterTableQuery.joinLeft(labelQuery).on(_.id === _.clusterId)
          .joinLeft(runtimeConfigs).on(_._1.runtimeConfigId === _.id)
      }

    val clusterStatusQueryByProject = googleProjectOpt match {
      case Some(googleProject) => clusterStatusQuery.filter {
        _._1._1.googleProject === googleProject
      }
      case None => clusterStatusQuery
    }

    val res = if (labelMap.isEmpty) {
      clusterStatusQueryByProject
    } else {
      clusterStatusQueryByProject.filter {
        case (clusterAndLabel, _) =>
          labelQuery
            .filter {
              _.clusterId === clusterAndLabel._1.id
            }
            // The following confusing line is equivalent to the much simpler:
            // .filter { lbl => (lbl.key, lbl.value) inSetBind labelMap.toSet }
            // Unfortunately slick doesn't support inSet/inSetBind for tuples.
            // https://github.com/slick/slick/issues/517
            .filter { lbl =>
              labelMap.map { case (k, v) => lbl.key === k && lbl.value === v }.reduce(_ || _)
            }
            .length === labelMap.size
      }
    }

    res.result.map {
      x =>
        val clusterLabelMap: Map[(ClusterRecord, Option[RuntimeConfig]), Map[String, Chain[String]]] = x.toList.foldMap {
          case (clusterRecordAndLabel, runTimeConfig) =>
            val labelMap = clusterRecordAndLabel._2.map(labelRecord => labelRecord.key -> Chain(labelRecord.value)).toMap
            Map((clusterRecordAndLabel._1, runTimeConfig.map(_.runtimeConfig)) -> labelMap)
        }

        clusterLabelMap.map {
          case (clusterAndRuntimeConfig, labelMp) =>
            val lmp = labelMp.mapValues(_.toList.toSet.head)
            val dataprocInfo = (clusterAndRuntimeConfig._1.googleId, clusterAndRuntimeConfig._1.operationName, clusterAndRuntimeConfig._1.stagingBucket).mapN {
              (googleId, operationName, stagingBucket) =>
                DataprocInfo(googleId, OperationName(operationName), GcsBucketName(stagingBucket), clusterAndRuntimeConfig._1.hostIp map IP)
            }

            val serviceAccountInfo = ServiceAccountInfo(
              clusterAndRuntimeConfig._1.serviceAccountInfo.clusterServiceAccount.map(WorkbenchEmail),
              clusterAndRuntimeConfig._1.serviceAccountInfo.notebookServiceAccount.map(WorkbenchEmail)
            )
            ListClusterResponse(
              clusterAndRuntimeConfig._1.id,
              ClusterInternalId(clusterAndRuntimeConfig._1.internalId),
              clusterAndRuntimeConfig._1.clusterName,
              clusterAndRuntimeConfig._1.googleProject,
              serviceAccountInfo,
              dataprocInfo,
              clusterAndRuntimeConfig._1.auditInfo,
              clusterAndRuntimeConfig._2.getOrElse(throw new Exception(s"no runtimeConfig found for ${clusterAndRuntimeConfig._1.id}")), //In theory, the exeception should never happen because it's enforced by db foreign key
              Cluster.getClusterUrl(clusterAndRuntimeConfig._1.googleProject, clusterAndRuntimeConfig._1.clusterName, Set.empty, lmp), //TODO: remove clusterImages field
              ClusterStatus.withName(clusterAndRuntimeConfig._1.status),
              lmp,
              clusterAndRuntimeConfig._1.jupyterExtensionUri,
              clusterAndRuntimeConfig._1.jupyterUserScriptUri,
              Set.empty, //TODO: remove instances from ListResponse
              clusterAndRuntimeConfig._1.autopauseThreshold,
              clusterAndRuntimeConfig._1.defaultClientId,
              clusterAndRuntimeConfig._1.stopAfterCreation,
              clusterAndRuntimeConfig._1.welderEnabled
            )
        }
          .toList
    }
  }

  def updateNumberOfWorkers(id: RuntimeConfigId, numberOfWorkers: Int, dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs.filter(x => x.id === id)
      .map(c => (c.numberOfWorkers, c.dateAccessed))
      .update((numberOfWorkers, dateAccessed))

  def updateNumberOfPreemptibleWorkers(id: RuntimeConfigId,
                                       numberOfPreemptibleWorkers: Option[Int],
                                       dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs.filter(x => x.id === id)
      .map(c => (c.numberOfPreemptibleWorkers, c.dateAccessed))
      .update((numberOfPreemptibleWorkers, dateAccessed))

  def updateMachineType(id: RuntimeConfigId, machineType: MachineType, dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs.filter(x => x.id === id)
      .map(c => (c.machineType, c.dateAccessed))
      .update((machineType, dateAccessed))

  def updateDiskSize(id: RuntimeConfigId, newSizeGb: Int, dateAccessed: Instant): DBIO[Int] =
    runtimeConfigs.filter(x => x.id === id)
      .map(c => (c.diskSize, c.dateAccessed))
      .update((newSizeGb, dateAccessed))
}