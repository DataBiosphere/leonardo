package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.data.Chain
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeConfigQueries._
import org.broadinstitute.dsde.workbench.leonardo.db.clusterQuery.{fullClusterQueryByUniqueKey, unmarshalFullCluster}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  GetRuntimeResponse,
  ListRuntimeResponse,
  RuntimeNotFoundException
}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import scala.concurrent.ExecutionContext

object LeonardoServiceDbQueries {

  type ClusterJoinLabel = Query[(ClusterTable, Rep[Option[LabelTable]]), (ClusterRecord, Option[LabelRecord]), Seq]

  def clusterLabelQuery(baseQuery: Query[ClusterTable, ClusterRecord, Seq]): ClusterJoinLabel =
    for {
      (cluster, label) <- baseQuery.joinLeft(labelQuery.runtimeLabels).on(_.id === _.resourceId)
    } yield (cluster, label)

  def getGetClusterResponse(googleProject: GoogleProject, clusterName: RuntimeName)(
    implicit executionContext: ExecutionContext
  ): DBIO[GetRuntimeResponse] = {
    val activeCluster = fullClusterQueryByUniqueKey(googleProject, clusterName, None)
      .joinLeft(runtimeConfigs)
      .on(_._1.runtimeConfigId === _.id)
    activeCluster.result.flatMap { recs =>
      val clusterRecs = recs.map(_._1)
      val res = for {
        cluster <- unmarshalFullCluster(clusterRecs).headOption
        runtimeConfig <- recs.headOption.flatMap(_._2)
      } yield GetRuntimeResponse.fromRuntime(cluster, runtimeConfig.runtimeConfig)
      res.fold[DBIO[GetRuntimeResponse]](DBIO.failed(RuntimeNotFoundException(googleProject, clusterName)))(r =>
        DBIO.successful(r)
      )
    }
  }

  // new runtime route return a lot less fields than legacy listCluster API
  // Once we remove listCluster API, we can optimize this query
  def listClusters(labelMap: LabelMap, includeDeleted: Boolean, googleProjectOpt: Option[GoogleProject] = None)(
    implicit ec: ExecutionContext
  ): DBIO[List[ListRuntimeResponse]] = {
    val clusterQueryFilteredByDeletion =
      if (includeDeleted) clusterQuery else clusterQuery.filterNot(_.status === "Deleted")
    val clusterQueryFilteredByProject = googleProjectOpt.fold(clusterQueryFilteredByDeletion)(p =>
      clusterQueryFilteredByDeletion.filter(_.googleProject === p)
    )
    val clusterQueryJoinedWithLabel = clusterLabelQuery(clusterQueryFilteredByProject)

    val clusterQueryFilteredByLabel = if (labelMap.isEmpty) {
      clusterQueryJoinedWithLabel
    } else {
      clusterQueryJoinedWithLabel.filter {
        case (clusterRec, _) =>
          labelQuery.runtimeLabels
            .filter {
              _.resourceId === clusterRec.id
            }
            // The following confusing line is equivalent to the much simpler:
            // .filter { lbl => (lbl.key, lbl.value) inSetBind labelMap.toSet }
            // Unfortunately slick doesn't support inSet/inSetBind for tuples.
            // https://github.com/slick/slick/issues/517
            .filter(lbl => labelMap.map { case (k, v) => lbl.key === k && lbl.value === v }.reduce(_ || _))
            .length === labelMap.size
      }
    }

    val clusterQueryFilteredByLabelAndJoinedWithRuntimeAndPatch = clusterLabelRuntimeConfigQuery(
      clusterQueryFilteredByLabel
    )

    clusterQueryFilteredByLabelAndJoinedWithRuntimeAndPatch.result.map { x =>
      val clusterLabelMap
        : Map[(ClusterRecord, Option[RuntimeConfig], Option[PatchRecord]), Map[String, Chain[String]]] =
        x.toList.foldMap {
          case (((clusterRec, labelRecOpt), runTimeConfigRecOpt), patchRecOpt) =>
            val labelMap = labelRecOpt.map(labelRec => labelRec.key -> Chain(labelRec.value)).toMap
            Map((clusterRec, runTimeConfigRecOpt.map(_.runtimeConfig), patchRecOpt) -> labelMap)
        }

      clusterLabelMap.map {
        case ((clusterRec, runTimeConfigRecOpt, patchRecOpt), labelMap) =>
          val lmp = labelMap.mapValues(_.toList.toSet.headOption.getOrElse(""))
          val dataprocInfo = (clusterRec.googleId, clusterRec.operationName, clusterRec.stagingBucket).mapN {
            (googleId, operationName, stagingBucket) =>
              AsyncRuntimeFields(googleId,
                                 OperationName(operationName),
                                 GcsBucketName(stagingBucket),
                                 clusterRec.hostIp map IP)
          }

          val patchInProgress = patchRecOpt match {
            case Some(patchRec) => patchRec.inProgress
            case None           => false
          }
          ListRuntimeResponse(
            clusterRec.id,
            RuntimeInternalId(clusterRec.internalId),
            clusterRec.clusterName,
            clusterRec.googleProject,
            clusterRec.serviceAccountInfo,
            dataprocInfo,
            clusterRec.auditInfo,
            clusterRec.kernelFoundBusyDate,
            runTimeConfigRecOpt
              .getOrElse(
                throw new Exception(s"No runtimeConfig found for cluster with id ${clusterRec.id}")
              ), //In theory, the exception should never happen because it's enforced by db foreign key
            Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase,
                                clusterRec.googleProject,
                                clusterRec.clusterName,
                                Set.empty,
                                lmp), //TODO: remove clusterImages field
            RuntimeStatus.withName(clusterRec.status),
            lmp,
            clusterRec.jupyterExtensionUri,
            clusterRec.jupyterUserScriptUri,
            Set.empty, //TODO: remove instances from ListResponse
            clusterRec.autopauseThreshold,
            clusterRec.defaultClientId,
            clusterRec.stopAfterCreation,
            clusterRec.welderEnabled,
            patchInProgress
          )
      }.toList
    }
  }
}
