package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.data.Chain
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.RuntimeSamResource
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeConfigQueries._
import org.broadinstitute.dsde.workbench.leonardo.http.service.ListRuntimeResponse
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import scala.concurrent.ExecutionContext

// TODO deprecated in favor of RuntimeServiceDbQueries
// This object is only used by the deprecated LeonardoService
object LeonardoServiceDbQueries {

  type ClusterJoinLabel = Query[(ClusterTable, Rep[Option[LabelTable]]), (ClusterRecord, Option[LabelRecord]), Seq]

  def clusterLabelQuery(baseQuery: Query[ClusterTable, ClusterRecord, Seq]): ClusterJoinLabel =
    for {
      (cluster, label) <- baseQuery.joinLeft(labelQuery).on {
        case (c, lbl) =>
          lbl.resourceId === c.id && lbl.resourceType === LabelResourceType.runtime
      }
    } yield (cluster, label)

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
          labelQuery
            .filter(lbl => lbl.resourceId === clusterRec.id && lbl.resourceType === LabelResourceType.runtime)
            // The following confusing line is equivalent to the much simpler:
            // .filter { lbl => (lbl.key, lbl.value) inSetBind labelMap.toSet }
            // Unfortunately slick doesn't support inSet/inSetBind for tuples.
            // https://github.com/slick/slick/issues/517
            .filter(lbl =>
              labelMap.map { case (k, v) => lbl.key === k && lbl.value === v }.fold[Rep[Boolean]](false)(_ || _)
            )
            .length === labelMap.size
      }
    }

    val clusterQueryFilteredByLabelAndJoinedWithRuntimeAndPatch = runtimeLabelRuntimeConfigQuery(
      clusterQueryFilteredByLabel
    )

    clusterQueryFilteredByLabelAndJoinedWithRuntimeAndPatch.result.map { x =>
      val clusterLabelMap: Map[(ClusterRecord, RuntimeConfig, Option[PatchRecord]), Map[String, Chain[String]]] =
        x.toList.foldMap {
          case (((clusterRec, labelRecOpt), runTimeConfigRec), patchRecOpt) =>
            val labelMap = labelRecOpt.map(labelRec => labelRec.key -> Chain(labelRec.value)).toMap
            Map((clusterRec, runTimeConfigRec.runtimeConfig, patchRecOpt) -> labelMap)
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
            RuntimeSamResource(clusterRec.internalId),
            clusterRec.clusterName,
            clusterRec.googleProject,
            clusterRec.serviceAccountInfo,
            dataprocInfo,
            clusterRec.auditInfo,
            clusterRec.kernelFoundBusyDate,
            runTimeConfigRecOpt,
            Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase,
                                clusterRec.googleProject,
                                clusterRec.clusterName,
                                Set.empty,
                                lmp), //TODO: remove clusterImages field
            RuntimeStatus.withName(clusterRec.status),
            lmp,
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
