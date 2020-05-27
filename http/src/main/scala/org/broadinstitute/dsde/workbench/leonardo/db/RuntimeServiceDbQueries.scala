package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.data.Chain
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.RuntimeSamResource
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeConfigQueries._
import org.broadinstitute.dsde.workbench.leonardo.http.api.ListRuntimeResponse2
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

object RuntimeServiceDbQueries {

  type ClusterJoinLabel = Query[(ClusterTable, Rep[Option[LabelTable]]), (ClusterRecord, Option[LabelRecord]), Seq]

  def getStatusByName(project: GoogleProject,
                      name: RuntimeName)(implicit ec: ExecutionContext): DBIO[Option[RuntimeStatus]] = {
    val res = clusterQuery
      .filter(_.googleProject === project)
      .filter(_.clusterName === name)
      .filter(_.destroyedDate === dummyDate)
      .map(_.status)
      .result

    res.map(recs => recs.headOption.flatMap(x => RuntimeStatus.withNameInsensitiveOption(x)))
  }

  def clusterLabelQuery(baseQuery: Query[ClusterTable, ClusterRecord, Seq]): ClusterJoinLabel =
    for {
      (cluster, label) <- baseQuery.joinLeft(labelQuery).on {
        case (c, lbl) =>
          lbl.resourceId === c.id && lbl.resourceType === LabelResourceType.runtime
      }
    } yield (cluster, label)

  // new runtime route return a lot less fields than legacy listCluster API
  // Once we remove listCluster API, we can optimize this query
  def listClusters(labelMap: LabelMap, includeDeleted: Boolean, googleProjectOpt: Option[GoogleProject] = None)(
    implicit ec: ExecutionContext
  ): DBIO[List[ListRuntimeResponse2]] = {
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

          val patchInProgress = patchRecOpt match {
            case Some(patchRec) => patchRec.inProgress
            case None           => false
          }
          ListRuntimeResponse2(
            clusterRec.id,
            RuntimeSamResource(clusterRec.internalId),
            clusterRec.clusterName,
            clusterRec.googleProject,
            clusterRec.auditInfo,
            runTimeConfigRecOpt
              .getOrElse(
                throw new Exception(s"No runtimeConfig found for cluster with id ${clusterRec.id}")
              ), //In theory, the exception should never happen because it's enforced by db foreign key
            Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase,
                                clusterRec.googleProject,
                                clusterRec.clusterName,
                                Set.empty,
                                lmp),
            RuntimeStatus.withName(clusterRec.status),
            lmp,
            patchInProgress
          )
      }.toList
    }
  }
}
