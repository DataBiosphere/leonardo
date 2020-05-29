package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.data.Chain
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.RuntimeSamResource
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeConfigQueries._
import org.broadinstitute.dsde.workbench.leonardo.db.clusterQuery.{fullClusterQueryByUniqueKey, unmarshalFullCluster}
import org.broadinstitute.dsde.workbench.leonardo.http.api.{DiskConfig, ListRuntimeResponse2}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{GetRuntimeResponse, RuntimeNotFoundException}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

object RuntimeServiceDbQueries {

  type RuntimeJoinLabel = Query[(ClusterTable, Rep[Option[LabelTable]]), (ClusterRecord, Option[LabelRecord]), Seq]

  def runtimeLabelQuery(baseQuery: Query[ClusterTable, ClusterRecord, Seq]): RuntimeJoinLabel =
    for {
      (runtime, label) <- baseQuery.joinLeft(labelQuery).on {
        case (r, lbl) =>
          lbl.resourceId === r.id && lbl.resourceType === LabelResourceType.runtime
      }
    } yield (runtime, label)

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

  def getRuntime(googleProject: GoogleProject, runtimeName: RuntimeName)(
    implicit executionContext: ExecutionContext
  ): DBIO[GetRuntimeResponse] = {
    val activeRuntime = fullClusterQueryByUniqueKey(googleProject, runtimeName, None)
      .join(runtimeConfigs)
      .on(_._1.runtimeConfigId === _.id)
      .joinLeft(persistentDiskQuery)
      .on { case (a, b) => a._1._1.persistentDiskId.isDefined && a._1._1.persistentDiskId === b.id }
    activeRuntime.result.flatMap { recs =>
      val runtimeRecs = recs.map(_._1._1)
      val res = for {
        runtime <- unmarshalFullCluster(runtimeRecs).headOption
        runtimeConfig <- recs.headOption.map(_._1._2)
        persistentDisk = recs.headOption
          .flatMap(_._2)
          .map(d => persistentDiskQuery.unmarshalPersistentDisk(d, Map.empty))
      } yield GetRuntimeResponse.fromRuntime(runtime,
                                             runtimeConfig.runtimeConfig,
                                             persistentDisk.map(DiskConfig.fromPersistentDisk))
      res.fold[DBIO[GetRuntimeResponse]](
        DBIO.failed(RuntimeNotFoundException(googleProject, runtimeName, "Not found in database"))
      )(r => DBIO.successful(r))
    }
  }

  def listRuntimes(labelMap: LabelMap, includeDeleted: Boolean, googleProjectOpt: Option[GoogleProject] = None)(
    implicit ec: ExecutionContext
  ): DBIO[List[ListRuntimeResponse2]] = {
    val runtimeQueryFilteredByDeletion =
      if (includeDeleted) clusterQuery else clusterQuery.filterNot(_.status === "Deleted")
    val clusterQueryFilteredByProject = googleProjectOpt.fold(runtimeQueryFilteredByDeletion)(p =>
      runtimeQueryFilteredByDeletion.filter(_.googleProject === p)
    )
    val runtimeQueryJoinedWithLabel = runtimeLabelQuery(clusterQueryFilteredByProject)

    val runtimeQueryFilteredByLabel = if (labelMap.isEmpty) {
      runtimeQueryJoinedWithLabel
    } else {
      runtimeQueryJoinedWithLabel.filter {
        case (runtimeRec, _) =>
          labelQuery
            .filter(lbl => lbl.resourceId === runtimeRec.id && lbl.resourceType === LabelResourceType.runtime)
            // The following confusing line is equivalent to the much simpler:
            // .filter { lbl => (lbl.key, lbl.value) inSetBind labelMap.toSet }
            // Unfortunately slick doesn't support inSet/inSetBind for tuples.
            // https://github.com/slick/slick/issues/517
            .filter(lbl => labelMap.map { case (k, v) => lbl.key === k && lbl.value === v }.reduce(_ || _))
            .length === labelMap.size
      }
    }

    val runtimeQueryFilteredByLabelAndJoinedWithRuntimeAndPatch = runtimeLabelRuntimeConfigQuery(
      runtimeQueryFilteredByLabel
    )

    runtimeQueryFilteredByLabelAndJoinedWithRuntimeAndPatch.result.map { x =>
      val runtimeLabelMap: Map[(ClusterRecord, RuntimeConfig, Option[PatchRecord]), Map[String, Chain[String]]] =
        x.toList.foldMap {
          case (((runtimeRec, labelRecOpt), runtimeConfigRec), patchRecOpt) =>
            val labelMap = labelRecOpt.map(labelRec => labelRec.key -> Chain(labelRec.value)).toMap
            Map((runtimeRec, runtimeConfigRec.runtimeConfig, patchRecOpt) -> labelMap)
        }

      runtimeLabelMap.map {
        case ((runtimeRec, runtimeConfig, patchRecOpt), labelMap) =>
          val lmp = labelMap.mapValues(_.toList.toSet.headOption.getOrElse(""))

          val patchInProgress = patchRecOpt match {
            case Some(patchRec) => patchRec.inProgress
            case None           => false
          }
          ListRuntimeResponse2(
            runtimeRec.id,
            RuntimeSamResource(runtimeRec.internalId),
            runtimeRec.clusterName,
            runtimeRec.googleProject,
            runtimeRec.auditInfo,
            runtimeConfig,
            Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase,
                                runtimeRec.googleProject,
                                runtimeRec.clusterName,
                                Set.empty,
                                lmp),
            RuntimeStatus.withName(runtimeRec.status),
            lmp,
            patchInProgress
          )
      }.toList
    }
  }

  def isDiskAttachedToRuntime(disk: PersistentDisk)(implicit ec: ExecutionContext): DBIO[Boolean] =
    clusterQuery.filter(x => x.persistentDiskId.isDefined && x.persistentDiskId === disk.id).length.result.map(_ > 0)
}
