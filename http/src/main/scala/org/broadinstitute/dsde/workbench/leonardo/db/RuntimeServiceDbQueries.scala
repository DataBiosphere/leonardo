package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.data.Chain
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.OperationName
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.RuntimeSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeConfigQueries._
import org.broadinstitute.dsde.workbench.leonardo.db.clusterQuery.getRuntimeQueryByUniqueKey
import org.broadinstitute.dsde.workbench.leonardo.http.api.ListRuntimeResponse2
import org.broadinstitute.dsde.workbench.leonardo.http.{DiskConfig, GetRuntimeResponse}
import org.broadinstitute.dsde.workbench.leonardo.model.RuntimeNotFoundException
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName

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

  def getStatusByName(cloudContext: CloudContext,
                      name: RuntimeName)(implicit ec: ExecutionContext): DBIO[Option[RuntimeStatus]] = {
    val res = clusterQuery
      .filter(_.cloudContextDb === cloudContext.asCloudContextDb)
      .filter(_.runtimeName === name)
      .filter(_.destroyedDate === dummyDate)
      .map(_.status)
      .result

    res.map(recs => recs.headOption)
  }

  def getRuntime(cloudContext: CloudContext, runtimeName: RuntimeName)(
    implicit executionContext: ExecutionContext
  ): DBIO[GetRuntimeResponse] = {
    val activeRuntime = getRuntimeQueryByUniqueKey(cloudContext, runtimeName, None)
      .join(runtimeConfigs)
      .on(_._1.runtimeConfigId === _.id)
      .joinLeft(persistentDiskQuery.tableQuery)
      .on { case (a, b) => a._2.persistentDiskId.isDefined && a._2.persistentDiskId === b.id }
    activeRuntime.result.flatMap { recs =>
      val runtimeRecs = recs.map(_._1._1)
      val res = for {
        runtimeConfig <- recs.headOption.map(_._1._2)
        persistentDisk = recs.headOption
          .flatMap(_._2)
          .map(d => persistentDiskQuery.unmarshalPersistentDisk(d, Map.empty))
        runtime <- unmarshalGetRuntime(runtimeRecs,
                                       runtimeConfig.runtimeConfig,
                                       persistentDisk.map(DiskConfig.fromPersistentDisk)).headOption
      } yield runtime
      res.fold[DBIO[GetRuntimeResponse]](
        DBIO.failed(RuntimeNotFoundException(cloudContext, runtimeName, "Not found in database"))
      )(r => DBIO.successful(r))
    }
  }

  def unmarshalGetRuntime(
    clusterRecords: Seq[
      (ClusterRecord,
       Option[ClusterErrorRecord],
       Option[LabelRecord],
       Option[ExtensionRecord],
       Option[ClusterImageRecord],
       Option[ScopeRecord],
       Option[PatchRecord])
    ],
    runtimeConfig: RuntimeConfig,
    diskConfig: Option[DiskConfig]
  ): Seq[GetRuntimeResponse] = {
    // Call foldMap to aggregate a flat sequence of (cluster, instance, label) triples returned by the query
    // to a grouped (cluster -> (instances, labels)) structure.
    // Note we use Chain instead of List inside the foldMap because the Chain monoid is much more efficient than the List monoid.
    // See: https://typelevel.org/cats/datatypes/chain.html
    val clusterRecordMap: Map[ClusterRecord,
                              (Chain[ClusterErrorRecord],
                               Map[String, Chain[String]],
                               Chain[ExtensionRecord],
                               Chain[ClusterImageRecord],
                               Chain[ScopeRecord],
                               Chain[PatchRecord])] = clusterRecords.toList.foldMap {
      case (clusterRecord, errorRecordOpt, labelRecordOpt, extensionOpt, clusterImageOpt, scopeOpt, patchOpt) =>
        val labelMap = labelRecordOpt.map(labelRecordOpt => labelRecordOpt.key -> Chain(labelRecordOpt.value)).toMap
        val errorList = errorRecordOpt.toList
        val extList = extensionOpt.toList
        val clusterImageList = clusterImageOpt.toList
        val scopeList = scopeOpt.toList
        val patchList = patchOpt.toList
        Map(
          clusterRecord -> (Chain.fromSeq(errorList), labelMap, Chain
            .fromSeq(extList), Chain.fromSeq(clusterImageList), Chain.fromSeq(scopeList), Chain.fromSeq(patchList))
        )
    }
    clusterRecordMap.map {
      case (clusterRecord, (errorRecords, labels, extensions, clusterImageRecords, scopes, patch)) =>
        val name = clusterRecord.runtimeName
        val dataprocInfo = (clusterRecord.googleId, clusterRecord.operationName, clusterRecord.stagingBucket).mapN {
          (googleId, operationName, stagingBucket) =>
            AsyncRuntimeFields(googleId,
                               OperationName(operationName),
                               GcsBucketName(stagingBucket),
                               clusterRecord.hostIp map IP)
        }
        val clusterImages = clusterImageRecords.toList map clusterImageQuery.unmarshalClusterImage toSet
        val patchInProgress = patch.headOption match {
          case Some(patchRec) => patchRec.inProgress
          case None           => false
        }

        val labelMap = labels.view.mapValues(_.toList.toSet.head).toMap
        GetRuntimeResponse(
          clusterRecord.id,
          RuntimeSamResourceId(clusterRecord.internalId),
          name,
          clusterRecord.cloudContext,
          clusterRecord.serviceAccountInfo,
          dataprocInfo,
          clusterRecord.auditInfo,
          clusterRecord.kernelFoundBusyDate,
          runtimeConfig,
          Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase,
                              clusterRecord.cloudContext,
                              name,
                              clusterImages,
                              labelMap),
          clusterRecord.status,
          labelMap,
          clusterRecord.userScriptUri,
          clusterRecord.startUserScriptUri,
          errorRecords.toList
            .groupBy(_.timestamp)
            .map(_._2.head)
            .toList map clusterErrorQuery.unmarshallClusterErrorRecord,
          extensionQuery.unmarshallExtensions(extensions.toList),
          clusterRecord.autopauseThreshold != 0,
          clusterRecord.autopauseThreshold,
          clusterRecord.defaultClientId,
          clusterImages,
          scopeQuery.unmarshallScopes(scopes.toList),
          clusterRecord.welderEnabled,
          patchInProgress,
          clusterRecord.customClusterEnvironmentVariables,
          diskConfig
        )
    }.toSeq
  }

  def listRuntimes(labelMap: LabelMap, includeDeleted: Boolean, cloudContext: Option[CloudContext] = None)(
    implicit ec: ExecutionContext
  ): DBIO[List[ListRuntimeResponse2]] = {
    val runtimeQueryFilteredByDeletion =
      if (includeDeleted) clusterQuery else clusterQuery.filterNot(_.status === (RuntimeStatus.Deleted: RuntimeStatus))
    val clusterQueryFilteredByProject = cloudContext.fold(runtimeQueryFilteredByDeletion)(p =>
      runtimeQueryFilteredByDeletion
        .filter(_.cloudContextDb === p.asCloudContextDb)
        .filter(_.cloudProvider === p.cloudProvider)
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
            .filter(lbl =>
              labelMap
                .map { case (k, v) => lbl.key === k && lbl.value === v }
                .fold[Rep[Boolean]](false)(_ || _)
            )
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
          val lmp = labelMap.view.mapValues(_.toList.toSet.headOption.getOrElse("")).toMap

          val patchInProgress = patchRecOpt match {
            case Some(patchRec) => patchRec.inProgress
            case None           => false
          }
          ListRuntimeResponse2(
            runtimeRec.id,
            RuntimeSamResourceId(runtimeRec.internalId),
            runtimeRec.runtimeName,
            runtimeRec.cloudContext,
            runtimeRec.auditInfo,
            runtimeConfig,
            Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase,
                                runtimeRec.cloudContext,
                                runtimeRec.runtimeName,
                                Set.empty,
                                lmp),
            runtimeRec.status,
            lmp,
            patchInProgress
          )
      }.toList
    }
  }
}
