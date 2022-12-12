package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.data.Chain
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.OperationName
import org.broadinstitute.dsde.workbench.leonardo.Runtime
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.RuntimeSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.GetResultInstances._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeConfigQueries._
import org.broadinstitute.dsde.workbench.leonardo.db.clusterQuery.{
  getActiveRuntimeQueryByWorkspaceId,
  getRuntimeQueryByUniqueKey
}
import org.broadinstitute.dsde.workbench.leonardo.http.{DiskConfig, GetRuntimeResponse, ListRuntimeResponse2}
import org.broadinstitute.dsde.workbench.leonardo.model.{
  RuntimeNotFoundByWorkspaceIdException,
  RuntimeNotFoundException
}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import slick.jdbc.GetResult

import scala.concurrent.ExecutionContext

object RuntimeServiceDbQueries {

  implicit val getResultListRuntimeResponse2 = GetResult { r =>
    val id = r.<<[Long]
    val workspaceId = r.<<?[WorkspaceId]
    val runtimeName = r.<<[RuntimeName]
    val cloudContext = r.<<[CloudContext]
    val ip = r.<<[Option[IP]]
    val auditInfo = r.<<[AuditInfo]
    val status = r.<<[RuntimeStatus]
    val samId = r.<<[RuntimeSamResourceId]
    val runtimeConfig = r.<<[RuntimeConfig]
    val labelMap = r.<<[LabelMap]
    val proxyUrl =
      Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase, cloudContext, runtimeName, Set.empty, ip, labelMap)

    ListRuntimeResponse2(
      id,
      workspaceId,
      samId,
      runtimeName,
      cloudContext,
      auditInfo,
      runtimeConfig,
      proxyUrl,
      status,
      labelMap,
      r.nextBoolean()
    )
  }

  type RuntimeJoinLabel = Query[(ClusterTable, Rep[Option[LabelTable]]), (ClusterRecord, Option[LabelRecord]), Seq]

  def runtimeLabelQuery(baseQuery: Query[ClusterTable, ClusterRecord, Seq]): RuntimeJoinLabel =
    for {
      (runtime, label) <- baseQuery.joinLeft(labelQuery).on { case (r, lbl) =>
        lbl.resourceId === r.id && lbl.resourceType === LabelResourceType.runtime
      }
    } yield (runtime, label)

  def getStatusByName(cloudContext: CloudContext, name: RuntimeName)(implicit
    ec: ExecutionContext
  ): DBIO[Option[RuntimeStatus]] = {
    val res = clusterQuery
      .filter(_.cloudContextDb === cloudContext.asCloudContextDb)
      .filter(_.runtimeName === name)
      .filter(_.destroyedDate === dummyDate)
      .map(_.status)
      .result

    res.map(recs => recs.headOption)
  }

  def getRuntime(cloudContext: CloudContext, runtimeName: RuntimeName)(implicit
    executionContext: ExecutionContext
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
                                       persistentDisk.map(DiskConfig.fromPersistentDisk)
        ).headOption
      } yield runtime
      res.fold[DBIO[GetRuntimeResponse]](
        DBIO.failed(RuntimeNotFoundException(cloudContext, runtimeName, "Not found in database"))
      )(r => DBIO.successful(r))
    }
  }

  def getActiveRuntime(workspaceId: WorkspaceId, runtimeName: RuntimeName)(implicit
    executionContext: ExecutionContext
  ): DBIO[GetRuntimeResponse] = {
    val activeRuntime = getActiveRuntimeQueryByWorkspaceId(workspaceId, runtimeName)
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
                                       persistentDisk.map(DiskConfig.fromPersistentDisk)
        ).headOption
      } yield runtime
      res.fold[DBIO[GetRuntimeResponse]](
        DBIO.failed(RuntimeNotFoundByWorkspaceIdException(workspaceId, runtimeName, "Not found in database"))
      )(r => DBIO.successful(r))
    }
  }

  def getActiveRuntimeRecord(workspaceId: WorkspaceId, runtimeName: RuntimeName)(implicit
    executionContext: ExecutionContext
  ): DBIO[ClusterRecord] = {
    val activeRuntime = clusterQuery
      .filterOpt(Some(workspaceId))(_.workspaceId === _)
      .filter(_.runtimeName === runtimeName)
      .filter(_.destroyedDate === dummyDate)

    activeRuntime.result.flatMap(rec =>
      rec.headOption match {
        case Some(value) => DBIO.successful(value)
        case None =>
          DBIO.failed(RuntimeNotFoundByWorkspaceIdException(workspaceId, runtimeName, "Not found in database"))
      }
    )
  }

  def unmarshalGetRuntime(
    clusterRecords: Seq[
      (ClusterRecord,
       Option[ClusterErrorRecord],
       Option[LabelRecord],
       Option[ExtensionRecord],
       Option[ClusterImageRecord],
       Option[ScopeRecord],
       Option[PatchRecord]
      )
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
                               Chain[PatchRecord]
                              )
    ] = clusterRecords.toList.foldMap {
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
                               clusterRecord.hostIp
            )
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
                              clusterRecord.hostIp,
                              labelMap
          ),
          clusterRecord.status,
          labelMap,
          clusterRecord.userScriptUri,
          clusterRecord.startUserScriptUri,
          errorRecords.toList
            .groupBy(_.timestamp)
            .map(_._2.head)
            .toList map clusterErrorQuery.unmarshallClusterErrorRecord,
          extensionQuery.unmarshallExtensions(extensions.toList),
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

  def listRuntimes(labelMap: LabelMap, includeDeleted: Boolean, cloudContext: Option[CloudContext] = None)(implicit
    ec: ExecutionContext
  ): DBIO[List[ListRuntimeResponse2]] = {
    val runtimeQueryFilteredByDeletion =
      if (includeDeleted) clusterQuery else clusterQuery.filterNot(_.status === (RuntimeStatus.Deleted: RuntimeStatus))
    val clusterQueryFilteredByProject = cloudContext.fold(runtimeQueryFilteredByDeletion)(p =>
      runtimeQueryFilteredByDeletion
        .filter(_.cloudContextDb === p.asCloudContextDb)
        .filter(_.cloudProvider === p.cloudProvider)
    )

    joinAndFilterByLabelForList(labelMap, clusterQueryFilteredByProject)
  }

  def listRuntimesForWorkspace(labelMap: LabelMap,
                               includeDeleted: Boolean,
                               creatorOnly: Option[WorkbenchEmail],
                               workspaceId: Option[WorkspaceId],
                               cloudProvider: Option[CloudProvider]
  ): DBIO[Vector[ListRuntimeResponse2]] = {
    val runtimeQueryFilteredByCreator = creatorOnly match {
      case Some(creator) => List(s"C.`creator` = '${creator.value}'")
      case None          => List.empty
    }
    val runtimeQueryFilteredByDeletion =
      if (includeDeleted) List.empty
      else List("C.`status` != 'Deleted'")

    val runtimeQueryFilteredByWorkspace = workspaceId match {
      case Some(wid) =>
        List(s"C.`workspaceId` = '${wid.value.toString}'")
      case None => List.empty
    }

    val runtimeQueryFilteredByProvider = cloudProvider match {
      case Some(provider) =>
        List(s"C.`cloudProvider` = '${provider.asString}'")
      case None => List.empty
    }

    val clusterFilters =
      (runtimeQueryFilteredByCreator ++ runtimeQueryFilteredByDeletion ++ runtimeQueryFilteredByWorkspace ++ runtimeQueryFilteredByProvider)
        .mkString(" AND ")

    val clusterFiltersFinal = if (clusterFilters.isEmpty) "" else s"where ${clusterFilters}"

    val labelMapFilters =
      if (labelMap.isEmpty)
        ""
      else {
        val query = labelMap
          .map { case (k, v) =>
            s"(LABEL.key = '${k}' and LABEL.value = '${v}')"
          }
          .mkString(" or ")

        s"""where (
           |   select 
           |     count(1) 
           |   from 
           |     `LABEL` 
           |   where 
           |     (`resourceId` = FILTERED_CLUSTER.id) 
           |     AND (`resourceType` = 'runtime')
           |     AND (${query})
           |    ) = ${labelMap.size}""".stripMargin
      }

    val sqlStatement =
      sql"""
         select  
          LABEL_FILTERED.`id`, 
          LABEL_FILTERED.`workspaceId`, 
          LABEL_FILTERED.`runtimeName`, 
          LABEL_FILTERED.`cloudProvider`, 
          LABEL_FILTERED.`cloudContext`,
          LABEL_FILTERED.`hostIp`,
          LABEL_FILTERED.`creator`,
          LABEL_FILTERED.`createdDate`,
          LABEL_FILTERED.`destroyedDate`,
          LABEL_FILTERED.`dateAccessed`,
          LABEL_FILTERED.`status`,
          LABEL_FILTERED.`internalId`,
          RG.`cloudService`, 
          RG.`numberOfWorkers`, 
          RG.`machineType`, 
          RG.`diskSize`, 
          RG.`bootDiskSize`, 
          RG.`workerMachineType`, 
          RG.`workerDiskSize`, 
          RG.`numberOfWorkerLocalSSDs`, 
          RG.`numberOfPreemptibleWorkers`, 
          RG.`dataprocProperties`, 
          RG.`persistentDiskId`, 
          RG.`zone`, 
          RG.`region`, 
          RG.`gpuType`, 
          RG.`numOfGpus`, 
          RG.`componentGatewayEnabled`, 
          RG.`workerPrivateAccess`,
          GROUP_CONCAT(labelKey) labelKeys, 
          GROUP_CONCAT(labelValue) labelValues,
          CP.`inProgress`
        from 
          (
            select
              `id`,
              `status`, 
              `cloudContext`, 
              `serviceAccount`, 
              `dateAccessed`, 
              `createdDate`, 
              `deletedFrom`, 
              `destroyedDate`, 
              `autopauseThreshold`, 
              `hostIp`, 
              `internalId`,
              `workspaceId`, 
              `cloudProvider`, 
              `runtimeName`, 
              `kernelFoundBusyDate`, 
              `creator`, 
              `proxyHostName`,
              `runtimeConfigId`,
              L.`key` as labelKey,
              L.`value` as labelValue
            from 
              (
                select
                  C.`status`, 
                  C.`cloudContext`, 
                  C.`serviceAccount`, 
                  C.`dateAccessed`, 
                  C.`createdDate`, 
                  C.`deletedFrom`, 
                  C.`destroyedDate`, 
                  C.`autopauseThreshold`, 
                  C.`hostIp`,
                  C.`internalId`, 
                  C.`workspaceId`, 
                  C.`cloudProvider`, 
                  C.`id`, 
                  C.`runtimeName`, 
                  C.`kernelFoundBusyDate`, 
                  C.`creator`, 
                  C.`proxyHostName`,
                  C.`runtimeConfigId`
                from 
                  `CLUSTER` AS C
                #${clusterFiltersFinal}
              ) AS FILTERED_CLUSTER 
              left join `LABEL` L on (L.`resourceId` = FILTERED_CLUSTER.id) 
              and (L.`resourceType` = 'runtime') 
              #${labelMapFilters}
          ) AS LABEL_FILTERED 
          inner join `RUNTIME_CONFIG` RG on LABEL_FILTERED.id = RG.`id` 
          left join `CLUSTER_PATCH` CP on LABEL_FILTERED.id = CP.`clusterId`
          GROUP BY LABEL_FILTERED.id"""

    sqlStatement.as[ListRuntimeResponse2]
  }

  private def joinAndFilterByLabelForList(labelMap: LabelMap, baseQuery: Query[ClusterTable, ClusterRecord, Seq])(
    implicit ec: ExecutionContext
  ): DBIO[List[ListRuntimeResponse2]] = {
    val runtimeQueryJoinedWithLabel = runtimeLabelQuery(baseQuery)

    val runtimeQueryFilteredByLabel = if (labelMap.isEmpty) {
      runtimeQueryJoinedWithLabel
    } else {
      runtimeQueryJoinedWithLabel.filter { case (runtimeRec, _) =>
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
        x.toList.foldMap { case (((runtimeRec, labelRecOpt), runtimeConfigRec), patchRecOpt) =>
          val labelMap = labelRecOpt.map(labelRec => labelRec.key -> Chain(labelRec.value)).toMap
          Map((runtimeRec, runtimeConfigRec.runtimeConfig, patchRecOpt) -> labelMap)
        }

      runtimeLabelMap.map { case ((runtimeRec, runtimeConfig, patchRecOpt), labelMap) =>
        val lmp = labelMap.view.mapValues(_.toList.toSet.headOption.getOrElse("")).toMap

        val patchInProgress = patchRecOpt match {
          case Some(patchRec) => patchRec.inProgress
          case None           => false
        }
        ListRuntimeResponse2(
          runtimeRec.id,
          runtimeRec.workspaceId,
          RuntimeSamResourceId(runtimeRec.internalId),
          runtimeRec.runtimeName,
          runtimeRec.cloudContext,
          runtimeRec.auditInfo,
          runtimeConfig,
          Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase,
                              runtimeRec.cloudContext,
                              runtimeRec.runtimeName,
                              Set.empty,
                              runtimeRec.hostIp,
                              lmp
          ),
          runtimeRec.status,
          lmp,
          patchInProgress
        )
      }.toList
    }
  }
}
