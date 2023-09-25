package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.data.Chain
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.OperationName
import org.broadinstitute.dsde.workbench.leonardo.Runtime
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{
  ProjectSamResourceId,
  RuntimeSamResourceId,
  WorkspaceResourceSamResourceId
}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.GetResultInstances._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeConfigQueries._
import org.broadinstitute.dsde.workbench.leonardo.db.clusterQuery.{
  getRuntimeQueryByUniqueKey,
  getRuntimeQueryByWorkspaceId
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

  implicit val getResultListRuntimeResponse2: GetResult[ListRuntimeResponse2] = GetResult { r =>
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

  def getStatusByName(cloudContext: CloudContext, name: RuntimeName)(implicit
    ec: ExecutionContext
  ): DBIO[Option[RuntimeStatus]] = {
    val res = clusterQuery
      .filter(_.cloudProvider === cloudContext.cloudProvider)
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

  def getRuntimeByWorkspaceId(workspaceId: WorkspaceId, runtimeName: RuntimeName)(implicit
    executionContext: ExecutionContext
  ): DBIO[GetRuntimeResponse] = {
    val activeRuntime = getRuntimeQueryByWorkspaceId(workspaceId, runtimeName)
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
    val clusterRecordMap: Map[
      ClusterRecord,
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

  def listRuntimes(labelMap: LabelMap,
                   excludeStatuses: List[RuntimeStatus],
                   creatorOnly: Option[WorkbenchEmail],
                   cloudContext: Option[CloudContext] = None
  )(implicit
    ec: ExecutionContext
  ): DBIO[List[ListRuntimeResponse2]] = {
    val cloudContextFilter = cloudContext match {
      case Some(cc) => Some(Left(cc))
      case None     => None
    }
    listRuntimesHelper(labelMap, excludeStatuses, creatorOnly, None, cloudContextFilter).map(
      _.toList
    )
  }

  def listRuntimesForWorkspace(labelMap: LabelMap,
                               excludeStatuses: List[RuntimeStatus],
                               creatorOnly: Option[WorkbenchEmail],
                               workspaceId: Option[WorkspaceId],
                               cloudProvider: Option[CloudProvider]
  ): DBIO[Vector[ListRuntimeResponse2]] = {
    val cp = cloudProvider.map(cp => Right(cp))
    listRuntimesHelper(labelMap, excludeStatuses, creatorOnly, workspaceId, cp)
  }

  /**
   * List runtimes filtered by the given terms. Only return authorized resources (those in reader*Ids and/or owner*Ids).
   */
  def listAuthorizedRuntimes(
    // Filters
    labelMap: LabelMap,
    excludeStatuses: List[RuntimeStatus],
    creatorOnly: Option[WorkbenchEmail],
    workspaceId: Option[WorkspaceId],
    cloudProvider: Option[CloudProvider],

    // Authorizations
    readerRuntimeIds: List[String],
    readerWorkspaceIds: List[String],
    ownerWorkspaceIds: List[String],
    readerGoogleProjectIds: List[String],
    ownerGoogleProjectIds: List[String]
  ): DBIO[Vector[ListRuntimeResponse2]] = {
    val cp = cloudProvider.map(cp => Right(cp))
    listAuthorizedRuntimesHelper(
      labelMap,
      excludeStatuses,
      creatorOnly,
      workspaceId,
      cp,
      readerRuntimeIds,
      readerWorkspaceIds,
      ownerWorkspaceIds,
      readerGoogleProjectIds,
      ownerGoogleProjectIds
    )
  }

  /**
   * Query the runtimes (CLUSTER) table with the given filters. Only return authorized resources (those in reader*Ids and/or owner*Ids).
   */
  private def listAuthorizedRuntimesHelper(
    // Filters
    labelMap: LabelMap,
    excludeStatuses: List[RuntimeStatus],
    creatorOnly: Option[WorkbenchEmail],
    workspaceId: Option[WorkspaceId],
    cloudContextOrCloudProvider: Option[Either[CloudContext, CloudProvider]],

    // Authorizations
    readerRuntimeIds: List[String],
    readerWorkspaceIds: List[String],
    ownerWorkspaceIds: List[String],
    readerGoogleProjectIds: List[String],
    ownerGoogleProjectIds: List[String]
  ): DBIO[Vector[ListRuntimeResponse2]] = {

    // Authorize: show only resources the user is permitted to see
    val filterVisibleIds = getFilterVisibleIds(readerRuntimeIds,
                                               readerWorkspaceIds,
                                               ownerWorkspaceIds,
                                               readerGoogleProjectIds,
                                               ownerGoogleProjectIds
    )

    // Filter: show only user selections
    val filterWorkspaceId = workspaceId match {
      case Some(wid) =>
        List(s"C.`workspaceId` = '${wid.value.toString}'")
      case None => List.empty
    }

    val filterCloud = cloudContextOrCloudProvider match {
      case Some(Left(CloudContext.Gcp(gp))) =>
        List(s"C.`cloudProvider` = ${CloudProvider.Gcp} AND C.`cloudContext` = '${gp.value}'")
      case Some(Left(CloudContext.Azure(actx))) =>
        List(s"C.`cloudProvider` = ${CloudProvider.Azure} AND C.`cloudContext` = '${actx.asString}'")
      case Some(Right(cloudProvider)) => List(s"C.`cloudProvider` = '${cloudProvider.asString}'")
      case None                       => List.empty
    }

    val filterNotDeleted = excludeStatuses.map(s => s"C.`status` != '${s.toString}'")

    val filterCreator = creatorOnly match {
      case Some(creator) => List(s"C.`creator` = '${creator.value}'")
      case None          => List.empty
    }

    val filterClusters = (
      filterVisibleIds ++
        filterWorkspaceId ++
        filterCloud ++
        filterNotDeleted ++ filterCreator
    ).filterNot(_.isEmpty).mkString(" AND ")

    val whereFilterClusters = if (filterClusters.isEmpty) "" else s"where ${filterClusters}"

    val whereFilterLabels =
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

    // Create full query
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
                #${whereFilterClusters}
              ) AS FILTERED_CLUSTER
              left join `LABEL` L on (L.`resourceId` = FILTERED_CLUSTER.id) and (L.`resourceType` = 'runtime')
              #${whereFilterLabels}
          ) AS LABEL_FILTERED
          inner join `RUNTIME_CONFIG` RG on LABEL_FILTERED.runtimeConfigId = RG.`id`
          left join `CLUSTER_PATCH` CP on LABEL_FILTERED.id = CP.`clusterId`
          GROUP BY LABEL_FILTERED.id""".stripMargin

    sqlStatement.as[ListRuntimeResponse2]
  }

  private def listRuntimesHelper(labelMap: LabelMap,
                                 excludeStatuses: List[RuntimeStatus],
                                 creatorOnly: Option[WorkbenchEmail],
                                 workspaceId: Option[WorkspaceId],
                                 cloudContextOrCloudProvider: Option[Either[CloudContext, CloudProvider]]
  ): DBIO[Vector[ListRuntimeResponse2]] = {
    val runtimeQueryFilteredByCreator = creatorOnly match {
      case Some(creator) => List(s"C.`creator` = '${creator.value}'")
      case None          => List.empty
    }
    val runtimeQueryFilteredByDeletion = excludeStatuses.map(s => s"C.`status` != '${s.toString}'")

    val runtimeQueryFilteredByWorkspace = workspaceId match {
      case Some(wid) =>
        List(s"C.`workspaceId` = '${wid.value.toString}'")
      case None => List.empty
    }

    val runtimeQueryFilteredByCloud = cloudContextOrCloudProvider match {
      case Some(Left(CloudContext.Gcp(gp)))     => List(s"C.`cloudContext` = '${gp.value}'")
      case Some(Left(CloudContext.Azure(actx))) => List(s"C.`cloudContext` = '${actx.asString}'")
      case Some(Right(cloudProvider))           => List(s"C.`cloudProvider` = '${cloudProvider.asString}'")
      case None                                 => List.empty
    }

    val clusterFilters =
      (runtimeQueryFilteredByCreator ++ runtimeQueryFilteredByDeletion ++ runtimeQueryFilteredByWorkspace ++ runtimeQueryFilteredByCloud)
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
              left join `LABEL` L on (L.`resourceId` = FILTERED_CLUSTER.id) and (L.`resourceType` = 'runtime')
              #${labelMapFilters}
          ) AS LABEL_FILTERED
          inner join `RUNTIME_CONFIG` RG on LABEL_FILTERED.runtimeConfigId = RG.`id`
          left join `CLUSTER_PATCH` CP on LABEL_FILTERED.id = CP.`clusterId`
          GROUP BY LABEL_FILTERED.id""".stripMargin

    sqlStatement.as[ListRuntimeResponse2]
  }

  /**
   * Build the filter expression which shows only runtimes the user has permission to read. If any of the parameter
   * lists is blank, the user is assumed to have no permissions (can see nothing).
   * @param readerRuntimeIds List[String] runtime IDs the user has any Sam role for
   * @param readerWorkspaceIds List[String] workspace IDs the user has any Sam role for
   * @param ownerWorkspaceIds List[String] Google project IDs the user has an owner role for
   * @param readerGoogleProjectIds List[String] Google project IDs the user has any Sam role for
   * @param ownerGoogleProjectIds List[String] Google project IDs the user has an owner role for
   * @return List[String] a one-element list with the SQL filter expression
   */
  private def getFilterVisibleIds(readerRuntimeIds: List[String],
                                  readerWorkspaceIds: List[String],
                                  ownerWorkspaceIds: List[String],
                                  readerGoogleProjectIds: List[String],
                                  ownerGoogleProjectIds: List[String]
  ): List[String] = {
    // is the record a readable runtime?
    val filterRuntimeIds = getInListExpression("C.`id`", readerRuntimeIds)
    // is the record in a readable workspace?
    val filterReaderWorkspaceIds = getInListExpression("C.`workspaceId`", readerWorkspaceIds)
    // is the record in a readable Google project?
    val filterReaderGoogleProjectIds = getInGoogleProjectIdsExpression(readerGoogleProjectIds)
    // is the record in an owned workspace?
    val filterOwnerWorkspaceIds = getInListExpression("C.`workspaceId`", ownerWorkspaceIds)
    // is the record in an owned Google project?
    val filterOwnerGoogleProjectIds = getInGoogleProjectIdsExpression(ownerGoogleProjectIds)

    // is the runtime readable and in a readable workspace/project?
    val filterReader = s"${filterRuntimeIds} AND ( ${filterReaderWorkspaceIds} OR ${filterReaderGoogleProjectIds} )"
    // is the runtime in an owned workspace/project?
    val filterOwner = s"${filterOwnerWorkspaceIds} OR ${filterOwnerGoogleProjectIds}"

    // is the runtime visible via direct read permissions or inference from container ownership?
    val filterVisibleIds = s"${filterReader} OR ${filterOwner}"
    List(filterVisibleIds)
  }

  private def getInGoogleProjectIdsExpression(projectIds: List[String]): String =
    if (projectIds.isEmpty)
      "0 = 1"
    else
      s"C.`cloudProvider` = ${CloudProvider.Gcp} AND ${getInListExpression("C.`cloudContext`", projectIds)}"

  private def getInListExpression(field: String, terms: List[String]): String =
    if (terms.isEmpty) "0 = 1" else s"${field} IN(${terms.mkString(",")})"
}
