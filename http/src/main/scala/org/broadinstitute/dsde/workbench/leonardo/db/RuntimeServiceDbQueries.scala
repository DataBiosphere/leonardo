package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.data.Chain
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.google2.OperationName
import org.broadinstitute.dsde.workbench.leonardo.{LabelMap, Runtime}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{
  ProjectSamResourceId,
  RuntimeSamResourceId,
  WorkspaceResourceSamResourceId
}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
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
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}

import java.util.UUID
import java.sql.SQLDataException
import java.time.Instant
import scala.concurrent.ExecutionContext

object RuntimeServiceDbQueries {

  case class ListRuntimesRecord(
    id: Long,
    cloudContextDb: CloudContextDb,
    cloudProvider: CloudProvider,
    creator: WorkbenchEmail,
    createdDate: Instant,
    destroyedDate: Instant,
    dateAccessed: Instant,
    hostIp: Option[IP],
    internalId: String,
    patchInProgress: Boolean,
    runtimeConfig: RuntimeConfigRecord,
    runtimeName: RuntimeName,
    status: RuntimeStatus,
    workspaceId: Option[WorkspaceId],
    label: Option[(String, String)]
  )

  private type ListRuntimesProduct = (
    Long,
    CloudContextDb,
    CloudProvider,
    WorkbenchEmail,
    Instant,
    Instant,
    Instant,
    Option[IP],
    String,
    Boolean,
    RuntimeConfigRecord,
    RuntimeName,
    RuntimeStatus,
    Option[WorkspaceId],
    Option[(String, String)]
  )
  private object ListRuntimesRecord {
    def apply(product: ListRuntimesProduct): ListRuntimesRecord = product match {
      case (l,
            db,
            provider,
            email,
            instant,
            instant1,
            instant2,
            maybeIp,
            str,
            bool,
            record,
            name,
            status,
            maybeId,
            maybeTuple
          ) =>
        new ListRuntimesRecord(l,
                               db,
                               provider,
                               email,
                               instant,
                               instant1,
                               instant2,
                               maybeIp,
                               str,
                               bool,
                               record,
                               name,
                               status,
                               maybeId,
                               maybeTuple
        )
    }
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
        runtime <- unmarshalGetRuntime(
          runtimeRecs,
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
        runtime <- unmarshalGetRuntime(
          runtimeRecs,
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

  private def unmarshalGetRuntime(
    clusterRecords: Seq[
      (
        ClusterRecord,
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
      (
        Chain[ClusterErrorRecord],
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
            AsyncRuntimeFields(
              googleId,
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
          Runtime.getProxyUrl(
            Config.proxyConfig.proxyUrlBase,
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

  def listRuntimeIdsForCreator(
    creator: WorkbenchEmail
  )(implicit ec: ExecutionContext): DBIO[Vector[ListRuntimeIdResponse]] = clusterQuery
    .filter(_.creator === creator)
    .map(runtime => (runtime.id, runtime.internalId))
    .result
    .map(records =>
      records.map { case (id: Long, internalId: String) =>
        ListRuntimeIdResponse(id, RuntimeSamResourceId(internalId))
      }.toVector
    )

  /**
   * List runtimes filtered by the given terms. Only return authorized resources (per reader*Ids and/or owner*Ids).
   * @param labelMap
   * @param excludeStatuses
   * @param creatorEmail
   * @param workspaceId
   * @param cloudProvider
   * @param readerRuntimeIds
   * @param readerWorkspaceIds
   * @param ownerWorkspaceIds
   * @param readerGoogleProjectIds
   * @param ownerGoogleProjectIds
   * @return
   */
  def listRuntimes(
    // Authorizations
    ownerGoogleProjectIds: Set[ProjectSamResourceId] = Set.empty,
    ownerWorkspaceIds: Set[WorkspaceResourceSamResourceId] = Set.empty,
    readerGoogleProjectIds: Set[ProjectSamResourceId] = Set.empty,
    readerRuntimeIds: Set[SamResourceId] = Set.empty,
    readerWorkspaceIds: Set[WorkspaceResourceSamResourceId] = Set.empty,

    // Filters
    cloudContext: Option[CloudContext] = None,
    cloudProvider: Option[CloudProvider] = None,
    creatorEmail: Option[WorkbenchEmail] = None,
    excludeStatuses: List[RuntimeStatus] = List.empty,
    labelMap: LabelMap = Map.empty[String, String],
    workspaceId: Option[WorkspaceId] = None
  )(implicit ec: ExecutionContext): DBIO[Vector[ListRuntimeResponse2]] = {
    // Normalize filter params
    val provider = if (cloudProvider.isEmpty) {
      cloudContext match {
        case Some(cContext) => Some(cContext.cloudProvider)
        case None           => None
      }
    } else cloudProvider

    // Optimize Google project list if filtering to a specific cloud provider or context
    val ownedProjects: Set[CloudContextDb] = ((provider, cloudContext) match {
      case (Some(CloudProvider.Azure), _) => Set.empty[CloudContextDb]
      case (Some(CloudProvider.Gcp), Some(CloudContext.Gcp(value))) =>
        ownerGoogleProjectIds.filter(samId => samId.googleProject == value)
      case _ => ownerGoogleProjectIds
    }).map { case samId: SamResourceId =>
      CloudContextDb(samId.resourceId)
    }
    val readProjects: Set[CloudContextDb] = ((provider, cloudContext) match {
      case (Some(CloudProvider.Azure), _) => Set.empty[CloudContextDb]
      case (Some(CloudProvider.Gcp), Some(CloudContext.Gcp(value))) =>
        readerGoogleProjectIds.filter(samId => samId.googleProject == value)
      case _ => readerGoogleProjectIds
    }).map { case samId: SamResourceId =>
      CloudContextDb(samId.resourceId)
    }

    // Optimize workspace list if filtering to a single workspace
    val ownedWorkspaces: Set[WorkspaceId] = (workspaceId match {
      case Some(wId) => ownerWorkspaceIds.filter(samId => WorkspaceId(UUID.fromString(samId.resourceId)) == wId)
      case None      => ownerWorkspaceIds
    }).map(samId => WorkspaceId(UUID.fromString(samId.resourceId)))
    val readWorkspaces: Set[WorkspaceId] = (workspaceId match {
      case Some(wId) => readerWorkspaceIds.filter(samId => WorkspaceId(UUID.fromString(samId.resourceId)) == wId)
      case None      => readerWorkspaceIds
    }).map(samId => WorkspaceId(UUID.fromString(samId.resourceId)))

    val readRuntimes: Set[String] = readerRuntimeIds.map(readId => readId.asString)

    val runtimeInReadWorkspaces: Option[ClusterTable => Rep[Option[Boolean]]] =
      if (readRuntimes.isEmpty || readWorkspaces.isEmpty)
        None
      else
        Some(runtime =>
          (runtime.internalId inSetBind readRuntimes) &&
            (runtime.workspaceId inSetBind readWorkspaces)
        )

    val runtimeInReadProjects: Option[ClusterTable => Rep[Option[Boolean]]] =
      if (readRuntimes.isEmpty || readProjects.isEmpty)
        None
      else
        Some(runtime =>
          (runtime.internalId inSetBind readRuntimes) &&
            (runtime.cloudProvider.? === (CloudProvider.Gcp: CloudProvider)) &&
            (runtime.cloudContextDb inSetBind readProjects)
        )

    val runtimeInOwnedWorkspaces: Option[ClusterTable => Rep[Option[Boolean]]] =
      if (ownedWorkspaces.isEmpty)
        None
      else
        Some(runtime => runtime.workspaceId inSetBind ownedWorkspaces)

    val runtimeInOwnedProjects: Option[ClusterTable => Rep[Option[Boolean]]] =
      if (ownedProjects.isEmpty)
        None
      else if (cloudContext.isDefined) {
        // If cloudContext is defined, we're already applying the filter elsewhere.
        // No need to filter by the list of user owned projects anymore as long as the specified
        // project is owned by the user.
        if (ownedProjects.exists(x => x.value == cloudContext.get.asString))
          Some(_ => Some(true))
        else None
      } else
        Some(runtime =>
          (runtime.cloudProvider.? === (CloudProvider.Gcp: CloudProvider)) &&
            (runtime.cloudContextDb inSetBind ownedProjects)
        )

    val runtimesAuthorized =
      clusterQuery.filter[Rep[Option[Boolean]]] { runtime: ClusterTable =>
        Seq(
          runtimeInReadWorkspaces,
          runtimeInOwnedWorkspaces,
          runtimeInReadProjects,
          runtimeInOwnedProjects
        )
          .mapFilter(opt => opt)
          .map(_(runtime))
          .reduceLeftOption(_ || _)
          .getOrElse(Some(false): Rep[Option[Boolean]])
      }

    val runtimesFiltered = runtimesAuthorized
      // Filter by params
      .filterOpt(workspaceId) { case (runtime, wId) =>
        runtime.workspaceId === (Some(wId): Rep[Option[WorkspaceId]])
      }
      .filterOpt(provider) { case (runtime, cProvider) =>
        runtime.cloudProvider === cProvider
      }
      .filterOpt(cloudContext) { case (runtime, cContext) =>
        runtime.cloudContextDb === cContext.asCloudContextDb
      }
      .filterOpt(creatorEmail) { case (runtime, cEmail) =>
        runtime.creator === cEmail
      }
      .filterIf(excludeStatuses.nonEmpty) { runtime =>
        excludeStatuses.map(status => runtime.status =!= status).reduce(_ && _)
      }
      .filterIf(labelMap.nonEmpty) { runtime =>
        labelQuery
          .filter(l =>
            l.resourceId === runtime.id &&
              l.resourceType === LabelResourceType.runtime &&
              labelMap
                .map { case (key, value) =>
                  l.key === key && l.value === value
                }
                .reduceLeft(_ || _)
          )
          .length === labelMap.size
      }

    // Assemble response
    val runtimesJoined = runtimesFiltered
      .join(runtimeConfigs)
      .on((runtime, runtimeConfig) => runtime.runtimeConfigId === runtimeConfig.id)
      .map { case (runtime, runtimeConfig) =>
        (
          runtime,
          runtimeConfig,
          patchQuery
            .filter(patch => runtime.id === patch.clusterId && patch.inProgress === true)
            .map(_ => true)
            .exists
        )
      }
      .joinLeft(labelQuery)
      .on { case ((runtime, _, _), label) =>
        runtime.id === label.resourceId &&
        label.resourceType === LabelResourceType.runtime
      }
      .map { case ((runtime, runtimeConfig, patchInProgress), label) =>
        (
          runtime.id,
          runtime.cloudContextDb,
          runtime.cloudProvider,
          runtime.creator,
          runtime.createdDate,
          runtime.destroyedDate,
          runtime.dateAccessed,
          runtime.hostIp,
          runtime.internalId,
          patchInProgress,
          runtimeConfig,
          runtime.runtimeName,
          runtime.status,
          runtime.workspaceId,
          label.map(l => (l.key, l.value))
        )
      }

    runtimesJoined.result
      .map { records: Seq[ListRuntimesProduct] =>
        records
          .map(record => ListRuntimesRecord(record))
          .groupBy(_.id)
          .map { case _ -> (values: Seq[ListRuntimesRecord]) =>
            val allLabels: LabelMap = Map(values.mapFilter(_.label): _*)
            values.head match {
              case record =>
                val auditInfo = AuditInfo(
                  record.creator,
                  record.createdDate,
                  if (record.destroyedDate == dummyDate) None else Some(record.destroyedDate),
                  record.dateAccessed
                )
                val cloudContext = (record.cloudProvider, record.cloudContextDb) match {
                  case (CloudProvider.Gcp, CloudContextDb(value)) =>
                    CloudContext.Gcp(GoogleProject(value))
                  case (CloudProvider.Azure, CloudContextDb(value)) =>
                    val azureContext =
                      AzureCloudContext.fromString(value).fold(s => throw new SQLDataException(s), identity)
                    CloudContext.Azure(azureContext)
                }
                val proxyUrl = Runtime.getProxyUrl(
                  Config.proxyConfig.proxyUrlBase,
                  cloudContext,
                  record.runtimeName,
                  Set.empty,
                  record.hostIp,
                  allLabels
                )
                val runtimeConfig = record.runtimeConfig.runtimeConfig
                val samResourceId = RuntimeSamResourceId(record.internalId)

                ListRuntimeResponse2(
                  record.id,
                  auditInfo = auditInfo,
                  cloudContext = cloudContext,
                  clusterName = record.runtimeName,
                  labels = allLabels,
                  patchInProgress = record.patchInProgress,
                  proxyUrl = proxyUrl,
                  runtimeConfig = runtimeConfig,
                  samResource = samResourceId,
                  status = record.status,
                  workspaceId = record.workspaceId
                )
            }
          }
          .toVector
      }
  }

}
