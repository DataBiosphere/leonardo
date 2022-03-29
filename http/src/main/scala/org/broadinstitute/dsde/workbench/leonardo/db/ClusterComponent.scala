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
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeConfigQueries.runtimeConfigs
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  ExtraInfoForCreateRuntime,
  RuntimeToAutoPause,
  RuntimeToMonitor
}
import org.broadinstitute.dsde.workbench.model.google.{
  parseGcsPath,
  GcsBucketName,
  GcsPath,
  GcsPathSupport,
  GoogleProject,
  ServiceAccountKeyId
}
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}

import java.sql.SQLDataException
import java.time.Instant
import scala.concurrent.ExecutionContext

final case class ClusterRecord(id: Long,
                               internalId: String,
                               runtimeName: RuntimeName,
                               googleId: Option[ProxyHostName],
                               cloudContext: CloudContext,
                               operationName: Option[String],
                               status: RuntimeStatus,
                               hostIp: Option[String],
                               userScriptUri: Option[UserScriptPath],
                               startUserScriptUri: Option[UserScriptPath],
                               initBucket: Option[String],
                               auditInfo: AuditInfo,
                               kernelFoundBusyDate: Option[Instant],
                               serviceAccountInfo: WorkbenchEmail,
                               stagingBucket: Option[String],
                               autopauseThreshold: Int,
                               defaultClientId: Option[String],
                               welderEnabled: Boolean,
                               customClusterEnvironmentVariables: Map[String, String],
                               runtimeConfigId: RuntimeConfigId,
                               deletedFrom: Option[String],
                               workspaceId: Option[WorkspaceId]) {
  def projectNameString: String = s"${cloudContext.asStringWithProvider}/${runtimeName.asString}"
}

class ClusterTable(tag: Tag) extends Table[ClusterRecord](tag, "CLUSTER") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def internalId = column[String]("internalId", O.Length(254))
  def runtimeName = column[RuntimeName]("runtimeName", O.Length(254))
  def proxyHostName = column[Option[ProxyHostName]]("proxyHostName")
  // For Google resources, cloudContext is google project;
  // For Azure resources, cloudContext is managed resource group
  def cloudContextDb = column[CloudContextDb]("cloudContext", O.Length(254))
  def cloudProvider = column[CloudProvider]("cloudProvider", O.Length(50))
  def serviceAccount = column[WorkbenchEmail]("serviceAccount", O.Length(254))
  def operationName = column[Option[String]]("operationName", O.Length(254))
  def status = column[RuntimeStatus]("status", O.Length(254))
  def hostIp = column[Option[String]]("hostIp", O.Length(254))
  def creator = column[WorkbenchEmail]("creator", O.Length(254))
  def createdDate = column[Instant]("createdDate", O.SqlType("TIMESTAMP(6)"))
  def destroyedDate: Rep[Instant] = column[Instant]("destroyedDate", O.SqlType("TIMESTAMP(6)"))
  def userScriptUri = column[Option[UserScriptPath]]("userScriptUri", O.Length(1024))
  def startUserScriptUri = column[Option[UserScriptPath]]("startUserScriptUri", O.Length(1024))
  def initBucket = column[Option[String]]("initBucket", O.Length(1024))
  def stagingBucket = column[Option[String]]("stagingBucket", O.Length(254))
  def dateAccessed = column[Instant]("dateAccessed", O.SqlType("TIMESTAMP(6)"))
  def autopauseThreshold = column[Int]("autopauseThreshold")
  def kernelFoundBusyDate = column[Option[Instant]]("kernelFoundBusyDate", O.SqlType("TIMESTAMP(6)"))
  def defaultClientId = column[Option[String]]("defaultClientId", O.Length(1024))
  def welderEnabled = column[Boolean]("welderEnabled")
  def runtimeConfigId = column[RuntimeConfigId]("runtimeConfigId")
  def customClusterEnvironmentVariables = column[Option[Map[String, String]]]("customClusterEnvironmentVariables")
  def deletedFrom = column[Option[String]]("deletedFrom")
  def workspaceId = column[Option[WorkspaceId]]("workspaceId")

  // Can't use the shorthand
  //   def * = (...) <> (ClusterRecord.tupled, ClusterRecord.unapply)
  // because CLUSTER has more than 22 columns.
  // So we split ClusterRecord into multiple case classes and bind them to slick in the following way.
  def * =
    (
      id,
      internalId,
      runtimeName,
      proxyHostName,
      (cloudProvider, cloudContextDb),
      operationName,
      status,
      hostIp,
      userScriptUri,
      startUserScriptUri,
      initBucket,
      (creator, createdDate, destroyedDate, dateAccessed),
      kernelFoundBusyDate,
      serviceAccount,
      stagingBucket,
      autopauseThreshold,
      defaultClientId,
      welderEnabled,
      customClusterEnvironmentVariables,
      runtimeConfigId,
      deletedFrom,
      workspaceId
    ).shaped <> ({
      case (id,
            internalId,
            clusterName,
            proxyHostname,
            (cloudProvider, cloudContextDb),
            operationName,
            status,
            hostIp,
            userScriptUri,
            startUserScriptUri,
            initBucket,
            auditInfo,
            kernelFoundBusyDate,
            serviceAccountInfo,
            stagingBucket,
            autopauseThreshold,
            defaultClientId,
            welderEnabled,
            customClusterEnvironmentVariables,
            runtimeConfigId,
            deletedFrom,
            workspaceId) =>
        ClusterRecord(
          id,
          internalId,
          clusterName,
          proxyHostname,
          cloudProvider match {
            case CloudProvider.Gcp =>
              CloudContext.Gcp(GoogleProject(cloudContextDb.value)): CloudContext
            case CloudProvider.Azure =>
              val context =
                AzureCloudContext.fromString(cloudContextDb.value).fold(s => throw new SQLDataException(s), identity)
              CloudContext.Azure(context): CloudContext
          },
          operationName,
          status,
          hostIp,
          userScriptUri,
          startUserScriptUri,
          initBucket,
          AuditInfo(
            auditInfo._1,
            auditInfo._2,
            LeoProfile.unmarshalDestroyedDate(auditInfo._3),
            auditInfo._4
          ),
          kernelFoundBusyDate,
          serviceAccountInfo,
          stagingBucket,
          autopauseThreshold,
          defaultClientId,
          welderEnabled,
          customClusterEnvironmentVariables.getOrElse(Map.empty),
          runtimeConfigId,
          deletedFrom,
          workspaceId
        )
    }, { c: ClusterRecord =>
      def ai(_ai: AuditInfo) = (
        _ai.creator,
        _ai.createdDate,
        _ai.destroyedDate.getOrElse(dummyDate),
        _ai.dateAccessed
      )
      Some(
        (
          c.id,
          c.internalId,
          c.runtimeName,
          c.googleId,
          c.cloudContext match {
            case CloudContext.Gcp(value) =>
              (CloudProvider.Gcp, CloudContextDb(value.value))
            case CloudContext.Azure(value) =>
              (CloudProvider.Azure, CloudContextDb(value.asString))
          },
          c.operationName,
          c.status,
          c.hostIp,
          c.userScriptUri,
          c.startUserScriptUri,
          c.initBucket,
          ai(c.auditInfo),
          c.kernelFoundBusyDate,
          c.serviceAccountInfo,
          c.stagingBucket,
          c.autopauseThreshold,
          c.defaultClientId,
          c.welderEnabled,
          if (c.customClusterEnvironmentVariables.isEmpty) None else Some(c.customClusterEnvironmentVariables),
          c.runtimeConfigId,
          c.deletedFrom,
          c.workspaceId
        )
      )
    })
}

object clusterQuery extends TableQuery(new ClusterTable(_)) {
  // just clusters and labels: no instances, extensions, etc.
  //   select * from cluster c
  //   left join label l on c.id = l.clusterId
  val clusterLabelQuery: Query[(ClusterTable, Rep[Option[LabelTable]]), (ClusterRecord, Option[LabelRecord]), Seq] = {
    for {
      (cluster, label) <- clusterQuery joinLeft labelQuery on {
        case (c, lbl) =>
          lbl.resourceId === c.id && lbl.resourceType === LabelResourceType.runtime
      }
    } yield (cluster, label)
  }

  val clusterLabelPatchQuery: Query[(ClusterTable, Rep[Option[LabelTable]], Rep[Option[PatchTable]]),
                                    (ClusterRecord, Option[LabelRecord], Option[PatchRecord]),
                                    Seq] = {
    for {
      ((cluster, label), patch) <- clusterQuery joinLeft
        labelQuery on { case (c, lbl) => lbl.resourceId === c.id && lbl.resourceType === LabelResourceType.runtime } joinLeft
        patchQuery on (_._1.id === _.clusterId)
    } yield (cluster, label, patch)
  }

  // select * from cluster c
  // join cluster_image ci on c.id = ce.cluster_id
  val clusterJoinClusterImageQuery
    : Query[(ClusterTable, ClusterImageTable), (ClusterRecord, ClusterImageRecord), Seq] = {
    for {
      (cluster, image) <- clusterQuery join clusterImageQuery on (_.id === _.clusterId)
    } yield (cluster, image)
  }

  def getRuntimeQueryByUniqueKey(cloudContext: CloudContext,
                                 clusterName: RuntimeName,
                                 destroyedDateOpt: Option[Instant]) = {
    val destroyedDate = destroyedDateOpt.getOrElse(dummyDate)
    val baseQuery = clusterQuery
      .filter(_.cloudContextDb === cloudContext.asCloudContextDb)
      .filter(_.runtimeName === clusterName)
      .filter(_.destroyedDate === destroyedDate)

    for {
      ((((((cluster, error), label), extension), image), scopes), patch) <- baseQuery joinLeft
        clusterErrorQuery on (_.id === _.clusterId) joinLeft
        labelQuery on {
        case (c, lbl) => lbl.resourceId === c._1.id && lbl.resourceType === LabelResourceType.runtime
      } joinLeft
        extensionQuery on (_._1._1.id === _.clusterId) joinLeft
        clusterImageQuery on (_._1._1._1.id === _.clusterId) joinLeft
        scopeQuery on (_._1._1._1._1.id === _.clusterId) joinLeft
        patchQuery on (_._1._1._1._1._1.id === _.clusterId)
    } yield (cluster, error, label, extension, image, scopes, patch)
  }

  def clusterRecordQueryByUniqueKey(cloudContext: CloudContext,
                                    clusterName: RuntimeName,
                                    destroyedDateOpt: Option[Instant]) = {
    val destroyedDate = destroyedDateOpt.getOrElse(dummyDate)
    clusterQuery
      .filter(_.cloudContextDb === cloudContext.asCloudContextDb)
      .filter(_.runtimeName === clusterName)
      .filter(_.destroyedDate === destroyedDate)
  }

  private def fullClusterQueryById(id: Long) =
    fullClusterQuery(findByIdQuery(id))

  private[leonardo] def fullClusterQuery(
    baseClusterQuery: Query[ClusterTable, ClusterRecord, Seq]
  ): Query[(ClusterTable,
            Rep[Option[ClusterErrorTable]],
            Rep[Option[LabelTable]],
            Rep[Option[ExtensionTable]],
            Rep[Option[ClusterImageTable]],
            Rep[Option[ScopeTable]],
            Rep[Option[PatchTable]]),
           (ClusterRecord,
            Option[ClusterErrorRecord],
            Option[LabelRecord],
            Option[ExtensionRecord],
            Option[ClusterImageRecord],
            Option[ScopeRecord],
            Option[PatchRecord]),
           Seq] =
    for {
      ((((((cluster, error), label), extension), image), scopes), patch) <- baseClusterQuery joinLeft
        clusterErrorQuery on (_.id === _.clusterId) joinLeft
        labelQuery on {
        case (c, lbl) => lbl.resourceId === c._1.id && lbl.resourceType === LabelResourceType.runtime
      } joinLeft
        extensionQuery on (_._1._1.id === _.clusterId) joinLeft
        clusterImageQuery on (_._1._1._1.id === _.clusterId) joinLeft
        scopeQuery on (_._1._1._1._1.id === _.clusterId) joinLeft
        patchQuery on (_._1._1._1._1._1.id === _.clusterId)
    } yield (cluster, error, label, extension, image, scopes, patch)

  private def findByIdQuery(id: Long): Query[ClusterTable, ClusterRecord, Seq] =
    clusterQuery.filter(_.id === id)

  def save(saveCluster: SaveCluster)(implicit ec: ExecutionContext): DBIO[Runtime] =
    for {
      runtimeConfigId <- RuntimeConfigQueries.insertRuntimeConfig(saveCluster.runtimeConfig, saveCluster.now)
      cluster = LeoLenses.runtimeToRuntimeConfigId.modify(_ => runtimeConfigId.value)(
        saveCluster.cluster
      ) // update runtimeConfigId

      clusterId <- clusterQuery returning clusterQuery.map(_.id) += marshalCluster(cluster,
                                                                                   saveCluster.initBucket.map(_.toUri),
                                                                                   saveCluster.workspaceId)

      _ <- labelQuery.saveAllForResource(clusterId, LabelResourceType.Runtime, cluster.labels)
      _ <- extensionQuery.saveAllForCluster(clusterId, cluster.userJupyterExtensionConfig)
      _ <- clusterImageQuery.saveAllForCluster(clusterId, cluster.runtimeImages.toSeq)
      _ <- scopeQuery.saveAllForCluster(clusterId, cluster.scopes)
    } yield cluster.copy(id = clusterId)

  // note: list* methods don't query the INSTANCE table

  def listWithLabels(implicit ec: ExecutionContext): DBIO[Seq[Runtime]] =
    clusterLabelPatchQuery.result.map(unmarshalMinimalCluster)

  def listActiveWithLabels(implicit ec: ExecutionContext): DBIO[Seq[Runtime]] =
    clusterLabelPatchQuery.filter(_._1.status inSetBind RuntimeStatus.activeStatuses).result map { recs =>
      unmarshalMinimalCluster(recs)
    }

  def listMonitored(implicit ec: ExecutionContext): DBIO[Seq[RuntimeToMonitor]] =
    clusterQuery
      .filter(_.status inSetBind RuntimeStatus.monitoredStatuses)
      .join(runtimeConfigs)
      .on(_.runtimeConfigId === _.id)
      .joinLeft(patchQuery)
      .on(_._1.id === _.clusterId)
      .result map { recs =>
      recs.map { rec =>
        val asyncFields = (rec._1._1.googleId, rec._1._1.operationName, rec._1._1.stagingBucket).mapN {
          (googleId, operationName, stagingBucket) =>
            AsyncRuntimeFields(googleId,
                               OperationName(operationName),
                               GcsBucketName(stagingBucket),
                               rec._1._1.hostIp map IP)
        }
        RuntimeToMonitor(
          rec._1._1.id,
          rec._1._1.cloudContext,
          rec._1._1.runtimeName,
          rec._1._1.status,
          rec._2.map(_.inProgress).getOrElse(false),
          rec._1._2.runtimeConfig,
          rec._1._1.serviceAccountInfo,
          asyncFields,
          rec._1._1.auditInfo,
          rec._1._1.userScriptUri,
          rec._1._1.startUserScriptUri,
          rec._1._1.defaultClientId,
          rec._1._1.welderEnabled,
          rec._1._1.customClusterEnvironmentVariables
        )
      }
    }

  def getExtraInfo(runtimeId: Long)(implicit ec: ExecutionContext): DBIO[ExtraInfoForCreateRuntime] =
    for {
      extention <- extensionQuery.getAllForCluster(runtimeId)
      images <- clusterImageQuery.getAllImagesForCluster(runtimeId)
      scopes <- scopeQuery.getAllForCluster(runtimeId)
    } yield ExtraInfoForCreateRuntime(images.toSet, extention, scopes)

  def listRunningOnly(implicit ec: ExecutionContext): DBIO[Seq[RunningRuntime]] =
    clusterJoinClusterImageQuery.filter(_._1.status === (RuntimeStatus.Running: RuntimeStatus)).result map {
      unmarshalRunningCluster
    }

  def countActiveByClusterServiceAccount(clusterServiceAccount: WorkbenchEmail) =
    clusterQuery
      .filter(_.serviceAccount === clusterServiceAccount)
      .filter(_.status inSetBind RuntimeStatus.activeStatuses)
      .length
      .result

  def countActiveByProject(cloudContext: CloudContext) =
    clusterQuery
      .filter(_.cloudContextDb === cloudContext.asCloudContextDb)
      .filter(_.status inSetBind RuntimeStatus.activeStatuses)
      .length
      .result

  def getActiveClusterByNameMinimal(cloudContext: CloudContext,
                                    name: RuntimeName)(implicit ec: ExecutionContext): DBIO[Option[Runtime]] = {
    val res = clusterQuery
      .filter(_.cloudContextDb === cloudContext.asCloudContextDb)
      .filter(_.runtimeName === name)
      .filter(_.destroyedDate === dummyDate)
      .result

    res.map { recs =>
      recs.headOption.map { clusterRec =>
        unmarshalCluster(clusterRec, List.empty, Map.empty, List.empty, List.empty, List.empty, List.empty)
      }
    }
  }

  def getActiveClusterRecordByName(cloudContext: CloudContext,
                                   name: RuntimeName)(implicit ec: ExecutionContext): DBIO[Option[ClusterRecord]] =
    clusterQuery
      .filter(_.cloudContextDb === cloudContext.asCloudContextDb)
      .filter(_.runtimeName === name)
      .filter(_.destroyedDate === dummyDate)
      .result
      .map(recs => recs.headOption)

  def getClusterById(id: Long)(implicit ec: ExecutionContext): DBIO[Option[Runtime]] =
    fullClusterQueryById(id).result map { recs => unmarshalFullCluster(recs).headOption }

  def getActiveClusterInternalIdByName(cloudContext: CloudContext, name: RuntimeName)(
    implicit ec: ExecutionContext
  ): DBIO[Option[RuntimeSamResourceId]] =
    clusterQuery
      .filter(_.cloudContextDb === cloudContext.asCloudContextDb)
      .filter(_.runtimeName === name)
      .filter(_.destroyedDate === dummyDate)
      .result
      .map(recs => recs.headOption.map(clusterRec => RuntimeSamResourceId(clusterRec.internalId)))

  def getInitBucket(cloudContext: CloudContext,
                    name: RuntimeName)(implicit ec: ExecutionContext): DBIO[Option[GcsPath]] =
    clusterQuery
      .filter(_.cloudContextDb === cloudContext.asCloudContextDb)
      .filter(_.runtimeName === name)
      .map(_.initBucket)
      .result
      .map(recs => recs.headOption.flatten.flatMap(head => parseGcsPath(head).toOption))

  def getStagingBucket(cloudContext: CloudContext,
                       name: RuntimeName)(implicit ec: ExecutionContext): DBIO[Option[GcsPath]] =
    clusterQuery
      .filter(_.cloudContextDb === cloudContext.asCloudContextDb)
      .filter(_.runtimeName === name)
      .map(_.stagingBucket)
      .result
      // staging bucket is saved as a bucket name rather than a path
      .map(recs => recs.headOption.flatten.flatMap(head => parseGcsPath("gs://" + head + "/").toOption))

  def getClusterStatus(id: Long): DBIO[Option[RuntimeStatus]] =
    findByIdQuery(id).map(_.status).result.headOption

  def getInitBucket(id: Long)(implicit ec: ExecutionContext): DBIO[Option[GcsPath]] =
    findByIdQuery(id)
      .map(_.initBucket)
      .result
      .map(recs => recs.headOption.flatten.flatMap(head => parseGcsPath(head).toOption))

  def getClustersReadyToAutoFreeze(implicit ec: ExecutionContext): DBIO[Seq[RuntimeToAutoPause]] = {
    val now = SimpleFunction.nullary[Instant]("NOW")
    val tsdiff = SimpleFunction.ternary[String, Instant, Instant, Int]("TIMESTAMPDIFF")
    val minute = SimpleLiteral[String]("MINUTE")

    val baseQuery = clusterQuery
      .filter(_.autopauseThreshold =!= autoPauseOffValue)
      .filter(record => tsdiff(minute, record.dateAccessed, now) >= record.autopauseThreshold)
      .filter(_.status inSetBind RuntimeStatus.stoppableStatuses)

    baseQuery.result map { recs =>
      recs.map(r => RuntimeToAutoPause(r.id, r.runtimeName, r.cloudContext, r.kernelFoundBusyDate))
    }
  }

  def markPendingDeletion(id: Long, dateAccessed: Instant): DBIO[Int] =
    findByIdQuery(id)
      .map(c => (c.status, c.hostIp, c.dateAccessed))
      .update((RuntimeStatus.Deleting, None, dateAccessed))

  // TODO: is there a better way to construct this query?
  def completeDeletion(id: Long, destroyedDate: Instant)(implicit ec: ExecutionContext): DBIO[Unit] =
    for {
      _ <- findByIdQuery(id)
        .map(c => (c.destroyedDate, c.status, c.hostIp, c.dateAccessed))
        .update((destroyedDate, RuntimeStatus.Deleted, None, destroyedDate)): DBIO[Int]
      rid <- findByIdQuery(id).result.head.map[RuntimeConfigId](_.runtimeConfigId): DBIO[RuntimeConfigId]
      _ <- RuntimeConfigQueries.updatePersistentDiskId(rid, None, destroyedDate): DBIO[Int]
    } yield ()

  def markDeleted(cloudContext: CloudContext,
                  runtimeName: RuntimeName,
                  destroyedDate: Instant,
                  deletedFrom: Option[String])(
    implicit ec: ExecutionContext
  ): DBIO[Unit] =
    clusterRecordQueryByUniqueKey(cloudContext, runtimeName, Some(dummyDate))
      .map(c => (c.destroyedDate, c.status, c.hostIp, c.dateAccessed, c.deletedFrom))
      .update((destroyedDate, RuntimeStatus.Deleted, None, destroyedDate, deletedFrom))
      .map(_ => ())

  def updateDeletedFrom(id: Long, deletedFrom: String): DBIO[Int] =
    findByIdQuery(id)
      .map(c => (c.deletedFrom))
      .update(Some(deletedFrom))

  def getDeletedFrom(id: Long)(implicit ec: ExecutionContext): DBIO[Option[String]] =
    findByIdQuery(id)
      .map(_.deletedFrom)
      .result
      .headOption
      .map(_.flatten)

  def updateClusterStatusAndHostIp(id: Long,
                                   status: RuntimeStatus,
                                   hostIp: Option[IP],
                                   dateAccessed: Instant): DBIO[Int] =
    findByIdQuery(id)
      .map(c => (c.status, c.hostIp, c.dateAccessed))
      .update((status, hostIp.map(_.asString), dateAccessed))

  def updateClusterHostIp(id: Long, hostIp: Option[IP], dateAccessed: Instant): DBIO[Int] =
    clusterQuery
      .filter(x => x.id === id)
      .map(c => (c.hostIp, c.dateAccessed))
      .update((hostIp.map(_.asString), dateAccessed))

  def updateAsyncClusterCreationFields(updateAsyncClusterCreationFields: UpdateAsyncClusterCreationFields): DBIO[Int] =
    findByIdQuery(updateAsyncClusterCreationFields.clusterId)
      .map(c => (c.initBucket, c.proxyHostName, c.operationName, c.stagingBucket, c.dateAccessed))
      .update(
        (
          updateAsyncClusterCreationFields.initBucket.map(_.toUri),
          updateAsyncClusterCreationFields.asyncRuntimeFields.map(_.proxyHostName),
          updateAsyncClusterCreationFields.asyncRuntimeFields.map(_.operationName.value),
          updateAsyncClusterCreationFields.asyncRuntimeFields.map(_.stagingBucket.value),
          updateAsyncClusterCreationFields.dateAccessed
        )
      )

  def clearAsyncClusterCreationFields(cluster: Runtime, dateAccessed: Instant): DBIO[Int] =
    findByIdQuery(cluster.id)
      .map(c => (c.initBucket, c.proxyHostName, c.operationName, c.stagingBucket, c.dateAccessed))
      .update((None, None, None, None, dateAccessed))

  def updateClusterStatus(id: Long, newStatus: RuntimeStatus, now: Instant): DBIO[Int] =
    if (newStatus == RuntimeStatus.Deleted)
      findByIdQuery(id)
        .map(c => (c.status, c.hostIp, c.dateAccessed, c.destroyedDate))
        .update((RuntimeStatus.Deleted, None, now, now))
    else
      findByIdQuery(id).map(c => (c.status, c.dateAccessed)).update((newStatus, now))

  // for testing only
  def updateClusterCreatedDate(id: Long, createdDate: Instant): DBIO[Int] =
    findByIdQuery(id).map(_.createdDate).update(createdDate)

  def updateDateAccessed(id: Long, dateAccessed: Instant): DBIO[Int] =
    findByIdQuery(id)
      .filter(_.dateAccessed < dateAccessed)
      .map(_.dateAccessed)
      .update(dateAccessed)

  def updateDateAccessedByProjectAndName(cloudContext: CloudContext, clusterName: RuntimeName, dateAccessed: Instant)(
    implicit ec: ExecutionContext
  ): DBIO[Int] =
    clusterQuery.getActiveClusterByNameMinimal(cloudContext, clusterName) flatMap {
      case Some(c) => clusterQuery.updateDateAccessed(c.id, dateAccessed)
      case None    => DBIO.successful(0)
    }

  def clearKernelFoundBusyDate(id: Long, dateAccessed: Instant): DBIO[Int] =
    findByIdQuery(id).map(c => (c.kernelFoundBusyDate, c.dateAccessed)).update((None, dateAccessed))

  def updateKernelFoundBusyDate(id: Long, kernelFoundBusyDate: Instant, dateAccessed: Instant): DBIO[Int] =
    findByIdQuery(id)
      .map(c => (c.kernelFoundBusyDate, c.dateAccessed))
      .update((Some(kernelFoundBusyDate), dateAccessed))

  def clearKernelFoundBusyDateByProjectAndName(cloudContext: CloudContext,
                                               clusterName: RuntimeName,
                                               dateAccessed: Instant)(implicit ec: ExecutionContext): DBIO[Int] =
    clusterQuery.getActiveClusterByNameMinimal(cloudContext, clusterName) flatMap {
      case Some(c) => clusterQuery.clearKernelFoundBusyDate(c.id, dateAccessed)
      case None    => DBIO.successful(0)
    }

  def updateAutopauseThreshold(id: Long, autopauseThreshold: Int, dateAccessed: Instant): DBIO[Int] =
    findByIdQuery(id)
      .map(c => (c.autopauseThreshold, c.dateAccessed))
      .update((autopauseThreshold, dateAccessed))

  def updateWelder(id: Long, welderImage: RuntimeImage, dateAccessed: Instant)(
    implicit ec: ExecutionContext
  ): DBIO[Unit] =
    for {
      _ <- findByIdQuery(id).map(c => (c.welderEnabled, c.dateAccessed)).update((true, dateAccessed))
      _ <- clusterImageQuery.upsert(id, welderImage)
    } yield ()

  def setToRunning(id: Long, hostIp: IP, dateAccessed: Instant): DBIO[Int] =
    updateClusterStatusAndHostIp(id, RuntimeStatus.Running, Some(hostIp), dateAccessed)

  def setToStopping(id: Long, dateAccessed: Instant): DBIO[Int] =
    updateClusterStatusAndHostIp(id, RuntimeStatus.Stopping, None, dateAccessed)

  /* WARNING: The init bucket and SA key ID is secret to Leo, which means we don't unmarshal it.
   * This function should only be called at cluster creation time, when the init bucket doesn't exist.
   */
  private def marshalCluster(runtime: Runtime,
                             initBucket: Option[String],
                             workspaceId: Option[WorkspaceId] = None): ClusterRecord =
    ClusterRecord(
      id = 0, // DB AutoInc
      runtime.samResource.resourceId,
      runtime.runtimeName,
      runtime.asyncRuntimeFields.map(_.proxyHostName),
      runtime.cloudContext,
      runtime.asyncRuntimeFields.map(_.operationName.value),
      runtime.status,
      runtime.asyncRuntimeFields.flatMap(_.hostIp.map(_.asString)),
      runtime.userScriptUri,
      runtime.startUserScriptUri,
      initBucket,
      runtime.auditInfo,
      runtime.kernelFoundBusyDate,
      runtime.serviceAccount,
      runtime.asyncRuntimeFields.map(_.stagingBucket.value),
      runtime.autopauseThreshold,
      runtime.defaultClientId,
      runtime.welderEnabled,
      runtime.customEnvironmentVariables,
      runtime.runtimeConfigId,
      None,
      workspaceId
    )

  private def unmarshalMinimalCluster(
    clusterLabels: Seq[(ClusterRecord, Option[LabelRecord], Option[PatchRecord])]
  ): Seq[Runtime] = {
    // Call foldMap to aggregate a Seq[(ClusterRecord, LabelRecord)] returned by the query to a Map[ClusterRecord, Map[labelKey, labelValue]].
    // Note we use Chain instead of List inside the foldMap because the Chain monoid is much more efficient than the List monoid.
    // See: https://typelevel.org/cats/datatypes/chain.html
    val clusterLabelMap: Map[ClusterRecord, (Map[String, Chain[String]], Chain[PatchRecord])] =
      clusterLabels.toList.foldMap {
        case (clusterRecord, labelRecordOpt, patchRecordOpt) =>
          val labelMap = labelRecordOpt.map(labelRecord => labelRecord.key -> Chain(labelRecord.value)).toMap
          val patchList = patchRecordOpt.toList
          Map(clusterRecord -> (labelMap, Chain.fromSeq(patchList)))
      }

    // Unmarshal each (ClusterRecord, Map[labelKey, labelValue]) to a Cluster object
    clusterLabelMap.map {
      case (clusterRec, (labelMap, patch)) =>
        unmarshalCluster(clusterRec,
                         List.empty,
                         labelMap.view.mapValues(_.toList.toSet.head).toMap,
                         List.empty,
                         List.empty,
                         List.empty,
                         patch.toList)
    }.toSeq
  }

  private def unmarshalRunningCluster(clusterImages: Seq[(ClusterRecord, ClusterImageRecord)]): Seq[RunningRuntime] = {
    val clusterContainerMap: Map[RunningRuntime, Chain[RuntimeContainerServiceType]] = clusterImages.toList.foldMap {
      case (clusterRec, clusterImageRec) =>
        val containers = Chain.fromSeq(
          RuntimeContainerServiceType.imageTypeToRuntimeContainerServiceType.get(clusterImageRec.imageType).toSeq
        )
        Map(RunningRuntime(clusterRec.cloudContext, clusterRec.runtimeName, List.empty) -> containers)
    }

    clusterContainerMap.toSeq.map {
      case (runningCluster, containers) => runningCluster.copy(containers = containers.toList)
    }
  }

  private[leonardo] def unmarshalFullCluster(
    clusterRecords: Seq[
      (ClusterRecord,
       Option[ClusterErrorRecord],
       Option[LabelRecord],
       Option[ExtensionRecord],
       Option[ClusterImageRecord],
       Option[ScopeRecord],
       Option[PatchRecord])
    ]
  ): Seq[Runtime] = {
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
      case (clusterRecord, (errorRecords, labels, extensions, clusterImages, scopes, patch)) =>
        unmarshalCluster(
          clusterRecord,
          errorRecords.toList.groupBy(_.timestamp).map(_._2.head).toList,
          labels.view.mapValues(_.toList.toSet.head).toMap,
          extensions.toList,
          clusterImages.toList,
          scopes.toList,
          patch.toList
        )
    }.toSeq
  }

  private def unmarshalCluster(clusterRecord: ClusterRecord,
                               errors: List[ClusterErrorRecord],
                               labels: LabelMap,
                               userJupyterExtensionConfig: List[ExtensionRecord],
                               clusterImageRecords: List[ClusterImageRecord],
                               scopes: List[ScopeRecord],
                               patch: List[PatchRecord]): Runtime = {
    val name = clusterRecord.runtimeName
    val cloudContext = clusterRecord.cloudContext
    val dataprocInfo = (clusterRecord.googleId, clusterRecord.operationName, clusterRecord.stagingBucket).mapN {
      (googleId, operationName, stagingBucket) =>
        AsyncRuntimeFields(googleId,
                           OperationName(operationName),
                           GcsBucketName(stagingBucket),
                           clusterRecord.hostIp map IP)
    }
    val clusterImages = clusterImageRecords map clusterImageQuery.unmarshalClusterImage toSet
    val patchInProgress = patch.headOption match {
      case Some(patchRec) => patchRec.inProgress
      case None           => false
    }

    Runtime(
      clusterRecord.id,
      RuntimeSamResourceId(clusterRecord.internalId),
      name,
      serviceAccount = clusterRecord.serviceAccountInfo,
      asyncRuntimeFields = dataprocInfo,
      auditInfo = clusterRecord.auditInfo,
      kernelFoundBusyDate = clusterRecord.kernelFoundBusyDate,
      proxyUrl = Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase, cloudContext, name, clusterImages, labels),
      status = clusterRecord.status,
      labels = labels,
      userScriptUri = clusterRecord.userScriptUri,
      startUserScriptUri = clusterRecord.startUserScriptUri,
      errors = errors map clusterErrorQuery.unmarshallClusterErrorRecord,
      userJupyterExtensionConfig = extensionQuery.unmarshallExtensions(userJupyterExtensionConfig),
      autopauseThreshold = clusterRecord.autopauseThreshold,
      defaultClientId = clusterRecord.defaultClientId,
      allowStop = false,
      runtimeImages = clusterImages,
      scopes = scopeQuery.unmarshallScopes(scopes),
      welderEnabled = clusterRecord.welderEnabled,
      customEnvironmentVariables = clusterRecord.customClusterEnvironmentVariables,
      runtimeConfigId = clusterRecord.runtimeConfigId.value,
      patchInProgress = patchInProgress,
      cloudContext = clusterRecord.cloudContext
    )
  }
}

final case class GetClusterKey(cloudContext: CloudContext, clusterName: RuntimeName, destroyedDate: Option[Instant])

final case class UpdateAsyncClusterCreationFields(initBucket: Option[GcsPath],
                                                  clusterId: Long,
                                                  asyncRuntimeFields: Option[AsyncRuntimeFields],
                                                  dateAccessed: Instant)

final case class SaveCluster(cluster: Runtime,
                             initBucket: Option[GcsPath] = None,
                             serviceAccountKeyId: Option[ServiceAccountKeyId] = None,
                             runtimeConfig: RuntimeConfig,
                             now: Instant,
                             workspaceId: Option[WorkspaceId] = None)
