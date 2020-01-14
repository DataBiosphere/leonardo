package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import cats.data.Chain
import cats.implicits._
import io.circe.syntax._
import io.circe.{Json, Printer}
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster.LabelMap
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{
  parseGcsPath,
  GcsBucketName,
  GcsPath,
  GcsPathSupport,
  GoogleProject,
  ServiceAccountKey,
  ServiceAccountKeyId
}

final case class ClusterRecord(id: Long,
                               internalId: String,
                               clusterName: String,
                               googleId: Option[UUID],
                               googleProject: String,
                               operationName: Option[String],
                               status: String,
                               hostIp: Option[String],
                               jupyterExtensionUri: Option[GcsPath],
                               jupyterUserScriptUri: Option[UserScriptPath],
                               jupyterStartUserScriptUri: Option[UserScriptPath],
                               initBucket: Option[String],
                               auditInfo: AuditInfoRecord,
                               machineConfig: MachineConfigRecord,
                               properties: Map[String, String],
                               serviceAccountInfo: ServiceAccountInfoRecord,
                               stagingBucket: Option[String],
                               autopauseThreshold: Int,
                               defaultClientId: Option[String],
                               stopAfterCreation: Boolean,
                               welderEnabled: Boolean,
                               customClusterEnvironmentVariables: Map[String, String])

final case class MachineConfigRecord(numberOfWorkers: Int,
                                     masterMachineType: String,
                                     masterDiskSize: Int,
                                     workerMachineType: Option[String],
                                     workerDiskSize: Option[Int],
                                     numberOfWorkerLocalSsds: Option[Int],
                                     numberOfPreemptibleWorkers: Option[Int])

final case class ServiceAccountInfoRecord(clusterServiceAccount: Option[String],
                                          notebookServiceAccount: Option[String],
                                          serviceAccountKeyId: Option[String])

final case class AuditInfoRecord(creator: String,
                                 createdDate: Timestamp,
                                 destroyedDate: Timestamp,
                                 dateAccessed: Timestamp,
                                 kernelFoundBusyDate: Option[Timestamp])

trait ClusterComponent extends LeoComponent {
  this: LabelComponent
    with ClusterErrorComponent
    with InstanceComponent
    with ExtensionComponent
    with ClusterImageComponent
    with ScopeComponent =>

  import profile.api._

  // mysql 5.6 doesns't support json. Hence writing properties field as string in json format
  implicit private val jsValueMappedColumnType: BaseColumnType[Json] =
    MappedColumnType
      .base[Json, String](_.printWith(Printer.noSpaces), s => io.circe.parser.parse(s).fold(e => throw e, identity))
  implicit private val userScriptPathMappedColumnType: BaseColumnType[UserScriptPath] =
    MappedColumnType
      .base[UserScriptPath, String](_.asString, s => UserScriptPath.stringToUserScriptPath(s).fold(e => throw e, identity))
  implicit private val gsPathMappedColumnType: BaseColumnType[GcsPath] =
    MappedColumnType
      .base[GcsPath, String](_.toUri, s => parseGcsPath(s).fold(e => throw new Exception(e.toString()), identity))

  class ClusterTable(tag: Tag) extends Table[ClusterRecord](tag, "CLUSTER") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def internalId = column[String]("internalId", O.Length(254))
    def clusterName = column[String]("clusterName", O.Length(254))
    def googleId = column[Option[UUID]]("googleId")
    def googleProject = column[String]("googleProject", O.Length(254))
    def clusterServiceAccount = column[Option[String]]("clusterServiceAccount", O.Length(254))
    def notebookServiceAccount = column[Option[String]]("notebookServiceAccount", O.Length(254))
    def numberOfWorkers = column[Int]("numberOfWorkers")
    def masterMachineType = column[String]("masterMachineType", O.Length(254))
    def masterDiskSize = column[Int]("masterDiskSize")
    def workerMachineType = column[Option[String]]("workerMachineType", O.Length(254))
    def workerDiskSize = column[Option[Int]]("workerDiskSize")
    def numberOfWorkerLocalSSDs = column[Option[Int]]("numberOfWorkerLocalSSDs")
    def numberOfPreemptibleWorkers = column[Option[Int]]("numberOfPreemptibleWorkers")
    def operationName = column[Option[String]]("operationName", O.Length(254))
    def status = column[String]("status", O.Length(254))
    def hostIp = column[Option[String]]("hostIp", O.Length(254))
    def creator = column[String]("creator", O.Length(254))
    def createdDate = column[Timestamp]("createdDate", O.SqlType("TIMESTAMP(6)"))
    def destroyedDate = column[Timestamp]("destroyedDate", O.SqlType("TIMESTAMP(6)"))
    def jupyterExtensionUri = column[Option[GcsPath]]("jupyterExtensionUri", O.Length(1024))
    def jupyterUserScriptUri = column[Option[UserScriptPath]]("jupyterUserScriptUri", O.Length(1024))
    def jupyterStartUserScriptUri = column[Option[UserScriptPath]]("jupyterStartUserScriptUri", O.Length(1024))
    def initBucket = column[Option[String]]("initBucket", O.Length(1024))
    def serviceAccountKeyId = column[Option[String]]("serviceAccountKeyId", O.Length(254))
    def stagingBucket = column[Option[String]]("stagingBucket", O.Length(254))
    def dateAccessed = column[Timestamp]("dateAccessed", O.SqlType("TIMESTAMP(6)"))
    def autopauseThreshold = column[Int]("autopauseThreshold")
    def kernelFoundBusyDate = column[Option[Timestamp]]("kernelFoundBusyDate", O.SqlType("TIMESTAMP(6)"))
    def defaultClientId = column[Option[String]]("defaultClientId", O.Length(1024))
    def stopAfterCreation = column[Boolean]("stopAfterCreation")
    def welderEnabled = column[Boolean]("welderEnabled")
    def properties = column[Option[Json]]("properties")
    def customClusterEnvironmentVariables = column[Option[Json]]("customClusterEnvironmentVariables")

    def uniqueKey = index("IDX_CLUSTER_UNIQUE", (googleProject, clusterName, destroyedDate), unique = true)

    // Can't use the shorthand
    //   def * = (...) <> (ClusterRecord.tupled, ClusterRecord.unapply)
    // because CLUSTER has more than 22 columns.
    // So we split ClusterRecord into multiple case classes and bind them to slick in the following way.
    def * =
      (
        id,
        internalId,
        clusterName,
        googleId,
        googleProject,
        operationName,
        status,
        hostIp,
        jupyterExtensionUri,
        jupyterUserScriptUri,
        jupyterStartUserScriptUri,
        initBucket,
        (creator, createdDate, destroyedDate, dateAccessed, kernelFoundBusyDate),
        (numberOfWorkers,
         masterMachineType,
         masterDiskSize,
         workerMachineType,
         workerDiskSize,
         numberOfWorkerLocalSSDs,
         numberOfPreemptibleWorkers),
        (clusterServiceAccount, notebookServiceAccount, serviceAccountKeyId),
        stagingBucket,
        autopauseThreshold,
        defaultClientId,
        stopAfterCreation,
        welderEnabled,
        properties,
        customClusterEnvironmentVariables
      ).shaped <> ({
        case (id,
              internalId,
              clusterName,
              googleId,
              googleProject,
              operationName,
              status,
              hostIp,
              jupyterExtensionUri,
              jupyterUserScriptUri,
              jupyterStartUserScriptUri,
              initBucket,
              auditInfo,
              machineConfig,
              serviceAccountInfo,
              stagingBucket,
              autopauseThreshold,
              defaultClientId,
              stopAfterCreation,
              welderEnabled,
              properties,
              customClusterEnvironmentVariables) =>
          ClusterRecord(
            id,
            internalId,
            clusterName,
            googleId,
            googleProject,
            operationName,
            status,
            hostIp,
            jupyterExtensionUri,
            jupyterUserScriptUri,
            jupyterStartUserScriptUri,
            initBucket,
            AuditInfoRecord.tupled.apply(auditInfo),
            MachineConfigRecord.tupled.apply(machineConfig),
            properties
              .map(
                x =>
                  x.as[Map[String, String]]
                    .fold(e => throw new RuntimeException(s"fail to read `properties` field due to ${e.getMessage}"),
                          identity)
              )
              .getOrElse(Map.empty), //in theory, throw should never happen
            ServiceAccountInfoRecord.tupled.apply(serviceAccountInfo),
            stagingBucket,
            autopauseThreshold,
            defaultClientId,
            stopAfterCreation,
            welderEnabled,
            customClusterEnvironmentVariables
              .map(
                x =>
                  x.as[Map[String, String]]
                    .fold(e =>
                            throw new RuntimeException(
                              s"fail to read `customClusterEnvironmentVariables` field due to ${e.getMessage}"
                            ),
                          identity)
              )
              .getOrElse(Map.empty) //in theory, throw should never happen
          )
      }, { c: ClusterRecord =>
        def mc(_mc: MachineConfigRecord) = MachineConfigRecord.unapply(_mc).get
        def sa(_sa: ServiceAccountInfoRecord) = ServiceAccountInfoRecord.unapply(_sa).get
        def ai(_ai: AuditInfoRecord) = AuditInfoRecord.unapply(_ai).get
        Some(
          (
            c.id,
            c.internalId,
            c.clusterName,
            c.googleId,
            c.googleProject,
            c.operationName,
            c.status,
            c.hostIp,
            c.jupyterExtensionUri,
            c.jupyterUserScriptUri,
            c.jupyterStartUserScriptUri,
            c.initBucket,
            ai(c.auditInfo),
            mc(c.machineConfig),
            sa(c.serviceAccountInfo),
            c.stagingBucket,
            c.autopauseThreshold,
            c.defaultClientId,
            c.stopAfterCreation,
            c.welderEnabled,
            if (c.properties.isEmpty) None else Some(c.properties.asJson),
            if (c.customClusterEnvironmentVariables.isEmpty) None else Some(c.customClusterEnvironmentVariables.asJson)
          )
        )
      })
  }

  object clusterQuery extends TableQuery(new ClusterTable(_)) {
    def save(cluster: Cluster,
             initBucket: Option[GcsPath] = None,
             serviceAccountKeyId: Option[ServiceAccountKeyId] = None): DBIO[Cluster] =
      for {
        clusterId <- clusterQuery returning clusterQuery.map(_.id) += marshalCluster(cluster,
                                                                                     initBucket.map(_.toUri),
                                                                                     serviceAccountKeyId)
        _ <- labelQuery.saveAllForCluster(clusterId, cluster.labels)
        _ <- instanceQuery.saveAllForCluster(clusterId, cluster.instances.toSeq)
        _ <- extensionQuery.saveAllForCluster(clusterId, cluster.userJupyterExtensionConfig)
        _ <- clusterImageQuery.saveAllForCluster(clusterId, cluster.clusterImages.toSeq)
        _ <- scopeQuery.saveAllForCluster(clusterId, cluster.scopes)
      } yield cluster.copy(id = clusterId)

    def mergeInstances(cluster: Cluster): DBIO[Cluster] =
      clusterQuery.filter(_.id === cluster.id).result.headOption.flatMap {
        case Some(rec) => instanceQuery.mergeForCluster(rec.id, cluster.instances.toSeq).map(_ => cluster)
        case None      => DBIO.successful(cluster)
      }

    // note: list* methods don't query the INSTANCE table

    def listWithLabels(): DBIO[Seq[Cluster]] =
      clusterLabelQuery.result.map(unmarshalMinimalCluster)

    def listActiveWithLabels(): DBIO[Seq[Cluster]] =
      clusterLabelQuery.filter { _._1.status inSetBind ClusterStatus.activeStatuses.map(_.toString) }.result map {
        recs =>
          unmarshalMinimalCluster(recs)
      }

    def listMonitoredClusterOnly(): DBIO[Seq[Cluster]] =
      clusterQuery.filter { _.status inSetBind ClusterStatus.monitoredStatuses.map(_.toString) }.result map { recs =>
        recs.map(rec => unmarshalCluster(rec, Seq.empty, List.empty, Map.empty, List.empty, List.empty, List.empty))
      }

    def listMonitored(): DBIO[Seq[Cluster]] =
      clusterLabelQuery.filter { _._1.status inSetBind ClusterStatus.monitoredStatuses.map(_.toString) }.result map {
        recs =>
          unmarshalMinimalCluster(recs)
      }

    def listRunningOnly(): DBIO[Seq[Cluster]] =
      clusterQuery.filter { _.status === ClusterStatus.Running.toString }.result map { recs =>
        recs.map(rec => unmarshalCluster(rec, Seq.empty, List.empty, Map.empty, List.empty, List.empty, List.empty))
      }

    def countActiveByClusterServiceAccount(clusterServiceAccount: WorkbenchEmail) =
      clusterQuery
        .filter { _.clusterServiceAccount === Option(clusterServiceAccount.value) }
        .filter { _.status inSetBind ClusterStatus.activeStatuses.map(_.toString) }
        .length
        .result

    def countActiveByProject(googleProject: GoogleProject) =
      clusterQuery
        .filter { _.googleProject === googleProject.value }
        .filter { _.status inSetBind ClusterStatus.activeStatuses.map(_.toString) }
        .length
        .result

    // find* and get* methods do query the INSTANCE table

    def getActiveClusterByName(project: GoogleProject, name: ClusterName): DBIO[Option[Cluster]] =
      fullClusterQueryByUniqueKey(project, name, Some(dummyDate)).result map { recs =>
        unmarshalFullCluster(recs).headOption
      }

    def getDeletingClusterByName(project: GoogleProject, name: ClusterName): DBIO[Option[Cluster]] =
      fullClusterQueryByUniqueKey(project, name, Some(dummyDate)).filter {
        _._1.status === ClusterStatus.Deleting.toString
      }.result map { recs =>
        unmarshalFullCluster(recs).headOption
      }

    def getActiveClusterByNameMinimal(project: GoogleProject, name: ClusterName): DBIO[Option[Cluster]] =
      clusterQuery
        .filter { _.googleProject === project.value }
        .filter { _.clusterName === name.value }
        .filter { _.destroyedDate === Timestamp.from(dummyDate) }
        .result
        .map { recs =>
          recs.headOption.map { clusterRec =>
            unmarshalCluster(clusterRec, Seq.empty, List.empty, Map.empty, List.empty, List.empty, List.empty)
          }
        }

    def getClusterById(id: Long): DBIO[Option[Cluster]] =
      fullClusterQueryById(id).result map { recs =>
        unmarshalFullCluster(recs).headOption
      }

    def getActiveClusterInternalIdByName(project: GoogleProject, name: ClusterName): DBIO[Option[ClusterInternalId]] =
      clusterQuery
        .filter { _.googleProject === project.value }
        .filter { _.clusterName === name.value }
        .filter { _.destroyedDate === Timestamp.from(dummyDate) }
        .result
        .map { recs =>
          recs.headOption.map { clusterRec =>
            ClusterInternalId(clusterRec.internalId)
          }
        }

    private[leonardo] def getIdByUniqueKey(cluster: Cluster): DBIO[Option[Long]] =
      getIdByUniqueKey(cluster.googleProject, cluster.clusterName, cluster.auditInfo.destroyedDate)

    private[leonardo] def getIdByUniqueKey(googleProject: GoogleProject,
                                           clusterName: ClusterName,
                                           destroyedDateOpt: Option[Instant]): DBIO[Option[Long]] =
      getClusterByUniqueKey(googleProject, clusterName, destroyedDateOpt).map(_.map(_.id))

    // Convenience method for tests, in several of which we define a cluster and later on need
    // to retrieve its updated status, etc. but don't know its id to look up
    private[leonardo] def getClusterByUniqueKey(cluster: Cluster): DBIO[Option[Cluster]] =
      getClusterByUniqueKey(cluster.googleProject, cluster.clusterName, cluster.auditInfo.destroyedDate)

    private[leonardo] def getClusterByUniqueKey(googleProject: GoogleProject,
                                                clusterName: ClusterName,
                                                destroyedDateOpt: Option[Instant]): DBIO[Option[Cluster]] =
      fullClusterQueryByUniqueKey(googleProject, clusterName, destroyedDateOpt).result map { recs =>
        unmarshalFullCluster(recs).headOption
      }

    def getInitBucket(project: GoogleProject, name: ClusterName): DBIO[Option[GcsPath]] =
      clusterQuery
        .filter { _.googleProject === project.value }
        .filter { _.clusterName === name.value }
        .map(_.initBucket)
        .result
        .map { recs =>
          recs.headOption.flatten.flatMap(head => parseGcsPath(head).toOption)
        }

    def getStagingBucket(project: GoogleProject, name: ClusterName): DBIO[Option[GcsPath]] =
      clusterQuery
        .filter { _.googleProject === project.value }
        .filter { _.clusterName === name.value }
        .map(_.stagingBucket)
        .result
        // staging bucket is saved as a bucket name rather than a path
        .map { recs =>
          recs.headOption.flatten.flatMap(head => parseGcsPath("gs://" + head + "/").toOption)
        }

    def getServiceAccountKeyId(project: GoogleProject, name: ClusterName): DBIO[Option[ServiceAccountKeyId]] =
      clusterQuery
        .filter { _.googleProject === project.value }
        .filter { _.clusterName === name.value }
        .map(_.serviceAccountKeyId)
        .result
        .map { recs =>
          recs.headOption.flatten.map(ServiceAccountKeyId)
        }

    def getClusterStatus(id: Long): DBIO[Option[ClusterStatus]] =
      findByIdQuery(id).map(_.status).result.headOption map { statusOpt =>
        statusOpt map ClusterStatus.withName
      }

    def getClustersReadyToAutoFreeze(): DBIO[Seq[Cluster]] = {
      val now = SimpleFunction.nullary[Timestamp]("NOW")
      val tsdiff = SimpleFunction.ternary[String, Timestamp, Timestamp, Int]("TIMESTAMPDIFF")
      val minute = SimpleLiteral[String]("MINUTE")

      val baseQuery = clusterQuery
        .filter { _.autopauseThreshold =!= autoPauseOffValue }
        .filter { record =>
          tsdiff(minute, record.dateAccessed, now) >= record.autopauseThreshold
        }
        .filter(_.status inSetBind ClusterStatus.stoppableStatuses.map(_.toString))

      fullClusterQuery(baseQuery).result map { recs =>
        unmarshalFullCluster(recs)
      }
    }

    def markPendingDeletion(id: Long, dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(id)
        .map(c => (c.status, c.hostIp, c.dateAccessed))
        .update((ClusterStatus.Deleting.toString, None, Timestamp.from(dateAccessed)))

    def completeDeletion(id: Long, destroyedDate: Instant): DBIO[Int] =
      findByIdQuery(id)
        .map(c => (c.destroyedDate, c.status, c.hostIp, c.dateAccessed))
        .update((Timestamp.from(destroyedDate), ClusterStatus.Deleted.toString, None, Timestamp.from(destroyedDate)))

    def updateClusterStatusAndHostIp(id: Long,
                                     status: ClusterStatus,
                                     hostIp: Option[IP],
                                     dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(id)
        .map(c => (c.status, c.hostIp, c.dateAccessed))
        .update((status.toString, hostIp.map(_.value), Timestamp.from(dateAccessed)))

    def updateClusterHostIp(id: Long, hostIp: Option[IP], dateAccessed: Instant): DBIO[Int] =
      clusterQuery
        .filter { _.id === id }
        .map(c => (c.hostIp, c.dateAccessed))
        .update((hostIp.map(_.value), Timestamp.from(dateAccessed)))

    def updateAsyncClusterCreationFields(initBucket: Option[GcsPath],
                                         serviceAccountKey: Option[ServiceAccountKey],
                                         cluster: Cluster,
                                         dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(cluster.id)
        .map(c => (c.initBucket, c.serviceAccountKeyId, c.googleId, c.operationName, c.stagingBucket, c.dateAccessed))
        .update(
          (
            initBucket.map(_.toUri),
            serviceAccountKey.map(_.id.value),
            cluster.dataprocInfo.map(_.googleId),
            cluster.dataprocInfo.map(_.operationName.value),
            cluster.dataprocInfo.map(_.stagingBucket.value),
            Timestamp.from(dateAccessed)
          )
        )

    def clearAsyncClusterCreationFields(cluster: Cluster, dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(cluster.id)
        .map(c => (c.initBucket, c.serviceAccountKeyId, c.googleId, c.operationName, c.stagingBucket, c.dateAccessed))
        .update((None, None, None, None, None, Timestamp.from(dateAccessed)))

    def updateClusterStatus(id: Long, newStatus: ClusterStatus, dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(id).map(c => (c.status, c.dateAccessed)).update((newStatus.toString, Timestamp.from(dateAccessed)))

    // for testing only
    def updateClusterCreatedDate(id: Long, createdDate: Instant): DBIO[Int] =
      findByIdQuery(id).map(_.createdDate).update(Timestamp.from(createdDate))

    def updateDateAccessed(id: Long, dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(id)
        .filter { _.dateAccessed < Timestamp.from(dateAccessed) }
        .map(_.dateAccessed)
        .update(Timestamp.from(dateAccessed))

    def updateDateAccessedByProjectAndName(googleProject: GoogleProject,
                                           clusterName: ClusterName,
                                           dateAccessed: Instant): DBIO[Int] =
      clusterQuery.getActiveClusterByNameMinimal(googleProject, clusterName) flatMap {
        case Some(c) => clusterQuery.updateDateAccessed(c.id, dateAccessed)
        case None    => DBIO.successful(0)
      }

    def clearKernelFoundBusyDate(id: Long, dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(id).map(c => (c.kernelFoundBusyDate, c.dateAccessed)).update((None, Timestamp.from(dateAccessed)))

    def updateKernelFoundBusyDate(id: Long, kernelFoundBusyDate: Instant, dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(id)
        .map(c => (c.kernelFoundBusyDate, c.dateAccessed))
        .update((Option(Timestamp.from(kernelFoundBusyDate)), Timestamp.from(dateAccessed)))

    def clearKernelFoundBusyDateByProjectAndName(googleProject: GoogleProject,
                                                 clusterName: ClusterName,
                                                 dateAccessed: Instant): DBIO[Int] =
      clusterQuery.getActiveClusterByNameMinimal(googleProject, clusterName) flatMap {
        case Some(c) => clusterQuery.clearKernelFoundBusyDate(c.id, dateAccessed)
        case None    => DBIO.successful(0)
      }

    def updateAutopauseThreshold(id: Long, autopauseThreshold: Int, dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(id)
        .map(c => (c.autopauseThreshold, c.dateAccessed))
        .update((autopauseThreshold, Timestamp.from(dateAccessed)))

    def updateNumberOfWorkers(id: Long, numberOfWorkers: Int, dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(id)
        .map(c => (c.numberOfWorkers, c.dateAccessed))
        .update((numberOfWorkers, Timestamp.from(dateAccessed)))

    def updateNumberOfPreemptibleWorkers(id: Long,
                                         numberOfPreemptibleWorkers: Option[Int],
                                         dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(id)
        .map(c => (c.numberOfPreemptibleWorkers, c.dateAccessed))
        .update((numberOfPreemptibleWorkers, Timestamp.from(dateAccessed)))

    def updateMasterMachineType(id: Long, newMachineType: MachineType, dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(id)
        .map(c => (c.masterMachineType, c.dateAccessed))
        .update((newMachineType.value, Timestamp.from(dateAccessed)))

    def updateMasterDiskSize(id: Long, newSizeGb: Int, dateAccessed: Instant): DBIO[Int] =
      findByIdQuery(id).map(c => (c.masterDiskSize, c.dateAccessed)).update((newSizeGb, Timestamp.from(dateAccessed)))

    def updateWelder(id: Long, welderImage: ClusterImage, dateAccessed: Instant): DBIO[Unit] =
      for {
        _ <- findByIdQuery(id).map(c => (c.welderEnabled, c.dateAccessed)).update((true, Timestamp.from(dateAccessed)))
        _ <- clusterImageQuery.upsert(id, welderImage)
      } yield ()

    def setToRunning(id: Long, hostIp: IP, dateAccessed: Instant): DBIO[Int] =
      updateClusterStatusAndHostIp(id, ClusterStatus.Running, Some(hostIp), dateAccessed)

    def setToStopping(id: Long, dateAccessed: Instant): DBIO[Int] =
      updateClusterStatusAndHostIp(id, ClusterStatus.Stopping, None, dateAccessed)

    def listByLabels(labelMap: LabelMap,
                     includeDeleted: Boolean,
                     googleProjectOpt: Option[GoogleProject] = None): DBIO[Seq[Cluster]] = {
      val clusterStatusQuery =
        if (includeDeleted) clusterLabelQuery else clusterLabelQuery.filterNot { _._1.status === "Deleted" }
      val clusterStatusQueryByProject = googleProjectOpt match {
        case Some(googleProject) => clusterStatusQuery.filter { _._1.googleProject === googleProject.value }
        case None                => clusterStatusQuery
      }
      val query = if (labelMap.isEmpty) {
        clusterStatusQueryByProject
      } else {
        // The trick is to find all clusters that have _at least_ all the labels in labelMap.
        // In other words, for a given cluster, the labels provided in the query string must be
        // a subset of its labels in the DB. The following SQL achieves this:
        //
        // select c.*, l.*
        // from cluster c
        // left join label l on l.clusterId = c.id
        // where (
        //   select count(*) from label
        //   where clusterId = c.id and (key, value) in ${labelMap}
        // ) = ${labelMap.size}
        //
        clusterStatusQueryByProject.filter {
          case (cluster, _) =>
            labelQuery
              .filter {
                _.clusterId === cluster.id
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
      query.result.map(unmarshalMinimalCluster)
    }

    /* WARNING: The init bucket and SA key ID is secret to Leo, which means we don't unmarshal it.
     * This function should only be called at cluster creation time, when the init bucket doesn't exist.
     */
    private def marshalCluster(cluster: Cluster,
                               initBucket: Option[String],
                               serviceAccountKeyId: Option[ServiceAccountKeyId]): ClusterRecord =
      ClusterRecord(
        id = 0, // DB AutoInc
        cluster.internalId.value,
        cluster.clusterName.value,
        cluster.dataprocInfo.map(_.googleId),
        cluster.googleProject.value,
        cluster.dataprocInfo.map(_.operationName.value),
        cluster.status.toString,
        cluster.dataprocInfo.flatMap(_.hostIp.map(_.value)),
        cluster.jupyterExtensionUri,
        cluster.jupyterUserScriptUri,
        cluster.jupyterStartUserScriptUri,
        initBucket,
        AuditInfoRecord(
          cluster.auditInfo.creator.value,
          Timestamp.from(cluster.auditInfo.createdDate),
          marshalDestroyedDate(cluster.auditInfo.destroyedDate),
          Timestamp.from(cluster.auditInfo.dateAccessed),
          cluster.auditInfo.kernelFoundBusyDate.map(attemptedDate => Timestamp.from(attemptedDate))
        ),
        MachineConfigRecord(
          cluster.machineConfig.numberOfWorkers.get, //a cluster should always have numberOfWorkers defined
          cluster.machineConfig.masterMachineType.get, //a cluster should always have masterMachineType defined
          cluster.machineConfig.masterDiskSize.get, //a cluster should always have masterDiskSize defined
          cluster.machineConfig.workerMachineType,
          cluster.machineConfig.workerDiskSize,
          cluster.machineConfig.numberOfWorkerLocalSSDs,
          cluster.machineConfig.numberOfPreemptibleWorkers
        ),
        cluster.properties,
        ServiceAccountInfoRecord(
          cluster.serviceAccountInfo.clusterServiceAccount.map(_.value),
          cluster.serviceAccountInfo.notebookServiceAccount.map(_.value),
          serviceAccountKeyId.map(_.value)
        ),
        cluster.dataprocInfo.map(_.stagingBucket.value),
        cluster.autopauseThreshold,
        cluster.defaultClientId,
        cluster.stopAfterCreation,
        cluster.welderEnabled,
        cluster.customClusterEnvironmentVariables
      )

    private def unmarshalMinimalCluster(clusterLabels: Seq[(ClusterRecord, Option[LabelRecord])]): Seq[Cluster] = {
      // Call foldMap to aggregate a Seq[(ClusterRecord, LabelRecord)] returned by the query to a Map[ClusterRecord, Map[labelKey, labelValue]].
      // Note we use Chain instead of List inside the foldMap because the Chain monoid is much more efficient than the List monoid.
      // See: https://typelevel.org/cats/datatypes/chain.html
      val clusterLabelMap: Map[ClusterRecord, Map[String, Chain[String]]] = clusterLabels.toList.foldMap {
        case (clusterRecord, labelRecordOpt) =>
          val labelMap = labelRecordOpt.map(labelRecord => labelRecord.key -> Chain(labelRecord.value)).toMap
          Map(clusterRecord -> labelMap)
      }

      // Unmarshal each (ClusterRecord, Map[labelKey, labelValue]) to a Cluster object
      clusterLabelMap.map {
        case (clusterRec, labelMap) =>
          unmarshalCluster(clusterRec,
                           Seq.empty,
                           List.empty,
                           labelMap.mapValues(_.toList.toSet.head),
                           List.empty,
                           List.empty,
                           List.empty)
      }.toSeq
    }

    private def unmarshalFullCluster(
      clusterRecords: Seq[
        (ClusterRecord,
         Option[InstanceRecord],
         Option[ClusterErrorRecord],
         Option[LabelRecord],
         Option[ExtensionRecord],
         Option[ClusterImageRecord],
         Option[ScopeRecord])
      ]
    ): Seq[Cluster] = {
      // Call foldMap to aggregate a flat sequence of (cluster, instance, label) triples returned by the query
      // to a grouped (cluster -> (instances, labels)) structure.
      // Note we use Chain instead of List inside the foldMap because the Chain monoid is much more efficient than the List monoid.
      // See: https://typelevel.org/cats/datatypes/chain.html
      val clusterRecordMap: Map[ClusterRecord,
                                (Chain[InstanceRecord],
                                 Chain[ClusterErrorRecord],
                                 Map[String, Chain[String]],
                                 Chain[ExtensionRecord],
                                 Chain[ClusterImageRecord],
                                 Chain[ScopeRecord])] = clusterRecords.toList.foldMap {
        case (clusterRecord,
              instanceRecordOpt,
              errorRecordOpt,
              labelRecordOpt,
              extensionOpt,
              clusterImageOpt,
              scopeOpt) =>
          val instanceList = instanceRecordOpt.toList
          val labelMap = labelRecordOpt.map(labelRecordOpt => labelRecordOpt.key -> Chain(labelRecordOpt.value)).toMap
          val errorList = errorRecordOpt.toList
          val extList = extensionOpt.toList
          val clusterImageList = clusterImageOpt.toList
          val scopeList = scopeOpt.toList
          Map(
            clusterRecord -> (Chain.fromSeq(instanceList), Chain.fromSeq(errorList), labelMap, Chain
              .fromSeq(extList), Chain.fromSeq(clusterImageList), Chain.fromSeq(scopeList))
          )
      }

      clusterRecordMap.map {
        case (clusterRecord, (instanceRecords, errorRecords, labels, extensions, clusterImages, scopes)) =>
          unmarshalCluster(
            clusterRecord,
            instanceRecords.toList,
            errorRecords.toList.groupBy(_.timestamp).map(_._2.head).toList,
            labels.mapValues(_.toList.toSet.head),
            extensions.toList,
            clusterImages.toList,
            scopes.toList
          )
      }.toSeq
    }

    private def unmarshalCluster(clusterRecord: ClusterRecord,
                                 instanceRecords: Seq[InstanceRecord],
                                 errors: List[ClusterErrorRecord],
                                 labels: LabelMap,
                                 userJupyterExtensionConfig: List[ExtensionRecord],
                                 clusterImageRecords: List[ClusterImageRecord],
                                 scopes: List[ScopeRecord]): Cluster = {
      val name = ClusterName(clusterRecord.clusterName)
      val project = GoogleProject(clusterRecord.googleProject)
      val machineConfig = MachineConfig(
        Some(clusterRecord.machineConfig.numberOfWorkers),
        Some(clusterRecord.machineConfig.masterMachineType),
        Some(clusterRecord.machineConfig.masterDiskSize),
        clusterRecord.machineConfig.workerMachineType,
        clusterRecord.machineConfig.workerDiskSize,
        clusterRecord.machineConfig.numberOfWorkerLocalSsds,
        clusterRecord.machineConfig.numberOfPreemptibleWorkers
      )
      val serviceAccountInfo = ServiceAccountInfo(
        clusterRecord.serviceAccountInfo.clusterServiceAccount.map(WorkbenchEmail),
        clusterRecord.serviceAccountInfo.notebookServiceAccount.map(WorkbenchEmail)
      )
      val dataprocInfo = (clusterRecord.googleId, clusterRecord.operationName, clusterRecord.stagingBucket).mapN {
        (googleId, operationName, stagingBucket) =>
          DataprocInfo(googleId,
                       OperationName(operationName),
                       GcsBucketName(stagingBucket),
                       clusterRecord.hostIp map IP)
      }
      val auditInfo = AuditInfo(
        WorkbenchEmail(clusterRecord.auditInfo.creator),
        clusterRecord.auditInfo.createdDate.toInstant,
        unmarshalDestroyedDate(clusterRecord.auditInfo.destroyedDate),
        clusterRecord.auditInfo.dateAccessed.toInstant,
        clusterRecord.auditInfo.kernelFoundBusyDate.map(_.toInstant)
      )
      val clusterImages = clusterImageRecords map ClusterComponent.this.clusterImageQuery.unmarshalClusterImage toSet

      Cluster(
        clusterRecord.id,
        ClusterInternalId(clusterRecord.internalId),
        name,
        project,
        serviceAccountInfo,
        dataprocInfo,
        auditInfo,
        machineConfig,
        clusterRecord.properties,
        Cluster.getClusterUrl(project, name, clusterImages, labels),
        ClusterStatus.withName(clusterRecord.status),
        labels,
        clusterRecord.jupyterExtensionUri,
        clusterRecord.jupyterUserScriptUri,
        clusterRecord.jupyterStartUserScriptUri,
        errors map clusterErrorQuery.unmarshallClusterErrorRecord,
        instanceRecords map ClusterComponent.this.instanceQuery.unmarshalInstance toSet,
        ClusterComponent.this.extensionQuery.unmarshallExtensions(userJupyterExtensionConfig),
        clusterRecord.autopauseThreshold,
        clusterRecord.defaultClientId,
        clusterRecord.stopAfterCreation,
        clusterImages,
        ClusterComponent.this.scopeQuery.unmarshallScopes(scopes),
        clusterRecord.welderEnabled,
        clusterRecord.customClusterEnvironmentVariables
      )
    }
  }

  // just clusters and labels: no instances, extensions, etc.
  //   select * from cluster c
  //   left join label l on c.id = l.clusterId
  val clusterLabelQuery: Query[(ClusterTable, Rep[Option[LabelTable]]), (ClusterRecord, Option[LabelRecord]), Seq] = {
    for {
      (cluster, label) <- clusterQuery joinLeft labelQuery on (_.id === _.clusterId)
    } yield (cluster, label)
  }

  def fullClusterQueryByUniqueKey(googleProject: GoogleProject,
                                  clusterName: ClusterName,
                                  destroyedDateOpt: Option[Instant]) = {
    val destroyedDate = destroyedDateOpt.getOrElse(dummyDate)
    val baseQuery = clusterQuery
      .filter(_.googleProject === googleProject.value)
      .filter(_.clusterName === clusterName.value)
      .filter(_.destroyedDate === Timestamp.from(destroyedDate))

    fullClusterQuery(baseQuery)
  }

  private def fullClusterQueryById(id: Long) =
    fullClusterQuery(findByIdQuery(id))

  private def fullClusterQuery(
    baseClusterQuery: Query[ClusterTable, ClusterRecord, Seq]
  ): Query[(ClusterTable,
            Rep[Option[InstanceTable]],
            Rep[Option[ClusterErrorTable]],
            Rep[Option[LabelTable]],
            Rep[Option[ExtensionTable]],
            Rep[Option[ClusterImageTable]],
            Rep[Option[ScopeTable]]),
           (ClusterRecord,
            Option[InstanceRecord],
            Option[ClusterErrorRecord],
            Option[LabelRecord],
            Option[ExtensionRecord],
            Option[ClusterImageRecord],
            Option[ScopeRecord]),
           Seq] =
    for {
      ((((((cluster, instance), error), label), extension), image), scopes) <- baseClusterQuery joinLeft
        instanceQuery on (_.id === _.clusterId) joinLeft
        clusterErrorQuery on (_._1.id === _.clusterId) joinLeft
        labelQuery on (_._1._1.id === _.clusterId) joinLeft
        extensionQuery on (_._1._1._1.id === _.clusterId) joinLeft
        clusterImageQuery on (_._1._1._1._1.id === _.clusterId) joinLeft
        scopeQuery on (_._1._1._1._1._1.id === _.clusterId)
    } yield (cluster, instance, error, label, extension, image, scopes)

  private def findByIdQuery(id: Long): Query[ClusterTable, ClusterRecord, Seq] =
    clusterQuery.filter { _.id === id }
}
