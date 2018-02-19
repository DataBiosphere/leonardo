package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import java.sql.Timestamp
import java.util.UUID

import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster.LabelMap
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsPath, GcsPathSupport, GoogleProject, ServiceAccountKeyId, parseGcsPath}

case class ClusterRecord(id: Long,
                         clusterName: String,
                         googleId: UUID,
                         googleProject: String,
                         operationName: String,
                         status: String,
                         hostIp: Option[String],
                         creator: String,
                         createdDate: Timestamp,
                         destroyedDate: Timestamp,
                         jupyterExtensionUri: Option[String],
                         jupyterUserScriptUri: Option[String],
                         initBucket: String,
                         machineConfig: MachineConfigRecord,
                         serviceAccountInfo: ServiceAccountInfoRecord,
                         stagingBucket: Option[String]
                        )

case class MachineConfigRecord(numberOfWorkers: Int,
                               masterMachineType: String,
                               masterDiskSize: Int,
                               workerMachineType: Option[String],
                               workerDiskSize: Option[Int],
                               numberOfWorkerLocalSsds: Option[Int],
                               numberOfPreemptibleWorkers: Option[Int])

case class ServiceAccountInfoRecord(clusterServiceAccount: Option[String],
                                    notebookServiceAccount: Option[String],
                                    serviceAccountKeyId: Option[String])

trait ClusterComponent extends LeoComponent {
  this: LabelComponent with InstanceComponent =>

  import profile.api._

  class ClusterTable(tag: Tag) extends Table[ClusterRecord](tag, "CLUSTER") {
    def id =                          column[Long]              ("id",                    O.PrimaryKey, O.AutoInc)
    def clusterName =                 column[String]            ("clusterName",           O.Length(254))
    def googleId =                    column[UUID]              ("googleId",              O.Unique)
    def googleProject =               column[String]            ("googleProject",         O.Length(254))
    def clusterServiceAccount =       column[Option[String]]    ("clusterServiceAccount", O.Length(254))
    def notebookServiceAccount =      column[Option[String]]    ("notebookServiceAccount", O.Length(254))
    def numberOfWorkers =             column[Int]               ("numberOfWorkers")
    def masterMachineType =           column[String]            ("masterMachineType",     O.Length(254))
    def masterDiskSize =              column[Int]               ("masterDiskSize")
    def workerMachineType =           column[Option[String]]    ("workerMachineType",     O.Length(254))
    def workerDiskSize =              column[Option[Int]]       ("workerDiskSize")
    def numberOfWorkerLocalSSDs =     column[Option[Int]]       ("numberOfWorkerLocalSSDs")
    def numberOfPreemptibleWorkers =  column[Option[Int]]       ("numberOfPreemptibleWorkers")
    def operationName =               column[String]            ("operationName",         O.Length(254))
    def status =                      column[String]            ("status",                O.Length(254))
    def hostIp =                      column[Option[String]]    ("hostIp",                O.Length(254))
    def creator =                     column[String]            ("creator",                O.Length(254))
    def createdDate =                 column[Timestamp]         ("createdDate",           O.SqlType("TIMESTAMP(6)"))
    def destroyedDate =               column[Timestamp]         ("destroyedDate",         O.SqlType("TIMESTAMP(6)"))
    def jupyterExtensionUri =         column[Option[String]]    ("jupyterExtensionUri",   O.Length(1024))
    def jupyterUserScriptUri =        column[Option[String]]    ("jupyterUserScriptUri",     O.Length(1024))
    def initBucket =                  column[String]            ("initBucket",            O.Length(1024))
    def serviceAccountKeyId =         column[Option[String]]    ("serviceAccountKeyId",   O.Length(254))
    def stagingBucket =               column[Option[String]]    ("stagingBucket",         O.Length(254))

    def uniqueKey = index("IDX_CLUSTER_UNIQUE", (googleProject, clusterName), unique = true)

    // Can't use the shorthand
    //   def * = (...) <> (ClusterRecord.tupled, ClusterRecord.unapply)
    // because CLUSTER has more than 22 columns.
    // So we split ClusterRecord into multiple case classes and bind them to slick in the following way.
    def * = (
      id, clusterName, googleId, googleProject, operationName, status, hostIp, creator,
      createdDate, destroyedDate, jupyterExtensionUri, jupyterUserScriptUri, initBucket,
      (numberOfWorkers, masterMachineType, masterDiskSize, workerMachineType, workerDiskSize, numberOfWorkerLocalSSDs, numberOfPreemptibleWorkers),
      (clusterServiceAccount, notebookServiceAccount, serviceAccountKeyId), stagingBucket
    ).shaped <> ({
      case (id, clusterName, googleId, googleProject, operationName, status, hostIp, creator,
            createdDate, destroyedDate, jupyterExtensionUri, jupyterUserScriptUri, initBucket, machineConfig, serviceAccountInfo, stagingBucket) =>
        ClusterRecord(
          id, clusterName, googleId, googleProject, operationName, status, hostIp, creator,
          createdDate, destroyedDate, jupyterExtensionUri, jupyterUserScriptUri, initBucket,
          MachineConfigRecord.tupled.apply(machineConfig),
          ServiceAccountInfoRecord.tupled.apply(serviceAccountInfo),
          stagingBucket)
    }, { c: ClusterRecord =>
      def mc(_mc: MachineConfigRecord) = MachineConfigRecord.unapply(_mc).get
      def sa(_sa: ServiceAccountInfoRecord) = ServiceAccountInfoRecord.unapply(_sa).get
      Some((
        c.id, c.clusterName, c.googleId, c.googleProject, c.operationName, c.status, c.hostIp, c.creator,
        c.createdDate, c.destroyedDate, c.jupyterExtensionUri, c.jupyterUserScriptUri, c.initBucket,
        mc(c.machineConfig), sa(c.serviceAccountInfo), c.stagingBucket
      ))
    })
  }

  object clusterQuery extends TableQuery(new ClusterTable(_)) {

    def save(cluster: Cluster, initBucket: GcsPath, serviceAccountKeyId: Option[ServiceAccountKeyId]): DBIO[Cluster] = {
      for {
        clusterId <- clusterQuery returning clusterQuery.map(_.id) += marshalCluster(cluster, initBucket.toUri, serviceAccountKeyId)
        _ <- labelQuery.saveAllForCluster(clusterId, cluster.labels)
        _ <- instanceQuery.saveAllForCluster(clusterId, cluster.instances.toSeq)
      } yield cluster
    }

    def upsertInstances(cluster: Cluster): DBIO[Cluster] = {
      clusterQuery.filter(_.googleId === cluster.googleId).result.headOption.flatMap {
        case Some(rec) => instanceQuery.upsertAllForCluster(rec.id, cluster.instances.toSeq).map(_ => cluster)
        case None => DBIO.successful(cluster)
      }
    }

    // note: list* methods don't query the INSTANCE table

    def list(): DBIO[Seq[Cluster]] = {
      clusterQueryWithLabels.result.map(unmarshalClustersWithLabels)
    }

    def listActive(): DBIO[Seq[Cluster]] = {
      clusterQueryWithLabels.filter { _._1.status inSetBind ClusterStatus.activeStatuses.map(_.toString) }.result map { recs =>
        unmarshalClustersWithLabels(recs)
      }
    }

    def listMonitored(): DBIO[Seq[Cluster]] = {
      clusterQueryWithLabels.filter { _._1.status inSetBind ClusterStatus.monitoredStatuses.map(_.toString) }.result map { recs =>
        unmarshalClustersWithLabels(recs)
      }
    }

    def countByClusterServiceAccountAndStatus(clusterServiceAccount: WorkbenchEmail, status: ClusterStatus) = {
      clusterQuery
        .filter { _.clusterServiceAccount === Option(clusterServiceAccount.value) }
        .filter { _.status === status.toString }
        .length
        .result
    }

    // find* and get* methods do query the INSTANCE table

    def findByName(project: GoogleProject, name: ClusterName) = {
      clusterQueryWithInstancesAndLabels.filter { _._1.googleProject === project.value }.filter { _._1.clusterName === name.value }
    }

    def getClusterByName(project: GoogleProject, name: ClusterName): DBIO[Option[Cluster]] = {
      findByName(project, name).result map { recs =>
        unmarshalClustersWithInstancesAndLabels(recs).headOption
      }
    }

    def getActiveClusterByName(project: GoogleProject, name: ClusterName): DBIO[Option[Cluster]] = {
      clusterQueryWithInstancesAndLabels
        .filter { _._1.googleProject === project.value }
        .filter { _._1.clusterName === name.value }
        .filter{_._1.destroyedDate === Timestamp.from(dummyDate)}
        .result map { recs =>
        unmarshalClustersWithInstancesAndLabels(recs).headOption
      }
    }

    def getDeletingClusterByName(project: GoogleProject, name: ClusterName): DBIO[Option[Cluster]] = {
      clusterQueryWithInstancesAndLabels
        .filter { _._1.googleProject === project.value }
        .filter { _._1.clusterName === name.value }
        .filter{_._1.status === ClusterStatus.Deleting.toString}
        .result map { recs =>
        unmarshalClustersWithInstancesAndLabels(recs).headOption
      }
    }

    def getByGoogleId(googleId: UUID): DBIO[Option[Cluster]] = {
      clusterQueryWithInstancesAndLabels.filter { _._1.googleId === googleId }.result map { recs =>
        unmarshalClustersWithInstancesAndLabels(recs).headOption
      }
    }

    def getInitBucket(project: GoogleProject, name: ClusterName): DBIO[Option[GcsPath]] = {
      clusterQuery
        .filter { _.googleProject === project.value }
        .filter { _.clusterName === name.value }
        .map(_.initBucket)
        .result map { recs =>
        recs.headOption.flatMap(head => parseGcsPath(head).toOption)
      }
    }

    def getServiceAccountKeyId(project: GoogleProject, name: ClusterName): DBIO[Option[ServiceAccountKeyId]] = {
      clusterQuery
        .filter { _.googleProject === project.value }
        .filter { _.clusterName === name.value }
        .map(_.serviceAccountKeyId)
        .result
        .map { recs => recs.headOption.flatten.map(ServiceAccountKeyId(_)) }
    }

    def markPendingDeletion(googleId: UUID): DBIO[Int] = {
      val deleteCluster = clusterQuery.filter(_.googleId === googleId)
        .map(c => (c.destroyedDate, c.status, c.hostIp))
        .update(Timestamp.from(Instant.now()), ClusterStatus.Deleting.toString, None)

      val deleteInstances = clusterQuery.filter(_.googleId === googleId).result.headOption.flatMap {
        case Some(rec) => instanceQuery.markPendingDeletionForCluster(rec.id)
        case None => DBIO.successful(0)
      }

      DBIO.fold(Seq(deleteCluster, deleteInstances), 0)(_ + _)
    }

    def completeDeletion(googleId: UUID): DBIO[Int] = {
      val deleteCluster = updateClusterStatus(googleId, ClusterStatus.Deleted)

      val deleteInstances = clusterQuery.filter(_.googleId === googleId).result.headOption.flatMap {
        case Some(rec) => instanceQuery.completeDeletionForCluster(rec.id)
        case None => DBIO.successful(0)
      }

      DBIO.fold(Seq(deleteCluster, deleteInstances), 0)(_ + _)
    }

    def setToRunning(googleId: UUID, hostIp: IP): DBIO[Int] = {
      clusterQuery.filter { _.googleId === googleId }
        .map(c => (c.status, c.hostIp))
        .update((ClusterStatus.Running.toString, Option(hostIp.value)))
    }

    def setToStopped(googleId: UUID): DBIO[Int] = {
      clusterQuery.filter { _.googleId === googleId }
        .map(c => (c.status, c.hostIp))
        .update((ClusterStatus.Stopped.toString, None))
    }

    def updateClusterStatus(googleId: UUID, newStatus: ClusterStatus): DBIO[Int] = {
      clusterQuery.filter { _.googleId === googleId }.map(_.status).update(newStatus.toString)
    }

    def getIdByGoogleId(googleId: UUID): DBIO[Option[Long]] = {
      clusterQuery.filter { _.googleId === googleId }.result map { recs =>
        recs.headOption map { _.id }
      }
    }

    def listByLabels(labelMap: LabelMap, includeDeleted: Boolean): DBIO[Seq[Cluster]] = {
      val clusterStatusQuery = if (includeDeleted) clusterQueryWithLabels else clusterQueryWithLabels.filterNot { _._1.status === "Deleted" }

      val query = if (labelMap.isEmpty) {
        clusterStatusQuery
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
        clusterStatusQuery.filter { case (cluster, _) =>
          labelQuery.filter { _.clusterId === cluster.id }
            // The following confusing line is equivalent to the much simpler:
            // .filter { lbl => (lbl.key, lbl.value) inSetBind labelMap.toSet }
            // Unfortunately slick doesn't support inSet/inSetBind for tuples.
            // https://github.com/slick/slick/issues/517
            .filter { lbl => labelMap.map { case (k, v) => lbl.key === k && lbl.value === v }.reduce(_ || _) }
            .length === labelMap.size
        }
      }
      query.result.map(unmarshalClustersWithLabels)
    }

    /* WARNING: The init bucket and SA key ID is secret to Leo, which means we don't unmarshal it.
     * This function should only be called at cluster creation time, when the init bucket doesn't exist.
     */
    private def marshalCluster(cluster: Cluster, initBucket: String, serviceAccountKeyId: Option[ServiceAccountKeyId]): ClusterRecord = {
      ClusterRecord(
        id = 0,    // DB AutoInc
        cluster.clusterName.value,
        cluster.googleId,
        cluster.googleProject.value,
        cluster.operationName.value,
        cluster.status.toString,
        cluster.hostIp map(_.value),
        cluster.creator.value,
        Timestamp.from(cluster.createdDate),
        marshalDestroyedDate(cluster.destroyedDate),
        cluster.jupyterExtensionUri map(_.toUri),
        cluster.jupyterUserScriptUri map(_.toUri),
        initBucket,
        MachineConfigRecord(
          cluster.machineConfig.numberOfWorkers.get,   //a cluster should always have numberOfWorkers defined
          cluster.machineConfig.masterMachineType.get, //a cluster should always have masterMachineType defined
          cluster.machineConfig.masterDiskSize.get,    //a cluster should always have masterDiskSize defined
          cluster.machineConfig.workerMachineType,
          cluster.machineConfig.workerDiskSize,
          cluster.machineConfig.numberOfWorkerLocalSSDs,
          cluster.machineConfig.numberOfPreemptibleWorkers
        ),
        ServiceAccountInfoRecord(
          cluster.serviceAccountInfo.clusterServiceAccount.map(_.value),
          cluster.serviceAccountInfo.notebookServiceAccount.map(_.value),
          serviceAccountKeyId.map(_.value)
        ),
        cluster.stagingBucket.map(_.value)
      )
    }

    private def unmarshalClustersWithLabels(clusterLabels: Seq[(ClusterRecord, Option[LabelRecord])]): Seq[Cluster] = {
      // Call foldMap to aggregate a Seq[(ClusterRecord, LabelRecord)] returned by the query to a Map[ClusterRecord, Map[labelKey, labelValue]].
      val clusterLabelMap: Map[ClusterRecord, LabelMap] = clusterLabels.toList.foldMap { case (clusterRecord, labelRecordOpt) =>
        val labelMap = labelRecordOpt.map(labelRecordOpt => labelRecordOpt.key -> labelRecordOpt.value).toMap
        Map(clusterRecord -> labelMap)
      }

      // Unmarshal each (ClusterRecord, Map[labelKey, labelValue]) to a Cluster object
      clusterLabelMap.map { case (clusterRec, labelMap) =>
        unmarshalCluster(clusterRec, Seq.empty, labelMap)
      }.toSeq
    }

    private def unmarshalClustersWithInstancesAndLabels(clusterInstanceLabels: Seq[(ClusterRecord, Option[InstanceRecord], Option[LabelRecord])]): Seq[Cluster] = {
      // Call foldMap to aggregate a flat sequence of (cluster, instance, label) triples returned by the query
      // to a grouped (cluster -> (instances, labels)) structure.
      val clusterInstanceLabelMap: Map[ClusterRecord, (List[InstanceRecord], Map[String, List[String]])] = clusterInstanceLabels.toList.foldMap { case (clusterRecord, instanceRecordOpt, labelRecordOpt) =>
        val instanceList = instanceRecordOpt.toList
        val labelMap = labelRecordOpt.map(labelRecordOpt => labelRecordOpt.key -> List(labelRecordOpt.value)).toMap
        Map(clusterRecord -> (instanceList, labelMap))
      }

      clusterInstanceLabelMap.map { case (clusterRecord, (instanceRecords, labels)) =>
        unmarshalCluster(clusterRecord, instanceRecords.toSet.toSeq, labels.mapValues(_.toSet.head))
      }.toSeq
    }

    private def unmarshalCluster(clusterRecord: ClusterRecord, instanceRecords: Seq[InstanceRecord], labels: LabelMap): Cluster = {
      val name = ClusterName(clusterRecord.clusterName)
      val project = GoogleProject(clusterRecord.googleProject)
      val machineConfig = MachineConfig(
        Some(clusterRecord.machineConfig.numberOfWorkers),
        Some(clusterRecord.machineConfig.masterMachineType),
        Some(clusterRecord.machineConfig.masterDiskSize),
        clusterRecord.machineConfig.workerMachineType,
        clusterRecord.machineConfig.workerDiskSize,
        clusterRecord.machineConfig.numberOfWorkerLocalSsds,
        clusterRecord.machineConfig.numberOfPreemptibleWorkers)
      val serviceAccountInfo = ServiceAccountInfo(
        clusterRecord.serviceAccountInfo.clusterServiceAccount.map(WorkbenchEmail),
        clusterRecord.serviceAccountInfo.notebookServiceAccount.map(WorkbenchEmail))

      Cluster(
        name,
        clusterRecord.googleId,
        project,
        serviceAccountInfo,
        machineConfig,
        Cluster.getClusterUrl(project, name),
        OperationName(clusterRecord.operationName),
        ClusterStatus.withName(clusterRecord.status),
        clusterRecord.hostIp map IP,
        WorkbenchEmail(clusterRecord.creator),
        clusterRecord.createdDate.toInstant,
        unmarshalDestroyedDate(clusterRecord.destroyedDate),
        labels,
        clusterRecord.jupyterExtensionUri flatMap { parseGcsPath(_).toOption },
        clusterRecord.jupyterUserScriptUri flatMap { parseGcsPath(_).toOption },
        clusterRecord.stagingBucket map GcsBucketName,
        instanceRecords map (ClusterComponent.this.instanceQuery.unmarshalInstance) toSet
      )
    }
  }

  // select * from cluster c left join label l on c.id = l.clusterId
  val clusterQueryWithLabels: Query[(ClusterTable, Rep[Option[LabelTable]]), (ClusterRecord, Option[LabelRecord]), Seq] = {
    for {
      (cluster, label) <- clusterQuery joinLeft labelQuery on (_.id === _.clusterId)
    } yield (cluster, label)
  }

  // select * from cluster c left join instance i on c.id = i.clusterId left join label l on c.id = l.clusterId
  val clusterQueryWithInstancesAndLabels: Query[(ClusterTable, Rep[Option[InstanceTable]], Rep[Option[LabelTable]]), (ClusterRecord, Option[InstanceRecord], Option[LabelRecord]), Seq] = {
    for {
      ((cluster, instance), label) <- clusterQuery joinLeft instanceQuery on (_.id === _.clusterId) joinLeft labelQuery on (_._1.id === _.clusterId)
    } yield (cluster, instance, label)
  }

}
