package org.broadinstitute.dsde.workbench.leonardo.db
import java.time.Instant
import java.sql.Timestamp
import java.util.UUID

import cats.implicits._
import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchUserServiceAccountEmail

import scala.util.Random

case class ClusterRecord(id: Long,
                         clusterName: String,
                         googleId: UUID,
                         googleProject: String,
                         googleServiceAccount: String,
                         googleBucket: String,
                         operationName: String,
                         status: String,
                         hostIp: Option[String],
                         createdDate: Timestamp,
                         destroyedDate: Timestamp,
                         jupyterExtensionUri: Option[String],
                         initBucket: String)

trait ClusterComponent extends LeoComponent {
  this: LabelComponent =>

  import profile.api._

  class ClusterTable(tag: Tag) extends Table[ClusterRecord](tag, "CLUSTER") {
    def id =                    column[Long]              ("id",                    O.PrimaryKey, O.AutoInc)
    def clusterName =           column[String]            ("clusterName",           O.Length(254))
    def googleId =              column[UUID]              ("googleId",              O.Unique)
    def googleProject =         column[String]            ("googleProject",         O.Length(254))
    def googleServiceAccount =  column[String]            ("googleServiceAccount",  O.Length(254))
    def googleBucket =          column[String]            ("googleBucket",          O.Length(1024))
    def operationName =         column[String]            ("operationName",         O.Length(254))
    def status =                column[String]            ("status",                O.Length(254))
    def hostIp =                column[Option[String]]    ("hostIp",                O.Length(254))
    def createdDate =           column[Timestamp]         ("createdDate",           O.SqlType("TIMESTAMP(6)"))
    def destroyedDate =         column[Timestamp]         ("destroyedDate",         O.SqlType("TIMESTAMP(6)"))
    def jupyterExtensionUri =   column[Option[String]]    ("jupyterExtensionUri",   O.Length(1024))
    def initBucket =            column[String]            ("initBucket",            O.Length(1024))

    def uniqueKey = index("IDX_CLUSTER_UNIQUE", (googleProject, clusterName), unique = true)

    def * = (id, clusterName, googleId, googleProject, googleServiceAccount, googleBucket, operationName, status, hostIp, createdDate, destroyedDate, jupyterExtensionUri, initBucket) <> (ClusterRecord.tupled, ClusterRecord.unapply)
  }

  object clusterQuery extends TableQuery(new ClusterTable(_)) {

    private final val dummyDate:Instant = Instant.ofEpochMilli(1000)

    def save(cluster: Cluster, initBucket: GcsPath): DBIO[Cluster] = {
      (clusterQuery returning clusterQuery.map(_.id) += marshalCluster(cluster, initBucket.toUri)) flatMap { clusterId =>
        labelQuery.saveAllForCluster(clusterId, cluster.labels)
      } map { _ => cluster }
    }

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

    def findByName(project: GoogleProject, name: ClusterName) = {
      clusterQueryWithLabels.filter { _._1.googleProject === project.string }.filter { _._1.clusterName === name.string }
    }

    def getActiveClusterByName(project: GoogleProject, name: ClusterName): DBIO[Option[Cluster]] = {
      clusterQueryWithLabels
        .filter { _._1.googleProject === project.string }
        .filter { _._1.clusterName === name.string }
        .filter{_._1.destroyedDate === Timestamp.from(dummyDate)}
        .result map { recs =>
          unmarshalClustersWithLabels(recs).headOption
      }
    }

    def getDeletingClusterByName(project: GoogleProject, name: ClusterName): DBIO[Option[Cluster]] = {
      clusterQueryWithLabels
        .filter { _._1.googleProject === project.string }
        .filter { _._1.clusterName === name.string }
        .filter{_._1.status === ClusterStatus.Deleting.toString}
        .result map { recs =>
        unmarshalClustersWithLabels(recs).headOption
      }
    }

    def getByGoogleId(googleId: UUID): DBIO[Option[Cluster]] = {
      clusterQueryWithLabels.filter { _._1.googleId === googleId }.result map { recs =>
        unmarshalClustersWithLabels(recs).headOption
      }
    }

    def getInitBucket(project: GoogleProject, name: ClusterName): DBIO[Option[GcsPath]] = {
      clusterQuery
        .filter { _.googleProject === project.string }
        .filter { _.clusterName === name.string }
        .map(_.initBucket)
        .result map { recs =>
        recs.headOption.flatMap(GcsPath.parse(_).toOption)
      }
    }

    def markPendingDeletion(googleId: UUID): DBIO[Int] = {
      clusterQuery.filter(_.googleId === googleId)
        .map(c => (c.destroyedDate, c.status, c.hostIp))
        .update(Timestamp.from(Instant.now()), ClusterStatus.Deleting.toString, None)
    }

    def completeDeletion(googleId: UUID, clusterName: ClusterName): DBIO[Int] = {
      updateClusterStatus(googleId, ClusterStatus.Deleted)
    }

    def setToRunning(googleId: UUID, hostIp: IP): DBIO[Int] = {
      clusterQuery.filter { _.googleId === googleId }
        .map(c => (c.status, c.hostIp))
        .update((ClusterStatus.Running.toString, Option(hostIp.string)))
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

    /* WARNING: The init bucket is secret to Leo, which means we don't unmarshal it.
     * This function should only be called at cluster creation time, when the init bucket doesn't exist.
     */
    private def marshalCluster(cluster: Cluster, initBucket: String): ClusterRecord = {
      ClusterRecord(
        id = 0,    // DB AutoInc
        cluster.clusterName.string,
        cluster.googleId,
        cluster.googleProject.string,
        cluster.googleServiceAccount.value,
        cluster.googleBucket.name,
        cluster.operationName.string,
        cluster.status.toString,
        cluster.hostIp map(_.string),
        Timestamp.from(cluster.createdDate),
        Timestamp.from(cluster.destroyedDate.getOrElse(dummyDate)),
        cluster.jupyterExtensionUri map(_.toUri),
        initBucket
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
        unmarshalCluster(clusterRec, labelMap)
      }.toSeq
    }

    private def unmarshalCluster(clusterRecord: ClusterRecord, labels: LabelMap): Cluster = {
      val name = ClusterName(clusterRecord.clusterName)
      val project = GoogleProject(clusterRecord.googleProject)
      Cluster(
        name,
        clusterRecord.googleId,
        project,
        WorkbenchUserServiceAccountEmail(clusterRecord.googleServiceAccount),
        GcsBucketName(clusterRecord.googleBucket),
        Cluster.getClusterUrl(project, name),
        OperationName(clusterRecord.operationName),
        ClusterStatus.withName(clusterRecord.status),
        clusterRecord.hostIp map IP,
        clusterRecord.createdDate.toInstant,
        getDestroyedDate(clusterRecord.destroyedDate),
        labels,
        clusterRecord.jupyterExtensionUri flatMap { GcsPath.parse(_).toOption }
      )
    }

    private def getDestroyedDate(destroyedDate:Timestamp): Option[Instant] = {
      if(destroyedDate.toInstant != dummyDate)
        Some(destroyedDate.toInstant)
      else
        None
    }
    private def updateClusterName(googleId: UUID, newName: String): DBIO[Int] = {
      clusterQuery.filter { _.googleId === googleId }.map(_.clusterName).update(newName)
    }

    private def appendRandomSuffix(str: String, n: Int = 6): String = {
      s"${str}_${Random.alphanumeric.take(n).mkString}"
    }

  }

  // select * from cluster c left join label l on c.id = l.clusterId
  val clusterQueryWithLabels: Query[(ClusterTable, Rep[Option[LabelTable]]), (ClusterRecord, Option[LabelRecord]), Seq] = {
    for {
      (cluster, label) <- clusterQuery joinLeft labelQuery on (_.id === _.clusterId)
    } yield (cluster, label)
  }

}
