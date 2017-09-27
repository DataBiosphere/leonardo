package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.Timestamp
import java.util.UUID

import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus._
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterStatus}

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
                         destroyedDate: Option[Timestamp],
                         jupyterExtensionUri: Option[String])

trait ClusterComponent extends LeoComponent {
  this: LabelComponent =>

  import profile.api._

  class ClusterTable(tag: Tag) extends Table[ClusterRecord](tag, "CLUSTER") {
    def id =                    column[Long]              ("id",                    O.PrimaryKey, O.AutoInc)
    def clusterName =           column[String]            ("clusterName",           O.Length(254))
    def googleId =              column[UUID]              ("googleId",              O.Unique)
    def googleProject =         column[String]            ("googleProject",         O.Length(254))
    def googleServiceAccount =  column[String]            ("googleServiceAccount",  O.Length(254))
    def googleBucket =          column[String]            ("googleBucket",          O.Length(254))
    def operationName =         column[String]            ("operationName",         O.Length(254))
    def status =                column[String]            ("status",                O.Length(254))
    def hostIp =                column[Option[String]]    ("hostIp",                O.Length(254))
    def createdDate =           column[Timestamp]         ("createdDate",           O.SqlType("TIMESTAMP(6)"))
    def destroyedDate =         column[Option[Timestamp]] ("destroyedDate",         O.SqlType("TIMESTAMP(6)"))
    def jupyterExtensionUri =   column[Option[String]]    ("jupyterExtensionUri",   O.Length(1024))

    def uniqueKey = index("IDX_CLUSTER_UNIQUE", (googleProject, clusterName), unique = true)

    def * = (id, clusterName, googleId, googleProject, googleServiceAccount, googleBucket, operationName, status, hostIp, createdDate, destroyedDate, jupyterExtensionUri) <> (ClusterRecord.tupled, ClusterRecord.unapply)
  }

  object clusterQuery extends TableQuery(new ClusterTable(_)) {

    def save(cluster: Cluster): DBIO[Cluster] = {
      (clusterQuery returning clusterQuery.map(_.id) += marshalCluster(cluster)) flatMap { clusterId =>
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

    // currently any string can be a GoogleProject
    // but I'm planning to fix that soon
    def getByName(project: GoogleProject, name: String): DBIO[Option[Cluster]] = {
      clusterQueryWithLabels.filter { _._1.googleProject === project }.filter { _._1.clusterName === name }.result map { recs =>
        unmarshalClustersWithLabels(recs).headOption
      }
    }

    def getByGoogleId(googleId: UUID): DBIO[Option[Cluster]] = {
      clusterQueryWithLabels.filter { _._1.googleId === googleId }.result map { recs =>
        unmarshalClustersWithLabels(recs).headOption
      }
    }

    def markPendingDeletion(googleId: UUID): DBIO[Int] = {
      clusterQuery.filter(_.googleId === googleId)
        .map(c => (c.destroyedDate, c.status, c.hostIp))
        .update((Option(Timestamp.from(java.time.Instant.now())), ClusterStatus.Deleting.toString, None))
    }

    def completeDeletion(googleId: UUID, clusterName: String): DBIO[Int] = {
      updateClusterStatus(googleId, ClusterStatus.Deleted) andThen
        // Append a random suffix to the cluster name to prevent unique key conflicts in case a cluster
        // with the same name is recreated.
        // TODO: This is a bit ugly; a better solution would be to have a unique key on (googleId, clusterName, destroyedDate)
        updateClusterName(googleId, appendRandomSuffix(clusterName))
    }

    def setToRunning(googleId: UUID, hostIp: String): DBIO[Int] = {
      clusterQuery.filter { _.googleId === googleId }
        .map(c => (c.status, c.hostIp))
        .update((ClusterStatus.Running.toString, Some(hostIp)))
    }

    def updateClusterStatus(googleId: UUID, newStatus: ClusterStatus): DBIO[Int] = {
      clusterQuery.filter { _.googleId === googleId }.map(_.status).update(newStatus.toString)
    }

    def getIdByGoogleId(googleId: UUID): DBIO[Option[Long]] = {
      clusterQuery.filter { _.googleId === googleId }.result map { recs =>
        recs.headOption map { _.id }
      }
    }

    def listByLabels(labelMap: Map[String, String], includeDeleted: Boolean): DBIO[Seq[Cluster]] = {
      val clusterStatusQuery = if (includeDeleted)
        clusterQueryWithLabels
      else
        clusterQueryWithLabels.filter { _._1.status inSetBind ClusterStatus.existingStatuses.map(_.toString) }


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

    private def marshalCluster(cluster: Cluster): ClusterRecord = {
      ClusterRecord(
        id = 0,    // DB AutoInc
        cluster.clusterName,
        cluster.googleId,
        cluster.googleProject,
        cluster.googleServiceAccount,
        cluster.googleBucket,
        cluster.operationName,
        cluster.status.toString,
        cluster.hostIp,
        Timestamp.from(cluster.createdDate),
        cluster.destroyedDate map Timestamp.from,
        cluster.jupyterExtensionUri
      )
    }

    private def unmarshalClustersWithLabels(clusterLabels: Seq[(ClusterRecord, Option[LabelRecord])]): Seq[Cluster] = {
      // Call foldMap to aggregate a Seq[(ClusterRecord, LabelRecord)] returned by the query to a Map[ClusterRecord, Map[labelKey, labelValue]].
      val clusterLabelMap: Map[ClusterRecord, Map[String, String]] = clusterLabels.toList.foldMap { case (clusterRecord, labelRecordOpt) =>
        val labelMap = labelRecordOpt.map(labelRecordOpt => labelRecordOpt.key -> labelRecordOpt.value).toMap
        Map(clusterRecord -> labelMap)
      }

      // Unmarshal each (ClusterRecord, Map[labelKey, labelValue]) to a Cluster object
      clusterLabelMap.map { case (clusterRec, labelMap) =>
        unmarshalCluster(clusterRec, labelMap)
      }.toSeq
    }

    private def unmarshalCluster(clusterRecord: ClusterRecord, labels: Map[String,String]): Cluster = {
      Cluster(
        clusterRecord.clusterName,
        clusterRecord.googleId,
        clusterRecord.googleProject,
        clusterRecord.googleServiceAccount,
        clusterRecord.googleBucket,
        Cluster.getClusterUrl(clusterRecord.googleProject, clusterRecord.clusterName),
        clusterRecord.operationName,
        ClusterStatus.withName(clusterRecord.status),
        clusterRecord.hostIp,
        clusterRecord.createdDate.toInstant,
        clusterRecord.destroyedDate map { _.toInstant },
        labels,
        clusterRecord.jupyterExtensionUri
      )
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
