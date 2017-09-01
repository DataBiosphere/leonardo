package org.broadinstitute.dsde.workbench.leonardo.db

import cats.implicits._
import java.sql.Timestamp
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterStatus}

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
                         destroyedDate: Option[Timestamp])

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

    def uniqueKey = index("IDX_CLUSTER_UNIQUE", (googleProject, clusterName), unique = true)

    def * = (id, clusterName, googleId, googleProject, googleServiceAccount, googleBucket, operationName, status, hostIp, createdDate, destroyedDate) <> (ClusterRecord.tupled, ClusterRecord.unapply)
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
      clusterQueryWithLabels.filter { rec => rec._1.status =!= ClusterStatus.Deleted.toString && rec._1.status =!= ClusterStatus.Deleting.toString }.result map { recs =>
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

    def deleteCluster(googleId: UUID):DBIO[Int] = {
       clusterQuery.filter(_.googleId === googleId)
         .map(c => (c.destroyedDate, c.status))
         .update((Option(Timestamp.from(java.time.Instant.now())), ClusterStatus.Deleting.toString))

    }

    def getIdByGoogleId(googleId: UUID): DBIO[Option[Long]] = {
      clusterQuery.filter { _.googleId === googleId }.result map { recs =>
        recs.headOption map { _.id }
      }
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
        cluster.destroyedDate map Timestamp.from
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
        labels
      )
    }

  }

  // select * from cluster c left join label l on c.id = l.clusterId
  val clusterQueryWithLabels: Query[(ClusterTable, Rep[Option[LabelTable]]), (ClusterRecord, Option[LabelRecord]), Seq] = {
    for {
      (cluster, label) <- clusterQuery joinLeft labelQuery on (_.id === _.clusterId)
    } yield (cluster, label)
  }

}
