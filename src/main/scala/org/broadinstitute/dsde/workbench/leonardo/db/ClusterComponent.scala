package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.Timestamp
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterStatus}

case class ClusterRecord(clusterId: UUID,
                         clusterName: String,
                         googleProject: String,
                         googleServiceAccount: String,
                         googleBucket: String,
                         operationName: String,
                         status: String,
                         createdDate: Timestamp,
                         destroyedDate: Option[Timestamp])

trait ClusterComponent extends LeoComponent {
  this: LabelComponent =>

  import profile.api._

  class ClusterTable(tag: Tag) extends Table[ClusterRecord](tag, "CLUSTER") {
    def clusterId =             column[UUID]              ("clusterId",             O.PrimaryKey)
    def clusterName =           column[String]            ("clusterName",           O.Length(254))
    def googleProject =         column[String]            ("googleProject",         O.Length(254))
    def googleServiceAccount =  column[String]            ("googleServiceAccount",  O.Length(254))
    def googleBucket =          column[String]            ("googleBucket",          O.Length(254))
    def operationName =         column[String]            ("operationName",         O.Length(254))
    def status =                column[String]            ("status",                O.Length(254))
    def createdDate =           column[Timestamp]         ("createdDate",           O.SqlType("TIMESTAMP(6)"))
    def destroyedDate =         column[Option[Timestamp]] ("destroyedDate",         O.SqlType("TIMESTAMP(6)"))

    def * = (clusterId, clusterName, googleProject, googleServiceAccount, googleBucket, operationName, status, createdDate, destroyedDate) <> (ClusterRecord.tupled, ClusterRecord.unapply)
  }

  object clusterQuery extends TableQuery(new ClusterTable(_)) {

    def save(cluster: Cluster): DBIO[Cluster] = {
      (clusterQuery += marshalCluster(cluster)) flatMap { _ =>
        labelQuery.saveAll(cluster.clusterId, cluster.labels)
      } map { _ => cluster }
    }

    def list(): DBIO[Seq[Cluster]] = {
      clusterQuery.result flatMap { recs =>
        val clusterActions = recs map { rec =>
          labelQuery.getAll(rec.clusterId) map { labels =>
            unmarshalCluster(rec, labels)
          }
        }
        DBIO.sequence(clusterActions)
      }
    }

    def delete(id: UUID): DBIO[Int] = {
      labelQuery.deleteAll(id) flatMap { _ =>
        clusterQuery.filter { _.clusterId === id }.delete
      }
    }

    def marshalCluster(cluster: Cluster): ClusterRecord = {
      ClusterRecord(
        cluster.clusterId,
        cluster.clusterName,
        cluster.googleProject,
        cluster.googleServiceAccount,
        cluster.googleBucket,
        cluster.operationName,
        cluster.status.toString,
        Timestamp.from(cluster.createdDate),
        cluster.destroyedDate map Timestamp.from
      )
    }

    private def unmarshalCluster(clusterRecord: ClusterRecord, labels: Map[String,String]): Cluster = {
      Cluster(
        clusterRecord.clusterId,
        clusterRecord.clusterName,
        clusterRecord.googleProject,
        clusterRecord.googleServiceAccount,
        clusterRecord.googleBucket,
        clusterRecord.operationName,
        ClusterStatus.withName(clusterRecord.status),
        clusterRecord.createdDate.toInstant,
        clusterRecord.destroyedDate map { _.toInstant },
        labels
      )
    }

  }

}
