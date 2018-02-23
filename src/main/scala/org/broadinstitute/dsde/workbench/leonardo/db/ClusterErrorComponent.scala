package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.Timestamp
import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.model.ClusterError

case class ClusterErrorRecord(id: Long, clusterId: Long, errorMessage: String, errorCode: Int, timestamp: Timestamp)

trait ClusterErrorComponent extends LeoComponent {
  this: ClusterComponent =>

  import profile.api._

  class ClusterErrorTable(tag: Tag) extends Table[ClusterErrorRecord](tag, "CLUSTER_ERROR") {
    def id = column[Long]("id", O.AutoInc)

    def clusterId = column[Long]("clusterId")

    def errorMessage = column[String]("errorMessage", O.Length(1024))

    def errorCode = column[Int]("errorCode")

    def timestamp = column[Timestamp]("timestamp", O.SqlType("TIMESTAMP(6)"))

    def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)

    def * = (id, clusterId, errorMessage, errorCode, timestamp) <> (ClusterErrorRecord.tupled, ClusterErrorRecord.unapply)
  }

  object clusterErrorQuery extends TableQuery(new ClusterErrorTable(_)) {

    def save(clusterId: Long, clusterError: ClusterError): DBIO[Int] = {
      clusterErrorQuery += ClusterErrorRecord(0, clusterId, clusterError.errorMessage, clusterError.errorCode, Timestamp.from(clusterError.timestamp))
    }

    def get(clusterId: Long): DBIO[List[ClusterError]] = {
      clusterErrorQuery.filter(_.clusterId === clusterId).result map { recs =>
        val errors = recs map { rec =>
          unmarshallClusterErrorRecord(rec)
        }
        errors.toList
      }
    }

    def delete(clusterId: Long): DBIO[Int] = {
      clusterErrorQuery.filter(_.clusterId === clusterId).delete
    }

    def unmarshallClusterErrorRecord(clusterErrorRecord: ClusterErrorRecord) : ClusterError = {
      ClusterError(clusterErrorRecord.errorMessage, clusterErrorRecord.errorCode, clusterErrorRecord.timestamp.toInstant)
    }

  }

}
