package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.Timestamp

import LeoProfile.api._

import scala.concurrent.ExecutionContext

case class ClusterErrorRecord(id: Long, clusterId: Long, errorMessage: String, errorCode: Int, timestamp: Timestamp)

class ClusterErrorTable(tag: Tag) extends Table[ClusterErrorRecord](tag, "CLUSTER_ERROR") {
  def id = column[Long]("id", O.AutoInc)

  def clusterId = column[Long]("clusterId")

  def errorMessage = column[String]("errorMessage", O.Length(1024))

  def errorCode = column[Int]("errorCode")

  def timestamp = column[Timestamp]("timestamp", O.SqlType("TIMESTAMP(6)"))

  def cluster = foreignKey("FK_CLUSTER_ID", clusterId, clusterQuery)(_.id)

  def * =
    (id, clusterId, errorMessage, errorCode, timestamp) <> (ClusterErrorRecord.tupled, ClusterErrorRecord.unapply)
}

object clusterErrorQuery extends TableQuery(new ClusterErrorTable(_)) {

  def save(clusterId: Long, clusterError: RuntimeError): DBIO[Int] =
    clusterErrorQuery += ClusterErrorRecord(0,
                                            clusterId,
                                            clusterError.errorMessage,
                                            clusterError.errorCode,
                                            Timestamp.from(clusterError.timestamp))

  def get(clusterId: Long)(implicit ec: ExecutionContext): DBIO[List[RuntimeError]] =
    clusterErrorQuery.filter(_.clusterId === clusterId).result map { recs =>
      val errors = recs map { rec =>
        unmarshallClusterErrorRecord(rec)
      }
      errors.toList
    }

  def unmarshallClusterErrorRecord(clusterErrorRecord: ClusterErrorRecord): RuntimeError =
    RuntimeError(clusterErrorRecord.errorMessage, clusterErrorRecord.errorCode, clusterErrorRecord.timestamp.toInstant)

}
