package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.Timestamp
import LeoProfile.api._
import org.broadinstitute.dsde.workbench.model.TraceId
import LeoProfile.mappedColumnImplicits._

import scala.concurrent.ExecutionContext

case class ClusterErrorRecord(id: Long,
                              clusterId: Long,
                              errorMessage: String,
                              errorCode: Option[Int],
                              timestamp: Timestamp,
                              traceId: Option[TraceId]
)

class ClusterErrorTable(tag: Tag) extends Table[ClusterErrorRecord](tag, "CLUSTER_ERROR") {
  def id = column[Long]("id", O.AutoInc)

  def clusterId = column[Long]("clusterId")

  def errorMessage = column[String]("errorMessage", O.Length(1024))

  def errorCode = column[Option[Int]]("errorCode")

  def timestamp = column[Timestamp]("timestamp", O.SqlType("TIMESTAMP(6)"))

  def traceId = column[Option[TraceId]]("traceId")

  def * =
    (id,
     clusterId,
     errorMessage,
     errorCode,
     timestamp,
     traceId
    ) <> (ClusterErrorRecord.tupled, ClusterErrorRecord.unapply)
}

object clusterErrorQuery extends TableQuery(new ClusterErrorTable(_)) {

  def save(clusterId: Long, runtimeError: RuntimeError): DBIO[Int] =
    clusterErrorQuery += ClusterErrorRecord(0,
                                            clusterId,
                                            runtimeError.errorMessage.take(1024),
                                            runtimeError.errorCode,
                                            Timestamp.from(runtimeError.timestamp),
                                            runtimeError.traceId
    )

  def get(clusterId: Long)(implicit ec: ExecutionContext): DBIO[List[RuntimeError]] =
    clusterErrorQuery.filter(_.clusterId === clusterId).result map { recs =>
      val errors = recs map { rec => unmarshallClusterErrorRecord(rec) }
      errors.toList
    }

  def unmarshallClusterErrorRecord(clusterErrorRecord: ClusterErrorRecord): RuntimeError =
    RuntimeError(clusterErrorRecord.errorMessage,
                 clusterErrorRecord.errorCode,
                 clusterErrorRecord.timestamp.toInstant,
                 clusterErrorRecord.traceId
    )

}
