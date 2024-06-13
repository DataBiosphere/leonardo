package org.broadinstitute.dsde.workbench.leonardo
package db

import org.broadinstitute.dsde.workbench.leonardo.{UpdateAppTableId, AppId, UpdateAppJobStatus}
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import slick.lifted.Tag
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._

import java.sql.SQLDataException
import java.time.Instant
import scala.concurrent.ExecutionContext

case class UpdateAppLogRecord(id: UpdateAppTableId,
                              jobId: UpdateAppJobId,
                              appId: AppId,
                              errorId: Option[KubernetesErrorId],
                              status: UpdateAppJobStatus,
                              startTime: Instant,
                              endTime: Option[Instant]
)

class UpdateAppLogTable(tag: Tag) extends Table[UpdateAppLogRecord](tag, "UPDATE_APP_LOG") {
  def id = column[UpdateAppTableId]("id", O.PrimaryKey, O.AutoInc)
  def jobId = column[UpdateAppJobId]("jobId")
  def appId = column[AppId]("appId")
  def errorId = column[Option[KubernetesErrorId]]("appErrorId")
  def status = column[UpdateAppJobStatus]("status", O.Length(254))
  def startTime = column[Instant]("startTime", O.SqlType("TIMESTAMP(6)"))
  def endTime = column[Option[Instant]]("endTime", O.SqlType("TIMESTAMP(6)"))

  def * =
    (id, jobId, appId, errorId, status, startTime, endTime) <> (UpdateAppLogRecord.tupled, UpdateAppLogRecord.unapply)
}

object updateAppLogQuery extends TableQuery(new UpdateAppLogTable(_)) {

  def save(jobId: UpdateAppJobId, appId: AppId, startTime: Instant): DBIO[Int] =
    updateAppLogQuery += UpdateAppLogRecord(
      UpdateAppTableId(0),
      jobId,
      appId,
      None,
      UpdateAppJobStatus.Running,
      startTime,
      None
    )

  def update(appId: AppId,
             jobId: UpdateAppJobId,
             status: UpdateAppJobStatus,
             errorId: Option[KubernetesErrorId] = None,
             endTime: Option[Instant] = None
  )(implicit ec: ExecutionContext): DBIO[Int] = {
    for {
      record <- get(appId, jobId)
      existingRecord = record.getOrElse(throw new SQLDataException(s"Cannot update a log record that does not exist"))
      int <- updateAppLogQuery
      .filter(_.appId === appId)
      .filter(_.jobId === jobId)
      .map(x => (x.errorId, x.status, x.endTime, x.startTime))
      .update((errorId, status, endTime, existingRecord.startTime))
    } yield int
  }

  def get(appId: AppId, jobId: UpdateAppJobId)(implicit ec: ExecutionContext): DBIO[Option[UpdateAppLogRecord]] =
    updateAppLogQuery
      .filter(_.appId === appId)
      .filter(_.jobId === jobId)
      .result map { recs =>
      val logRecords = recs map { rec => unmarshalAppUpdateLogRecord(rec) }
      logRecords.toList.headOption
    }

  def getByAppId(appId: AppId)(implicit ec: ExecutionContext): DBIO[List[UpdateAppLogRecord]] =
    updateAppLogQuery.filter(_.appId === appId).result map { recs =>
      val logRecords = recs map { rec => unmarshalAppUpdateLogRecord(rec) }
      logRecords.toList
    }

  def getByJobId(jobId: UpdateAppJobId)(implicit ec: ExecutionContext): DBIO[List[UpdateAppLogRecord]] =
    updateAppLogQuery.filter(_.jobId === jobId).result map { recs =>
      val logRecords = recs map { rec => unmarshalAppUpdateLogRecord(rec) }
      logRecords.toList
    }

  def unmarshalAppUpdateLogRecord(appErrorRecord: UpdateAppLogRecord): UpdateAppLogRecord =
    UpdateAppLogRecord(
      appErrorRecord.id,
      appErrorRecord.jobId,
      appErrorRecord.appId,
      appErrorRecord.errorId,
      appErrorRecord.status,
      appErrorRecord.startTime,
      appErrorRecord.endTime
    )

}
