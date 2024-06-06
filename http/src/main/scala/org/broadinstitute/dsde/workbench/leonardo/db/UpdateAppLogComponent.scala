package org.broadinstitute.dsde.workbench.leonardo
package db

import org.broadinstitute.dsde.workbench.leonardo.{AppId, UpdateAppJobStatus, UpdateAppTableId}
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import slick.lifted.Tag
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._

import java.time.Instant
import scala.concurrent.ExecutionContext

case class UpdateAppLogRecord(id: UpdateAppTableId,
                              jobId: UpdateAppJobId,
                              appId: AppId,
                              status: UpdateAppJobStatus,
                              startTime: Instant,
                              endTime: Option[Instant]
)

class UpdateAppLogTable(tag: Tag) extends Table[UpdateAppLogRecord](tag, "APP_UPDATE_LOG") {
  def id = column[UpdateAppTableId]("id", O.PrimaryKey, O.AutoInc)
  def jobId = column[UpdateAppJobId]("jobId")
  def appId = column[AppId]("appId")
  def status = column[UpdateAppJobStatus]("status", O.Length(254))
  def startTime = column[Instant]("startTime", O.SqlType("TIMESTAMP(6)"))
  def endTime = column[Option[Instant]]("endTime", O.SqlType("TIMESTAMP(6)"))

  def * =
    (id, jobId, appId, status, startTime, endTime) <> (UpdateAppLogRecord.tupled, UpdateAppLogRecord.unapply)
}

object updateAppLogQuery extends TableQuery(new UpdateAppLogTable(_)) {

  def save(jobId: UpdateAppJobId, appId: AppId, startTime: Instant): DBIO[Int] =
    updateAppLogQuery += UpdateAppLogRecord(
      UpdateAppTableId(0),
      jobId,
      appId,
      UpdateAppJobStatus.Running,
      startTime,
      None
    )

  def update(appId: AppId,
             jobId: UpdateAppJobId,
             status: UpdateAppJobStatus,
             endTime: Option[Instant] = None
  ): DBIO[Int] =
    updateAppLogQuery
      .filter(_.appId === appId)
      .filter(_.jobId === jobId)
      .map(x => (x.status, x.endTime))
      .update((status, endTime))

  def get(appId: AppId, jobId: UpdateAppJobId)(implicit ec: ExecutionContext): DBIO[Option[UpdateAppLogRecord]] =
    updateAppLogQuery
      .filter(_.appId === appId)
      .filter(_.jobId === jobId)
      .result map { recs =>
      val logRecords = recs map { rec => unmarshallAppUpdateLogRecord(rec) }
      logRecords.toList.headOption
    }

  def getByAppId(appId: AppId)(implicit ec: ExecutionContext): DBIO[List[UpdateAppLogRecord]] =
    updateAppLogQuery.filter(_.appId === appId).result map { recs =>
      val logRecords = recs map { rec => unmarshallAppUpdateLogRecord(rec) }
      logRecords.toList
    }

  def getByJobId(jobId: UpdateAppJobId)(implicit ec: ExecutionContext): DBIO[List[UpdateAppLogRecord]] =
    updateAppLogQuery.filter(_.jobId === jobId).result map { recs =>
      val logRecords = recs map { rec => unmarshallAppUpdateLogRecord(rec) }
      logRecords.toList
    }

  def unmarshallAppUpdateLogRecord(appErrorRecord: UpdateAppLogRecord): UpdateAppLogRecord =
    UpdateAppLogRecord(appErrorRecord.id,
                       appErrorRecord.jobId,
                       appErrorRecord.appId,
                       appErrorRecord.status,
                       appErrorRecord.startTime,
                       appErrorRecord.endTime
    )

}
