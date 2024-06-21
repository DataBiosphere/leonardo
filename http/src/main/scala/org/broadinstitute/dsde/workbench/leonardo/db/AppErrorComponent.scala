package org.broadinstitute.dsde.workbench.leonardo
package db

import java.time.Instant
import LeoProfile.api._
import LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.concurrent.ExecutionContext

case class AppErrorRecord(id: KubernetesErrorId,
                          appId: AppId,
                          errorMessage: String,
                          timestamp: Instant,
                          action: ErrorAction,
                          source: ErrorSource,
                          googleErrorCode: Option[Int],
                          traceId: Option[TraceId]
)

class AppErrorTable(tag: Tag) extends Table[AppErrorRecord](tag, "APP_ERROR") {
  def id = column[KubernetesErrorId]("id", O.AutoInc)
  def appId = column[AppId]("appId")
  def errorMessage = column[String]("errorMessage", O.Length(1024))
  def timestamp = column[Instant]("timestamp", O.SqlType("TIMESTAMP(6)"))
  def action = column[ErrorAction]("action", O.Length(254))
  def source = column[ErrorSource]("source", O.Length(254))
  def googleErrorCode = column[Option[Int]]("googleErrorCode")
  def traceId = column[Option[TraceId]]("traceId")

  def * =
    (id,
     appId,
     errorMessage,
     timestamp,
     action,
     source,
     googleErrorCode,
     traceId
    ) <> (AppErrorRecord.tupled, AppErrorRecord.unapply)
}

object appErrorQuery extends TableQuery(new AppErrorTable(_)) {

  def save(appId: AppId, error: AppError)(implicit ec: ExecutionContext): DBIO[KubernetesErrorId] = {
    val record = AppErrorRecord(
      KubernetesErrorId(0),
      appId,
      Option(error.errorMessage).map(_.take(1024)).getOrElse("null"),
      error.timestamp,
      error.action,
      error.source,
      error.googleErrorCode,
      error.traceId
    )

    for {
      id <- appErrorQuery returning appErrorQuery.map(_.id) += record
    } yield id
  }

  def get(appId: AppId)(implicit ec: ExecutionContext): DBIO[List[AppError]] =
    appErrorQuery.filter(_.appId === appId).result map { recs =>
      val errors = recs map { rec => unmarshallAppErrorRecord(rec) }
      errors.toList
    }

  def unmarshallAppErrorRecord(appErrorRecord: AppErrorRecord): AppError =
    AppError(appErrorRecord.errorMessage,
             appErrorRecord.timestamp,
             appErrorRecord.action,
             appErrorRecord.source,
             appErrorRecord.googleErrorCode
    )

}
