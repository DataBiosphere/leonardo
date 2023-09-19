package org.broadinstitute.dsde.workbench.leonardo
package db

import org.broadinstitute.dsde.workbench.leonardo.AppId

import java.time.Instant
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._

import java.sql.SQLDataException
import scala.concurrent.ExecutionContext

class AppUasageTable(tag: Tag) extends Table[AppUsageRecord](tag, "APP_USAGE") {
  def id = column[AppUsageId]("id", O.PrimaryKey, O.AutoInc)
  def appId = column[AppId]("appId")

  def startTime = column[Instant]("startTime", O.SqlType("TIMESTAMP(6)"))
  def stopTime = column[Instant]("stopTime", O.SqlType("TIMESTAMP(6)"))

  def * = (id, appId, startTime, stopTime) <> (AppUsageRecord.tupled, AppUsageRecord.unapply)
}

object appUsageQuery extends TableQuery(new AppUasageTable(_)) {
  def recordStart(appId: AppId, startTime: Instant)(implicit
    ec: ExecutionContext
  ): DBIO[AppUsageId] =
    for {
      recordExists <- appUsageQuery
        .filter(_.appId === appId)
        .filter(_.stopTime === dummyDate)
        .exists
        .result // Make sure there's no existing appId that doesn't have endTime recorded already
      id <-
        if (recordExists)
          DBIO.failed(
            new SQLDataException("app usage startTime was recorded previously, but no endTime recorded")
          )
        else
          appUsageQuery returning appUsageQuery.map(_.id) += AppUsageRecord(AppUsageId(-1), appId, startTime, dummyDate)
    } yield id

  def recordStop(appId: AppId, stopTime: Instant)(implicit
    ec: ExecutionContext
  ): DBIO[Unit] =
    appUsageQuery
      .filter(x => x.appId === appId)
      .filter(_.stopTime === dummyDate)
      .map(_.stopTime)
      .update(stopTime)
      .flatMap { x =>
        if (x == 1)
          DBIO.successful(())
        else
          DBIO.failed(
            new RuntimeException(s"Cannot record stopTime because there's no existing startTime for ${appId.id}")
          )
      }

  def get(appUsageId: AppUsageId)(implicit ec: ExecutionContext): DBIO[Option[AppUsageRecord]] =
    appUsageQuery.filter(_.id === appUsageId).result.map(_.headOption)
}

final case class AppUsageId(id: Long) extends Product with Serializable
final case class AppUsageRecord(id: AppUsageId, appId: AppId, startTime: Instant, stopTime: Instant)
