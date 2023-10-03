package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.effect.Sync
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.AppId
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.Logger

import java.sql.SQLDataException
import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace

class AppUsageTable(tag: Tag) extends Table[AppUsageRecord](tag, "APP_USAGE") {
  def id = column[AppUsageId]("id", O.PrimaryKey, O.AutoInc)
  def appId = column[AppId]("appId")

  def startTime = column[Instant]("startTime", O.SqlType("TIMESTAMP(6)"))
  def stopTime = column[Instant]("stopTime", O.SqlType("TIMESTAMP(6)"))

  def * = (id, appId, startTime, stopTime) <> (AppUsageRecord.tupled, AppUsageRecord.unapply)
}

object appUsageQuery extends TableQuery(new AppUsageTable(_)) {
  def recordStart[F[_]](appId: AppId, startTime: Instant)(implicit
    ec: ExecutionContext,
    dbReference: DbReference[F],
    metrics: OpenTelemetryMetrics[F],
    F: Sync[F],
    logger: Logger[F]
  ): F[AppUsageId] = {

    val dbio = for {
      recordExists <- appUsageQuery
        .filter(_.appId === appId)
        .filter(_.stopTime === dummyDate)
        .exists
        .result // Make sure there's no existing appId that doesn't have endTime recorded already
      id <-
        if (recordExists)
          DBIO.failed(
            new SQLDataException(s"app(${appId.id}) usage startTime was recorded previously with no endTime recorded")
          )
        else
          appUsageQuery returning appUsageQuery.map(_.id) += AppUsageRecord(AppUsageId(-1), appId, startTime, dummyDate)
    } yield id

    for {
      res <- dbio.transaction.attempt
      id <- res match {
        case Left(e) if e.getMessage.contains("usage startTime was recorded previously") =>
          // We should alert if this happens
          metrics.incrementCounter("appStartUsageTimeRecordingFailure") >> logger.error(e)(e.getMessage) >> F
            .raiseError(e)
        case Left(e)      => F.raiseError(e)
        case Right(appId) => F.pure(appId)
      }
    } yield id
  }

  def recordStop[F[_]](appId: AppId, stopTime: Instant)(implicit
    ec: ExecutionContext,
    dbReference: DbReference[F],
    metrics: OpenTelemetryMetrics[F],
    F: Sync[F],
    logger: Logger[F]
  ): F[Unit] = {
    val dbio = appUsageQuery
      .filter(x => x.appId === appId)
      .filter(_.stopTime === dummyDate)
      .map(_.stopTime)
      .update(stopTime)
      .flatMap { x =>
        if (x == 1)
          DBIO.successful(())
        else
          DBIO.failed(
            FailToRecordStoptime(appId)
          )
      }

    for {
      res <- dbio.transaction.attempt
      _ <- res match {
        case Left(e) if e.getMessage.contains("Cannot record stopTime") =>
          // We should alert if this happens
          metrics.incrementCounter("appStopUsageTimeRecordingFailure") >> logger.error(e)(e.getMessage) >> F.raiseError(
            e
          )
        case Left(e)      => F.raiseError(e)
        case Right(appId) => F.pure(appId)
      }
    } yield ()
  }

  def get(appUsageId: AppUsageId)(implicit ec: ExecutionContext): DBIO[Option[AppUsageRecord]] =
    appUsageQuery.filter(_.id === appUsageId).result.map(_.headOption)
}

final case class AppUsageId(id: Long) extends Product with Serializable
final case class AppUsageRecord(id: AppUsageId, appId: AppId, startTime: Instant, stopTime: Instant)
final case class FailToRecordStoptime(appId: AppId) extends NoStackTrace {
  override def getMessage: String =
    s"Cannot record stopTime because there's no existing unresolved startTime for ${appId.id}"
}
