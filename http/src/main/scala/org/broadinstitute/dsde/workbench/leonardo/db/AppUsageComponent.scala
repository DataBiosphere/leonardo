package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.effect.Async
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.AppId
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.Logger
import slick.jdbc.TransactionIsolation
import cats.effect.std.Random

import java.sql.SQLDataException
import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
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
    F: Async[F],
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
      res <- dbio.transaction(TransactionIsolation.Serializable).attempt
      id <- res match {
        case Left(e) if e.getMessage.contains("usage startTime was recorded previously") =>
          // We should alert if this happens
          metrics.incrementCounter("appStartUsageTimeRecordingFailure") >> logger.error(e)(e.getMessage) >> F
            .raiseError(e)
        case Left(e: com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException)
            if e.getMessage contains "Deadlock" =>
          // When two threads start the same transaction at the same time, they can both be deadlocked.
          // Add some jitter to retry to avoid the deadlock.
          for {
            random <- Random.scalaUtilRandom[F]
            numOfMilliseconds <- random.nextAlphaNumeric.map(_.toInt)
            _ <- logger.info(s"Deadlock detected, sleeping for $numOfMilliseconds ms")
            _ <- F.sleep(numOfMilliseconds milliseconds)
            id <- recordStart(appId, startTime)
          } yield id
        case Left(e)   => F.raiseError(e)
        case Right(id) => F.pure(id)
      }
    } yield id
  }

  def recordStop[F[_]](appId: AppId, stopTime: Instant)(implicit
    dbReference: DbReference[F],
    metrics: OpenTelemetryMetrics[F],
    F: Async[F],
    logger: Logger[F]
  ): F[Unit] = {
    val dbio = appUsageQuery
      .filter(x => x.appId === appId)
      .filter(_.stopTime === dummyDate)
      .map(_.stopTime)
      .update(stopTime)

    for {
      res <- dbio.transaction
      _ <- res match {
        case 0 =>
          val error = FailToRecordStoptime(appId)
          metrics.incrementCounter("appStopUsageTimeRecordingFailure") >> logger.error(error)(error.getMessage) >> F
            .raiseError(
              error
            )
        case 1 => F.unit
        case x =>
          metrics.incrementCounter("duplicateStartTime") >> logger.warn(
            s"App(${appId.id} has ${x} rows of unresolved startTime recorded. This is an anomaly that needs to be addressed"
          )
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
