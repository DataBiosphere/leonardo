package org.broadinstitute.dsde.workbench.leonardo

import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.{Applicative, Functor}
import cats.effect.{Blocker, ContextShift, Sync, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import fs2._
import org.broadinstitute.dsde.workbench.leonardo.db.DBIOOps
import org.broadinstitute.dsde.workbench.leonardo.http.api.RuntimeServiceContext
import org.broadinstitute.dsde.workbench.leonardo.util.CloudServiceOps
import org.broadinstitute.dsde.workbench.model.{ErrorReportSource, TraceId}
import slick.dbio.DBIO

package object http {
  implicit val errorReportSource = ErrorReportSource("leonardo")
  implicit def dbioToIO[A](dbio: DBIO[A]): DBIOOps[A] = new DBIOOps(dbio)
  implicit def cloudServiceOps(cloudService: CloudService): CloudServiceOps = new CloudServiceOps(cloudService)

  def readFileToString[F[_]: Sync: ContextShift](path: Path, blocker: Blocker): F[String] =
    io.file
      .readAll[F](path, blocker, 4096)
      .through(text.utf8Decode)
      .through(text.lines)
      .fold(List.empty[String]) { case (acc, str) => str :: acc }
      .map(_.reverse.mkString("\n"))
      .compile
      .lastOrError

  // converts an ApplicativeAsk[F, RuntimeServiceContext] to an  ApplicativeAsk[F, TraceId]
  // (you'd think ApplicativeAsk would have a `map` function)
  implicit def ctxConversion[F[_]: Applicative](
    implicit as: ApplicativeAsk[F, RuntimeServiceContext]
  ): ApplicativeAsk[F, TraceId] =
    new ApplicativeAsk[F, TraceId] {
      override val applicative: Applicative[F] = Applicative[F]
      override def ask: F[TraceId] = as.ask.map(_.traceId)
      override def reader[A](f: TraceId => A): F[A] = ask.map(f)
    }

  // convenience to get now as a F[Instant] using a Timer
  def nowInstant[F[_]: Timer: Functor]: F[Instant] =
    Timer[F].clock.realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)
}
