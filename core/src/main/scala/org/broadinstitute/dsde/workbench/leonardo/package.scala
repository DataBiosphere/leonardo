package org.broadinstitute.dsde.workbench

import java.time.Instant
import java.util.concurrent.TimeUnit
import cats.syntax.all._
import cats.{Applicative, Functor}
import cats.effect.{Blocker, ContextShift, Sync, Timer}
import cats.mtl.Ask
import fs2.{io, text, Stream}
import org.broadinstitute.dsde.workbench.leonardo.db.DBIOOps
import org.broadinstitute.dsde.workbench.model.TraceId
import slick.dbio.DBIO

import java.nio.file.{Files, Path}

package object leonardo {
  type LabelMap = Map[String, String]
  //this value is the default for autopause, if none is specified. An autopauseThreshold of 0 indicates no autopause
  val autoPauseOffValue = 0
  val traceIdHeaderString = "X-Cloud-Trace-Context"

  // convenience to get now as a F[Instant] using a Timer
  def nowInstant[F[_]: Timer: Functor]: F[Instant] =
    Timer[F].clock.realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)

  // converts an Ask[F, RuntimeServiceContext] to an  Ask[F, TraceId]
  // (you'd think Ask would have a `map` function)
  implicit def ctxConversion[F[_]: Applicative](
    implicit as: Ask[F, AppContext]
  ): Ask[F, TraceId] =
    new Ask[F, TraceId] {
      override def applicative: Applicative[F] = as.applicative
      override def ask[E2 >: TraceId]: F[E2] = as.ask.map(_.traceId)
    }

  private val leoNameReg = "([a-z|0-9|-])*".r

  def validateName(nameString: String): Either[String, String] =
    nameString match {
      case leoNameReg(_) => Right(nameString)
      case _ =>
        Left(
          s"Invalid name ${nameString}. Only lowercase alphanumeric characters, numbers and dashes are allowed in leo names"
        )
    }

  implicit def dbioToIO[A](dbio: DBIO[A]): DBIOOps[A] = new DBIOOps(dbio)

  def writeTempFile[F[_]: Sync: ContextShift](prefix: String, data: Array[Byte], blocker: Blocker): F[Path] =
    for {
      path <- Sync[F].delay(Files.createTempFile(prefix, null))
      _ <- Sync[F].delay(path.toFile.deleteOnExit())
      _ <- Stream.emits(data).through(io.file.writeAll(path, blocker)).compile.drain
    } yield path

  def readFileToBytes[F[_]: Sync: ContextShift](path: Path, blocker: Blocker): F[List[Byte]] =
    io.file
      .readAll(path, blocker, 4096)
      .compile
      .to(List)

  def readFileToString[F[_]: Sync: ContextShift](path: Path, blocker: Blocker): F[String] =
    io.file
      .readAll[F](path, blocker, 4096)
      .through(text.utf8Decode)
      .through(text.lines)
      .fold(List.empty[String]) { case (acc, str) => str :: acc }
      .map(_.reverse.mkString("\n"))
      .compile
      .lastOrError
}
