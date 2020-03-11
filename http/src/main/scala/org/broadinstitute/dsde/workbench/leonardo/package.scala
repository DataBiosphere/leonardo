package org.broadinstitute.dsde.workbench.leonardo

import java.nio.file.Path

import cats.effect.{Blocker, ContextShift, Sync}
import fs2._
import org.broadinstitute.dsde.workbench.leonardo.db.DBIOOps
import org.broadinstitute.dsde.workbench.leonardo.util.CloudServiceOps
import org.broadinstitute.dsde.workbench.model.ErrorReportSource
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
}
