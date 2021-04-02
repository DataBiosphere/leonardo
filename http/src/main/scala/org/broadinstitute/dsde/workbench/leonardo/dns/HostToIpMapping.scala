package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import cats.effect.{Blocker, ContextShift, Sync}
import cats.effect.concurrent.{Ref, Semaphore}
import cats.syntax.all._
import fs2._
import org.broadinstitute.dsde.workbench.model.IP

import java.nio.file.{Files, Path, Paths, StandardOpenOption}

// TODO: don't need this class if Host header works
final class HostToIpMapping2[F[_]](path: Path,
                                   mapping: Ref[F, Map[Host, IP]],
                                   semaphore: Semaphore[F],
                                   blocker: Blocker)(
  implicit F: Sync[F],
  cs: ContextShift[F]
) {

  def addHostMapping(host: Host, ip: IP): F[Unit] =
    for {
      prev <- mapping.getAndUpdate(_ + (host -> ip))
      _ <- prev.get(host) match {
        case Some(oldIp) if oldIp == ip => F.unit
        case Some(_)                    =>
          // update existing entry
          replaceEntry(host, Some(ip))
        case None =>
          // append new entry
          appendEntry(host, ip)
      }
    } yield ()

  def removeHostMapping(host: Host): F[Unit] =
    for {
      _ <- mapping.update(_.removed(host))
      _ <- replaceEntry(host, None)
    } yield ()

  private def replaceEntry(host: Host, ip: Option[IP]): F[Unit] =
    semaphore.withPermit {
      for {
        tmpPath <- F.delay(Paths.get(path.toFile.getAbsolutePath + ".2"))

        // write tmp file with updated host
        _ <- io.file
          .readAll(path, blocker, 4096)
          .through(text.utf8Decode)
          .through(text.lines)
          .filter(s => !s.contains(host.address()))
          .intersperse("\n")
          .append(ip match {
            case Some(ip) => Stream(buildLine(host, ip))
            case None     => Stream.empty
          })
          .through(text.utf8Encode)
          .through(io.file.writeAll(tmpPath, blocker))
          .compile
          .drain

        // copy tmp file back to old file
        _ <- F.delay(Files.move(tmpPath, path))

        // delete tmp file
        _ <- F.delay(Files.delete(tmpPath))
      } yield ()
    }

  private def appendEntry(host: Host, ip: IP): F[Unit] =
    semaphore.withPermit {
      Stream(buildLine(host, ip))
        .covary[F]
        .through(text.utf8Encode)
        .through(
          io.file.writeAll(
            path,
            blocker,
            Seq(StandardOpenOption.CREATE, StandardOpenOption.APPEND)
          )
        )
        .compile
        .drain
    }

  private def buildLine(host: Host, ip: IP): String =
    s"${ip.asString}  ${host.address()}"
}
