package org.broadinstitute.dsde.workbench.leonardo

import java.nio.file.{Files, Path}
import java.sql.SQLDataException

import io.opencensus.trace.{AttributeValue, Span}
import io.opencensus.scala.http.ServiceData
import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import fs2._
import org.broadinstitute.dsde.workbench.errorReporting.ReportWorthy
import org.broadinstitute.dsde.workbench.leonardo.db.DBIOOps
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  InvalidMonitorRequest,
  RuntimeConfigInCreateRuntimeMessage,
  RuntimeMonitor
}
import org.broadinstitute.dsde.workbench.leonardo.util.CloudServiceOps
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{ErrorReportSource, TraceId}
import org.broadinstitute.dsde.workbench.leonardo.http.api.BuildTimeVersion
import slick.dbio.DBIO
import shapeless._

package object http {
  implicit val errorReportSource = ErrorReportSource("leonardo")
  implicit def dbioToIO[A](dbio: DBIO[A]): DBIOOps[A] = new DBIOOps(dbio)
  implicit def cloudServiceOps(cloudService: CloudService): CloudServiceOps = new CloudServiceOps(cloudService)

  val serviceData = ServiceData(Some("leonardo"), BuildTimeVersion.version)
  def readFileToString[F[_]: Sync: ContextShift](path: Path, blocker: Blocker): F[String] =
    io.file
      .readAll[F](path, blocker, 4096)
      .through(text.utf8Decode)
      .through(text.lines)
      .fold(List.empty[String]) { case (acc, str) => str :: acc }
      .map(_.reverse.mkString("\n"))
      .compile
      .lastOrError

  def readFileToBytes[F[_]: Sync: ContextShift](path: Path, blocker: Blocker): F[List[Byte]] =
    io.file
      .readAll(path, blocker, 4096)
      .compile
      .to(List)

  def writeTempFile[F[_]: Sync: ContextShift](prefix: String, data: Array[Byte], blocker: Blocker): F[Path] =
    for {
      path <- Sync[F].delay(Files.createTempFile(prefix, null))
      _ <- Sync[F].delay(path.toFile.deleteOnExit())
      _ <- Stream.emits(data).through(io.file.writeAll(path, blocker)).compile.drain
    } yield path

  val userScriptStartupOutputUriMetadataKey = "user-startup-script-output-url"
  implicit def cloudServiceSyntax[F[_], A](
    a: A
  )(implicit ev: RuntimeMonitor[F, A]): CloudServiceMonitorOps[F, A] =
    CloudServiceMonitorOps[F, A](a)

  def spanResource[F[_]: Sync](span: Span, apiName: String): Resource[F, Unit] =
    Resource.make[F, Unit](Sync[F].delay(span.putAttribute("api", AttributeValue.stringAttributeValue(apiName))))(_ =>
      Sync[F].delay(span.end())
    )

  val genericDataprocRuntimeConfig = Generic[RuntimeConfig.DataprocConfig]
  val genericDataprocRuntimeConfigInCreateRuntimeMessage = Generic[RuntimeConfigInCreateRuntimeMessage.DataprocConfig]

  def dataprocRuntimeToDataprocInCreateRuntimeMsg(
    from: RuntimeConfig.DataprocConfig
  ): RuntimeConfigInCreateRuntimeMessage.DataprocConfig =
    genericDataprocRuntimeConfigInCreateRuntimeMessage.from(genericDataprocRuntimeConfig.to(from))

  def dataprocInCreateRuntimeMsgToDataprocRuntime(
    from: RuntimeConfigInCreateRuntimeMessage.DataprocConfig
  ): RuntimeConfig.DataprocConfig =
    genericDataprocRuntimeConfig.from(genericDataprocRuntimeConfigInCreateRuntimeMessage.to(from))

  implicit val throwableReportWorthy: ReportWorthy[Throwable] = e =>
    e match {
      case _: SQLDataException      => true
      case _: InvalidMonitorRequest => true
      case _                        => false
    }
}

final case class CloudServiceMonitorOps[F[_], A](a: A)(
  implicit monitor: RuntimeMonitor[F, A]
) {
  def process(runtimeId: Long, action: RuntimeStatus)(implicit ev: ApplicativeAsk[F, TraceId]): Stream[F, Unit] =
    monitor.process(a)(runtimeId, action)

  // Function used for transitions that we can get an Operation
  def pollCheck(googleProject: GoogleProject,
                runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                operation: com.google.cloud.compute.v1.Operation,
                action: RuntimeStatus)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    monitor.pollCheck(a)(googleProject, runtimeAndRuntimeConfig, operation, action)
}
