package org.broadinstitute.dsde.workbench.leonardo

import java.nio.file.Path

import io.opencensus.trace.{AttributeValue, Span}
import io.opencensus.scala.http.ServiceData
import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.mtl.ApplicativeAsk
import fs2._
import org.broadinstitute.dsde.workbench.leonardo.db.DBIOOps
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeMonitor
import org.broadinstitute.dsde.workbench.leonardo.util.CloudServiceOps
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{ErrorReportSource, TraceId}
import org.broadinstitute.dsde.workbench.leonardo.http.api.BuildTimeVersion
import slick.dbio.DBIO

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

  val userScriptStartupOutputUriMetadataKey = "user-startup-script-output-url"
  implicit def cloudServiceSyntax[F[_], A](
    a: A
  )(implicit ev: RuntimeMonitor[F, A]): CloudServiceMonitorOps[F, A] =
    CloudServiceMonitorOps[F, A](a)

  def spanResource[F[_]: Sync](span: Span, apiName: String): Resource[F, Unit] =
    Resource.make[F, Unit](Sync[F].delay(span.putAttribute("api", AttributeValue.stringAttributeValue(apiName))))(_ =>
      Sync[F].delay(span.end())
    )
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
