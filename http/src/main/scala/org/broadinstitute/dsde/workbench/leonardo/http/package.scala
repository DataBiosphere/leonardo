package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{Resource, Sync}
import cats.mtl.Ask
import io.opencensus.scala.http.ServiceData
import io.opencensus.trace.{AttributeValue, Span}
import fs2._
import org.broadinstitute.dsde.workbench.errorReporting.ReportWorthy
import org.broadinstitute.dsde.workbench.leonardo.http.api.BuildTimeVersion
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  InvalidMonitorRequest,
  MonitorAtBootException,
  RuntimeConfigInCreateRuntimeMessage,
  RuntimeMonitor
}
import org.broadinstitute.dsde.workbench.leonardo.util.CloudServiceOps
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import shapeless._

import java.sql.SQLDataException

package object http {
  val includeDeletedKey = "includeDeleted"
  val bucketPathMaxLength = 1024

  implicit def cloudServiceOps(cloudService: CloudService): CloudServiceOps = new CloudServiceOps(cloudService)

  val serviceData = ServiceData(Some("leonardo"), BuildTimeVersion.version)

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
      case _: SQLDataException       => true
      case _: InvalidMonitorRequest  => true
      case _: MonitorAtBootException => true
      case _                         => false
    }

  // Leonardo's base URL should be unique in each running Application. Hence it's safe to define it as an implicit and make it available globally
  implicit val leonardoBaseUrl: LeonardoBaseUrl = config.Config.proxyConfig.proxyUrlBase
}

final case class CloudServiceMonitorOps[F[_], A](a: A)(
  implicit monitor: RuntimeMonitor[F, A]
) {
  def process(runtimeId: Long, action: RuntimeStatus)(implicit ev: Ask[F, TraceId]): Stream[F, Unit] =
    monitor.process(a)(runtimeId, action)

  // Function used for transitions that we can get an Operation
  def pollCheck(googleProject: GoogleProject,
                runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                operation: com.google.cloud.compute.v1.Operation,
                action: RuntimeStatus)(implicit ev: Ask[F, TraceId]): F[Unit] =
    monitor.pollCheck(a)(googleProject, runtimeAndRuntimeConfig, operation, action)
}
