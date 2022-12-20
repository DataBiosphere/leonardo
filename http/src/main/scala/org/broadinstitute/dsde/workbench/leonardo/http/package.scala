package org.broadinstitute.dsde.workbench.leonardo

import akka.http.scaladsl.model.Uri.Host
import cats.Applicative
import cats.effect.{Resource, Sync}
import cats.mtl.Ask
import cats.syntax.all._
import io.circe.Encoder
import io.opencensus.scala.http.ServiceData
import io.opencensus.trace.{AttributeValue, Span}
import fs2._
import fs2.io.file.Files
import org.broadinstitute.dsde.workbench.leonardo.db.DBIOOps
import org.broadinstitute.dsde.workbench.leonardo.http.api.BuildTimeVersion
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  MonitorContext,
  RuntimeConfigInCreateRuntimeMessage,
  RuntimeMonitor
}
import org.broadinstitute.dsde.workbench.leonardo.util.CloudServiceOps
import org.broadinstitute.dsde.workbench.model.{ErrorReportSource, TraceId}
import shapeless._
import slick.dbio.DBIO

import java.nio.file.Path
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

package object http {
  val includeDeletedKey = "includeDeleted"
  val includeLabelsKey = "includeLabels"
  val creatorOnlyKey = "role"
  val creatorOnlyValue = "creator"
  val bucketPathMaxLength = 1024
  val WORKSPACE_NAME_KEY = "WORKSPACE_NAME"

  implicit val errorReportSource = ErrorReportSource("leonardo")
  implicit def dbioToIO[A](dbio: DBIO[A]): DBIOOps[A] = new DBIOOps(dbio)
  implicit def cloudServiceOps(cloudService: CloudService): CloudServiceOps = new CloudServiceOps(cloudService)
  implicit val serviceDataEncoder: Encoder[ServiceData] = Encoder.forProduct2(
    "service",
    "version"
  )(x => (x.name, x.version))
  // converts an Ask[F, RuntimeServiceContext] to an  Ask[F, TraceId]
  // (you'd think Ask would have a `map` function)
  implicit def ctxConversion[F[_]: Applicative](implicit
    as: Ask[F, AppContext]
  ): Ask[F, TraceId] =
    new Ask[F, TraceId] {
      override def applicative: Applicative[F] = as.applicative
      override def ask[E2 >: TraceId]: F[E2] = as.ask.map(_.traceId)
    }

  val serviceData = ServiceData(Some("leonardo"), BuildTimeVersion.version)
  def readFileToString[F[_]: Sync: Files](path: Path): F[String] =
    Files[F]
      .readAll(fs2.io.file.Path.fromNioPath(path))
      .through(text.utf8.decode)
      .through(text.lines)
      .fold(List.empty[String]) { case (acc, str) => str :: acc }
      .map(_.reverse.mkString("\n"))
      .compile
      .lastOrError

  def readFileToBytes[F[_]: Sync: Files](path: Path): F[List[Byte]] =
    Files[F]
      .readAll(fs2.io.file.Path.fromNioPath(path))
      .compile
      .to(List)

  def writeTempFile[F[_]: Sync: Files](prefix: String, data: Array[Byte]): F[Path] =
    for {
      path <- Sync[F].delay(java.nio.file.Files.createTempFile(prefix, null))
      _ <- Sync[F].delay(path.toFile.deleteOnExit())
      _ <- Stream.emits(data).through(Files[F].writeAll(fs2.io.file.Path.fromNioPath(path))).compile.drain
    } yield path

  // This hostname is used by the ProxyService and also needs to be specified in the Galaxy ingress resource
  def kubernetesProxyHost(cluster: KubernetesCluster, proxyDomain: String): Host = {
    val prefix = Math.abs(cluster.getClusterId.toString.hashCode).toString
    Host(prefix + proxyDomain)
  }

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
}

final case class CloudServiceMonitorOps[F[_], A](a: A)(implicit
  monitor: RuntimeMonitor[F, A]
) {
  def process(runtimeId: Long, action: RuntimeStatus, timeoutInMinutes: Option[FiniteDuration])(implicit
    ev: Ask[F, TraceId]
  ): Stream[F, Unit] =
    monitor.process(a)(runtimeId, action, timeoutInMinutes)

  def handlePollCheckCompletion(monitorContext: MonitorContext,
                                runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  ): F[Unit] =
    monitor.handlePollCheckCompletion(a)(monitorContext, runtimeAndRuntimeConfig)
}

final case class AppContext(traceId: TraceId, now: Instant, requestUri: String = "", span: Option[Span] = None) {
  override def toString: String = s"${traceId.asString}"
  val loggingCtx = Map("traceId" -> traceId.asString)
}

object AppContext {
  def generate[F[_]: Sync](span: Option[Span] = None, requestUri: String): F[AppContext] =
    for {
      traceId <- span.fold(Sync[F].delay(UUID.randomUUID().toString))(s =>
        Sync[F].pure(s.getContext.getTraceId.toLowerBase16())
      )
      now <- Sync[F].realTimeInstant
    } yield AppContext(TraceId(traceId), now, requestUri, span)

  def lift[F[_]: Sync](span: Option[Span] = None, requestUri: String): F[Ask[F, AppContext]] =
    for {
      context <- AppContext.generate[F](span, requestUri)
    } yield Ask.const[F, AppContext](
      context
    )

}
