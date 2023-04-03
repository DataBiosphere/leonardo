package org.broadinstitute.dsde.workbench.leonardo
package http

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives.{optionalHeaderValueByName, provide}
import akka.http.scaladsl.server.PathMatchers.Segment
import cats.effect.IO
import cats.mtl.Ask
import io.opencensus.trace.Span
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.dao.TerminalName
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import java.time.Instant
import java.util.UUID

import io.circe.{Encoder, KeyEncoder}
import org.broadinstitute.dsde.workbench.leonardo.model.BadRequestException
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus}
import org.broadinstitute.dsde.workbench.util.health.Subsystems.Subsystem

import scala.concurrent.Future

package object api {
  implicit def ioMarshaller[A, B](implicit m: Marshaller[Future[A], B]): Marshaller[IO[A], B] =
    Marshaller(implicit ec => x => m(x.unsafeToFuture()(cats.effect.unsafe.IORuntime.global)))

  implicit val subsystemEncoder: KeyEncoder[Subsystem] = KeyEncoder.encodeKeyString.contramap(_.value)
  implicit val subsystemStatusEncoder: Encoder[SubsystemStatus] =
    Encoder.forProduct2("ok", "messages")(x => SubsystemStatus.unapply(x).get)
  implicit val statusCheckResponseEncoder: Encoder[StatusCheckResponse] =
    Encoder.forProduct2("ok", "systems")(x => StatusCheckResponse.unapply(x).get)

  val googleProjectSegment = Segment.map(GoogleProject)
  val runtimeNameSegment = Segment.map(RuntimeName)
  val runtimeNameSegmentWithValidation = Segment.map(x =>
    validateName(x).map(RuntimeName).getOrElse(throw BadRequestException(s"Invalid runtime name $x", None))
  )
  val diskIdSegment = Segment.map(x => DiskId(x.toLong))

  val workspaceIdSegment = Segment.map { x =>
    val components: Array[String] = x.split("-")
    if (components.length != 5)
      throw BadRequestException(s"Invalid workspace id $x, workspace id must be a valid UUID", None)
    else WorkspaceId(UUID.fromString(x))
  }
  val appNameSegment = Segment.map(AppName)
  val serviceNameSegment = Segment.map(ServiceName)
  val terminalNameSegment = Segment.map(TerminalName)

  // Adds `orElse` to Directive1[Option[A]]
  implicit private[api] class Directive1Support[A](d1: Directive1[Option[A]]) {
    def orElse(d2: => Directive1[Option[A]]): Directive1[Option[A]] =
      d1.flatMap {
        case Some(a) => provide(Some(a))
        case None    => d2
      }
  }

  def extractAppContext(span: Option[Span], requestUri: String = ""): Directive1[Ask[IO, AppContext]] =
    optionalHeaderValueByName(traceIdHeaderString.toString).map { case uuidOpt =>
      val traceId = uuidOpt.getOrElse(UUID.randomUUID().toString)
      val now = Instant.now()
      val appContext = AppContext(TraceId(traceId), now, requestUri, span)
      Ask.const[IO, AppContext](appContext)
    }
}

object ImplicitConversions {
  implicit def ioToFuture[A](ioa: IO[A]): Future[A] =
    ioa.unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
}
