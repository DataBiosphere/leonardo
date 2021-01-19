package org.broadinstitute.dsde.workbench.leonardo.http

import java.time.Instant
import java.util.UUID
import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives.provide
import akka.http.scaladsl.server.Directives.optionalHeaderValueByName
import cats.effect.IO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import akka.http.scaladsl.server.PathMatchers.Segment
import cats.mtl.Ask
import io.opencensus.trace.Span
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.{traceIdHeaderString, AppContext, AppName, RuntimeName}
import org.broadinstitute.dsde.workbench.leonardo.dao.TerminalName
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.concurrent.Future

package object api {
  implicit def ioMarshaller[A, B](implicit m: Marshaller[Future[A], B]): Marshaller[IO[A], B] =
    Marshaller(implicit ec => (x => m(x.unsafeToFuture())))

  val googleProjectSegment = Segment.map(GoogleProject)
  val runtimeNameSegment = Segment.map(RuntimeName)
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

  def extractAppContext(span: Option[Span]): Directive1[Ask[IO, AppContext]] =
    optionalHeaderValueByName(traceIdHeaderString).map {
      case uuidOpt =>
        val traceId = uuidOpt.getOrElse(UUID.randomUUID().toString)
        val now = Instant.now()
        val appContext = AppContext(TraceId(traceId), now, span)
        Ask.const[IO, AppContext](appContext)
    }
}

object ImplicitConversions {
  implicit def ioToFuture[A](ioa: IO[A]): Future[A] =
    ioa.unsafeToFuture()
}
