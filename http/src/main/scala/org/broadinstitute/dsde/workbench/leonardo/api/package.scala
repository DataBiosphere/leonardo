package org.broadinstitute.dsde.workbench.leonardo
package http

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives.provide
import akka.http.scaladsl.server.Directives.optionalHeaderValueByName
import cats.effect.IO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import spray.json._
import akka.http.scaladsl.server.PathMatchers.Segment
import cats.mtl.Ask
import io.opencensus.trace.Span
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.dao.TerminalName
import org.broadinstitute.dsde.workbench.model.TraceId

import scala.concurrent.Future

package object api {
  implicit def ioMarshaller[A, B](implicit m: Marshaller[Future[A], B]): Marshaller[IO[A], B] =
    Marshaller(implicit ec => (x => m(x.unsafeToFuture())))

  // This is adapted from spray.json.CollectionFormats.listFormat
  implicit def listRootJsonWriter[T: RootJsonWriter]: RootJsonWriter[Vector[T]] =
    (list: Vector[T]) => JsArray(list.map(_.toJson))

  // This is adapted from spray.json.CollectionFormats.listFormat
  implicit def listRootJsonReader[T: RootJsonReader]: RootJsonReader[List[T]] =
    (value: JsValue) =>
      value match {
        case JsArray(elements) => elements.map(_.convertTo[T]).view.to(List)
        case x                 => deserializationError("Expected List as JsArray, but got " + x)
      }

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
