package org.broadinstitute.dsde.workbench.leonardo
package http

import akka.http.scaladsl.marshalling.Marshaller
import akka.http.scaladsl.server.directives.OnSuccessMagnet
import akka.http.scaladsl.server.util.Tupler
import cats.effect.IO
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import spray.json._
import akka.http.scaladsl.server.PathMatchers.Segment

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
        case JsArray(elements) => elements.map(_.convertTo[T])(collection.breakOut)
        case x                 => deserializationError("Expected List as JsArray, but got " + x)
      }

  val googleProjectSegment = Segment.map(GoogleProject)
}

object ImplicitConversions {
  import scala.language.implicitConversions

  implicit def ioOnSuccessMagnet[A](ioa: IO[A])(implicit tupler: Tupler[A]): OnSuccessMagnet { type Out = tupler.Out } =
    OnSuccessMagnet.apply(ioa.unsafeToFuture())
}
