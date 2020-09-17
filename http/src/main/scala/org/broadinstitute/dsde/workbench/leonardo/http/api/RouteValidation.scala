package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.server.Directives.reject
import akka.http.scaladsl.server.{Directive, Directive1, ValidationRejection}

import scala.util.control.NoStackTrace

object RouteValidation {
  def validateNameDirective[A](nameString: String, apply: String => A): Directive1[A] =
    Directive { inner =>
      validateName(nameString) match {
        case Left(e) =>
          reject(ValidationRejection(s"${nameString} is not valid name", Some(RequestValidationError(e))))
        case Right(c) => inner(Tuple1(apply(c)))
      }
    }
}

final case class RequestValidationError(message: String) extends NoStackTrace {
  override def getMessage: String = message
}
