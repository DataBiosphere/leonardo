package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.server.{Directive, Directive1}
import akka.http.scaladsl.server.Directives.failWith
import org.broadinstitute.dsde.workbench.google2.KubernetesName

import scala.util.control.NoStackTrace

object RouteValidation {

  final case class RequestValidationError(message: String) extends NoStackTrace {
    override def getMessage: String = message
  }

  def validateNameDirective[A](nameString: String, apply: String => A): Directive1[A] =
    Directive { inner =>
      validateName(nameString) match {
        case Left(e)  => failWith(RequestValidationError(e))
        case Right(c) => inner(Tuple1(apply(c)))
      }
    }

  def validateKubernetesName[A](nameString: String, apply: String => A): Directive1[A] =
    Directive { inner =>
      KubernetesName.withValidation(nameString, apply) match {
        case Left(e)  => failWith(e)
        case Right(c) => inner(Tuple1(c))
      }
    }
}
