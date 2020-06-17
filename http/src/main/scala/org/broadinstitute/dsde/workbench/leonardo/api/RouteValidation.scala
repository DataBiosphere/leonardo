package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.server.{Directive, Directive1}
import akka.http.scaladsl.server.Directives.failWith
import org.broadinstitute.dsde.workbench.google2.KubernetesName

import scala.util.control.NoStackTrace

object RouteValidation {
  private val leoNameReg = "([a-z|0-9|-])*".r

  final case class RequestValidationError(message: String) extends NoStackTrace {
    override def getMessage: String = message
  }

  private def validateName(nameString: String): Either[Throwable, String] =
    nameString match {
      case leoNameReg(_) => Right(nameString)
      case _ =>
        Left(
          RequestValidationError(
            s"Invalid name ${nameString}. Only lowercase alphanumeric characters, numbers and dashes are allowed in leo names"
          )
        )
    }

  def validateNameDirective[A](nameString: String, apply: String => A): Directive1[A] =
    Directive { inner =>
      validateName(nameString) match {
        case Left(e)  => failWith(e)
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
