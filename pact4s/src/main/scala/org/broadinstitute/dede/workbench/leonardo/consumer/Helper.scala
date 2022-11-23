package org.broadinstitute.dede.workbench.leonardo.consumer

import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.http4s.{AuthScheme, Credentials}
import org.http4s.Credentials.Token

case object InvalidCredentials extends Exception

case object UserAlreadyExists extends Exception

case object UnknownError extends Exception

object AuthHelper {
  def mockBearerHeader(workbenchEmail: WorkbenchEmail) = s"Bearer TokenFor$workbenchEmail"
  def mockAuthToken(workbenchEmail: WorkbenchEmail): Token =
    Credentials.Token(AuthScheme.Bearer, s"TokenFor$workbenchEmail")
}
