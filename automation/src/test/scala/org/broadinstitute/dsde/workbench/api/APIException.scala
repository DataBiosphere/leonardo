package org.broadinstitute.dsde.workbench.api

case class APIException (message: String = null, cause: Throwable = null) extends Exception(message, cause)
