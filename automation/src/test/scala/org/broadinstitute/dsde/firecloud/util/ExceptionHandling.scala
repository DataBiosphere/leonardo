package org.broadinstitute.dsde.firecloud.util

import com.typesafe.scalalogging.LazyLogging

import scala.util.control.NonFatal

/**
  */
//trait ExceptionHandling { self: LazyLogging =>
trait ExceptionHandling extends LazyLogging {
  /**
    * Return a partial function that logs and suppresses any "non-fatal"
    * exceptions. To be used liberally during test clean-up operations to
    * avoid overshadowing exceptions and failures from the test itself:
    *
    * <pre>
    *   try cleanUp() catch nonFatalAndLog
    * </pre>
    */
  def nonFatalAndLog: PartialFunction[Throwable, Unit] = {
    case NonFatal(e) => logger.warn(e.getMessage)
  }

  /**
    * Return a partial function that logs and suppresses any "non-fatal"
    * exceptions. To be used liberally during test clean-up operations to
    * avoid overshadowing exceptions and failures from the test itself:
    *
    * <pre>
    *   try cleanUp() catch nonFatalAndLog("Oops")
    * </pre>
    */
  def nonFatalAndLog(message: String): PartialFunction[Throwable, Unit] = {
    case NonFatal(e) => logger.warn(s"$message", e)
  }
}
