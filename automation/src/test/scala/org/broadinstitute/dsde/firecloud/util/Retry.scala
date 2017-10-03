package org.broadinstitute.dsde.firecloud.util

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.FiniteDuration

/**
  */
object Retry extends LazyLogging {

  /**
    * Retry an operation periodically until it returns something or no more
    * retries remain. Returns when the operation returns Some or all retries
    * have been exhausted.
    *
    * @param remainingBackOffIntervals wait intervals between retries
    * @param op operation to retry
    * @return the result of the operation
    */
  def retry[T](remainingBackOffIntervals: Seq[FiniteDuration])(op: => Option[T]): Option[T] = {
    op match {
      case Some(x) => Some(x)
      case None => remainingBackOffIntervals match {
        case Nil => None
        case h :: t =>
          logger.info(s"Retrying: ${remainingBackOffIntervals.size} retries remaining, retrying in $h")
          Thread sleep h.toMillis
          retry(t)(op)
      }
    }
  }

  def retry[T](interval: FiniteDuration, timeout: FiniteDuration)(op: => Option[T]): Option[T] = {
    val iterations = (timeout / interval).round.toInt
    retry(Seq.fill(iterations)(interval))(op)
  }
}
