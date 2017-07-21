package org.broadinstitute.dsde.workbench.leonardo.model

import akka.actor.ActorSystem
import akka.pattern._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by tsharpe on 9/21/15.
 */
trait Retry {
  this: LazyLogging =>
  val system: ActorSystem

  type Predicate[A] = A => Boolean

  def always[A]: Predicate[A] = _ => true

  val defaultErrorMessage = "retry-able operation failed"

  def retry[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    retry(allBackoffIntervals)(op, pred, failureLogMessage)
  }

  def retryExponentially[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    retry(exponentialBackOffIntervals)(op, pred, failureLogMessage)
  }

  /**
   * will retry at the given interval until success or the overall timeout has passed
   * @param pred which failures to retry
   * @param interval how often to retry
   * @param timeout how long from now to give up
   * @param op what to try
   * @param executionContext
   * @tparam T
   * @return
   */
  def retryUntilSuccessOrTimeout[T](pred: Predicate[Throwable] = always, failureLogMessage: String = defaultErrorMessage)(interval: FiniteDuration, timeout: FiniteDuration)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val trialCount = Math.ceil(timeout / interval).toInt
    retry(Seq.fill(trialCount)(interval))(op, pred, failureLogMessage)
  }

  private def retry[T](remainingBackoffIntervals: Seq[FiniteDuration])(op: => () => Future[T], pred: Predicate[Throwable], failureLogMessage: String)(implicit executionContext: ExecutionContext): Future[T] = {
    op().recoverWith {
      case t if pred(t) && !remainingBackoffIntervals.isEmpty =>
        logger.info(s"$failureLogMessage: ${remainingBackoffIntervals.size} retries remaining, retrying in ${remainingBackoffIntervals.head}", t)
        after(remainingBackoffIntervals.head, system.scheduler) {
          retry(remainingBackoffIntervals.tail)(op, pred, failureLogMessage)
        }

      case t =>
        if (remainingBackoffIntervals.isEmpty) {
          logger.info(s"$failureLogMessage: no retries remaining", t)
        } else {
          logger.info(s"$failureLogMessage: retries remain but predicate failed, not retrying", t)
        }

        Future.failed(t)
    }
  }

  private val allBackoffIntervals = Seq(100 milliseconds, 1 second, 3 seconds)

  private def exponentialBackOffIntervals: Seq[FiniteDuration] = {
    val plainIntervals = Seq(1000 milliseconds, 2000 milliseconds, 4000 milliseconds, 8000 milliseconds, 16000 milliseconds, 32000 milliseconds)
    plainIntervals.map(i => addJitter(i, 1000 milliseconds))
  }

  private def toScalaDuration(javaDuration: java.time.Duration) = Duration.fromNanos(javaDuration.toNanos)

  private def addJitter(baseTime: FiniteDuration, maxJitterToAdd: FiniteDuration): FiniteDuration = {
    baseTime + ((scala.util.Random.nextFloat * maxJitterToAdd.toNanos) nanoseconds)
  }

  private def addJitter(baseTime: FiniteDuration): FiniteDuration = {
    if(baseTime < (1 second)) {
      addJitter(baseTime, 100 milliseconds)
    } else if (baseTime < (10 seconds)) {
      addJitter(baseTime, 500 milliseconds)
    } else {
      addJitter(baseTime, 1 second)
    }
  }

  /**
    * Converts a [[java.util.Map.Entry]] to a [[scala.Tuple2]]
    */
  implicit class JavaEntrySupport[A, B](entry: java.util.Map.Entry[A, B]) {
    def toTuple: (A, B) = (entry.getKey, entry.getValue)
  }

}
