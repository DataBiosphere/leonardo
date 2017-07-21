package org.broadinstitute.dsde.workbench.leonardo.model

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 10/5/15.
 */
trait FutureSupport {
  /**
   * Converts a Future[T] to a Future[Try[T]]. Even if the operation of the Future failed, the resulting
   * Future is considered a success and the error is available in the Try.
   *
   * @param f
   * @tparam T
   * @return
   */
  def toFutureTry[T](f: Future[T])(implicit executionContext: ExecutionContext): Future[Try[T]] = f map(Success(_)) recover { case t => Failure(t) }

  /**
   * Returns a failed future if any of the input tries have failed, otherwise returns the input with tries unwrapped in a successful Future
   */
  def assertSuccessfulTries[K, T](tries: Map[K, Try[T]])(implicit executionContext: ExecutionContext): Future[Map[K, T]] = {
    val failures = tries.values.collect{ case Failure(t) => t }
    if (failures.isEmpty) Future.successful(tries.map { case (k, v) => k -> v.get}) else Future.failed(failures.head)
  }


}
