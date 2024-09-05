package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import cats.effect.Async
import org.apache.http.HttpStatus
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.client.sam.ApiException
import org.broadinstitute.dsde.workbench.util2.addJitter

import java.net.SocketTimeoutException
import scala.concurrent.duration._

object SamRetry {

  /**
   * Initial delay of 1 second, exponential retry up to 5 times.
   */
  private val defaultSamRetryConfig =
    RetryConfig(addJitter(1 seconds, 1 seconds), _ * 2, 5, isRetryable)

  /**
   * Requests made through the Sam client library sometimes fail with timeouts, generally due to
   * transient network or connection issues. When this happens, the client library will throw an API
   * exceptions with status code 0 wrapping a SocketTimeoutException. These errors should always be
   * retried.
   */
  private def isTimeoutException(apiException: ApiException): Boolean =
    (apiException.getCode == 0) && apiException.getCause.isInstanceOf[SocketTimeoutException]

  private def isRetryable(throwable: Throwable): Boolean = throwable match {
    case e: ApiException =>
      isTimeoutException(e) ||
      e.getCode == HttpStatus.SC_INTERNAL_SERVER_ERROR ||
      e.getCode == HttpStatus.SC_BAD_GATEWAY ||
      e.getCode == HttpStatus.SC_SERVICE_UNAVAILABLE ||
      e.getCode == HttpStatus.SC_GATEWAY_TIMEOUT
    case _ => false
  }

  /**
   * Retries an effect with a given retry configuration. Delegates to
   * fs2.Stream.retry under the hood.
   * @param retryConfig the retry config to use
   * @param fa the effect to retry
   * @return the first successful effect or the last error after retrying
   */
  def retry[F[_], A](retryConfig: RetryConfig)(fa: F[A])(implicit
    F: Async[F]
  ): F[A] =
    fs2.Stream
      .retry[F, A](fa,
                   retryConfig.retryInitialDelay,
                   retryConfig.retryNextDelay,
                   retryConfig.maxAttempts,
                   retryConfig.retryable
      )
      .compile
      .lastOrError

  /**
   * Convenience method which uses the default Sam retry policy and takes a
   * thunk instead of an effect. Wraps the thunk in F.blocking(), which is convenient
   * for working with the Java Sam client.
   */
  def retry[F[_], A](thunk: => A)(implicit
    F: Async[F]
  ): F[A] =
    retry(defaultSamRetryConfig)(F.blocking(thunk))
}
