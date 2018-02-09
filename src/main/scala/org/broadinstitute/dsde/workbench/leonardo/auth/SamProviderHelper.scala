package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.util.FutureSupport

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

case class SamProviderException(providerClassName: String)
  extends LeoException(s"Call to Sam provider $providerClassName failed", StatusCodes.InternalServerError)

/**
  * Created by rtitle on 2/9/18.
  */
trait SamProviderHelper[T] extends LazyLogging with FutureSupport {
  val system: ActorSystem
  val wrappedProvider: T

  private val samTimeout = 15 seconds
  private implicit val scheduler = system.scheduler

  protected def safeCallSam[T](future: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val exceptionHandler: PartialFunction[Throwable, Future[Nothing]] = {
      case e: LeoException => Future.failed(e)
      case NonFatal(e) =>
        val wrappedProviderClassName = wrappedProvider.getClass.getSimpleName
        logger.error(s"Sam provider $wrappedProviderClassName throw an exception", e)
        Future.failed(SamProviderException(wrappedProviderClassName))
    }

    // recover from failed futures AND catch thrown exceptions
    // Also enforce a Sam timeout to ensure Sam unavailability doesn't cause Leo to time out.
    try { future.recoverWith(exceptionHandler).withTimeout(samTimeout, s"Call to Sam timed out after $samTimeout") } catch exceptionHandler
  }

}
