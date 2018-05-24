package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util.FutureSupport

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.control.NonFatal

case class ServiceAccountProviderException(serviceAccountProviderClassName: String, isTimeout: Boolean = false)
  extends LeoException(s"Call to $serviceAccountProviderClassName service account provider ${if (isTimeout) "timed out" else "failed"}", StatusCodes.InternalServerError)

/**
  * Wraps a ServiceAccountProvider and provides error handling so provider-thrown errors don't bubble up our app.
  */
object ServiceAccountProviderHelper {
  def apply(wrappedServiceAccountProvider: ServiceAccountProvider, config: Config)(implicit system: ActorSystem): ServiceAccountProviderHelper = {
    new ServiceAccountProviderHelper(wrappedServiceAccountProvider, config)
  }

  def create(className: String, config: Config)(implicit system: ActorSystem): ServiceAccountProviderHelper = {
    val serviceAccountProvider = Class.forName(className)
      .getConstructor(classOf[Config])
      .newInstance(config)
      .asInstanceOf[ServiceAccountProvider]

    apply(serviceAccountProvider, config)
  }
}

class ServiceAccountProviderHelper(wrappedServiceAccountProvider: ServiceAccountProvider, config: Config)(implicit system: ActorSystem)
  extends ServiceAccountProvider(config) with FutureSupport with LazyLogging {

  // Default timeout is specified in reference.conf
  private lazy val providerTimeout = config.as[FiniteDuration]("providerTimeout")
  private implicit val scheduler = system.scheduler

  private def safeCall[T](future: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val exceptionHandler: PartialFunction[Throwable, Future[Nothing]] = {
      case e: LeoException => Future.failed(e)
      case te: TimeoutException =>
        val wrappedClassName = wrappedServiceAccountProvider.getClass.getSimpleName
        logger.error(s"Service Account provider $wrappedClassName timed out after $providerTimeout", te)
        Future.failed(ServiceAccountProviderException(wrappedClassName, isTimeout = true))
      case NonFatal(e) =>
        val wrappedClassName = wrappedServiceAccountProvider.getClass.getSimpleName
        logger.error(s"Service account provider $wrappedClassName throw an exception", e)
        Future.failed(ServiceAccountProviderException(wrappedClassName))
    }

    // recover from failed futures AND catch thrown exceptions
    try { future.withTimeout(providerTimeout, "" /* errMsg, not used */).recoverWith(exceptionHandler) } catch exceptionHandler
  }

  override def getClusterServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    safeCall {
      wrappedServiceAccountProvider.getClusterServiceAccount(userInfo, googleProject)
    }
  }

  override def getNotebookServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    safeCall {
      wrappedServiceAccountProvider.getNotebookServiceAccount(userInfo, googleProject)
    }
  }

  override def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    safeCall {
      wrappedServiceAccountProvider.listGroupsStagingBucketReaders(userEmail)
    }
  }

  override def listUsersStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    safeCall {
      wrappedServiceAccountProvider.listUsersStagingBucketReaders(userEmail)
    }
  }

  override def getAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[String]] = {
    safeCall {
      wrappedServiceAccountProvider.getAccessToken(userEmail, googleProject)
    }
  }
}