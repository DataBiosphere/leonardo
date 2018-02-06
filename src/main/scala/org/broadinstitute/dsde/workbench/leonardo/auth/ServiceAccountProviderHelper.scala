package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

case class ServiceAccountProviderException(serviceAccountProviderClassName: String)
  extends LeoException(s"Call to $serviceAccountProviderClassName service account provider failed", StatusCodes.InternalServerError)

/**
  * Wraps a ServiceAccountProvider and provides error handling so provider-thrown errors don't bubble up our app.
  */
object ServiceAccountProviderHelper {
  def apply(wrappedServiceAccountProvider: ServiceAccountProvider, config: Config): ServiceAccountProviderHelper = {
    new ServiceAccountProviderHelper(wrappedServiceAccountProvider, config)
  }

  def create(className: String, config: Config): ServiceAccountProviderHelper = {
    val serviceAccountProvider = Class.forName(className)
      .getConstructor(classOf[Config])
      .newInstance(config)
      .asInstanceOf[ServiceAccountProvider]

    apply(serviceAccountProvider, config)
  }
}

class ServiceAccountProviderHelper(wrappedServiceAccountProvider: ServiceAccountProvider, config: Config) extends ServiceAccountProvider(config) with LazyLogging {

  private def safeCall[T](future: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val exceptionHandler: PartialFunction[Throwable, Future[Nothing]] = {
      case e: LeoException => Future.failed(e)
      case NonFatal(e) =>
        val wrappedClassName = wrappedServiceAccountProvider.getClass.getSimpleName
        logger.error(s"Service account provider $wrappedClassName throw an exception", e)
        Future.failed(ServiceAccountProviderException(wrappedClassName))
    }

    // recover from failed futures AND catch thrown exceptions
    try { future.recoverWith(exceptionHandler) } catch exceptionHandler
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
}