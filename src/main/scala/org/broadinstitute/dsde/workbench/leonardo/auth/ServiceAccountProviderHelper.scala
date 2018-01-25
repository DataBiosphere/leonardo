package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

case class ServiceAccountProviderException(serviceAccountProviderClassName: String)
  extends LeoException(s"Call to $serviceAccountProviderClassName service account provider failed", StatusCodes.InternalServerError)

object ServiceAccountProviderHelper {
  def create(className: String, config: Config): ServiceAccountProvider = {
    val serviceAccountProvider = Class.forName(className)
      .getConstructor(classOf[Config])
      .newInstance(config)
      .asInstanceOf[ServiceAccountProvider]

    new ServiceAccountProviderHelper(serviceAccountProvider, config)
  }
}

/**
  * Wraps a ServiceAccountProvider and provides error handling so provider-thrown errors don't bubble up our app.
  */
class ServiceAccountProviderHelper(wrappedServiceAccountProvider: ServiceAccountProvider, config: Config) extends ServiceAccountProvider(config) {

  private def safeCall[T](future: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    future.recover {
      case e: LeoException => throw e
      case NonFatal(_) => throw ServiceAccountProviderException(wrappedServiceAccountProvider.getClass.getSimpleName)
    }
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
}