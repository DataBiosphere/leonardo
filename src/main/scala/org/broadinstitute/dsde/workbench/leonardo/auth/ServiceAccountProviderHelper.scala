package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProvider
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

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

class ServiceAccountProviderHelper(val wrappedProvider: ServiceAccountProvider, config: Config)(implicit val system: ActorSystem)
  extends ServiceAccountProvider(config) with SamProviderHelper[ServiceAccountProvider] {

  override def getClusterServiceAccount(workbenchEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    safeCallSam {
      wrappedProvider.getClusterServiceAccount(workbenchEmail, googleProject)
    }
  }

  override def getNotebookServiceAccount(workbenchEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Option[WorkbenchEmail]] = {
    safeCallSam {
      wrappedProvider.getNotebookServiceAccount(workbenchEmail, googleProject)
    }
  }

  override def listGroupsStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    safeCallSam {
      wrappedProvider.listGroupsStagingBucketReaders(userEmail)
    }
  }

  override def listUsersStagingBucketReaders(userEmail: WorkbenchEmail)(implicit executionContext: ExecutionContext): Future[List[WorkbenchEmail]] = {
    safeCallSam {
      wrappedProvider.listUsersStagingBucketReaders(userEmail)
    }
  }
}