package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

case class AuthProviderException(authProviderClassName: String)
  extends LeoException(s"Call to $authProviderClassName auth provider failed", StatusCodes.InternalServerError)

/**
  * Wraps a LeoAuthProvider and provides error handling so provider-thrown errors don't bubble up our app.
  */
object LeoAuthProviderHelper {
  def apply(wrappedAuthProvider: LeoAuthProvider, config: Config, serviceAccountProvider: ServiceAccountProvider): LeoAuthProviderHelper = {
    new LeoAuthProviderHelper(wrappedAuthProvider, config, serviceAccountProvider)
  }

  def create(className: String, config: Config, serviceAccountProvider: ServiceAccountProvider): LeoAuthProviderHelper = {
    val authProvider = Class.forName(className)
      .getConstructor(classOf[Config], classOf[ServiceAccountProvider])
      .newInstance(config, serviceAccountProvider)
      .asInstanceOf[LeoAuthProvider]

    apply(authProvider, config, serviceAccountProvider)
  }
}

class LeoAuthProviderHelper(wrappedAuthProvider: LeoAuthProvider, authConfig: Config, serviceAccountProvider: ServiceAccountProvider) extends LeoAuthProvider(authConfig, serviceAccountProvider) {

  private def safeCall[T](future: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    future.recover {
      case e: LeoException => throw e
      case NonFatal(_) => throw AuthProviderException(wrappedAuthProvider.getClass.getSimpleName)
    }
  }

  override def hasProjectPermission(userEmail: WorkbenchEmail, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    safeCall {
      wrappedAuthProvider.hasProjectPermission(userEmail, action, googleProject)
    }
  }

  override def canSeeAllClustersInProject(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    safeCall {
      wrappedAuthProvider.canSeeAllClustersInProject(userEmail, googleProject)
    }
  }

  override def hasNotebookClusterPermission(userEmail: WorkbenchEmail, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    safeCall {
      wrappedAuthProvider.hasNotebookClusterPermission(userEmail, action, googleProject, clusterName)
    }
  }

  override def notifyClusterCreated(creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    safeCall {
      wrappedAuthProvider.notifyClusterCreated(creatorEmail, googleProject, clusterName)
    }
  }

  override def notifyClusterDeleted(userEmail: WorkbenchEmail, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    safeCall {
      wrappedAuthProvider.notifyClusterDeleted(userEmail, creatorEmail, googleProject, clusterName)
    }
  }
}