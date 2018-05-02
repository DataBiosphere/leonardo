package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util.FutureSupport

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.control.NonFatal

case class AuthProviderException(authProviderClassName: String, isTimeout: Boolean = false)
  extends LeoException(s"Call to $authProviderClassName auth provider ${if (isTimeout) "timed out" else "failed"}", StatusCodes.InternalServerError)

/**
  * Wraps a LeoAuthProvider and provides error handling so provider-thrown errors don't bubble up our app.
  */
object LeoAuthProviderHelper {
  def apply(wrappedAuthProvider: LeoAuthProvider, config: Config, serviceAccountProvider: ServiceAccountProvider)(implicit system: ActorSystem): LeoAuthProviderHelper = {
    new LeoAuthProviderHelper(wrappedAuthProvider, config, serviceAccountProvider)
  }

  def create(className: String, config: Config, serviceAccountProvider: ServiceAccountProvider)(implicit system: ActorSystem): LeoAuthProviderHelper = {
    val authProvider = Class.forName(className)
      .getConstructor(classOf[Config], classOf[ServiceAccountProvider])
      .newInstance(config, serviceAccountProvider)
      .asInstanceOf[LeoAuthProvider]

    apply(authProvider, config, serviceAccountProvider)
  }
}

class LeoAuthProviderHelper(private[leonardo] val wrappedAuthProvider: LeoAuthProvider, authConfig: Config, serviceAccountProvider: ServiceAccountProvider)(implicit system: ActorSystem)
  extends LeoAuthProvider(authConfig, serviceAccountProvider) with FutureSupport with LazyLogging {

  // Default timeout is specified in reference.conf
  private lazy val providerTimeout = authConfig.as[FiniteDuration]("providerTimeout")
  private implicit val scheduler = system.scheduler

  private def safeCall[T](future: => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val exceptionHandler: PartialFunction[Throwable, Future[Nothing]] = {
      case e: LeoException => Future.failed(e)
      case te: TimeoutException =>
        val wrappedClassName = wrappedAuthProvider.getClass.getSimpleName
        logger.error(s"Auth provider $wrappedClassName timed out after $providerTimeout", te)
        Future.failed(AuthProviderException(wrappedClassName, isTimeout = true))
      case NonFatal(e) =>
        val wrappedClassName = wrappedAuthProvider.getClass.getSimpleName
        logger.error(s"Auth provider $wrappedClassName throw an exception", e)
        Future.failed(AuthProviderException(wrappedClassName))
    }

    // recover from failed futures AND catch thrown exceptions
    try { future.withTimeout(providerTimeout, "" /* errMsg, not used */).recoverWith(exceptionHandler) } catch exceptionHandler
  }

  override def hasProjectPermission(userInfo: UserInfo, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    safeCall {
      wrappedAuthProvider.hasProjectPermission(userInfo, action, googleProject)
    }
  }

  override def hasNotebookClusterPermission(userInfo: UserInfo, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    safeCall {
      wrappedAuthProvider.hasNotebookClusterPermission(userInfo, action, googleProject, clusterName)
    }
  }

  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, ClusterName)])(implicit executionContext: ExecutionContext): Future[List[(GoogleProject, ClusterName)]] = {
    safeCall {
      wrappedAuthProvider.filterUserVisibleClusters(userInfo, clusters)
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