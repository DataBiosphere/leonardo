package org.broadinstitute.dsde.workbench.leonardo.auth

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.leonardo.auth.LeoAuthProviderHelper.NotebookAuthCacheKey
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions.NotebookClusterAction
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Wraps a LeoAuthProvider and provides error handling so provider-thrown errors don't bubble up our app.
  */
object LeoAuthProviderHelper {

  private[LeoAuthProviderHelper] case class NotebookAuthCacheKey(userEmail: WorkbenchEmail, action: NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName, executionContext: ExecutionContext)

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

class LeoAuthProviderHelper(val wrappedProvider: LeoAuthProvider, authConfig: Config, serviceAccountProvider: ServiceAccountProvider)(implicit val system: ActorSystem)
  extends LeoAuthProvider(authConfig, serviceAccountProvider) with SamProviderHelper[LeoAuthProvider] {

  // Cache notebook auth results from Sam as this is called very often by the proxy and the "list clusters" endpoint.
  // Project-level auth is not called as frequently so it's not as important to cache it.
  private val notebookAuthCache = CacheBuilder.newBuilder()
    .expireAfterAccess(5, TimeUnit.MINUTES)
    .maximumSize(1000)
    .build(
      new CacheLoader[NotebookAuthCacheKey, Future[Boolean]] {
        def load(key: NotebookAuthCacheKey) = {
          implicit val ec = key.executionContext
          safeCallSam {
            wrappedProvider.hasNotebookClusterPermission(key.userEmail, key.action, key.googleProject, key.clusterName)
          }
        }
      }
    )

  override def hasProjectPermission(userEmail: WorkbenchEmail, action: ProjectActions.ProjectAction, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    safeCallSam {
      wrappedProvider.hasProjectPermission(userEmail, action, googleProject)
    }
  }

  override def canSeeAllClustersInProject(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    safeCallSam {
      wrappedProvider.canSeeAllClustersInProject(userEmail, googleProject)
    }
  }

  override def hasNotebookClusterPermission(userEmail: WorkbenchEmail, action: NotebookClusterActions.NotebookClusterAction, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    // Consult the notebook auth cache
    notebookAuthCache.get(NotebookAuthCacheKey(userEmail, action, googleProject, clusterName, executionContext))
  }

  override def notifyClusterCreated(creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    safeCallSam {
      wrappedProvider.notifyClusterCreated(creatorEmail, googleProject, clusterName)
    }
  }

  override def notifyClusterDeleted(userEmail: WorkbenchEmail, creatorEmail: WorkbenchEmail, googleProject: GoogleProject, clusterName: ClusterName)(implicit executionContext: ExecutionContext): Future[Unit] = {
    safeCallSam {
      wrappedProvider.notifyClusterDeleted(userEmail, creatorEmail, googleProject, clusterName)
    }
  }
}