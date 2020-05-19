package org.broadinstitute.dsde.workbench.leonardo
package auth

import java.util.concurrent.TimeUnit

import cats.implicits._
import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.mtl.ApplicativeAsk
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.Token
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, HttpGoogleIamDAO}
import org.broadinstitute.dsde.workbench.leonardo.auth.IamProxyAuthProvider.{CacheKey, ProjectAuthCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterAction
import org.broadinstitute.dsde.workbench.leonardo.model.PersistentDiskAction
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectAction
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, IamPermission}

import scala.concurrent.duration.{FiniteDuration, MINUTES}
import scala.concurrent.{ExecutionContext, Future}

object IamProxyAuthProvider {
  sealed private trait CacheKey
  private case class ProjectAuthCacheKey(userEmail: WorkbenchEmail,
                                         userToken: OAuth2BearerToken,
                                         googleProject: GoogleProject,
                                         executionContext: ExecutionContext)
      extends CacheKey
}

class IamProxyAuthProvider(config: Config, saProvider: ServiceAccountProvider[Future])(
  implicit val ec: ExecutionContext
) extends LeoAuthProvider[Future] {
  override def serviceAccountProvider: ServiceAccountProvider[Future] = saProvider

  // Create implicit actor needed by the GoogleIamDAO.
  implicit val system = ActorSystem("iam-proxy-auth-actor-system")
  // Load config values.
  private val cacheEnabled = config.getOrElse("cacheEnabled", true)
  private val cacheMaxSize = config.getAs[Int]("cacheMaxSize").getOrElse(1000)
  private val cacheExpiryTime = config.getAs[FiniteDuration]("cacheExpiryTime").getOrElse(FiniteDuration(15, MINUTES))
  private val applicationName = config.getString("applicationName")
  private val requiredPermissions: Set[IamPermission] =
    config.as[Set[String]]("requiredProjectIamPermissions").map(p => IamPermission(p))

  // Cache notebook auth results from the IAM service. This API is called very often by the notebook proxy
  // and the "list clusters" endpoint.
  private val notebookAuthCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(cacheExpiryTime.toSeconds, TimeUnit.SECONDS)
    .maximumSize(cacheMaxSize)
    .build(
      new CacheLoader[CacheKey, Future[Boolean]] {
        def load(key: CacheKey) =
          key match {
            case ProjectAuthCacheKey(userEmail, userToken, googleProject, _) =>
              checkUserAccessFromIam(userEmail, userToken, googleProject)
          }
      }
    )

  protected def userGoogleIamDao(token: String): GoogleIamDAO =
    new HttpGoogleIamDAO(applicationName, Token(() => token), "google")

  protected def checkUserAccessFromIam(userEmail: WorkbenchEmail,
                                       userToken: OAuth2BearerToken,
                                       googleProject: GoogleProject): Future[Boolean] = {
    val iamDAO: GoogleIamDAO = userGoogleIamDao(userToken.token)
    iamDAO.testIamPermission(googleProject, requiredPermissions).map { foundPermissions =>
      foundPermissions == requiredPermissions
    }
  }

  protected def checkUserAccess(userInfo: UserInfo, googleProject: GoogleProject): Future[Boolean] =
    if (cacheEnabled) {
      notebookAuthCache.get(ProjectAuthCacheKey(userInfo.userEmail, userInfo.accessToken, googleProject, ec))
    } else {
      checkUserAccessFromIam(userInfo.userEmail, userInfo.accessToken, googleProject)
    }

  override def hasProjectPermission(userInfo: UserInfo, action: ProjectAction, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[Future, TraceId]
  ): Future[Boolean] =
    checkUserAccess(userInfo, googleProject)

  override def hasNotebookClusterPermission(
    internalId: RuntimeInternalId,
    userInfo: UserInfo,
    action: NotebookClusterAction,
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit ev: ApplicativeAsk[Future, TraceId]): Future[Boolean] =
    checkUserAccess(userInfo, googleProject)

  override def hasPersistentDiskPermission(
    internalId: DiskSamResourceId,
    userInfo: UserInfo,
    action: PersistentDiskAction,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[Future, TraceId]): Future[Boolean] =
    checkUserAccessFromIam(userInfo.userEmail, userInfo.accessToken, googleProject)

  override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, RuntimeInternalId)])(
    implicit ev: ApplicativeAsk[Future, TraceId]
  ): Future[List[(GoogleProject, RuntimeInternalId)]] = {
    // Check each project for user-access exactly once, then filter by project.
    val projects = clusters.map(lv => lv._1).toSet
    val projectAccess = projects.map(p => p.value -> checkUserAccess(userInfo, p)).toMap
    clusters.traverseFilter { c =>
      projectAccess(c._1.value).map {
        case true  => Some(c)
        case false => None
      }
    }
  }

  override def filterUserVisiblePersistentDisks(userInfo: UserInfo, disks: List[(GoogleProject, DiskSamResourceId)])(
    implicit ev: ApplicativeAsk[Future, TraceId]
  ): Future[List[(GoogleProject, DiskSamResourceId)]] = {
    // Check each project for user-access exactly once, then filter by project.
    val projects = disks.map(lv => lv._1).toSet
    val projectAccess = projects.map(p => p.value -> checkUserAccess(userInfo, p)).toMap
    disks.traverseFilter { c =>
      projectAccess(c._1.value).map {
        case true  => Some(c)
        case false => None
      }
    }
  }

  override def notifyClusterCreated(
    internalId: RuntimeInternalId,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit ev: ApplicativeAsk[Future, TraceId]): Future[Unit] = Future.unit

  override def notifyClusterDeleted(
    internalId: RuntimeInternalId,
    userEmail: WorkbenchEmail,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject,
    runtimeName: RuntimeName
  )(implicit ev: ApplicativeAsk[Future, TraceId]): Future[Unit] = Future.unit

  override def notifyPersistentDiskCreated(
    internalId: DiskSamResourceId,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[Future, TraceId]): Future[Unit] = Future.unit

  override def notifyPersistentDiskDeleted(
    internalId: DiskSamResourceId,
    userEmail: WorkbenchEmail,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[Future, TraceId]): Future[Unit] = Future.unit
}
