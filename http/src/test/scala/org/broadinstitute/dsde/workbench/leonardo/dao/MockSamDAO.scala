package org.broadinstitute.dsde.workbench.leonardo
package dao

import java.util.UUID

import cats.effect.IO
import cats.implicits._
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{
  AppSamResource,
  PersistentDiskSamResource,
  ProjectSamResource,
  RuntimeSamResource
}
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class MockSamDAO extends SamDAO[IO] {
  val billingProjects: mutable.Map[(ProjectSamResource, Authorization), Set[ProjectAction]] = new TrieMap()
  val runtimes: mutable.Map[(RuntimeSamResource, Authorization), Set[RuntimeAction]] = new TrieMap()
  val persistentDisks: mutable.Map[(PersistentDiskSamResource, Authorization), Set[PersistentDiskAction]] =
    new TrieMap()
  val apps: mutable.Map[(AppSamResource, Authorization), Set[AppAction]] = new TrieMap()

  val projectOwners: mutable.Map[Authorization, Set[SamResourcePolicy]] = new TrieMap()
  val runtimeCreators: mutable.Map[Authorization, Set[SamResourcePolicy]] = new TrieMap()
  val diskCreators: mutable.Map[Authorization, Set[SamResourcePolicy]] = new TrieMap()
  val appCreators: mutable.Map[Authorization, Set[SamResourcePolicy]] = new TrieMap()
  val appManagers: mutable.Map[Authorization, Set[SamResourcePolicy]] = new TrieMap()

  val petSA = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val projectOwnerEmail = WorkbenchEmail("project-owner@test.org")
  val appManagerActions = Set(AppAction.GetAppStatus, AppAction.DeleteApp)

  //we don't care much about traceId in unit tests, hence providing a constant UUID here
  implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))

  override def hasResourcePermission(resource: SamResource, action: String, authHeader: Authorization)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Boolean] =
    resource.resourceType match {
      case SamResourceType.Project =>
        val res = billingProjects
          .get((ProjectSamResource(GoogleProject(resource.resourceId)), authHeader))
          .map(_.map(_.asString).contains(action))
          .getOrElse(false)
        IO.pure(res)
      case SamResourceType.Runtime =>
        val res = runtimes
          .get((RuntimeSamResource(resource.resourceId), authHeader))
          .map(_.map(_.asString).contains(action))
          .getOrElse(false)
        IO.pure(res)
      case SamResourceType.PersistentDisk =>
        val res = persistentDisks
          .get((PersistentDiskSamResource(resource.resourceId), authHeader))
          .map(_.map(_.asString).contains(action))
          .getOrElse(false)
        IO.pure(res)
      case SamResourceType.App =>
        val res = apps
          .get((AppSamResource(resource.resourceId), authHeader))
          .map(_.map(_.asString).contains(action))
          .getOrElse(false)
        IO.pure(res)
    }

  override def getResourcePolicies(
    authHeader: Authorization,
    resourceType: SamResourceType
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[List[SamResourcePolicy]] =
    resourceType match {
      case SamResourceType.Runtime =>
        IO.pure(runtimeCreators.get(authHeader).map(_.toList).getOrElse(List.empty))
      case SamResourceType.Project =>
        IO.pure(projectOwners.get(authHeader).map(_.toList).getOrElse(List.empty))
      case SamResourceType.PersistentDisk =>
        IO.pure(diskCreators.get(authHeader).map(_.toList).getOrElse(List.empty))
      case SamResourceType.App =>
        IO.pure((appCreators.toMap |+| appManagers.toMap).get(authHeader).map(_.toList).getOrElse(List.empty))
    }

  override def createResource(resource: SamResource, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    resource match {
      case r: RuntimeSamResource =>
        IO(runtimes += (r, userEmailToAuthorization(creatorEmail)) -> RuntimeAction.allActions)
      case r: PersistentDiskSamResource =>
        IO(persistentDisks += (r, userEmailToAuthorization(creatorEmail)) -> PersistentDiskAction.allActions)
      case r: ProjectSamResource =>
        IO(billingProjects += (r, userEmailToAuthorization(creatorEmail)) -> ProjectAction.allActions)
      case r: AppSamResource =>
        IO(apps += (r, userEmailToAuthorization(creatorEmail)) -> AppAction.allActions)
    }

  override def createResourceWithManagerPolicy(
    resource: SamResource,
    creatorEmail: WorkbenchEmail,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    resource match {
      case r: RuntimeSamResource =>
        IO(runtimes += (r, userEmailToAuthorization(creatorEmail)) -> RuntimeAction.allActions)
      case r: PersistentDiskSamResource =>
        IO(persistentDisks += (r, userEmailToAuthorization(creatorEmail)) -> PersistentDiskAction.allActions)
      case r: ProjectSamResource =>
        IO(billingProjects += (r, userEmailToAuthorization(creatorEmail)) -> ProjectAction.allActions)
      case r: AppSamResource =>
        IO(
          apps ++=
            Map((r, userEmailToAuthorization(creatorEmail)) -> AppAction.allActions,
                (r, userEmailToAuthorization(projectOwnerEmail)) -> appManagerActions)
        )
    }

  override def deleteResource(resource: SamResource,
                              userEmail: WorkbenchEmail,
                              creatorEmail: WorkbenchEmail,
                              googleProject: GoogleProject)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    resource match {
      case r: RuntimeSamResource =>
        IO(runtimes.remove((r, userEmailToAuthorization(userEmail))))
      case r: PersistentDiskSamResource =>
        IO(persistentDisks.remove((r, userEmailToAuthorization(userEmail))))
      case r: ProjectSamResource =>
        IO(billingProjects.remove((r, userEmailToAuthorization(userEmail))))
      case r: AppSamResource =>
        IO(apps.remove((r, userEmailToAuthorization(userEmail))))
    }

  override def getPetServiceAccount(authorization: Authorization, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[WorkbenchEmail]] =
    IO.pure(Some(petSA))

  override def getUserProxy(
    userEmail: WorkbenchEmail
  )(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Option[WorkbenchEmail]] =
    IO.pure(Some(WorkbenchEmail("PROXY_1234567890@dev.test.firecloud.org")))

  override def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Option[String]] = IO.pure(Some("token"))

  override def getStatus(implicit ev: ApplicativeAsk[IO, TraceId]): IO[StatusCheckResponse] =
    IO.pure(StatusCheckResponse(true, Map.empty))

  override def getListOfResourcePermissions(resource: SamResource, authHeader: Authorization)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[List[String]] =
    resource.resourceType match {
      case SamResourceType.Project =>
        val res = billingProjects
          .get((ProjectSamResource(GoogleProject(resource.resourceId)), authHeader))
          .map(_.toList)
          .map(_.map(_.asString))
          .getOrElse(List.empty)
        IO.pure(res)
      case SamResourceType.Runtime =>
        val res = runtimes
          .get((RuntimeSamResource(resource.resourceId), authHeader))
          .map(_.toList)
          .map(_.map(_.asString))
          .getOrElse(List.empty)
        IO.pure(res)
      case SamResourceType.PersistentDisk =>
        val res = persistentDisks
          .get((PersistentDiskSamResource(resource.resourceId), authHeader))
          .map(_.toList)
          .map(_.map(_.asString))
          .getOrElse(List.empty)
        IO.pure(res)
      case SamResourceType.App =>
        val res = apps
          .get((AppSamResource(resource.resourceId), authHeader))
          .map(_.toList)
          .map(_.map(_.asString))
          .getOrElse(List.empty)
        IO.pure(res)
    }
}

object MockSamDAO {
  def userEmailToAuthorization(workbenchEmail: WorkbenchEmail): Authorization =
    Authorization(Credentials.Token(AuthScheme.Bearer, s"TokenFor${workbenchEmail}"))
}
