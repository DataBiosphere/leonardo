package org.broadinstitute.dsde.workbench.leonardo
package dao

import java.util.UUID

import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{
  AppSamResource,
  PersistentDiskSamResource,
  RuntimeSamResource
}
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials, EntityDecoder}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class MockSamDAO extends SamDAO[IO] {
  val billingProjects: mutable.Map[(GoogleProject, Authorization), Set[String]] = new TrieMap()
  val runtimes: mutable.Map[(RuntimeSamResource, Authorization), Set[String]] = new TrieMap()
  val persistentDisks: mutable.Map[(PersistentDiskSamResource, Authorization), Set[String]] = new TrieMap()
  val projectOwners: mutable.Map[Authorization, Set[SamProjectPolicy]] = new TrieMap()
  val runtimeCreators: mutable.Map[Authorization, Set[SamRuntimePolicy]] = new TrieMap()
  val diskCreators: mutable.Map[Authorization, Set[SamPersistentDiskPolicy]] = new TrieMap()
  val petSA = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val runtimeActions = Set("status", "connect", "sync", "delete", "read_policies")
  val diskActions = Set("read", "attach", "modify", "delete", "read_policies")
  val appActions = Set()
  implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID())) //we don't care much about traceId in unit tests, hence providing a constant UUID here

  override def hasResourcePermission(resource: SamResource, action: String, authHeader: Authorization)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Boolean] =
    resource.resourceType match {
      case SamResourceType.Project =>
        val res = billingProjects
          .get((GoogleProject(resource.resourceId), authHeader)) //look it up: Option[Set]
          .map(_.contains(action)) //open the option to peek the set: Option[Bool]
          .getOrElse(false) //unpack the resulting option and handle the project never having existed
        IO.pure(res)
      case SamResourceType.Runtime =>
        val res = runtimes
          .get((RuntimeSamResource(resource.resourceId), authHeader)) //look it up: Option[Set]
          .map(_.contains(action)) //open the option to peek the set: Option[Bool]
          .getOrElse(false) //unpack the resulting option and handle the runtime never having existed
        IO.pure(res)
      case SamResourceType.PersistentDisk =>
        val res = persistentDisks
          .get((PersistentDiskSamResource(resource.resourceId), authHeader)) //look it up: Option[Set]
          .map(_.contains(action)) //open the option to peek the set: Option[Bool]
          .getOrElse(false) //unpack the resulting option and handle the disk never having existed
        IO.pure(res)
      case SamResourceType.App =>
        IO.pure(true) //TODO: fix this properly
    }

  override def getResourcePolicies[A](
    authHeader: Authorization,
    resourceType: SamResourceType
  )(implicit decoder: EntityDecoder[IO, List[A]], ev: ApplicativeAsk[IO, TraceId]): IO[List[A]] =
    resourceType match {
      case SamResourceType.Runtime =>
        IO.pure(runtimeCreators.get(authHeader).map(_.toList).getOrElse(List.empty)).map(_.asInstanceOf[List[A]])
      case SamResourceType.Project =>
        IO.pure(projectOwners.get(authHeader).map(_.toList).getOrElse(List.empty)).map(_.asInstanceOf[List[A]])
      case SamResourceType.PersistentDisk =>
        IO.pure(diskCreators.get(authHeader).map(_.toList).getOrElse(List.empty)).map(_.asInstanceOf[List[A]])
      case SamResourceType.App =>
        IO.pure(List[A]()) //TODO
    }

  override def createResource(resource: SamResource, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[IO, TraceId]
  ): IO[Unit] =
    resource match {
      case r: RuntimeSamResource =>
        IO(runtimes += (r, userEmailToAuthorization(creatorEmail)) -> runtimeActions)
      case r: PersistentDiskSamResource =>
        IO(persistentDisks += (r, userEmailToAuthorization(creatorEmail)) -> diskActions)
      case r: AppSamResource => IO.unit //TODO
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
      case r: AppSamResource => IO.unit //TODO
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
          .get((GoogleProject(resource.resourceId), authHeader))
          .map(_.toList)
          .getOrElse(List.empty)
        IO.pure(res)
      case SamResourceType.Runtime =>
        val res = runtimes
          .get((RuntimeSamResource(resource.resourceId), authHeader))
          .map(_.toList)
          .getOrElse(List.empty)
        IO.pure(res)
      case SamResourceType.PersistentDisk =>
        val res = persistentDisks
          .get((PersistentDiskSamResource(resource.resourceId), authHeader))
          .map(_.toList)
          .getOrElse(List.empty)
        IO.pure(res)
      case SamResourceType.App => IO.pure(List.empty) //TODO
    }
}

object MockSamDAO {
  def userEmailToAuthorization(workbenchEmail: WorkbenchEmail): Authorization =
    Authorization(Credentials.Token(AuthScheme.Bearer, s"TokenFor${workbenchEmail}"))
}
