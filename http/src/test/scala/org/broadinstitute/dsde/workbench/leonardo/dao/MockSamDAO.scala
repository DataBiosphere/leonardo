package org.broadinstitute.dsde.workbench.leonardo
package dao

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import cats.mtl.Ask
import cats.syntax.all._
import io.circe.{Decoder, Encoder}
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.leonardo.dao.MockSamDAO._
import org.broadinstitute.dsde.workbench.leonardo.model.{SamResource, SamResourceAction}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.StatusCheckResponse
import org.http4s.headers.Authorization
import org.http4s.{AuthScheme, Credentials}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

// TODO this class is full of .asInstanceOf calls to make our generic methods work with the mutable Maps.
// There may be a better way to structure this mock.
class MockSamDAO extends SamDAO[IO] {
  val billingProjects: mutable.Map[(ProjectSamResourceId, Authorization), Set[ProjectAction]] = new TrieMap()
  val runtimes: mutable.Map[(RuntimeSamResourceId, Authorization), Set[RuntimeAction]] = new TrieMap()
  val persistentDisks: mutable.Map[(PersistentDiskSamResourceId, Authorization), Set[PersistentDiskAction]] =
    new TrieMap()
  val workspaces: mutable.Map[(WorkspaceResourceSamResourceId, Authorization), Set[WorkspaceAction]] =
    new TrieMap()
  val wsmResources: mutable.Map[(WsmResourceSamResourceId, Authorization), Set[WsmResourceAction]] =
    new TrieMap()
  val apps: mutable.Map[(AppSamResourceId, Authorization), Set[AppAction]] = new TrieMap()

  var projectOwners: Map[Authorization, Set[(ProjectSamResourceId, SamPolicyName)]] = Map.empty
  var projectUsers: Map[Authorization, Set[(ProjectSamResourceId, SamPolicyName)]] = Map.empty
  var runtimeCreators: Map[Authorization, Set[(RuntimeSamResourceId, SamPolicyName)]] = Map.empty
  var diskCreators: Map[Authorization, Set[(PersistentDiskSamResourceId, SamPolicyName)]] = Map.empty
  var appCreators: Map[Authorization, Set[(AppSamResourceId, SamPolicyName)]] = Map.empty
  var wmsResourceCreators: Map[Authorization, Set[(AppSamResourceId, SamPolicyName)]] = Map.empty
  var workspaceCreators: Map[Authorization, Set[(AppSamResourceId, SamPolicyName)]] = Map.empty

  // we don't care much about traceId in unit tests, hence providing a constant UUID here
  implicit val traceId: Ask[IO, TraceId] = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))

  val workspaceId = UUID.randomUUID()

  override def registerLeo(implicit ev: Ask[IO, TraceId]): IO[Unit] = IO.unit
  override def hasResourcePermissionUnchecked(resourceType: SamResourceType,
                                              resource: String,
                                              action: String,
                                              authHeader: Authorization
  )(implicit
    ev: Ask[IO, TraceId]
  ): IO[Boolean] =
    resourceType match {
      case SamResourceType.Project =>
        val res = billingProjects
          .get((ProjectSamResourceId(GoogleProject(resource)), authHeader))
          .map(_.map(_.asString).contains(action))
          .getOrElse(false)
        IO.pure(res)
      case SamResourceType.Runtime =>
        val res = runtimes
          .get((RuntimeSamResourceId(resource), authHeader))
          .map(_.map(_.asString).contains(action))
          .getOrElse(false)
        IO.pure(res)
      case SamResourceType.PersistentDisk =>
        val res = persistentDisks
          .get((PersistentDiskSamResourceId(resource), authHeader))
          .map(_.map(_.asString).contains(action))
          .getOrElse(false)
        IO.pure(res)
      case SamResourceType.App =>
        val res = apps
          .get((AppSamResourceId(resource, None), authHeader))
          .map(_.map(_.asString).contains(action))
          .getOrElse(false)
        IO.pure(res)
      case SamResourceType.SharedApp =>
        val res = apps
          .get((AppSamResourceId(resource, Some(AppAccessScope.WorkspaceShared)), authHeader))
          .map(_.map(_.asString).contains(action))
          .getOrElse(false)
        IO.pure(res)
      case SamResourceType.Workspace =>
        val res = workspaces
          .get((WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString(resource))), authHeader))
          .map(_.map(_.asString).contains(action))
          .getOrElse(false)
        IO.pure(res)
      case SamResourceType.WsmResource =>
        val res = wsmResources
          .get((WsmResourceSamResourceId(WsmControlledResourceId(UUID.fromString(resource))), authHeader))
          .map(_.map(_.asString).contains(action))
          .getOrElse(false)
        IO.pure(res)
    }

  override def listResourceIdsWithRole[R <: SamResourceId](
    authHeader: Authorization
  )(implicit
    resourceDefinition: SamResource[R],
    resourceIdDecoder: Decoder[R],
    ev: Ask[IO, TraceId]
  ): IO[List[(R, SamRole)]] = resourceDefinition.resourceType match {
    case SamResourceType.Runtime =>
      IO.pure(
        runtimeCreators
          .get(authHeader)
          .map(_.toList)
          .getOrElse(List.empty)
          .map { case (resource, pn) => (resource, SamRole.stringToRole(pn.toString)) }
          .asInstanceOf[List[(R, SamRole)]]
      )
    case SamResourceType.Project =>
      IO.pure(
        (projectOwners ++ projectUsers)
          .get(authHeader)
          .map(_.toList)
          .getOrElse(List.empty)
          .map { case (resource, pn) => (resource, SamRole.stringToRole(pn.toString)) }
          .asInstanceOf[List[(R, SamRole)]]
      )
    case SamResourceType.PersistentDisk =>
      IO.pure(
        diskCreators
          .get(authHeader)
          .map(_.toList)
          .getOrElse(List.empty)
          .map { case (resource, pn) => (resource, SamRole.stringToRole(pn.toString)) }
          .asInstanceOf[List[(R, SamRole)]]
      )
    case SamResourceType.SharedApp => throw new Exception("SharedApp not supported")
    case SamResourceType.App       => throw new Exception("App not supported")
    case SamResourceType.Workspace =>
      IO.pure(
        workspaceCreators
          .get(authHeader)
          .map(_.toList)
          .getOrElse(List.empty)
          .map { case (resource, pn) => (resource, SamRole.stringToRole(pn.toString)) }
          .asInstanceOf[List[(R, SamRole)]]
      )
    case SamResourceType.WsmResource =>
      IO.pure(
        wmsResourceCreators
          .get(authHeader)
          .map(_.toList)
          .getOrElse(List.empty)
          .map { case (resource, pn) => (resource, SamRole.stringToRole(pn.toString)) }
          .asInstanceOf[List[(R, SamRole)]]
      )
  }

  override def getResourcePolicies[R](authHeader: Authorization, resourceType: SamResourceType)(implicit
    sr: SamResource[R],
    decoder: Decoder[R],
    ev: Ask[IO, TraceId]
  ): IO[List[(R, SamPolicyName)]] =
    resourceType match {
      case SamResourceType.Runtime =>
        IO.pure(
          runtimeCreators.get(authHeader).map(_.toList).getOrElse(List.empty).asInstanceOf[List[(R, SamPolicyName)]]
        )
      case SamResourceType.Project =>
        IO.pure(
          (projectOwners ++ projectUsers)
            .get(authHeader)
            .map(_.toList)
            .getOrElse(List.empty)
            .asInstanceOf[List[(R, SamPolicyName)]]
        )
      case SamResourceType.PersistentDisk =>
        IO.pure(diskCreators.get(authHeader).map(_.toList).getOrElse(List.empty).asInstanceOf[List[(R, SamPolicyName)]])
      case SamResourceType.App =>
        IO.pure(appCreators.get(authHeader).map(_.toList).getOrElse(List.empty).asInstanceOf[List[(R, SamPolicyName)]])
      case SamResourceType.SharedApp =>
        IO.pure(appCreators.get(authHeader).map(_.toList).getOrElse(List.empty).asInstanceOf[List[(R, SamPolicyName)]])
      case SamResourceType.Workspace =>
        IO.pure(
          workspaceCreators.get(authHeader).map(_.toList).getOrElse(List.empty).asInstanceOf[List[(R, SamPolicyName)]]
        )
      case SamResourceType.WsmResource =>
        IO.pure(
          wmsResourceCreators.get(authHeader).map(_.toList).getOrElse(List.empty).asInstanceOf[List[(R, SamPolicyName)]]
        )
    }

  override def createResourceAsGcpPet[R](resource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit
    sr: SamResource[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = {
    val authHeader = userEmailToAuthorization(creatorEmail)
    resource match {
      case r: RuntimeSamResourceId =>
        IO(runtimes += (r, authHeader) -> RuntimeAction.allActions) >>
          IO {
            runtimeCreators = runtimeCreators |+| Map(authHeader -> Set((r, SamPolicyName.Creator)))
          }.void
      case r: PersistentDiskSamResourceId =>
        IO(persistentDisks += (r, authHeader) -> PersistentDiskAction.allActions) >>
          IO {
            diskCreators = diskCreators |+| Map(
              authHeader -> Set(
                (r, SamPolicyName.Creator)
              )
            )
          }.void
      case r: ProjectSamResourceId =>
        IO(billingProjects += (r, authHeader) -> ProjectAction.allActions) >>
          IO {
            projectOwners = projectOwners |+| Map(
              authHeader -> Set(
                (r, SamPolicyName.Owner)
              )
            )
          }.void
      case r: AppSamResourceId =>
        IO(apps += (r, authHeader) -> AppAction.allActions) >>
          IO {
            appCreators = appCreators |+| Map(
              authHeader -> Set(
                (r, SamPolicyName.Creator)
              )
            )
          }.void
    }
  }

  override def createResourceWithGoogleProjectParent[R](resource: R,
                                                        creatorEmail: WorkbenchEmail,
                                                        googleProject: GoogleProject
  )(implicit
    sr: SamResource[R],
    encoder: Encoder[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = {
    val authHeader = userEmailToAuthorization(creatorEmail)
    val ownerAuthHeader = userEmailToAuthorization(projectOwnerEmail)
    resource match {
      case r: RuntimeSamResourceId =>
        IO(runtimes += (r, authHeader) -> RuntimeAction.allActions) >>
          IO {
            runtimeCreators = runtimeCreators |+| Map(
              authHeader -> Set(
                (r, SamPolicyName.Creator)
              )
            )
          }.void
      case r: PersistentDiskSamResourceId =>
        IO(persistentDisks += (r, authHeader) -> PersistentDiskAction.allActions) >>
          IO {
            diskCreators = diskCreators |+| Map(
              authHeader -> Set(
                (r, SamPolicyName.Creator)
              )
            )
          }.void
      case r: ProjectSamResourceId =>
        IO(billingProjects += (r, authHeader) -> ProjectAction.allActions) >> IO {
          projectOwners = projectOwners |+| Map(
            authHeader -> Set(
              (r, SamPolicyName.Owner)
            )
          )
        }.void
      case r: AppSamResourceId =>
        IO(
          apps ++=
            Map((r, authHeader) -> AppAction.allActions, (r, ownerAuthHeader) -> appManagerActions)
        ) >>
          IO {
            appCreators = appCreators |+| Map(
              authHeader -> Set((r, SamPolicyName.Creator)),
              ownerAuthHeader -> Set((r, SamPolicyName.Manager))
            )
          }.void
    }
  }

  override def createResourceWithWorkspaceParent[R](resource: R,
                                                    creatorEmail: WorkbenchEmail,
                                                    userInfo: UserInfo,
                                                    workspaceId: WorkspaceId
  )(implicit
    sr: SamResource[R],
    encoder: Encoder[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = {
    val authHeader = userEmailToAuthorization(creatorEmail)
    val projectOwnerAuthHeader = userEmailToAuthorization(projectOwnerEmail)
    val workspaceOwnerAuthHeader = userEmailToAuthorization(workspaceOwnerEmail)
    resource match {
      case r: RuntimeSamResourceId =>
        IO(runtimes += (r, authHeader) -> RuntimeAction.allActions) >>
          IO {
            runtimeCreators = runtimeCreators |+| Map(
              authHeader -> Set(
                (r, SamPolicyName.Creator)
              )
            )
          }.void
      case r: PersistentDiskSamResourceId =>
        IO(persistentDisks += (r, authHeader) -> PersistentDiskAction.allActions) >>
          IO {
            diskCreators = diskCreators |+| Map(
              authHeader -> Set(
                (r, SamPolicyName.Creator)
              )
            )
          }.void
      case r: ProjectSamResourceId =>
        IO(billingProjects += (r, authHeader) -> ProjectAction.allActions) >> IO {
          projectOwners = projectOwners |+| Map(
            authHeader -> Set(
              (r, SamPolicyName.Owner)
            )
          )
        }.void
      case r: AppSamResourceId =>
        r.resourceType match {
          case SamResourceType.App =>
            IO(
              apps ++=
                Map((r, authHeader) -> AppAction.allActions, (r, projectOwnerAuthHeader) -> appManagerActions)
            ) >>
              IO {
                appCreators = appCreators |+| Map(
                  authHeader -> Set((r, SamPolicyName.Creator)),
                  projectOwnerAuthHeader -> Set((r, SamPolicyName.Manager))
                )
              }.void
          case _ =>
            IO(
              apps ++=
                Map((r, authHeader) -> AppAction.allActions, (r, workspaceOwnerAuthHeader) -> appManagerActions)
            ) >>
              IO {
                appCreators = appCreators |+| Map(
                  authHeader -> Set((r, SamPolicyName.Owner)),
                  workspaceOwnerAuthHeader -> Set((r, SamPolicyName.User))
                )
              }.void
        }
    }
  }

  override def deleteResourceAsGcpPet[R](resource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit
    sr: SamResource[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] =
    resource match {
      case r: RuntimeSamResourceId =>
        IO(runtimes.remove((r, userEmailToAuthorization(creatorEmail))))
      case r: PersistentDiskSamResourceId =>
        IO(persistentDisks.remove((r, userEmailToAuthorization(creatorEmail))))
      case r: ProjectSamResourceId =>
        IO(billingProjects.remove((r, userEmailToAuthorization(creatorEmail))))
      case r: AppSamResourceId =>
        IO(apps.remove((r, userEmailToAuthorization(creatorEmail)))) >>
          IO(apps.remove((r, userEmailToAuthorization(projectOwnerEmail)))).void
    }

  override def getPetServiceAccount(authorization: Authorization, googleProject: GoogleProject)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Option[WorkbenchEmail]] =
    IO.pure(Some(petSA))

  override def getPetManagedIdentity(authorization: Authorization, cloudContext: AzureCloudContext)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Option[WorkbenchEmail]] =
    IO.pure(Some(petMI))

  override def getUserProxy(
    userEmail: WorkbenchEmail
  )(implicit ev: Ask[IO, TraceId]): IO[Option[WorkbenchEmail]] =
    IO.pure(Some(WorkbenchEmail("PROXY_1234567890@dev.test.firecloud.org")))

  override def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Option[String]] = IO.pure(Some("token"))

  override def getStatus(implicit ev: Ask[IO, TraceId]): IO[StatusCheckResponse] =
    IO.pure(StatusCheckResponse(true, Map.empty))

  override def getListOfResourcePermissions[R, A](resource: R, authHeader: Authorization)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[IO, TraceId]
  ): IO[List[A]] =
    sr.resourceType(resource) match {
      case SamResourceType.Project =>
        val res = billingProjects
          .get((resource.asInstanceOf[ProjectSamResourceId], authHeader))
          .map(_.toList)
          .getOrElse(List.empty)
          .asInstanceOf[List[A]]
        IO.pure(res)
      case SamResourceType.Runtime =>
        val res = runtimes
          .get((resource.asInstanceOf[RuntimeSamResourceId], authHeader))
          .map(_.toList)
          .getOrElse(List.empty)
          .asInstanceOf[List[A]]
        IO.pure(res)
      case SamResourceType.PersistentDisk =>
        val res = persistentDisks
          .get((resource.asInstanceOf[PersistentDiskSamResourceId], authHeader))
          .map(_.toList)
          .getOrElse(List.empty)
          .asInstanceOf[List[A]]
        IO.pure(res)
      case SamResourceType.App =>
        val res = apps
          .get((resource.asInstanceOf[AppSamResourceId], authHeader))
          .map(_.toList)
          .getOrElse(List.empty)
          .asInstanceOf[List[A]]
        IO.pure(res)
      case SamResourceType.SharedApp =>
        val res = apps
          .get((resource.asInstanceOf[AppSamResourceId], authHeader))
          .map(_.toList)
          .getOrElse(List.empty)
          .asInstanceOf[List[A]]
        IO.pure(res)
      case SamResourceType.Workspace =>
        val res = workspaces
          .get((resource.asInstanceOf[WorkspaceResourceSamResourceId], authHeader))
          .map(_.toList)
          .getOrElse(List.empty)
          .asInstanceOf[List[A]]
        IO.pure(res)
      case SamResourceType.WsmResource =>
        val res = wsmResources
          .get((resource.asInstanceOf[WsmResourceSamResourceId], authHeader))
          .map(_.toList)
          .getOrElse(List.empty)
          .asInstanceOf[List[A]]
        IO.pure(res)
    }

  def getUserSubjectId(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Option[UserSubjectId]] = IO.pure(None)

  def addUserToProject(creatorEmail: WorkbenchEmail, googleProject: GoogleProject): IO[Unit] = {
    val authHeader = userEmailToAuthorization(creatorEmail)
    val project = ProjectSamResourceId(googleProject)
    IO {
      projectUsers = projectUsers |+| Map(authHeader -> Set((project, SamPolicyName.User)))
    }.void
  }

  override def getLeoAuthToken: IO[Authorization] =
    IO.pure(Authorization(Credentials.Token(AuthScheme.Bearer, "")))

  override def getSamUserInfo(token: String)(implicit ev: Ask[IO, TraceId]): IO[Option[SamUserInfo]] =
    if (token == OAuth2BearerToken(s"TokenFor${MockSamDAO.disabledUserEmail}").token)
      IO.pure(
        Some(SamUserInfo(UserSubjectId("test-disabled"), WorkbenchEmail("test-disabled@gmail.com"), enabled = false))
      )
    else
      IO.pure(Some(SamUserInfo(UserSubjectId("test"), WorkbenchEmail("test@gmail.com"), enabled = true)))

  override def isGroupMembersOrAdmin(groupName: GroupName, workbenchEmail: WorkbenchEmail)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Boolean] = IO.pure(true)

  override def isAdminUser(userInfo: UserInfo)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Boolean] = IO.pure(false)

  override def createResourceWithUserInfo[R](resource: R, userInfo: UserInfo)(implicit
    sr: SamResource[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = ???

  override def deleteResourceWithUserInfo[R](resource: R, userInfo: UserInfo)(implicit
    sr: SamResource[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = ???

  override def getCachedArbitraryPetAccessToken(userEmail: WorkbenchEmail)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Option[String]] = IO.pure(Some("token"))

  /** Deletes a Sam resource R using the provided bearer token. */
  override def deleteResourceInternal[R](resource: R, authHeader: Authorization)(implicit
    sr: SamResource[R],
    ev: Ask[IO, TraceId]
  ): IO[Unit] = ???

  /** Returns all roles for the user for a given resource.  */
  override def getResourceRoles(authHeader: Authorization, resourceId: SamResourceId)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Set[SamRole]] = ???

  /** Gets an action managed identity from Sam as the calling user for the given resource type,
   * resource ID, and action. Returns the managed identity object ID. */
  override def getAzureActionManagedIdentity(authHeader: Authorization,
                                             resource: PrivateAzureStorageAccountSamResourceId,
                                             action: PrivateAzureStorageAccountAction
  )(implicit ev: Ask[IO, TraceId]): IO[Option[String]] = IO(None)

  /** Gets the parent resource if the given resource ID, if one exists. */
  override def getResourceParent(authHeader: Authorization, resource: SamResourceId)(implicit
    ev: Ask[IO, TraceId]
  ): IO[Option[GetResourceParentResponse]] =
    IO(Some(GetResourceParentResponse(SamResourceType.Workspace, workspaceId.toString)))
}

object MockSamDAO {
  val petSA = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val petMI = WorkbenchEmail("/subscriptions/foo/resourceGroups/bar/userAssignedManagedIdentities/pet-1234")
  val disabledUserEmail = WorkbenchEmail("disabled-user@test.org")
  val projectOwnerEmail = WorkbenchEmail("project-owner@test.org")
  val workspaceOwnerEmail = WorkbenchEmail("workspace-owner@test.org")
  val appManagerActions = Set(AppAction.GetAppStatus, AppAction.DeleteApp)
  def userEmailToAuthorization(workbenchEmail: WorkbenchEmail): Authorization =
    Authorization(Credentials.Token(AuthScheme.Bearer, s"TokenFor${workbenchEmail}"))
}
