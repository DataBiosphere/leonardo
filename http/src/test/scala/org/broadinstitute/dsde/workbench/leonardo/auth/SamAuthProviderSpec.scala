package org.broadinstitute.dsde.workbench.leonardo
package auth

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.github.benmanes.caffeine.cache.Caffeine
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.http.ctxConversion
import org.broadinstitute.dsde.workbench.leonardo.model.SamResource.AppSamResource
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchUserId}
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import scalacache.Cache
import scalacache.caffeine.CaffeineCache

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class SamAuthProviderSpec extends AnyFlatSpec with LeonardoTestSuite with BeforeAndAfter {
  val samAuthProviderConfigWithoutCache: SamAuthProviderConfig =
    SamAuthProviderConfig(false, customAppCreationAllowedGroup = GroupName("custom_app_users"))
  val samAuthProviderConfigWithCache: SamAuthProviderConfig = SamAuthProviderConfig(
    true,
    10,
    1.minutes,
    customAppCreationAllowedGroup = GroupName("custom_app_users")
  )

  val disabledUserInfo =
    UserInfo(OAuth2BearerToken(s"TokenFor${MockSamDAO.disabledUserEmail}"),
             WorkbenchUserId("disabled-user"),
             MockSamDAO.disabledUserEmail,
             0
    )
  val userInfo =
    UserInfo(OAuth2BearerToken(s"TokenFor${userEmail}"), WorkbenchUserId("user1"), userEmail, 0)
  val userInfo2 =
    UserInfo(OAuth2BearerToken(s"TokenFor${userEmail2}"), WorkbenchUserId("user2"), userEmail2, 0)
  val projectOwnerUserInfo =
    UserInfo(OAuth2BearerToken(s"TokenFor${MockSamDAO.projectOwnerEmail}"),
             WorkbenchUserId("project-owner"),
             MockSamDAO.projectOwnerEmail,
             0
    )
  val workspaceOwnerUserInfo =
    UserInfo(OAuth2BearerToken(s"TokenFor${MockSamDAO.workspaceOwnerEmail}"),
             WorkbenchUserId("workspace-owner"),
             MockSamDAO.workspaceOwnerEmail,
             0
    )
  val projectOwnerAuthHeader = MockSamDAO.userEmailToAuthorization(MockSamDAO.projectOwnerEmail)
  val workspaceOwnerAuthHeader = MockSamDAO.userEmailToAuthorization(MockSamDAO.workspaceOwnerEmail)
  val authHeader = MockSamDAO.userEmailToAuthorization(userInfo.userEmail)

  val underlyingCaffeineCache =
    Caffeine.newBuilder().maximumSize(10000L).build[AuthCacheKey, scalacache.Entry[Boolean]]()
  implicit val authCache: Cache[IO, AuthCacheKey, Boolean] =
    CaffeineCache[IO, AuthCacheKey, Boolean](underlyingCaffeineCache)
  var mockSam: MockSamDAO = _
  var samAuthProvider: SamAuthProvider[IO] = _
  var samAuthProviderWithCache: SamAuthProvider[IO] = _

  before {
    setUpMockSam()
    authCache.removeAll
    samAuthProvider = new SamAuthProvider(mockSam, samAuthProviderConfigWithoutCache, serviceAccountProvider, authCache)
    samAuthProviderWithCache =
      new SamAuthProvider(mockSam, samAuthProviderConfigWithCache, serviceAccountProvider, authCache)
  }

  "SamAuthProvider" should "check resource permissions" in {
    // positive tests
    ProjectAction.allActions.foreach { a =>
      samAuthProvider
        .hasPermission(ProjectSamResourceId(project), a, projectOwnerUserInfo)
        .unsafeRunSync() shouldBe true
    }
    RuntimeAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(runtimeSamResource, a, userInfo).unsafeRunSync() shouldBe true
    }
    PersistentDiskAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(diskSamResource, a, userInfo).unsafeRunSync() shouldBe true
    }
    AppAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(appSamId, a, userInfo).unsafeRunSync() shouldBe true
    }
    MockSamDAO.appManagerActions.foreach { a =>
      samAuthProvider.hasPermission(appSamId, a, projectOwnerUserInfo).unsafeRunSync() shouldBe true
    }
    AppAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(sharedAppSamId, a, userInfo).unsafeRunSync() shouldBe true
    }
    MockSamDAO.appManagerActions.foreach { a =>
      samAuthProvider.hasPermission(sharedAppSamId, a, workspaceOwnerUserInfo).unsafeRunSync() shouldBe true
    }

    // negative tests
    ProjectAction.allActions.foreach { a =>
      samAuthProvider
        .hasPermission(ProjectSamResourceId(project), a, unauthorizedUserInfo)
        .unsafeRunSync() shouldBe false
    }
    RuntimeAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(runtimeSamResource, a, unauthorizedUserInfo).unsafeRunSync() shouldBe false
      samAuthProvider.hasPermission(runtimeSamResource, a, projectOwnerUserInfo).unsafeRunSync() shouldBe false
    }
    PersistentDiskAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(diskSamResource, a, unauthorizedUserInfo).unsafeRunSync() shouldBe false
      samAuthProvider.hasPermission(diskSamResource, a, projectOwnerUserInfo).unsafeRunSync() shouldBe false
    }
    AppAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(appSamId, a, unauthorizedUserInfo).unsafeRunSync() shouldBe false
    }
    AppAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(sharedAppSamId, a, unauthorizedUserInfo).unsafeRunSync() shouldBe false
    }
  }

  it should "check resource permissions with project fallback" in {
    // positive tests
    List(
      (RuntimeAction.GetRuntimeStatus, ProjectAction.GetRuntimeStatus),
      (RuntimeAction.DeleteRuntime, ProjectAction.DeleteRuntime),
      (RuntimeAction.StopStartRuntime, ProjectAction.StopStartRuntime)
    ).foreach { case (runtimeAction, projectAction) =>
      // project fallback should work as the user
      samAuthProvider
        .hasPermissionWithProjectFallback(runtimeSamResource, runtimeAction, projectAction, userInfo, project)
        .unsafeRunSync() shouldBe true

      // project fallback should work as the project owner
      samAuthProvider
        .hasPermissionWithProjectFallback(runtimeSamResource,
                                          runtimeAction,
                                          projectAction,
                                          projectOwnerUserInfo,
                                          project
        )
        .unsafeRunSync() shouldBe true

    }
    List(
      (PersistentDiskAction.ReadPersistentDisk, ProjectAction.ReadPersistentDisk),
      (PersistentDiskAction.DeletePersistentDisk, ProjectAction.DeletePersistentDisk)
    ).foreach { case (diskAction, projectAction) =>
      // project fallback should work as the user
      samAuthProvider
        .hasPermissionWithProjectFallback(diskSamResource, diskAction, projectAction, userInfo, project)
        .unsafeRunSync() shouldBe true

      // project fallback should work as the project owner
      samAuthProvider
        .hasPermissionWithProjectFallback(diskSamResource, diskAction, projectAction, projectOwnerUserInfo, project)
        .unsafeRunSync() shouldBe true
    }

    // negative tests
    List(
      (RuntimeAction.GetRuntimeStatus, ProjectAction.GetRuntimeStatus),
      (RuntimeAction.DeleteRuntime, ProjectAction.DeleteRuntime),
      (RuntimeAction.StopStartRuntime, ProjectAction.StopStartRuntime)
    ).foreach { case (runtimeAction, projectAction) =>
      samAuthProvider
        .hasPermissionWithProjectFallback(runtimeSamResource,
                                          runtimeAction,
                                          projectAction,
                                          unauthorizedUserInfo,
                                          project
        )
        .unsafeRunSync() shouldBe false
    }
    List(
      (PersistentDiskAction.ReadPersistentDisk, ProjectAction.ReadPersistentDisk),
      (PersistentDiskAction.DeletePersistentDisk, ProjectAction.DeletePersistentDisk)
    ).foreach { case (diskAction, projectAction) =>
      samAuthProvider
        .hasPermissionWithProjectFallback(diskSamResource, diskAction, projectAction, unauthorizedUserInfo, project)
        .unsafeRunSync() shouldBe false
    }
  }

  it should "get actions" in {
    // positive tests
    samAuthProvider
      .getActions(ProjectSamResourceId(project), projectOwnerUserInfo)
      .unsafeRunSync()
      .toSet shouldBe ProjectAction.allActions
    samAuthProvider
      .getActions(runtimeSamResource, userInfo)
      .unsafeRunSync()
      .toSet shouldBe RuntimeAction.allActions
    samAuthProvider
      .getActions(diskSamResource, userInfo)
      .unsafeRunSync()
      .toSet shouldBe PersistentDiskAction.allActions
    samAuthProvider.getActions(appSamId, userInfo).unsafeRunSync().toSet shouldBe AppAction.allActions
    samAuthProvider
      .getActions(appSamId, projectOwnerUserInfo)
      .unsafeRunSync()
      .toSet shouldBe MockSamDAO.appManagerActions
    samAuthProvider.getActions(sharedAppSamId, userInfo).unsafeRunSync().toSet shouldBe AppAction.allActions
    samAuthProvider
      .getActions(sharedAppSamId, workspaceOwnerUserInfo)
      .unsafeRunSync()
      .toSet shouldBe MockSamDAO.appManagerActions

    // negative tests
    samAuthProvider
      .getActions(ProjectSamResourceId(project), unauthorizedUserInfo)
      .unsafeRunSync() shouldBe List.empty
    samAuthProvider.getActions(runtimeSamResource, unauthorizedUserInfo).unsafeRunSync() shouldBe List.empty
    samAuthProvider.getActions(runtimeSamResource, projectOwnerUserInfo).unsafeRunSync() shouldBe List.empty
    samAuthProvider.getActions(diskSamResource, unauthorizedUserInfo).unsafeRunSync() shouldBe List.empty
    samAuthProvider.getActions(diskSamResource, projectOwnerUserInfo).unsafeRunSync() shouldBe List.empty
    samAuthProvider.getActions(appSamId, unauthorizedUserInfo).unsafeRunSync() shouldBe List.empty
    samAuthProvider.getActions(sharedAppSamId, unauthorizedUserInfo).unsafeRunSync() shouldBe List.empty
  }

  it should "get actions with project fallback" in {
    // positive tests
    samAuthProvider
      .getActionsWithProjectFallback(runtimeSamResource, project, userInfo)
      .unsafeRunSync()
      .leftMap(_.toSet) shouldBe (RuntimeAction.allActions, List.empty)
    samAuthProvider
      .getActionsWithProjectFallback(runtimeSamResource, project, projectOwnerUserInfo)
      .unsafeRunSync()
      .map(_.toSet) shouldBe (List.empty, ProjectAction.allActions)
    samAuthProvider
      .getActionsWithProjectFallback(diskSamResource, project, userInfo)
      .unsafeRunSync()
      .leftMap(_.toSet) shouldBe (PersistentDiskAction.allActions, List.empty)
    samAuthProvider
      .getActionsWithProjectFallback(diskSamResource, project, projectOwnerUserInfo)
      .unsafeRunSync()
      .map(_.toSet) shouldBe (List.empty, ProjectAction.allActions)

    // negative tests
    samAuthProvider
      .getActionsWithProjectFallback(runtimeSamResource, project, unauthorizedUserInfo)
      .unsafeRunSync() shouldBe (List.empty, List.empty)
    samAuthProvider
      .getActionsWithProjectFallback(diskSamResource, project, unauthorizedUserInfo)
      .unsafeRunSync() shouldBe (List.empty, List.empty)
  }

  it should "cache hasPermission results" in {
    underlyingCaffeineCache.asMap().size shouldBe 0

    // these actions should be cached
    List(ProjectAction.GetRuntimeStatus, ProjectAction.ReadPersistentDisk).foreach { a =>
      samAuthProviderWithCache
        .hasPermission(ProjectSamResourceId(project), a, projectOwnerUserInfo)
        .unsafeRunSync() shouldBe true
    }
    List(RuntimeAction.GetRuntimeStatus, RuntimeAction.ConnectToRuntime).foreach { a =>
      samAuthProviderWithCache
        .hasPermission(runtimeSamResource, a, userInfo)
        .unsafeRunSync() shouldBe true
//    This is duplicated to make sure cache works as expected
      samAuthProviderWithCache
        .hasPermission(runtimeSamResource, a, userInfo)
        .unsafeRunSync() shouldBe true
    }
    List(PersistentDiskAction.ReadPersistentDisk).foreach { a =>
      samAuthProviderWithCache
        .hasPermission[PersistentDiskSamResourceId, PersistentDiskAction](diskSamResource, a, userInfo)
        .unsafeRunSync() shouldBe true
    }
    List(AppAction.ConnectToApp, AppAction.GetAppStatus).foreach { a =>
      samAuthProviderWithCache.hasPermission(appSamId, a, userInfo).unsafeRunSync() shouldBe true
    }
    List(AppAction.ConnectToApp, AppAction.GetAppStatus).foreach { a =>
      samAuthProviderWithCache.hasPermission(sharedAppSamId, a, userInfo).unsafeRunSync() shouldBe true
    }

    // these actions should not be cached
    List(
      ProjectAction.CreateApp,
      ProjectAction.CreatePersistentDisk,
      ProjectAction.CreateRuntime,
      ProjectAction.DeletePersistentDisk,
      ProjectAction.DeleteRuntime,
      ProjectAction.StopStartRuntime
    ).foreach { a =>
      samAuthProviderWithCache
        .hasPermission(ProjectSamResourceId(project), a, projectOwnerUserInfo)
        .unsafeRunSync() shouldBe true
    }
    List(RuntimeAction.ModifyRuntime, RuntimeAction.DeleteRuntime, RuntimeAction.StopStartRuntime).foreach { a =>
      samAuthProviderWithCache
        .hasPermission(runtimeSamResource, a, userInfo)
        .unsafeRunSync() shouldBe true
    }
    List(PersistentDiskAction.DeletePersistentDisk,
         PersistentDiskAction.ModifyPersistentDisk,
         PersistentDiskAction.AttachPersistentDisk
    ).foreach { a =>
      samAuthProviderWithCache
        .hasPermission(diskSamResource, a, userInfo)
        .unsafeRunSync() shouldBe true
    }
    List(AppAction.DeleteApp, AppAction.UpdateApp).foreach { a =>
      samAuthProviderWithCache
        .hasPermission(appSamId, a, userInfo)
        .unsafeRunSync() shouldBe true
    }
    List(AppAction.DeleteApp, AppAction.UpdateApp).foreach { a =>
      samAuthProviderWithCache
        .hasPermission(sharedAppSamId, a, userInfo)
        .unsafeRunSync() shouldBe true
    }

    // check cache
    val cacheMap = underlyingCaffeineCache.asMap()
    cacheMap.size shouldBe 9
    val expectedCache = Set(
      AuthCacheKey(SamResourceType.Project,
                   project.value,
                   projectOwnerAuthHeader,
                   ProjectAction.GetRuntimeStatus.asString
      ),
      AuthCacheKey(SamResourceType.Project,
                   project.value,
                   projectOwnerAuthHeader,
                   ProjectAction.ReadPersistentDisk.asString
      ),
      AuthCacheKey(SamResourceType.Runtime,
                   runtimeSamResource.resourceId,
                   authHeader,
                   RuntimeAction.ConnectToRuntime.asString
      ),
      AuthCacheKey(SamResourceType.Runtime,
                   runtimeSamResource.resourceId,
                   authHeader,
                   RuntimeAction.GetRuntimeStatus.asString
      ),
      AuthCacheKey(SamResourceType.PersistentDisk,
                   diskSamResource.resourceId,
                   authHeader,
                   PersistentDiskAction.ReadPersistentDisk.asString
      ),
      AuthCacheKey(SamResourceType.App, appSamId.resourceId, authHeader, AppAction.GetAppStatus.asString),
      AuthCacheKey(SamResourceType.App, appSamId.resourceId, authHeader, AppAction.ConnectToApp.asString),
      AuthCacheKey(SamResourceType.SharedApp, sharedAppSamId.resourceId, authHeader, AppAction.GetAppStatus.asString),
      AuthCacheKey(SamResourceType.SharedApp, sharedAppSamId.resourceId, authHeader, AppAction.ConnectToApp.asString)
    )

    cacheMap.asScala.keySet.toSet should contain theSameElementsAs expectedCache
    cacheMap.asScala.values.map(_.value).toSet shouldBe Set(true)
  }

  it should "create a resource" in {
    val newRuntime = RuntimeSamResourceId("new_runtime")
    samAuthProvider.notifyResourceCreated(newRuntime, userEmail, project).unsafeRunSync()
    mockSam.runtimes.get((newRuntime, authHeader)) shouldBe Some(
      RuntimeAction.allActions
    )

    val newDisk = PersistentDiskSamResourceId("new_disk")
    samAuthProvider.notifyResourceCreated(newDisk, userEmail, project).unsafeRunSync()
    mockSam.persistentDisks.get((newDisk, authHeader)) shouldBe Some(
      PersistentDiskAction.allActions
    )

    val newApp = AppSamResourceId("new_app", None)
    samAuthProvider.notifyResourceCreated(newApp, userEmail, project).unsafeRunSync()
    mockSam.apps.get((newApp, authHeader)) shouldBe Some(
      AppAction.allActions
    )
    mockSam.apps.get((newApp, projectOwnerAuthHeader)) shouldBe Some(
      MockSamDAO.appManagerActions
    )

    val newSharedApp = AppSamResourceId("new_shared_app", Some(AppAccessScope.WorkspaceShared))
    samAuthProvider
      .notifyResourceCreatedV2(newSharedApp, userEmail, cloudContextGcp, workspaceId, userInfo)
      .unsafeRunSync()
    mockSam.apps.get((newSharedApp, authHeader)) shouldBe Some(
      AppAction.allActions
    )
    mockSam.apps.get((newSharedApp, workspaceOwnerAuthHeader)) shouldBe Some(
      MockSamDAO.appManagerActions
    )
  }

  it should "delete a resource" in {
    samAuthProvider.notifyResourceDeleted(runtimeSamResource, userEmail, project).unsafeRunSync()
    mockSam.runtimes.get((runtimeSamResource, authHeader)) shouldBe None

    samAuthProvider.notifyResourceDeleted(diskSamResource, userEmail, project).unsafeRunSync()
    mockSam.persistentDisks.get((diskSamResource, authHeader)) shouldBe None

    samAuthProvider.notifyResourceDeleted(appSamId, userEmail, project).unsafeRunSync()
    mockSam.apps.get((appSamId, authHeader)) shouldBe None
    mockSam.apps.get((appSamId, projectOwnerAuthHeader)) shouldBe None

    samAuthProvider.notifyResourceDeleted(sharedAppSamId, userEmail, project).unsafeRunSync()
    mockSam.apps.get((sharedAppSamId, authHeader)) shouldBe None
    mockSam.apps.get((sharedAppSamId, projectOwnerAuthHeader)) shouldBe None
  }

  it should "filter user visible resources" in {
    // positive tests
    val newRuntime = RuntimeSamResourceId("new_runtime")
    mockSam.createResourceAsGcpPet(newRuntime, userEmail2, project).unsafeRunSync()
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, runtimeSamResource), (project, newRuntime)), userInfo)
      .unsafeRunSync() shouldBe List(
      (project, runtimeSamResource)
    )

    val newDisk = PersistentDiskSamResourceId("new_disk")
    mockSam.createResourceAsGcpPet(newDisk, userEmail2, project).unsafeRunSync()
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, diskSamResource), (project, newDisk)), userInfo)
      .unsafeRunSync() shouldBe List(
      (project, diskSamResource)
    )

    val newApp = AppSamResourceId("new_app", None)
    mockSam.createResourceWithGoogleProjectParent(newApp, userEmail2, project).unsafeRunSync()
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, appSamId), (project, newApp)), userInfo)(implicitly,
                                                                                                       appSamIdDecoder,
                                                                                                       implicitly
      )
      .unsafeRunSync() shouldBe List(
      (project, appSamId)
    )
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, appSamId), (project, newApp)), projectOwnerUserInfo)(
        implicitly,
        appSamIdDecoder,
        implicitly
      )
      .unsafeRunSync() shouldBe List(
      (project, appSamId),
      (project, newApp)
    )

    val newSharedApp = AppSamResourceId("new_shared_app", Some(AppAccessScope.WorkspaceShared))
    mockSam.createResourceWithWorkspaceParent(newSharedApp, userEmail3, userInfo, workspaceId).unsafeRunSync()
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, appSamId), (project, newSharedApp)), userInfo)(
        implicitly,
        sharedAppSamIdDecoder,
        implicitly
      )
      .unsafeRunSync() shouldBe List(
      (project, newSharedApp)
    )
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, sharedAppSamId), (project, newSharedApp)),
                                    workspaceOwnerUserInfo
      )(implicitly, sharedAppSamIdDecoder, implicitly)
      .unsafeRunSync() shouldBe List(
      (project, sharedAppSamId),
      (project, newSharedApp)
    )

    // negative tests
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, runtimeSamResource), (project, newRuntime)),
                                    unauthorizedUserInfo
      )
      .unsafeRunSync() shouldBe List.empty
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, runtimeSamResource), (project, newRuntime)),
                                    projectOwnerUserInfo
      )
      .unsafeRunSync() shouldBe List.empty
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, diskSamResource), (project, newDisk)),
                                    unauthorizedUserInfo
      )
      .unsafeRunSync() shouldBe List.empty
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, diskSamResource), (project, newDisk)),
                                    projectOwnerUserInfo
      )
      .unsafeRunSync() shouldBe List.empty
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, appSamId), (project, newApp)), unauthorizedUserInfo)(
        implicitly,
        appSamIdDecoder,
        implicitly
      )
      .unsafeRunSync() shouldBe List.empty
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, sharedAppSamId), (project, newSharedApp)),
                                    unauthorizedUserInfo
      )(implicitly, sharedAppSamIdDecoder, implicitly)
      .unsafeRunSync() shouldBe List.empty
  }

  it should "filter user visible resources with project fallback" in {
    // positive tests
    mockSam.addUserToProject(userInfo.userEmail, project).unsafeRunSync()
    val newRuntime = RuntimeSamResourceId("new_runtime")
    mockSam.createResourceAsGcpPet(newRuntime, userEmail2, project).unsafeRunSync()
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, runtimeSamResource), (project, newRuntime)), userInfo)
      .unsafeRunSync() shouldBe List((project, runtimeSamResource))
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, runtimeSamResource), (project, newRuntime)),
                                    projectOwnerUserInfo
      )
      .unsafeRunSync() shouldBe List((project, runtimeSamResource), (project, newRuntime))

    val newDisk = PersistentDiskSamResourceId("new_disk")
    mockSam.createResourceAsGcpPet(newDisk, userEmail2, project).unsafeRunSync()
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, diskSamResource), (project, newDisk)), userInfo)
      .unsafeRunSync() shouldBe List((project, diskSamResource))
    samAuthProvider
      .filterResourceProjectVisible(NonEmptyList.of((project, diskSamResource), (project, newDisk)),
                                    projectOwnerUserInfo
      )
      .unsafeRunSync() shouldBe List((project, diskSamResource), (project, newDisk))

    // negative tests
    samAuthProvider
      .filterUserVisible(NonEmptyList.of(runtimeSamResource, newRuntime), unauthorizedUserInfo)
      .unsafeRunSync() shouldBe List.empty
    samAuthProvider
      .filterUserVisible(NonEmptyList.of(diskSamResource, newDisk), unauthorizedUserInfo)
      .unsafeRunSync() shouldBe List.empty
  }

  it should "filter user visible resources by project access" in {
    val newRuntime = RuntimeSamResourceId("new_runtime")
    mockSam.createResourceAsGcpPet(newRuntime, userEmail, project).unsafeRunSync()

    println(s"users: ${mockSam.projectUsers}")
    println(s"owners: ${mockSam.projectOwners}")
    println(s"userinfo: $userInfo")
    // negative tests
    samAuthProvider
      .filterUserVisible(NonEmptyList.of(runtimeSamResource, newRuntime), userInfo)
      .unsafeRunSync() shouldBe List.empty

    // positive tests
//    mockSam.addUserToProject(userInfo.userEmail, project).unsafeRunSync()
//    samAuthProvider
//      .filterResourceProjectVisible(NonEmptyList.of((project, runtimeSamResource), (project, newRuntime)), userInfo)
//      .unsafeRunSync() shouldBe List((project, runtimeSamResource))
//    samAuthProvider
//      .filterResourceProjectVisible(NonEmptyList.of((project, runtimeSamResource), (project, newRuntime)),
//        projectOwnerUserInfo
//      )
//      .unsafeRunSync() shouldBe List((project, runtimeSamResource), (project, newRuntime))
  }

  it should "tell if user is enabled" in {
    // positive test
    noException shouldBe thrownBy(samAuthProvider.checkUserEnabled(userInfo).unsafeRunSync())

    // negative test
    an[AuthProviderException] shouldBe thrownBy(samAuthProvider.checkUserEnabled(disabledUserInfo).unsafeRunSync())
  }

  private def setUpMockSam(): SamDAO[IO] = {
    mockSam = new MockSamDAO
    // set up mock sam with a project, runtime, disk, and app
    mockSam.createResourceAsGcpPet(ProjectSamResourceId(project), MockSamDAO.projectOwnerEmail, project).unsafeRunSync()
    mockSam.billingProjects.get(
      (ProjectSamResourceId(project), projectOwnerAuthHeader)
    ) shouldBe Some(
      ProjectAction.allActions
    )
    mockSam.createResourceAsGcpPet(runtimeSamResource, userEmail, project).unsafeRunSync()
    mockSam.runtimes.get((runtimeSamResource, authHeader)) shouldBe Some(
      RuntimeAction.allActions
    )
    mockSam.createResourceAsGcpPet(diskSamResource, userEmail, project).unsafeRunSync()
    mockSam.persistentDisks.get((diskSamResource, authHeader)) shouldBe Some(
      PersistentDiskAction.allActions
    )
    mockSam.createResourceWithGoogleProjectParent(KubernetesTestData.appSamId, userEmail, project).unsafeRunSync()
    mockSam.apps.get((appSamId, authHeader)) shouldBe Some(
      AppAction.allActions
    )
    mockSam.apps.get((appSamId, projectOwnerAuthHeader)) shouldBe Some(
      MockSamDAO.appManagerActions
    )
    mockSam
      .createResourceWithWorkspaceParent(KubernetesTestData.sharedAppSamId, userEmail, userInfo, workspaceId)
      .unsafeRunSync()
    mockSam.apps.get((sharedAppSamId, authHeader)) shouldBe Some(
      AppAction.allActions
    )
    mockSam.apps.get((sharedAppSamId, workspaceOwnerAuthHeader)) shouldBe Some(
      MockSamDAO.appManagerActions
    )
    mockSam
  }
}
