package org.broadinstitute.dsde.workbench.leonardo
package auth.sam

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.ProjectSamResource
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchUserId}
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration._

class SamAuthProviderSpec extends AnyFlatSpec with LeonardoTestSuite with BeforeAndAfter {
//  val fakeUserInfo = UserInfo(OAuth2BearerToken(s"TokenFor${userEmail}"), WorkbenchUserId("user1"), userEmail, 0)
//  val fakeUserInfo2 = UserInfo(OAuth2BearerToken(s"TokenFor${userEmail}"), WorkbenchUserId("user1"), userEmail, 0)

  val mockSam = new MockSamDAO
  val samAuthProviderConfigWithoutCache: SamAuthProviderConfig = SamAuthProviderConfig(false)
  val samAuthProviderConfigWithCache: SamAuthProviderConfig = SamAuthProviderConfig(
    true,
    10,
    1.minutes
  )
  val samAuthProvider =
    new SamAuthProvider(mockSam, samAuthProviderConfigWithoutCache, serviceAccountProvider, blocker)
  val samAuthProviderWithCache =
    new SamAuthProvider(mockSam, samAuthProviderConfigWithCache, serviceAccountProvider, blocker)

  val projectOwnerUserInfo =
    UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), mockSamDAO.projectOwnerEmail, 0)
  val projectOwnerAuthHeader = MockSamDAO.userEmailToAuthorization(mockSam.projectOwnerEmail)
  val authHeader = MockSamDAO.userEmailToAuthorization(userInfo.userEmail)

  before {
    setUpMock()
  }

  "SamAuthProvider" should "check resource permissions" in {
    // positive tests
    ProjectAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(ProjectSamResource(project), a, projectOwnerUserInfo).unsafeRunSync() shouldBe true
    }
    RuntimeAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(runtimeSamResource, a, userInfo).unsafeRunSync() shouldBe true
    }
    PersistentDiskAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(diskSamResource, a, userInfo).unsafeRunSync() shouldBe true
    }
    AppAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(appSamId, a, userInfo).unsafeRunSync() shouldBe true
      samAuthProvider.hasPermission(appSamId, a, projectOwnerUserInfo).unsafeRunSync() shouldBe true
    }

    // negative tests
    ProjectAction.allActions.foreach { a =>
      samAuthProvider.hasPermission(ProjectSamResource(project), a, unauthorizedUserInfo).unsafeRunSync() shouldBe false
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
  }

  it should "check resource permissions with project fallback" in {
    // positive tests
    List(
      (RuntimeAction.GetRuntimeStatus, ProjectAction.GetRuntimeStatus),
      (RuntimeAction.DeleteRuntime, ProjectAction.DeleteRuntime),
      (RuntimeAction.StopStartRuntime, ProjectAction.StopStartRuntime)
    ).foreach {
      case (runtimeAction, projectAction) =>
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
                                            project)
          .unsafeRunSync() shouldBe true

    }
    List(
      (PersistentDiskAction.ReadPersistentDisk, ProjectAction.ReadPersistentDisk),
      (PersistentDiskAction.DeletePersistentDisk, ProjectAction.DeletePersistentDisk)
    ).foreach {
      case (diskAction, projectAction) =>
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
    ).foreach {
      case (runtimeAction, projectAction) =>
        samAuthProvider
          .hasPermissionWithProjectFallback(runtimeSamResource,
                                            runtimeAction,
                                            projectAction,
                                            unauthorizedUserInfo,
                                            project)
          .unsafeRunSync() shouldBe false
    }
    List(
      (PersistentDiskAction.ReadPersistentDisk, ProjectAction.ReadPersistentDisk),
      (PersistentDiskAction.DeletePersistentDisk, ProjectAction.DeletePersistentDisk)
    ).foreach {
      case (diskAction, projectAction) =>
        samAuthProvider
          .hasPermissionWithProjectFallback(diskSamResource, diskAction, projectAction, unauthorizedUserInfo, project)
          .unsafeRunSync() shouldBe false
    }
  }

  it should "get actions" in {
    // positive tests
    samAuthProvider
      .getActions(ProjectSamResource(project), projectOwnerUserInfo)
      .unsafeRunSync()
      .toSet shouldBe ProjectAction.allActions
    samAuthProvider.getActions(runtimeSamResource, userInfo).unsafeRunSync().toSet shouldBe RuntimeAction.allActions
    samAuthProvider.getActions(diskSamResource, userInfo).unsafeRunSync().toSet shouldBe PersistentDiskAction.allActions
    samAuthProvider.getActions(appSamId, userInfo).unsafeRunSync().toSet shouldBe AppAction.allActions
    samAuthProvider
      .getActions(appSamId, projectOwnerUserInfo)
      .unsafeRunSync()
      .toSet shouldBe mockSamDAO.appManagerActions

    // negative tests
    samAuthProvider.getActions(ProjectSamResource(project), unauthorizedUserInfo).unsafeRunSync() shouldBe List.empty
    samAuthProvider.getActions(runtimeSamResource, unauthorizedUserInfo).unsafeRunSync() shouldBe List.empty
    samAuthProvider.getActions(runtimeSamResource, projectOwnerUserInfo).unsafeRunSync() shouldBe List.empty
    samAuthProvider.getActions(diskSamResource, unauthorizedUserInfo).unsafeRunSync() shouldBe List.empty
    samAuthProvider.getActions(diskSamResource, projectOwnerUserInfo).unsafeRunSync() shouldBe List.empty
    samAuthProvider.getActions(appSamId, unauthorizedUserInfo).unsafeRunSync() shouldBe List.empty
  }

  it should "get actions with project fallback" in {
    // positive tests
    samAuthProvider
      .getActionsWithProjectFallback(runtimeSamResource, project, userInfo)
      .unsafeRunSync()
      .toSet shouldBe RuntimeAction.allActions
    samAuthProvider
      .getActionsWithProjectFallback(runtimeSamResource, project, projectOwnerUserInfo)
      .unsafeRunSync()
      .toSet shouldBe ProjectAction.allActions
    samAuthProvider
      .getActionsWithProjectFallback(diskSamResource, project, userInfo)
      .unsafeRunSync()
      .toSet shouldBe PersistentDiskAction.allActions
    samAuthProvider
      .getActionsWithProjectFallback(diskSamResource, project, projectOwnerUserInfo)
      .unsafeRunSync()
      .toSet shouldBe ProjectAction.allActions

    // negative tests
    samAuthProvider
      .getActionsWithProjectFallback(runtimeSamResource, project, unauthorizedUserInfo)
      .unsafeRunSync() shouldBe List.empty
    samAuthProvider
      .getActionsWithProjectFallback(diskSamResource, project, unauthorizedUserInfo)
      .unsafeRunSync() shouldBe List.empty
  }

  it should "cache hasPermission results" in {}

  it should "consider userInfo objects with different tokenExpiresIn values as the same" in {}

  it should "create a resource" in {}

  it should "delete a resource" in {}

  it should "filter user visible resources" in {}

  it should "filter user visible resources with project fallback" in {}
//
//  "hasProjectPermission should return true if user has project permissions and false if they do not" in {
//    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("launch_notebook_cluster",
//                                                                       "create_persistent_disk")
//    samAuthProvider.hasProjectPermission(fakeUserInfo, CreateRuntime, project).unsafeRunSync() shouldBe true
//    samAuthProvider.hasProjectPermission(fakeUserInfo, CreatePersistentDisk, project).unsafeRunSync() shouldBe true
//
//    samAuthProvider.hasProjectPermission(unauthorizedUserInfo, CreateRuntime, project).unsafeRunSync() shouldBe false
//    samAuthProvider
//      .hasProjectPermission(unauthorizedUserInfo, CreatePersistentDisk, project)
//      .unsafeRunSync() shouldBe false
//    samAuthProvider
//      .hasProjectPermission(fakeUserInfo, CreateRuntime, GoogleProject("leo-fake-project"))
//      .unsafeRunSync() shouldBe false
//    samAuthProvider
//      .hasProjectPermission(fakeUserInfo, CreatePersistentDisk, GoogleProject("leo-fake-project"))
//      .unsafeRunSync() shouldBe false
//
//    mockSam.billingProjects.remove((project, fakeUserAuthorization))
//  }
//
//  "should add and delete a runtime resource with correct actions for the user when a runtime is created and then destroyed" in {
//    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("launch_notebook_cluster")
////     check the sam auth provider has no notebook-cluster resource
//    mockSam.runtimes shouldBe empty
//    val defaultPermittedActions = List("status", "connect", "sync", "delete")
//    defaultPermittedActions.foreach { action =>
//      mockSam
//        .hasResourcePermission(runtimeSamResource, action, fakeUserAuthorization)
//        .unsafeRunSync() shouldBe false
//    }
//
//    // creating a cluster would call notify
//    samAuthProvider.notifyResourceCreated(runtimeSamResource, fakeUserInfo.userEmail, project).unsafeRunSync()
//
//    // check the resource exists for the user and actions
//    defaultPermittedActions.foreach { action =>
//      mockSam
//        .hasResourcePermission(runtimeSamResource, action, fakeUserAuthorization)
//        .unsafeRunSync() shouldBe true
//    }
//    // deleting a cluster would call notify
//    samAuthProvider
//      .notifyResourceDeleted(runtimeSamResource, fakeUserInfo.userEmail, fakeUserInfo.userEmail, project)
//      .unsafeRunSync()
//
//    mockSam.runtimes shouldBe empty
//    defaultPermittedActions.foreach { action =>
//      mockSam
//        .hasResourcePermission(runtimeSamResource, action, fakeUserAuthorization)
//        .unsafeRunSync() shouldBe false
//    }
//
//    mockSam.billingProjects.remove((project, fakeUserAuthorization))
//  }
//
//  "hasNotebookClusterPermission should return true if user has notebook cluster permissions and false if they do not" in {
//    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set("sync")
//    samAuthProvider
//      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, SyncDataToRuntime, project)
//      .unsafeRunSync() shouldBe true
//
//    samAuthProvider
//      .hasRuntimePermission(runtimeSamResource, unauthorizedUserInfo, SyncDataToRuntime, project)
//      .unsafeRunSync() shouldBe false
//    samAuthProvider
//      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, DeleteRuntime, project)
//      .unsafeRunSync() shouldBe false
//    mockSam.runtimes.remove((runtimeSamResource, fakeUserAuthorization))
//  }
//
//  "hasNotebookClusterPermission should return true if user does not have notebook cluster permissions but does have project permissions" in {
//    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("sync_notebook_cluster")
//    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set()
//
//    samAuthProvider
//      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, SyncDataToRuntime, project)
//      .unsafeRunSync() shouldBe true
//
//    mockSam.billingProjects.remove((project, fakeUserAuthorization))
//    mockSam.runtimes.remove((runtimeSamResource, fakeUserAuthorization))
//  }
//
//  "getRuntimeActionsWithProjectFallback should return project permissions as well" in {
//    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("sync_notebook_cluster")
//    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set()
//
//    samAuthProvider
//      .getRuntimeActionsWithProjectFallback(project, runtimeSamResource, fakeUserInfo)
//      .unsafeRunSync() shouldBe List(SyncDataToRuntime)
//
//    mockSam.billingProjects.remove((project, fakeUserAuthorization))
//    mockSam.runtimes.remove((runtimeSamResource, fakeUserAuthorization))
//  }
//
//  "notifyClusterCreated should create a new cluster resource" in {
//    mockSam.runtimes shouldBe empty
//    samAuthProvider.notifyResourceCreated(runtimeSamResource, fakeUserInfo.userEmail, project).unsafeRunSync()
//    mockSam.runtimes.toList should contain(
//      (runtimeSamResource, fakeUserAuthorization) -> Set("connect", "read_policies", "status", "delete", "sync")
//    )
//    mockSam.runtimes.remove((runtimeSamResource, fakeUserAuthorization))
//  }
//
//  "notifyClusterDeleted should delete a cluster resource" in {
//    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set()
//
//    samAuthProvider
//      .notifyResourceDeleted(runtimeSamResource, userInfo.userEmail, userInfo.userEmail, project)
//      .unsafeRunSync()
//    mockSam.runtimes.toList should not contain ((runtimeSamResource, fakeUserAuthorization) -> Set(
//      "connect",
//      "read_policies",
//      "status",
//      "delete",
//      "sync"
//    ))
//  }
//
//  "should cache hasNotebookClusterPermission results" in {
//    // cache should be empty
//    samAuthProviderWithCache.notebookAuthCache.size shouldBe 0
//
//    // populate backing samClient
//    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set("sync")
//
//    // call provider method
//    samAuthProviderWithCache
//      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, SyncDataToRuntime, project)
//      .unsafeRunSync() shouldBe true
//
//    // cache should contain 1 entry
//    samAuthProviderWithCache.notebookAuthCache.size shouldBe 1
//    val key = NotebookAuthCacheKey(runtimeSamResource, fakeUserAuthorization, SyncDataToRuntime, project)
//    samAuthProviderWithCache.notebookAuthCache.asMap.containsKey(key) shouldBe true
//    samAuthProviderWithCache.notebookAuthCache.asMap.get(key) shouldBe true
//    // remove info from samClient
//    mockSam.runtimes.remove((runtimeSamResource, fakeUserAuthorization))
//    // provider should still return true because the info is cached
//    samAuthProviderWithCache
//      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, SyncDataToRuntime, project)
//      .unsafeRunSync() shouldBe true
//  }
//
//  "should consider userInfo objects with different tokenExpiresIn values as the same" in {
//    samAuthProviderWithCache.notebookAuthCache.invalidateAll()
//    // cache should be empty
//    samAuthProviderWithCache.notebookAuthCache.size shouldBe 0
//
//    // populate backing samClient
//    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set("sync")
//
//    // call provider method
//    samAuthProviderWithCache
//      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, SyncDataToRuntime, project)
//      .unsafeRunSync() shouldBe true
//
//    samAuthProviderWithCache
//      .hasRuntimePermission(runtimeSamResource,
//                            fakeUserInfo.copy(tokenExpiresIn = userInfo.tokenExpiresIn + 10),
//                            SyncDataToRuntime,
//                            project)
//      .unsafeRunSync() shouldBe true
//
//    samAuthProviderWithCache.notebookAuthCache.size shouldBe 1
//  }
//
//  "filterRuntimes should return runtimes that were created by the user or whose project is owned by the user" in {
//    val runtime1 = RuntimeSamResource(name1.asString)
//    val runtime2 = RuntimeSamResource(name2.asString)
//    // initial filterClusters should return empty list
//    samAuthProvider
//      .filterUserVisibleRuntimes(fakeUserInfo, List(project -> runtime1, project -> runtime2))
//      .unsafeRunSync() shouldBe List.empty
//
//    val runtimePolicy = SamRuntimePolicy(AccessPolicyName.Creator, runtime2)
//    // pretend user created name2
//    mockSam.runtimeCreators += fakeUserAuthorization -> Set(runtimePolicy)
//
//    // name2 should now be returned
//    samAuthProvider
//      .filterUserVisibleRuntimes(fakeUserInfo, List(project -> runtime1, project -> runtime2))
//      .unsafeRunSync() shouldBe List(project -> runtime2)
//
//    val projectPolicy = SamProjectPolicy(AccessPolicyName.Owner, project)
//    // pretend user owns the project
//    mockSam.projectOwners += fakeUserAuthorization -> Set(projectPolicy)
//
//    // name1 and name2 should now be returned
//    samAuthProvider
//      .filterUserVisibleRuntimes(fakeUserInfo, List(project -> runtime1, project -> runtime2))
//      .unsafeRunSync() shouldBe List(project -> runtime1, project -> runtime2)
//
//    mockSam.projectOwners.remove(fakeUserAuthorization)
//  }
//
//  "should add and delete a persistent-disk resource with correct actions for the user when a disk is created and then destroyed" in {
//    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("create_persistent_disk")
//    //     check the sam auth provider has no persistent-disk resource
//    mockSam.persistentDisks shouldBe empty
//    val defaultPermittedActions = List("read", "attach", "modify", "delete")
//    defaultPermittedActions.foreach { action =>
//      mockSam
//        .hasResourcePermission(diskSamResource, action, fakeUserAuthorization)
//        .unsafeRunSync() shouldBe false
//    }
//
//    // creating a disk would call notify
//    samAuthProvider.notifyResourceCreated(diskSamResource, fakeUserInfo.userEmail, project).unsafeRunSync()
//
//    // check the resource exists for the user and actions
//    defaultPermittedActions.foreach { action =>
//      mockSam
//        .hasResourcePermission(diskSamResource, action, fakeUserAuthorization)
//        .unsafeRunSync() shouldBe true
//    }
//    // deleting a disk would call notify
//    samAuthProvider
//      .notifyResourceDeleted(diskSamResource, fakeUserInfo.userEmail, fakeUserInfo.userEmail, project)
//      .unsafeRunSync()
//
//    mockSam.persistentDisks shouldBe empty
//    defaultPermittedActions.foreach { action =>
//      mockSam
//        .hasResourcePermission(diskSamResource, action, fakeUserAuthorization)
//        .unsafeRunSync() shouldBe false
//    }
//
//    mockSam.billingProjects.remove((project, fakeUserAuthorization))
//  }
//
//  "hasPersistentDiskPermission should return true if user has persistent disk permissions and false if they do not" in {
//    mockSam.persistentDisks += (diskSamResource, fakeUserAuthorization) -> Set("attach")
//    samAuthProvider
//      .hasPersistentDiskPermission(diskSamResource, fakeUserInfo, AttachPersistentDisk, project)
//      .unsafeRunSync() shouldBe true
//    samAuthProvider
//      .hasPersistentDiskPermission(diskSamResource, unauthorizedUserInfo, AttachPersistentDisk, project)
//      .unsafeRunSync() shouldBe false
//    samAuthProvider
//      .hasPersistentDiskPermission(diskSamResource, fakeUserInfo, DeletePersistentDisk, project)
//      .unsafeRunSync() shouldBe false
//    mockSam.persistentDisks.remove((diskSamResource, fakeUserAuthorization))
//  }
//
//  "hasPersistentDiskPermission should return true if user does not have persistent disk permissions but does have project permissions" in {
//    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("delete_persistent_disk")
//    mockSam.persistentDisks += (diskSamResource, fakeUserAuthorization) -> Set()
//
//    samAuthProvider
//      .hasPersistentDiskPermission(diskSamResource, fakeUserInfo, DeletePersistentDisk, project)
//      .unsafeRunSync() shouldBe true
//
//    mockSam.billingProjects.remove((project, fakeUserAuthorization))
//    mockSam.persistentDisks.remove((diskSamResource, fakeUserAuthorization))
//  }
//
//  "notifyPersistentDiskCreated should create a new persistent disk resource" in {
//    mockSam.persistentDisks shouldBe empty
//    samAuthProvider.notifyResourceCreated(diskSamResource, fakeUserInfo.userEmail, project).unsafeRunSync()
//    mockSam.persistentDisks.toList should contain(
//      (diskSamResource, fakeUserAuthorization) -> Set("read", "read_policies", "attach", "modify", "delete")
//    )
//    mockSam.persistentDisks.remove((diskSamResource, fakeUserAuthorization))
//  }
//
//  "notifyPersistentDiskDeleted should delete a persistent disk resource" in {
//    mockSam.persistentDisks += (diskSamResource, fakeUserAuthorization) -> Set()
//
//    samAuthProvider
//      .notifyResourceDeleted(diskSamResource, userInfo.userEmail, userInfo.userEmail, project)
//      .unsafeRunSync()
//    mockSam.persistentDisks.toList should not contain ((diskSamResource, fakeUserAuthorization) -> Set("read",
//                                                                                                       "attach",
//                                                                                                       "modify",
//                                                                                                       "delete"))
//  }
//
//  "filterUserVisiblePersistentDisks should return disks that were created by the user or whose project is owned by the user" in {
//    val disk1 = PersistentDiskSamResource(name1.asString)
//    val disk2 = PersistentDiskSamResource(name2.asString)
//    // initial filterDisks should return empty list
//    samAuthProvider
//      .filterUserVisiblePersistentDisks(fakeUserInfo, List(project -> disk1, project -> disk2))
//      .unsafeRunSync() shouldBe List.empty
//
//    val persistentDiskPolicy = SamPersistentDiskPolicy(AccessPolicyName.Creator, disk2)
//    // pretend user created name2
//    mockSam.diskCreators += fakeUserAuthorization -> Set(persistentDiskPolicy)
//
//    // name2 should now be returned
//    samAuthProvider
//      .filterUserVisiblePersistentDisks(fakeUserInfo, List(project -> disk1, project -> disk2))
//      .unsafeRunSync() shouldBe List(project -> disk2)
//
//    val projectPolicy = SamProjectPolicy(AccessPolicyName.Owner, project)
//    // pretend user owns the project
//    mockSam.projectOwners += fakeUserAuthorization -> Set(projectPolicy)
//
//    // name1 and name2 should now be returned
//    samAuthProvider
//      .filterUserVisiblePersistentDisks(fakeUserInfo, List(project -> disk1, project -> disk2))
//      .unsafeRunSync() shouldBe List(project -> disk1, project -> disk2)
//
//    mockSam.projectOwners.remove(fakeUserAuthorization)
//  }

  private def setUpMock(): Unit = {
    // set up mock sam with a project, runtime, disk, and app
    mockSam.createResource(ProjectSamResource(project), mockSam.projectOwnerEmail, project)
    mockSam.billingProjects.get(
      (ProjectSamResource(project), projectOwnerAuthHeader)
    ) shouldBe Some(
      ProjectAction.allActions
    )
    mockSam.createResource(runtimeSamResource, userEmail, project)
    mockSam.runtimes.get((runtimeSamResource, authHeader)) shouldBe Some(
      RuntimeAction.allActions
    )
    mockSam.createResource(diskSamResource, userEmail, project)
    mockSam.persistentDisks.get((diskSamResource, authHeader)) shouldBe Some(
      PersistentDiskAction.allActions
    )
    mockSam.createResourceWithManagerPolicy(KubernetesTestData.appSamId, userEmail, project)
    mockSam.apps.get((appSamId, authHeader)) shouldBe Some(
      AppAction.allActions
    )
  }
}
