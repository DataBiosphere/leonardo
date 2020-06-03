package org.broadinstitute.dsde.workbench.leonardo
package auth.sam

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.testkit.TestKit
import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleIamDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.model.RuntimeAction.{DeleteRuntime, SyncDataToRuntime}
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectAction.{CreatePersistentDisk, CreateRuntime}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchUserId}
import org.http4s.HttpApp
import org.http4s.client.Client
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{PersistentDiskSamResource, RuntimeSamResource}
import org.broadinstitute.dsde.workbench.leonardo.model.PersistentDiskAction.{
  AttachPersistentDisk,
  DeletePersistentDisk
}

import scala.concurrent.duration._

class SamAuthProviderSpec
    extends TestKit(ActorSystem("leonardotest"))
    with FreeSpecLike
    with Matchers
    with TestComponent
    with ScalaFutures
    with BeforeAndAfterAll
    with LazyLogging {
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val gdDAO = new MockGoogleDataprocDAO
  val iamDAO = new MockGoogleIamDAO
  val samAuthProviderConfigWithoutCache: SamAuthProviderConfig = SamAuthProviderConfig(
    false,
    10,
    1.minutes
  )

  val fakeUserInfo = UserInfo(OAuth2BearerToken(s"TokenFor${userEmail}"), WorkbenchUserId("user1"), userEmail, 0)
  val mockSam = new MockSamDAO
  val samAuthProvider = new SamAuthProvider(mockSam, samAuthProviderConfigWithoutCache, serviceAccountProvider, blocker)
  val fakeUserAuthorization = MockSamDAO.userEmailToAuthorization(fakeUserInfo.userEmail)

  val samAuthProviderConfigWithCache: SamAuthProviderConfig = SamAuthProviderConfig(
    true,
    10,
    1.minutes
  )
  val samAuthProviderWithCache =
    new SamAuthProvider(mockSam, samAuthProviderConfigWithCache, serviceAccountProvider, blocker)

  "hasProjectPermission should return true if user has project permissions and false if they do not" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("launch_notebook_cluster",
                                                                       "create_persistent_disk")
    samAuthProvider.hasProjectPermission(fakeUserInfo, CreateRuntime, project).unsafeRunSync() shouldBe true
    samAuthProvider.hasProjectPermission(fakeUserInfo, CreatePersistentDisk, project).unsafeRunSync() shouldBe true

    samAuthProvider.hasProjectPermission(unauthorizedUserInfo, CreateRuntime, project).unsafeRunSync() shouldBe false
    samAuthProvider
      .hasProjectPermission(unauthorizedUserInfo, CreatePersistentDisk, project)
      .unsafeRunSync() shouldBe false
    samAuthProvider
      .hasProjectPermission(fakeUserInfo, CreateRuntime, GoogleProject("leo-fake-project"))
      .unsafeRunSync() shouldBe false
    samAuthProvider
      .hasProjectPermission(fakeUserInfo, CreatePersistentDisk, GoogleProject("leo-fake-project"))
      .unsafeRunSync() shouldBe false

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
  }

  "should add and delete a runtime resource with correct actions for the user when a runtime is created and then destroyed" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("launch_notebook_cluster")
//     check the sam auth provider has no notebook-cluster resource
    mockSam.runtimes shouldBe empty
    val defaultPermittedActions = List("status", "connect", "sync", "delete")
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(runtimeSamResource, action, fakeUserAuthorization)
        .unsafeRunSync() shouldBe false
    }

    // creating a cluster would call notify
    samAuthProvider.notifyResourceCreated(runtimeSamResource, fakeUserInfo.userEmail, project).unsafeRunSync()

    // check the resource exists for the user and actions
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(runtimeSamResource, action, fakeUserAuthorization)
        .unsafeRunSync() shouldBe true
    }
    // deleting a cluster would call notify
    samAuthProvider
      .notifyResourceDeleted(runtimeSamResource, fakeUserInfo.userEmail, fakeUserInfo.userEmail, project)
      .unsafeRunSync()

    mockSam.runtimes shouldBe empty
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(runtimeSamResource, action, fakeUserAuthorization)
        .unsafeRunSync() shouldBe false
    }

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
  }

  "hasNotebookClusterPermission should return true if user has notebook cluster permissions and false if they do not" in {
    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set("sync")
    samAuthProvider
      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, SyncDataToRuntime, project)
      .unsafeRunSync() shouldBe true

    samAuthProvider
      .hasRuntimePermission(runtimeSamResource, unauthorizedUserInfo, SyncDataToRuntime, project)
      .unsafeRunSync() shouldBe false
    samAuthProvider
      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, DeleteRuntime, project)
      .unsafeRunSync() shouldBe false
    mockSam.runtimes.remove((runtimeSamResource, fakeUserAuthorization))
  }

  "hasNotebookClusterPermission should return true if user does not have notebook cluster permissions but does have project permissions" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("sync_notebook_cluster")
    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set()

    samAuthProvider
      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, SyncDataToRuntime, project)
      .unsafeRunSync() shouldBe true

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
    mockSam.runtimes.remove((runtimeSamResource, fakeUserAuthorization))
  }

  "getRuntimeActionsWithProjectFallback should return project permissions as well" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("sync_notebook_cluster")
    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set()

    samAuthProvider
      .getRuntimeActionsWithProjectFallback(project, runtimeSamResource, fakeUserInfo)
      .unsafeRunSync() shouldBe List(SyncDataToRuntime)

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
    mockSam.runtimes.remove((runtimeSamResource, fakeUserAuthorization))
  }

  "notifyClusterCreated should create a new cluster resource" in {
    mockSam.runtimes shouldBe empty
    samAuthProvider.notifyResourceCreated(runtimeSamResource, fakeUserInfo.userEmail, project).unsafeRunSync()
    mockSam.runtimes.toList should contain(
      (runtimeSamResource, fakeUserAuthorization) -> Set("connect", "read_policies", "status", "delete", "sync")
    )
    mockSam.runtimes.remove((runtimeSamResource, fakeUserAuthorization))
  }

  "notifyClusterDeleted should delete a cluster resource" in {
    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set()

    samAuthProvider
      .notifyResourceDeleted(runtimeSamResource, userInfo.userEmail, userInfo.userEmail, project)
      .unsafeRunSync()
    mockSam.runtimes.toList should not contain ((runtimeSamResource, fakeUserAuthorization) -> Set(
      "connect",
      "read_policies",
      "status",
      "delete",
      "sync"
    ))
  }

  "should cache hasNotebookClusterPermission results" in {
    // cache should be empty
    samAuthProviderWithCache.notebookAuthCache.size shouldBe 0

    // populate backing samClient
    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set("sync")

    // call provider method
    samAuthProviderWithCache
      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, SyncDataToRuntime, project)
      .unsafeRunSync() shouldBe true

    // cache should contain 1 entry
    samAuthProviderWithCache.notebookAuthCache.size shouldBe 1
    val key = NotebookAuthCacheKey(runtimeSamResource, fakeUserAuthorization, SyncDataToRuntime, project)
    samAuthProviderWithCache.notebookAuthCache.asMap.containsKey(key) shouldBe true
    samAuthProviderWithCache.notebookAuthCache.asMap.get(key) shouldBe true
    // remove info from samClient
    mockSam.runtimes.remove((runtimeSamResource, fakeUserAuthorization))
    // provider should still return true because the info is cached
    samAuthProviderWithCache
      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, SyncDataToRuntime, project)
      .unsafeRunSync() shouldBe true
  }

  "should consider userInfo objects with different tokenExpiresIn values as the same" in {
    samAuthProviderWithCache.notebookAuthCache.invalidateAll()
    // cache should be empty
    samAuthProviderWithCache.notebookAuthCache.size shouldBe 0

    // populate backing samClient
    mockSam.runtimes += (runtimeSamResource, fakeUserAuthorization) -> Set("sync")

    // call provider method
    samAuthProviderWithCache
      .hasRuntimePermission(runtimeSamResource, fakeUserInfo, SyncDataToRuntime, project)
      .unsafeRunSync() shouldBe true

    samAuthProviderWithCache
      .hasRuntimePermission(runtimeSamResource,
                            fakeUserInfo.copy(tokenExpiresIn = userInfo.tokenExpiresIn + 10),
                            SyncDataToRuntime,
                            project)
      .unsafeRunSync() shouldBe true

    samAuthProviderWithCache.notebookAuthCache.size shouldBe 1
  }

  "filterRuntimes should return runtimes that were created by the user or whose project is owned by the user" in {
    val runtime1 = RuntimeSamResource(name1.asString)
    val runtime2 = RuntimeSamResource(name2.asString)
    // initial filterClusters should return empty list
    samAuthProvider
      .filterUserVisibleRuntimes(fakeUserInfo, List(project -> runtime1, project -> runtime2))
      .unsafeRunSync() shouldBe List.empty

    val runtimePolicy = SamRuntimePolicy(AccessPolicyName.Creator, runtime2)
    // pretend user created name2
    mockSam.runtimeCreators += fakeUserAuthorization -> Set(runtimePolicy)

    // name2 should now be returned
    samAuthProvider
      .filterUserVisibleRuntimes(fakeUserInfo, List(project -> runtime1, project -> runtime2))
      .unsafeRunSync() shouldBe List(project -> runtime2)

    val projectPolicy = SamProjectPolicy(AccessPolicyName.Owner, project)
    // pretend user owns the project
    mockSam.projectOwners += fakeUserAuthorization -> Set(projectPolicy)

    // name1 and name2 should now be returned
    samAuthProvider
      .filterUserVisibleRuntimes(fakeUserInfo, List(project -> runtime1, project -> runtime2))
      .unsafeRunSync() shouldBe List(project -> runtime1, project -> runtime2)

    mockSam.projectOwners.remove(fakeUserAuthorization)
  }

  "should add and delete a persistent-disk resource with correct actions for the user when a disk is created and then destroyed" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("create_persistent_disk")
    //     check the sam auth provider has no persistent-disk resource
    mockSam.persistentDisks shouldBe empty
    val defaultPermittedActions = List("read", "attach", "modify", "delete")
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(diskSamResource, action, fakeUserAuthorization)
        .unsafeRunSync() shouldBe false
    }

    // creating a disk would call notify
    samAuthProvider.notifyResourceCreated(diskSamResource, fakeUserInfo.userEmail, project).unsafeRunSync()

    // check the resource exists for the user and actions
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(diskSamResource, action, fakeUserAuthorization)
        .unsafeRunSync() shouldBe true
    }
    // deleting a disk would call notify
    samAuthProvider
      .notifyResourceDeleted(diskSamResource, fakeUserInfo.userEmail, fakeUserInfo.userEmail, project)
      .unsafeRunSync()

    mockSam.persistentDisks shouldBe empty
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(diskSamResource, action, fakeUserAuthorization)
        .unsafeRunSync() shouldBe false
    }

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
  }

  "hasPersistentDiskPermission should return true if user has persistent disk permissions and false if they do not" in {
    mockSam.persistentDisks += (diskSamResource, fakeUserAuthorization) -> Set("attach")
    samAuthProvider
      .hasPersistentDiskPermission(diskSamResource, fakeUserInfo, AttachPersistentDisk, project)
      .unsafeRunSync() shouldBe true
    samAuthProvider
      .hasPersistentDiskPermission(diskSamResource, unauthorizedUserInfo, AttachPersistentDisk, project)
      .unsafeRunSync() shouldBe false
    samAuthProvider
      .hasPersistentDiskPermission(diskSamResource, fakeUserInfo, DeletePersistentDisk, project)
      .unsafeRunSync() shouldBe false
    mockSam.persistentDisks.remove((diskSamResource, fakeUserAuthorization))
  }

  "hasPersistentDiskPermission should return true if user does not have persistent disk permissions but does have project permissions" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("delete_persistent_disk")
    mockSam.persistentDisks += (diskSamResource, fakeUserAuthorization) -> Set()

    samAuthProvider
      .hasPersistentDiskPermission(diskSamResource, fakeUserInfo, DeletePersistentDisk, project)
      .unsafeRunSync() shouldBe true

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
    mockSam.persistentDisks.remove((diskSamResource, fakeUserAuthorization))
  }

  "notifyPersistentDiskCreated should create a new persistent disk resource" in {
    mockSam.persistentDisks shouldBe empty
    samAuthProvider.notifyResourceCreated(diskSamResource, fakeUserInfo.userEmail, project).unsafeRunSync()
    mockSam.persistentDisks.toList should contain(
      (diskSamResource, fakeUserAuthorization) -> Set("read", "read_policies", "attach", "modify", "delete")
    )
    mockSam.persistentDisks.remove((diskSamResource, fakeUserAuthorization))
  }

  "notifyPersistentDiskDeleted should delete a persistent disk resource" in {
    mockSam.persistentDisks += (diskSamResource, fakeUserAuthorization) -> Set()

    samAuthProvider
      .notifyResourceDeleted(diskSamResource, userInfo.userEmail, userInfo.userEmail, project)
      .unsafeRunSync()
    mockSam.persistentDisks.toList should not contain ((diskSamResource, fakeUserAuthorization) -> Set("read",
                                                                                                       "attach",
                                                                                                       "modify",
                                                                                                       "delete"))
  }

  "filterUserVisiblePersistentDisks should return disks that were created by the user or whose project is owned by the user" in {
    val disk1 = PersistentDiskSamResource(name1.asString)
    val disk2 = PersistentDiskSamResource(name2.asString)
    // initial filterDisks should return empty list
    samAuthProvider
      .filterUserVisiblePersistentDisks(fakeUserInfo, List(project -> disk1, project -> disk2))
      .unsafeRunSync() shouldBe List.empty

    val persistentDiskPolicy = SamPersistentDiskPolicy(AccessPolicyName.Creator, disk2)
    // pretend user created name2
    mockSam.diskCreators += fakeUserAuthorization -> Set(persistentDiskPolicy)

    // name2 should now be returned
    samAuthProvider
      .filterUserVisiblePersistentDisks(fakeUserInfo, List(project -> disk1, project -> disk2))
      .unsafeRunSync() shouldBe List(project -> disk2)

    val projectPolicy = SamProjectPolicy(AccessPolicyName.Owner, project)
    // pretend user owns the project
    mockSam.projectOwners += fakeUserAuthorization -> Set(projectPolicy)

    // name1 and name2 should now be returned
    samAuthProvider
      .filterUserVisiblePersistentDisks(fakeUserInfo, List(project -> disk1, project -> disk2))
      .unsafeRunSync() shouldBe List(project -> disk1, project -> disk2)

    mockSam.projectOwners.remove(fakeUserAuthorization)
  }
}

object SamAuthProviderSpec {
  def testClient(app: HttpApp[IO]): Client[IO] =
    Client.fromHttpApp[IO](app)
}
