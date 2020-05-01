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
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterAction.{DeleteCluster, SyncDataToCluster}
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectAction.{CreateClusters, CreatePersistentDisk}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchUserId}
import org.http4s.HttpApp
import org.http4s.client.Client
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import CommonTestData._
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
    samAuthProvider.hasProjectPermission(fakeUserInfo, CreateClusters, project).unsafeRunSync() shouldBe true
    samAuthProvider.hasProjectPermission(fakeUserInfo, CreatePersistentDisk, project).unsafeRunSync() shouldBe true

    samAuthProvider.hasProjectPermission(unauthorizedUserInfo, CreateClusters, project).unsafeRunSync() shouldBe false
    samAuthProvider
      .hasProjectPermission(unauthorizedUserInfo, CreatePersistentDisk, project)
      .unsafeRunSync() shouldBe false
    samAuthProvider
      .hasProjectPermission(fakeUserInfo, CreateClusters, GoogleProject("leo-fake-project"))
      .unsafeRunSync() shouldBe false
    samAuthProvider
      .hasProjectPermission(fakeUserInfo, CreatePersistentDisk, GoogleProject("leo-fake-project"))
      .unsafeRunSync() shouldBe false

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
  }

  "should add and delete a notebook-cluster resource with correct actions for the user when a cluster is created and then destroyed" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("launch_notebook_cluster")
//     check the sam auth provider has no notebook-cluster resource
    mockSam.notebookClusters shouldBe empty
    val defaultPermittedActions = List("status", "connect", "sync", "delete")
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(runtimeInternalId.asString,
                               action,
                               ResourceTypeName.NotebookCluster,
                               fakeUserAuthorization)
        .unsafeRunSync() shouldBe false
    }

    // creating a cluster would call notify
    samAuthProvider.notifyClusterCreated(runtimeInternalId, fakeUserInfo.userEmail, project, name1).unsafeRunSync()

    // check the resource exists for the user and actions
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(runtimeInternalId.asString,
                               action,
                               ResourceTypeName.NotebookCluster,
                               fakeUserAuthorization)
        .unsafeRunSync() shouldBe true
    }
    // deleting a cluster would call notify
    samAuthProvider
      .notifyClusterDeleted(runtimeInternalId, fakeUserInfo.userEmail, fakeUserInfo.userEmail, project, name1)
      .unsafeRunSync()

    mockSam.notebookClusters shouldBe empty
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(runtimeInternalId.asString,
                               action,
                               ResourceTypeName.NotebookCluster,
                               fakeUserAuthorization)
        .unsafeRunSync() shouldBe false
    }

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
  }

  "hasNotebookClusterPermission should return true if user has notebook cluster permissions and false if they do not" in {
    mockSam.notebookClusters += (runtimeInternalId, fakeUserAuthorization) -> Set("sync")
    samAuthProvider
      .hasNotebookClusterPermission(runtimeInternalId, fakeUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe true

    samAuthProvider
      .hasNotebookClusterPermission(runtimeInternalId, unauthorizedUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe false
    samAuthProvider
      .hasNotebookClusterPermission(runtimeInternalId, fakeUserInfo, DeleteCluster, project, name1)
      .unsafeRunSync() shouldBe false
    mockSam.notebookClusters.remove((runtimeInternalId, fakeUserAuthorization))
  }

  "hasNotebookClusterPermission should return true if user does not have notebook cluster permissions but does have project permissions" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("sync_notebook_cluster")
    mockSam.notebookClusters += (runtimeInternalId, fakeUserAuthorization) -> Set()

    samAuthProvider
      .hasNotebookClusterPermission(runtimeInternalId, fakeUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe true

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
    mockSam.notebookClusters.remove((runtimeInternalId, fakeUserAuthorization))
  }

  "notifyClusterCreated should create a new cluster resource" in {
    mockSam.notebookClusters shouldBe empty
    samAuthProvider.notifyClusterCreated(runtimeInternalId, fakeUserInfo.userEmail, project, name1).unsafeRunSync()
    mockSam.notebookClusters.toList should contain(
      (runtimeInternalId, fakeUserAuthorization) -> Set("connect", "read_policies", "status", "delete", "sync")
    )
    mockSam.notebookClusters.remove((runtimeInternalId, fakeUserAuthorization))
  }

  "notifyClusterDeleted should delete a cluster resource" in {
    mockSam.notebookClusters += (runtimeInternalId, fakeUserAuthorization) -> Set()

    samAuthProvider
      .notifyClusterDeleted(runtimeInternalId, userInfo.userEmail, userInfo.userEmail, project, name1)
      .unsafeRunSync()
    mockSam.notebookClusters.toList should not contain ((runtimeInternalId, fakeUserAuthorization) -> Set(
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
    mockSam.notebookClusters += (runtimeInternalId, fakeUserAuthorization) -> Set("sync")

    // call provider method
    samAuthProviderWithCache
      .hasNotebookClusterPermission(runtimeInternalId, fakeUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe true

    // cache should contain 1 entry
    samAuthProviderWithCache.notebookAuthCache.size shouldBe 1
    val key = NotebookAuthCacheKey(runtimeInternalId, fakeUserAuthorization, SyncDataToCluster, project, name1)
    samAuthProviderWithCache.notebookAuthCache.asMap.containsKey(key) shouldBe true
    samAuthProviderWithCache.notebookAuthCache.asMap.get(key) shouldBe true
    // remove info from samClient
    mockSam.notebookClusters.remove((runtimeInternalId, fakeUserAuthorization))
    // provider should still return true because the info is cached
    samAuthProviderWithCache
      .hasNotebookClusterPermission(runtimeInternalId, fakeUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe true
  }

  "should consider userInfo objects with different tokenExpiresIn values as the same" in {
    samAuthProviderWithCache.notebookAuthCache.invalidateAll()
    // cache should be empty
    samAuthProviderWithCache.notebookAuthCache.size shouldBe 0

    // populate backing samClient
    mockSam.notebookClusters += (runtimeInternalId, fakeUserAuthorization) -> Set("sync")

    // call provider method
    samAuthProviderWithCache
      .hasNotebookClusterPermission(runtimeInternalId, fakeUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe true

    samAuthProviderWithCache
      .hasNotebookClusterPermission(runtimeInternalId,
                                    fakeUserInfo.copy(tokenExpiresIn = userInfo.tokenExpiresIn + 10),
                                    SyncDataToCluster,
                                    project,
                                    name1)
      .unsafeRunSync() shouldBe true

    samAuthProviderWithCache.notebookAuthCache.size shouldBe 1
  }

  "filterClusters should return clusters that were created by the user or whose project is owned by the user" in {
    val cluster1 = RuntimeInternalId(name1.asString)
    val cluster2 = RuntimeInternalId(name2.asString)
    // initial filterClusters should return empty list
    samAuthProvider
      .filterUserVisibleClusters(fakeUserInfo, List(project -> cluster1, project -> cluster2))
      .unsafeRunSync() shouldBe List.empty

    val notebookClusterPolicy = SamNotebookClusterPolicy(AccessPolicyName.Creator, cluster2)
    // pretend user created name2
    mockSam.clusterCreators += fakeUserAuthorization -> Set(notebookClusterPolicy)

    // name2 should now be returned
    samAuthProvider
      .filterUserVisibleClusters(fakeUserInfo, List(project -> cluster1, project -> cluster2))
      .unsafeRunSync() shouldBe List(project -> cluster2)

    val projectPolicy = SamProjectPolicy(AccessPolicyName.Owner, project)
    // pretend user owns the project
    mockSam.projectOwners += fakeUserAuthorization -> Set(projectPolicy)

    // name1 and name2 should now be returned
    samAuthProvider
      .filterUserVisibleClusters(fakeUserInfo, List(project -> cluster1, project -> cluster2))
      .unsafeRunSync() shouldBe List(project -> cluster1, project -> cluster2)

    mockSam.projectOwners.remove(fakeUserAuthorization)
  }

  "should add and delete a persistent-disk resource with correct actions for the user when a disk is created and then destroyed" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("create_persistent_disk")
    //     check the sam auth provider has no persistent-disk resource
    mockSam.persistentDisks shouldBe empty
    val defaultPermittedActions = List("read", "attach", "modify", "delete")
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(diskInternalId.asString, action, ResourceTypeName.PersistentDisk, fakeUserAuthorization)
        .unsafeRunSync() shouldBe false
    }

    // creating a disk would call notify
    samAuthProvider.notifyPersistentDiskCreated(diskInternalId, fakeUserInfo.userEmail, project).unsafeRunSync()

    // check the resource exists for the user and actions
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(diskInternalId.asString, action, ResourceTypeName.PersistentDisk, fakeUserAuthorization)
        .unsafeRunSync() shouldBe true
    }
    // deleting a disk would call notify
    samAuthProvider
      .notifyPersistentDiskDeleted(diskInternalId, fakeUserInfo.userEmail, fakeUserInfo.userEmail, project)
      .unsafeRunSync()

    mockSam.persistentDisks shouldBe empty
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(diskInternalId.asString, action, ResourceTypeName.PersistentDisk, fakeUserAuthorization)
        .unsafeRunSync() shouldBe false
    }

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
  }

  "hasPersistentDiskPermission should return true if user has persistent disk permissions and false if they do not" in {
    mockSam.persistentDisks += (diskInternalId, fakeUserAuthorization) -> Set("attach")
    samAuthProvider
      .hasPersistentDiskPermission(diskInternalId, fakeUserInfo, AttachPersistentDisk, project)
      .unsafeRunSync() shouldBe true
    samAuthProvider
      .hasPersistentDiskPermission(diskInternalId, unauthorizedUserInfo, AttachPersistentDisk, project)
      .unsafeRunSync() shouldBe false
    samAuthProvider
      .hasPersistentDiskPermission(diskInternalId, fakeUserInfo, DeletePersistentDisk, project)
      .unsafeRunSync() shouldBe false
    mockSam.persistentDisks.remove((diskInternalId, fakeUserAuthorization))
  }

  "hasPersistentDiskPermission should return true if user does not have persistent disk permissions but does have project permissions" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("delete_persistent_disk")
    mockSam.persistentDisks += (diskInternalId, fakeUserAuthorization) -> Set()

    samAuthProvider
      .hasPersistentDiskPermission(diskInternalId, fakeUserInfo, DeletePersistentDisk, project)
      .unsafeRunSync() shouldBe true

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
    mockSam.persistentDisks.remove((diskInternalId, fakeUserAuthorization))
  }

  "notifyPersistentDiskCreated should create a new persistent disk resource" in {
    mockSam.persistentDisks shouldBe empty
    samAuthProvider.notifyPersistentDiskCreated(diskInternalId, fakeUserInfo.userEmail, project).unsafeRunSync()
    mockSam.persistentDisks.toList should contain(
      (diskInternalId, fakeUserAuthorization) -> Set("read", "read_policies", "attach", "modify", "delete")
    )
    mockSam.persistentDisks.remove((diskInternalId, fakeUserAuthorization))
  }

  "notifyPersistentDiskDeleted should delete a persistent disk resource" in {
    mockSam.persistentDisks += (diskInternalId, fakeUserAuthorization) -> Set()

    samAuthProvider
      .notifyPersistentDiskDeleted(diskInternalId, userInfo.userEmail, userInfo.userEmail, project)
      .unsafeRunSync()
    mockSam.persistentDisks.toList should not contain ((diskInternalId, fakeUserAuthorization) -> Set("read",
                                                                                                      "attach",
                                                                                                      "modify",
                                                                                                      "delete"))
  }

  "filterUserVisiblePersistentDisks should return disks that were created by the user or whose project is owned by the user" in {
    val disk1 = PersistentDiskInternalId(name1.asString)
    val disk2 = PersistentDiskInternalId(name2.asString)
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
