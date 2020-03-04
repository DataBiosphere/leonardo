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
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions.{DeleteCluster, SyncDataToCluster}
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.CreateClusters
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchUserId}
import org.http4s.HttpApp
import org.http4s.client.Client
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import CommonTestData._

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

  "should add and delete a notebook-cluster resource with correct actions for the user when a cluster is created and then destroyed" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("launch_notebook_cluster")
//     check the sam auth provider has no notebook-cluster resource
    mockSam.notebookClusters shouldBe empty
    val defaultPermittedActions = List("status", "connect", "sync", "delete")
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(internalId.asString, action, ResourceTypeName.NotebookCluster, fakeUserAuthorization)
        .unsafeRunSync() shouldBe false
    }

    // creating a cluster would call notify
    samAuthProvider.notifyClusterCreated(internalId, fakeUserInfo.userEmail, project, name1).unsafeRunSync()

    // check the resource exists for the user and actions
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(internalId.asString, action, ResourceTypeName.NotebookCluster, fakeUserAuthorization)
        .unsafeRunSync() shouldBe true
    }
    // deleting a cluster would call notify
    samAuthProvider
      .notifyClusterDeleted(internalId, fakeUserInfo.userEmail, fakeUserInfo.userEmail, project, name1)
      .unsafeRunSync()

    mockSam.notebookClusters shouldBe empty
    defaultPermittedActions.foreach { action =>
      mockSam
        .hasResourcePermission(internalId.asString, action, ResourceTypeName.NotebookCluster, fakeUserAuthorization)
        .unsafeRunSync() shouldBe false
    }

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
  }

  "hasProjectPermission should return true if user has project permissions and false if they do not" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("launch_notebook_cluster")
    samAuthProvider.hasProjectPermission(fakeUserInfo, CreateClusters, project).unsafeRunSync() shouldBe true

    samAuthProvider.hasProjectPermission(unauthorizedUserInfo, CreateClusters, project).unsafeRunSync() shouldBe false
    samAuthProvider
      .hasProjectPermission(fakeUserInfo, CreateClusters, GoogleProject("leo-fake-project"))
      .unsafeRunSync() shouldBe false

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
  }

  "hasNotebookClusterPermission should return true if user has notebook cluster permissions and false if they do not" in {
    mockSam.notebookClusters += (internalId, fakeUserAuthorization) -> Set("sync")
    samAuthProvider
      .hasNotebookClusterPermission(internalId, fakeUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe true

    samAuthProvider
      .hasNotebookClusterPermission(internalId, unauthorizedUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe false
    samAuthProvider
      .hasNotebookClusterPermission(internalId, fakeUserInfo, DeleteCluster, project, name1)
      .unsafeRunSync() shouldBe false
    mockSam.notebookClusters.remove((internalId, fakeUserAuthorization))
  }

  "hasNotebookClusterPermission should return true if user does not have notebook cluster permissions but does have project permissions" in {
    mockSam.billingProjects += (project, fakeUserAuthorization) -> Set("sync_notebook_cluster")
    mockSam.notebookClusters += (internalId, fakeUserAuthorization) -> Set()

    samAuthProvider
      .hasNotebookClusterPermission(internalId, fakeUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe true

    mockSam.billingProjects.remove((project, fakeUserAuthorization))
    mockSam.notebookClusters.remove((internalId, fakeUserAuthorization))
  }

  "notifyClusterCreated should create a new cluster resource" in {
    mockSam.notebookClusters shouldBe empty
    samAuthProvider.notifyClusterCreated(internalId, fakeUserInfo.userEmail, project, name1).unsafeRunSync()
    mockSam.notebookClusters.toList should contain(
      (internalId, fakeUserAuthorization) -> Set("connect", "read_policies", "status", "delete", "sync")
    )
    mockSam.notebookClusters.remove((internalId, fakeUserAuthorization))
  }

  "notifyClusterDeleted should delete a cluster resource" in {
    mockSam.notebookClusters += (internalId, fakeUserAuthorization) -> Set()

    samAuthProvider
      .notifyClusterDeleted(internalId, userInfo.userEmail, userInfo.userEmail, project, name1)
      .unsafeRunSync()
    mockSam.notebookClusters.toList should not contain ((internalId, fakeUserAuthorization) -> Set("connect",
                                                                                                   "read_policies",
                                                                                                   "status",
                                                                                                   "delete",
                                                                                                   "sync"))
  }

  "should cache hasNotebookClusterPermission results" in {
    // cache should be empty
    samAuthProviderWithCache.notebookAuthCache.size shouldBe 0

    // populate backing samClient
    mockSam.notebookClusters += (internalId, fakeUserAuthorization) -> Set("sync")

    // call provider method
    samAuthProviderWithCache
      .hasNotebookClusterPermission(internalId, fakeUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe true

    // cache should contain 1 entry
    samAuthProviderWithCache.notebookAuthCache.size shouldBe 1
    val key = NotebookAuthCacheKey(internalId, fakeUserAuthorization, SyncDataToCluster, project, name1)
    samAuthProviderWithCache.notebookAuthCache.asMap.containsKey(key) shouldBe true
    samAuthProviderWithCache.notebookAuthCache.asMap.get(key) shouldBe true
    // remove info from samClient
    mockSam.notebookClusters.remove((internalId, fakeUserAuthorization))
    // provider should still return true because the info is cached
    samAuthProviderWithCache
      .hasNotebookClusterPermission(internalId, fakeUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe true
  }

  "should consider userInfo objects with different tokenExpiresIn values as the same" in {
    samAuthProviderWithCache.notebookAuthCache.invalidateAll()
    // cache should be empty
    samAuthProviderWithCache.notebookAuthCache.size shouldBe 0

    // populate backing samClient
    mockSam.notebookClusters += (internalId, fakeUserAuthorization) -> Set("sync")

    // call provider method
    samAuthProviderWithCache
      .hasNotebookClusterPermission(internalId, fakeUserInfo, SyncDataToCluster, project, name1)
      .unsafeRunSync() shouldBe true

    samAuthProviderWithCache
      .hasNotebookClusterPermission(internalId,
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
  }
}

object SamAuthProviderSpec {
  def testClient(app: HttpApp[IO]): Client[IO] =
    Client.fromHttpApp[IO](app)
}
