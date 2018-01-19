package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.Config
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.dao.MockGoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions.{DeleteCluster, SyncDataToCluster}
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.CreateClusters
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterName, ServiceAccountProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, TestProxy}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.mockito.MockitoSugar
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class TestSamAuthProvider(authConfig: Config, serviceAccountProvider: ServiceAccountProvider) extends SamAuthProvider(authConfig, serviceAccountProvider) {
  override val samClient = new MockSwaggerSamClient()
}

class SamAuthProviderSpec extends FreeSpec with ScalatestRouteTest with Matchers with MockitoSugar with BeforeAndAfter with BeforeAndAfterAll with TestComponent with CommonTestData with TestProxy with ScalaFutures with OptionValues {
  val googleProject = project.value
  val clusterName = name1.string

  val routeTest = this

  private val samAuthProvider = new TestSamAuthProvider(config.getConfig("auth.samAuthProviderConfig"),serviceAccountProvider)

  val gdDAO = new MockGoogleDataprocDAO(dataprocConfig, proxyConfig, clusterDefaultsConfig)
  val iamDAO = new MockGoogleIamDAO

  val leo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, gdDAO, iamDAO, DbSingleton.ref, system.actorOf(NoopActor.props), samAuthProvider, serviceAccountProvider, whitelist)

  "should add and delete a notebook-cluster resource with correct actions for the user when a cluster is created and then destroyed" in isolatedDbTest {
    samAuthProvider.samClient.billingProjects += (project, userInfo.userEmail) -> Set("launch_notebook_cluster")
    // check the sam auth provider has no notebook-cluster resource
    samAuthProvider.samClient.notebookClusters shouldBe empty
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "status") shouldBe false
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "connect") shouldBe false
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "sync") shouldBe false
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "delete") shouldBe false

    // create a cluster
    val cluster = leo.createCluster(userInfo,project,name1, testClusterRequest).futureValue
    val clusterStatus = leo.getActiveClusterDetails(userInfo, project, name1).futureValue
    cluster shouldEqual clusterStatus

    // check the resource exists for the user and actions
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "status") shouldBe true
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "connect") shouldBe true
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "sync") shouldBe true
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "delete") shouldBe true

    leo.deleteCluster(userInfo, project, name1).futureValue

    samAuthProvider.samClient.notebookClusters shouldBe empty
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "status") shouldBe false
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "connect") shouldBe false
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "sync") shouldBe false
    samAuthProvider.samClient.hasActionOnNotebookClusterResource(userInfo.userEmail, project, name1, "delete") shouldBe false

    samAuthProvider.samClient.billingProjects.remove((project, userInfo.userEmail))

  }

  "hasProjectPermission should return true if user has project permissions and false if they do not" in isolatedDbTest {
    samAuthProvider.samClient.billingProjects += (project, userInfo.userEmail) -> Set("launch_notebook_cluster")
    samAuthProvider.hasProjectPermission(userInfo.userEmail, CreateClusters, project).futureValue shouldBe true

    samAuthProvider.hasProjectPermission(WorkbenchEmail("somecreep@example.com"), CreateClusters, project).futureValue shouldBe false
    samAuthProvider.hasProjectPermission(userInfo.userEmail, CreateClusters, GoogleProject("leo-fake-project")).futureValue shouldBe false

    samAuthProvider.samClient.billingProjects.remove((project, userInfo.userEmail))
  }

  "canSeeAllClustersInProject should return true if user has list permissions on a project and false if they do not" in isolatedDbTest {
    samAuthProvider.samClient.billingProjects += (project, userInfo.userEmail) -> Set("list_notebook_cluster")
    samAuthProvider.canSeeAllClustersInProject(userInfo.userEmail, project).futureValue shouldBe true

    samAuthProvider.canSeeAllClustersInProject(WorkbenchEmail("somecreep@example.com"), project).futureValue shouldBe false
    samAuthProvider.canSeeAllClustersInProject(userInfo.userEmail,GoogleProject("leo-fake-project")).futureValue shouldBe false

    samAuthProvider.samClient.billingProjects.remove((project, userInfo.userEmail))
  }

  "hasNotebookClusterPermission should return true if user has notebook cluster permissions and false if they do not" in isolatedDbTest {
    samAuthProvider.samClient.notebookClusters += (project, name1, userInfo.userEmail) -> Set("sync")
    samAuthProvider.hasNotebookClusterPermission(userInfo.userEmail, SyncDataToCluster, project, name1).futureValue shouldBe true

    samAuthProvider.hasNotebookClusterPermission(WorkbenchEmail("somecreep@example.com"), SyncDataToCluster, project, name1).futureValue shouldBe false
    samAuthProvider.hasNotebookClusterPermission(userInfo.userEmail, DeleteCluster, project, name1).futureValue shouldBe false
    samAuthProvider.hasNotebookClusterPermission(userInfo.userEmail, SyncDataToCluster, GoogleProject("leo-fake-project"), name1).futureValue shouldBe false
    samAuthProvider.hasNotebookClusterPermission(userInfo.userEmail, SyncDataToCluster, project, ClusterName("fake-cluster")).futureValue shouldBe false

    samAuthProvider.samClient.notebookClusters.remove((project, name1, userInfo.userEmail))
  }

  "hasNotebookClusterPermission should return true if user does not have notebook cluster permissions but does have project permissions" in isolatedDbTest {
    samAuthProvider.samClient.billingProjects += (project, userInfo.userEmail) -> Set("sync_notebook_cluster")
    samAuthProvider.samClient.notebookClusters += (project, name1, userInfo.userEmail) -> Set()

    samAuthProvider.hasNotebookClusterPermission(userInfo.userEmail, SyncDataToCluster, project, name1).futureValue shouldBe true

    samAuthProvider.samClient.billingProjects.remove((project, userInfo.userEmail))
    samAuthProvider.samClient.notebookClusters.remove((project, name1, userInfo.userEmail))
  }

  "notifyClusterCreated should create a new cluster resource" in isolatedDbTest {
    samAuthProvider.samClient.notebookClusters shouldBe empty
    samAuthProvider.notifyClusterCreated(userInfo.userEmail, project, name1).futureValue
    samAuthProvider.samClient.notebookClusters should contain ((project, name1, userInfo.userEmail) -> Set("connect", "read_policies", "status", "delete", "sync"))
    samAuthProvider.samClient.notebookClusters.remove((project, name1, userInfo.userEmail))
  }

  "notifyClusterDeleted should delete a cluster resource" in isolatedDbTest {
    samAuthProvider.samClient.notebookClusters += (project, name1, userInfo.userEmail) -> Set()

    samAuthProvider.notifyClusterDeleted(userInfo.userEmail, project, name1).futureValue
    samAuthProvider.samClient.notebookClusters should not contain ((project, name1, userInfo.userEmail) -> Set("connect", "read_policies", "status", "delete", "sync"))
  }

}
