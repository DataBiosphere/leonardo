package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockGoogleDataprocDAO, MockSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions.{DeleteCluster, SyncDataToCluster}
import org.broadinstitute.dsde.workbench.leonardo.model.ProjectActions.CreateClusters
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterName, ClusterRequest}
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, MockProxyService, TestProxy}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.mockito.MockitoSugar
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures


class SamAuthProviderSpec extends FreeSpec with ScalatestRouteTest with Matchers with MockitoSugar with BeforeAndAfter with BeforeAndAfterAll with TestComponent with TestProxy with ScalaFutures with OptionValues {
  val project = GoogleProject("dsp-leo-test")
  val name1 = ClusterName("name1")
  val googleProject = project.value
  val clusterName = name1.string
  val userInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)
  private val testClusterRequest = ClusterRequest(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), None)

  val routeTest = this

  private val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val swaggerConfig = config.as[SwaggerConfig]("swagger")
  val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")
  val whitelist = config.as[(Set[String])]("auth.whitelistProviderConfig.whitelist").map(_.toLowerCase)
  private val serviceAccountProvider = new MockPetsPerProjectServiceAccountProvider(config.getConfig("serviceAccounts.config"))
  private val samAuthProvider = new TestSamAuthProvider

  val gdDAO = new MockGoogleDataprocDAO(dataprocConfig, proxyConfig, clusterDefaultsConfig)
  val iamDAO = new MockGoogleIamDAO
  val samDAO = new MockSamDAO

  class TestSamAuthProvider extends SamAuthProvider(config.getConfig("auth.samAuthProviderConfig"),serviceAccountProvider) {
    override val samClient = new MockSwaggerSamClient()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    startProxyServer()
  }

  override def afterAll(): Unit = {
    shutdownProxyServer()
    super.afterAll()
  }

  val leo = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, gdDAO, iamDAO, DbSingleton.ref, system.actorOf(NoopActor.props), samAuthProvider, serviceAccountProvider, whitelist)
  val proxy = new MockProxyService(proxyConfig, gdDAO, DbSingleton.ref, samAuthProvider)


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
