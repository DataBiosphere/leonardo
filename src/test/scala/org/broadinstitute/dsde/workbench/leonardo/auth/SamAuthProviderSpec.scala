package org.broadinstitute.dsde.workbench.leonardo.auth

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.model.headers.{HttpCookiePair, OAuth2BearerToken}
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.config
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockGoogleDataprocDAO, MockSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterName, ClusterRequest, LeoAuthProvider}
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, MockProxyService, ProxyService, TestProxy}
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.mockito.Mockito
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
 // private val mockSwaggerSamClient = new MockSwaggerSamClient()
  private val samAuthProvider = new TestSamAuthProvider
  //samAuthProvider.samAPI = mockSwaggerSamClient
//  private val samAuthProvider = Mockito.mock(samAuth.getClass)
//  Mockito.when(samAuthProvider.samAPI).thenReturn(mockSwaggerSamClient)


  val gdDAO = new MockGoogleDataprocDAO(dataprocConfig, proxyConfig, clusterDefaultsConfig)
  val iamDAO = new MockGoogleIamDAO
  val samDAO = new MockSamDAO
 // val tokenCookie = HttpCookiePair("LeoToken", "me")


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


  "should add a notebook-cluster resource with correct actions for the user when a new cluster is created" in isolatedDbTest {
    samAuthProvider.samClient.billingProjects += (project, userInfo.userEmail) -> Set("launch_notebook_cluster")
    assert(samAuthProvider.samClient.billingProjects.contains((project, userInfo.userEmail)))
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

  }

}
