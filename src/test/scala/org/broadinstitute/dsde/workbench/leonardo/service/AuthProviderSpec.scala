package org.broadinstitute.dsde.workbench.leonardo.service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers.{HttpCookiePair, OAuth2BearerToken}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.GcsPathUtils
import org.broadinstitute.dsde.workbench.leonardo.auth.{MockLeoAuthProvider, MockPetsPerProjectServiceAccountProvider}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig, SwaggerConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockGoogleDataprocDAO, MockSamDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers, OptionValues}
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.model.NotebookClusterActions.GetClusterStatus
import org.mockito.Mockito

class AuthProviderSpec extends FreeSpec with ScalatestRouteTest with Matchers with MockitoSugar with TestComponent with ScalaFutures with OptionValues with GcsPathUtils with TestProxy with BeforeAndAfterAll {
  val name1 = ClusterName("name1")
  val project = GoogleProject("dsp-leo-test")
  val userInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("user1"), WorkbenchEmail("user1@example.com"), 0)
  val userEmail = WorkbenchEmail("user1@example.com")
  val serviceAccountEmail = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val clusterName = name1.string
  val googleProject = project.value

  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val whitelist = config.as[Set[String]]("auth.whitelistProviderConfig.whitelist").map(_.toLowerCase)
  val dataprocConfig = config.as[DataprocConfig]("dataproc")
  val proxyConfig = config.as[ProxyConfig]("proxy")
  val clusterFilesConfig = config.as[ClusterFilesConfig]("clusterFiles")
  val clusterResourcesConfig = config.as[ClusterResourcesConfig]("clusterResources")
  val swaggerConfig = config.as[SwaggerConfig]("swagger")
  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockSamDAO = new MockSamDAO
  val clusterDefaultsConfig = config.as[ClusterDefaultsConfig]("clusterDefaults")

  val routeTest = this

  private val testClusterRequest = ClusterRequest(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), None)
  private val serviceAccountProvider = new MockPetsPerProjectServiceAccountProvider(config.getConfig("serviceAccounts.config"))
  private val alwaysYesProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider)
  private val alwaysNoProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysNoProviderConfig"), serviceAccountProvider)

  val c1 = Cluster(
    clusterName = name1,
    googleId = UUID.randomUUID(),
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1),
    operationName = OperationName("op1"),
    status = ClusterStatus.Unknown,
    hostIp = Some(IP("numbers.and.dots")),
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = None)

  val gdDAO = new MockGoogleDataprocDAO(dataprocConfig, proxyConfig, clusterDefaultsConfig)
  val iamDAO = new MockGoogleIamDAO
  val samDAO = new MockSamDAO
  val tokenCookie = HttpCookiePair("LeoToken", "me")

  override def beforeAll(): Unit = {
    super.beforeAll()
    startProxyServer()
  }

  override def afterAll(): Unit = {
    shutdownProxyServer()
    super.afterAll()
  }

  def leoWithAuthProvider(authProvider: LeoAuthProvider): LeonardoService = {
    new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, gdDAO, iamDAO, DbSingleton.ref, system.actorOf(NoopActor.props), authProvider, serviceAccountProvider, whitelist)
  }

  def proxyWithAuthProvider(authProvider: LeoAuthProvider): ProxyService = {
    new MockProxyService(proxyConfig, gdDAO, DbSingleton.ref, authProvider)
  }

  "Leo with an AuthProvider" - {
    "should let you do things if the auth provider says yes" in isolatedDbTest {
      val spyProvider = spy(alwaysYesProvider)
      val leo = leoWithAuthProvider(spyProvider)
      val proxy = proxyWithAuthProvider(spyProvider)

      // create
      val cluster1 = leo.createCluster(userInfo, project, name1, testClusterRequest).futureValue

      // get status
      val clusterStatus = leo.getActiveClusterDetails(userInfo, project, name1).futureValue

      cluster1 shouldEqual clusterStatus

      // list
      val listResponse = leo.listClusters(userInfo, Map()).futureValue
      listResponse shouldBe Seq(cluster1)

      //connect
      val proxyRequest = HttpRequest(GET, Uri(s"/notebooks/$googleProject/$clusterName"))
      proxy.proxyNotebook(userInfo, GoogleProject(googleProject), ClusterName(clusterName), proxyRequest).futureValue

      //sync
      val syncRequest = HttpRequest(POST, Uri(s"/notebooks/$googleProject/$clusterName/api/localize"))
      proxy.proxyLocalize(userInfo, GoogleProject(googleProject), ClusterName(clusterName), syncRequest).futureValue

      //delete
      leo.deleteCluster(userInfo, project, name1).futureValue

      //verify we correctly notified the auth provider
      verify(spyProvider).notifyClusterCreated(userEmail, project, name1)
      verify(spyProvider).notifyClusterDeleted(userEmail, project, name1)
    }

    "should not let you do things if the auth provider says no" in isolatedDbTest {
      val spyProvider = spy(alwaysNoProvider)
      val leo = leoWithAuthProvider(spyProvider)
      val proxy = proxyWithAuthProvider(spyProvider)

      //can't make a cluster
      val clusterCreateException = leo.createCluster(userInfo, project, name1, testClusterRequest).failed.futureValue
      clusterCreateException shouldBe a [AuthorizationError]
      clusterCreateException.asInstanceOf[AuthorizationError].statusCode shouldBe StatusCodes.Unauthorized

      //can't get details on an existing cluster
      //poke a cluster into the database so we actually have something to look for
      dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) }

      val clusterGetResponseException = leo.getActiveClusterDetails(userInfo, c1.googleProject, c1.clusterName).failed.futureValue
      clusterGetResponseException shouldBe a [ClusterNotFoundException]
      clusterGetResponseException.asInstanceOf[ClusterNotFoundException].statusCode shouldBe StatusCodes.NotFound

      //list all clusters should be fine, but empty
      val emptyList = leo.listClusters(userInfo, Map()).futureValue
      emptyList shouldBe 'empty

      //connect to cluster
      val httpRequest = HttpRequest(GET, Uri(s"/notebooks/$googleProject/$clusterName"))
      val clusterNotFoundException = proxy.proxyNotebook(userInfo, GoogleProject(googleProject), ClusterName(clusterName), httpRequest).failed.futureValue
      clusterNotFoundException shouldBe a [ClusterNotFoundException]

      //sync
      val syncRequest = HttpRequest(POST, Uri(s"/notebooks/$googleProject/$clusterName/api/localize"))
      val syncNotFoundException = proxy.proxyLocalize(userInfo, GoogleProject(googleProject), ClusterName(clusterName), syncRequest).failed.futureValue
      syncNotFoundException shouldBe a [ClusterNotFoundException]

      //destroy cluster
      val clusterNotFoundAgain = leo.deleteCluster(userInfo, project, name1).failed.futureValue
      clusterNotFoundAgain shouldBe a [ClusterNotFoundException]

      //verify we never notified the auth provider of clusters happening because they didn't
      verify(spyProvider, Mockito.never).notifyClusterCreated(userEmail, project, name1)
      verify(spyProvider, Mockito.never).notifyClusterDeleted(userEmail, project, name1)
    }

    "should give you a 401 if you can see a cluster's details but can't do the more specific action" in isolatedDbTest {
      val readOnlyProvider = new MockLeoAuthProvider(config.getConfig("auth.readOnlyProviderConfig"), serviceAccountProvider)
      val spyProvider = spy(readOnlyProvider)
      val leo = leoWithAuthProvider(spyProvider)
      val proxy = proxyWithAuthProvider(spyProvider)

      //poke a cluster into the database so we actually have something to look for
      dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) }

      // status should work for this user
      val clusterStatus = leo.getActiveClusterDetails(userInfo, project, name1).futureValue
      clusterStatus shouldBe c1

      // list should work for this user
      //list all clusters should be fine, but empty
      val clusterList = leo.listClusters(userInfo, Map()).futureValue
      clusterList shouldBe Seq(c1)

      //connect should 401
      val httpRequest = HttpRequest(GET, Uri(s"/notebooks/$googleProject/$clusterName"))
      val clusterAuthException = proxy.proxyNotebook(userInfo, GoogleProject(googleProject), ClusterName(clusterName), httpRequest).failed.futureValue
      clusterAuthException shouldBe a [AuthorizationError]

      //sync
      val syncRequest = HttpRequest(POST, Uri(s"/notebooks/$googleProject/$clusterName/api/localize"))
      val syncNotFoundException = proxy.proxyLocalize(userInfo, GoogleProject(googleProject), ClusterName(clusterName), syncRequest).failed.futureValue
      syncNotFoundException shouldBe a [AuthorizationError]

      //destroy should 401 too
      val clusterDestroyException = leo.deleteCluster(userInfo, project, name1).failed.futureValue
      clusterDestroyException shouldBe a [AuthorizationError]

      //verify we never notified the auth provider of clusters happening because they didn't
      verify(spyProvider, Mockito.never).notifyClusterCreated(userEmail, project, name1)
      verify(spyProvider, Mockito.never).notifyClusterDeleted(userEmail, project, name1)
    }

    "should not create a cluster if auth provider notifyClusterCreated returns failure" in isolatedDbTest {
      val badNotifyProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider, notifySucceeds = false)
      val spyProvider = spy(badNotifyProvider)
      val leo = leoWithAuthProvider(spyProvider)

      val clusterCreateExc = leo.createCluster(userInfo, project, name1, testClusterRequest).failed.futureValue
      clusterCreateExc shouldBe a [RuntimeException]

      // no cluster should have been made
      val clusterLookup = dbFutureValue { _.clusterQuery.getClusterByName(project, name1) }
      clusterLookup shouldBe 'empty

      // check that the cluster does not exist
      gdDAO.clusters should not contain key (name1)

      verify(spyProvider).notifyClusterCreated(userEmail, project, name1)
      verify(spyProvider).notifyClusterDeleted(userEmail, project, name1)
    }

    "should use optimized canSeeAllClustersInProject in listClusters where appropriate" in isolatedDbTest {
      val optimizedListClustersProvider = new MockLeoAuthProvider(config.getConfig("auth.optimizedListClustersConfig"), serviceAccountProvider)
      val spyProvider = spy(optimizedListClustersProvider)
      val leo = leoWithAuthProvider(spyProvider)

      //poke two clusters in the db: one in an "everything is visible in this project" project and a normal one
      val visibleClusterName = ClusterName("visible-cluster")
      val visibleClusterProject = GoogleProject("visible-project")
      val visibleCluster = c1.copy(
        clusterName = visibleClusterName,
        googleId = UUID.randomUUID(),
        googleProject = visibleClusterProject,
        clusterUrl = Cluster.getClusterUrl(visibleClusterProject, visibleClusterName))
      dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) }
      dbFutureValue { _.clusterQuery.save(visibleCluster, gcsPath("gs://bucket1"), None) }

      val clusterList = leo.listClusters(userInfo, Map.empty).futureValue
      clusterList should contain theSameElementsAs Seq(c1, visibleCluster)

      //verify we optimized the sequence of calls to auth provider

      //unoptimized version: try the project first, fail, fall back to the cluster
      verify(spyProvider).canSeeAllClustersInProject(userInfo.userEmail, project)
      verify(spyProvider).hasNotebookClusterPermission(userInfo.userEmail, GetClusterStatus, project, name1)

      //optimized version: try the project first, succeed, never fall back to the cluster
      verify(spyProvider).canSeeAllClustersInProject(userInfo.userEmail, visibleClusterProject)
      verify(spyProvider, Mockito.never).hasNotebookClusterPermission(userInfo.userEmail, GetClusterStatus, visibleClusterProject, visibleClusterName)
    }


  }
}
