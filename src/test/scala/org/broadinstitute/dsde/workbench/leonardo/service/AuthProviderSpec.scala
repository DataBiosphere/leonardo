package org.broadinstitute.dsde.workbench.leonardo.service

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest

import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleIamDAO, MockGoogleStorageDAO}

import org.broadinstitute.dsde.workbench.leonardo.auth.MockLeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.NoopActor
import org.broadinstitute.dsde.workbench.leonardo.util.BucketHelper
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.{clusterEq, clusterSetEq}
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers, OptionValues}

class AuthProviderSpec extends FreeSpec with ScalatestRouteTest with Matchers with MockitoSugar with TestComponent with ScalaFutures with OptionValues with GcsPathUtils with TestProxy with BeforeAndAfterAll with CommonTestData{
  val clusterName = name1.value
  val googleProject = project.value

  val c1 = Cluster(
    clusterName = name1,
    googleId = Option(UUID.randomUUID()),
    googleProject = project,
    serviceAccountInfo = serviceAccountInfo,
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
    operationName = Option(OperationName("op1")),
    status = ClusterStatus.Unknown,
    hostIp = Some(IP("numbers.and.dots")),
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = None,
    jupyterUserScriptUri = None,
    stagingBucket = Some(GcsBucketName("testStagingBucket1")),
    errors = List.empty,
    instances = Set.empty,
    userJupyterExtensionConfig = None,
    dateAccessed = Instant.now(),
    autopauseThreshold = 0)

  val routeTest = this

  private val alwaysYesProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider)
  private val alwaysNoProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysNoProviderConfig"), serviceAccountProvider)

  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockGoogleStorageDAO = new MockGoogleStorageDAO
  val bucketHelper = new BucketHelper(dataprocConfig, mockGoogleDataprocDAO, mockGoogleComputeDAO, mockGoogleStorageDAO, serviceAccountProvider)

  override def beforeAll(): Unit = {
    super.beforeAll()
    startProxyServer()
  }

  override def afterAll(): Unit = {
    shutdownProxyServer()
    super.afterAll()
  }

  def leoWithAuthProvider(authProvider: LeoAuthProvider): LeonardoService = {
    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }
    new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, mockGoogleDataprocDAO, mockGoogleComputeDAO , mockGoogleIamDAO, mockGoogleStorageDAO ,mockPetGoogleStorageDAO, DbSingleton.ref, system.actorOf(NoopActor.props), authProvider, serviceAccountProvider, whitelist, bucketHelper, contentSecurityPolicy)
  }

  def proxyWithAuthProvider(authProvider: LeoAuthProvider): ProxyService = {
    new MockProxyService(proxyConfig, mockGoogleDataprocDAO, DbSingleton.ref, authProvider)
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

      // notification of deletion happens only after it has been fully deleted
      verify(spyProvider, never).notifyClusterDeleted(userEmail, userEmail, project, name1)
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
      dbFutureValue { _.clusterQuery.save(c1, Option(gcsPath("gs://bucket1")), None) }

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
      verify(spyProvider, Mockito.never).notifyClusterDeleted(userEmail, userEmail, project, name1)
    }

    "should give you a 401 if you can see a cluster's details but can't do the more specific action" in isolatedDbTest {
      val readOnlyProvider = new MockLeoAuthProvider(config.getConfig("auth.readOnlyProviderConfig"), serviceAccountProvider)
      val spyProvider = spy(readOnlyProvider)
      val leo = leoWithAuthProvider(spyProvider)
      val proxy = proxyWithAuthProvider(spyProvider)

      //poke a cluster into the database so we actually have something to look for
      dbFutureValue { _.clusterQuery.save(c1, Option(gcsPath("gs://bucket1")), None) }

      // status should work for this user
      leo.getActiveClusterDetails(userInfo, project, name1).futureValue shouldEqual c1

      // list should work for this user
      //list all clusters should be fine, but empty
      leo.listClusters(userInfo, Map()).futureValue.toSet shouldEqual Set(c1)

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
      verify(spyProvider, Mockito.never).notifyClusterDeleted(userEmail, userEmail, project, name1)
    }

    "should not create a cluster if auth provider notifyClusterCreated returns failure" in isolatedDbTest {
      val badNotifyProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider, notifySucceeds = false)
      val spyProvider = spy(badNotifyProvider)
      val leo = leoWithAuthProvider(spyProvider)

      val clusterCreateExc = leo.createCluster(userInfo, project, name1, testClusterRequest).failed.futureValue
      clusterCreateExc shouldBe a [RuntimeException]

      // no cluster should have been made
      val clusterLookup = dbFutureValue { _.clusterQuery.getActiveClusterByName(project, name1) }
      clusterLookup shouldBe 'empty

      // check that the cluster does not exist
      mockGoogleDataprocDAO.clusters should not contain key (name1)

      // creation and deletion notifications should have been fired
      verify(spyProvider).notifyClusterCreated(userEmail, project, name1)
      verify(spyProvider).notifyClusterDeleted(userEmail, userEmail, project, name1)
    }
  }
}
