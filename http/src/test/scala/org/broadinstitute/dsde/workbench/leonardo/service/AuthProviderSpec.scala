package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.time.Instant

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock.{
  MockGoogleDirectoryDAO,
  MockGoogleIamDAO,
  MockGoogleProjectDAO,
  MockGoogleStorageDAO
}
import org.broadinstitute.dsde.workbench.google2.FirewallRuleName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.auth.MockLeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeService
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockDockerDAO, MockWelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.FakeGoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.{GcsObjectName, GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.newrelic.mock.FakeNewRelicMetricsInterpreter
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers, OptionValues}

class AuthProviderSpec
    extends FreeSpec
    with ScalatestRouteTest
    with Matchers
    with TestComponent
    with ScalaFutures
    with OptionValues
    with GcsPathUtils
    with TestProxy
    with BeforeAndAfterAll {

  implicit val nr = FakeNewRelicMetricsInterpreter

  val cluster1 = makeCluster(1)
  val cluster1Name = cluster1.runtimeName

  val clusterName = cluster1Name.asString
  val googleProject = project.value

  def proxyConfig: ProxyConfig = CommonTestData.proxyConfig

  val routeTest = this

  private val alwaysYesProvider =
    new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider)
  private val alwaysNoProvider =
    new MockLeoAuthProvider(config.getConfig("auth.alwaysNoProviderConfig"), serviceAccountProvider)
  private val noVisibleClustersProvider =
    new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider) {
      override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, ClusterInternalId)])(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[List[(GoogleProject, ClusterInternalId)]] =
        IO.pure(List.empty)
    }

  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO()
  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockGoogleStorageDAO = new MockGoogleStorageDAO
  val mockGoogleProjectDAO = new MockGoogleProjectDAO
  val mockWelderDAO = new MockWelderDAO
  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig, clusterResourcesConfig)
  val bucketHelper =
    new BucketHelper(bucketHelperConfig,
                     MockGoogleComputeService,
                     mockGoogleStorageDAO,
                     FakeGoogleStorageService,
                     mockGoogleProjectDAO,
                     serviceAccountProvider,
                     blocker)(cs)
  val vpcHelperConfig =
    VPCHelperConfig("lbl1", "lbl2", FirewallRuleName("test-firewall-rule"), firewallRuleTargetTags = List.empty)
  val vpcHelper = new VPCHelper(vpcHelperConfig, mockGoogleProjectDAO, MockGoogleComputeService)
  val clusterHelper =
    new ClusterHelper(dataprocConfig,
                      imageConfig,
                      googleGroupsConfig,
                      proxyConfig,
                      clusterResourcesConfig,
                      clusterFilesConfig,
                      monitorConfig,
                      welderConfig,
                      bucketHelper,
                      vpcHelper,
                      mockGoogleDataprocDAO,
                      MockGoogleComputeService,
                      mockGoogleDirectoryDAO,
                      mockGoogleIamDAO,
                      mockGoogleProjectDAO,
                      mockWelderDAO,
                      blocker)
  val clusterDnsCache = new ClusterDnsCache(proxyConfig, DbSingleton.dbRef, dnsCacheConfig, blocker)

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Set up the mock directoryDAO to have the Google group used to grant permission to users to pull the custom dataproc image
    mockGoogleDirectoryDAO
      .createGroup(dataprocImageProjectGroupName,
                   dataprocImageProjectGroupEmail,
                   Option(mockGoogleDirectoryDAO.lockedDownGroupSettings))
      .futureValue
    startProxyServer()
  }

  override def afterAll(): Unit = {
    shutdownProxyServer()
    super.afterAll()
  }

  def leoWithAuthProvider(authProvider: LeoAuthProvider[IO]): LeonardoService = {
    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }
    new LeonardoService(dataprocConfig,
                        imageConfig,
                        MockWelderDAO,
                        proxyConfig,
                        swaggerConfig,
                        autoFreezeConfig,
                        welderConfig,
                        mockPetGoogleStorageDAO,
                        authProvider,
                        serviceAccountProvider,
                        bucketHelper,
                        clusterHelper,
                        new MockDockerDAO,
                        QueueFactory.makePublisherQueue())
  }

  def proxyWithAuthProvider(authProvider: LeoAuthProvider[IO]): ProxyService =
    new MockProxyService(proxyConfig, mockGoogleDataprocDAO, authProvider, clusterDnsCache)

  "Leo with an AuthProvider" - {
    "should let you do things if the auth provider says yes" in isolatedDbTest {
      val spyProvider = spy(alwaysYesProvider)
      val leo = leoWithAuthProvider(spyProvider)
      val proxy = proxyWithAuthProvider(spyProvider)

      // create
      val cluster1 = leo.createCluster(userInfo, project, cluster1Name, testClusterRequest).unsafeRunSync()
      // get status
      val clusterStatus = leo.getActiveClusterDetails(userInfo, project, cluster1Name).unsafeRunSync()

      cluster1.id shouldEqual clusterStatus.id

      // list
      val listResponse = leo.listClusters(userInfo, Map()).unsafeToFuture.futureValue
      listResponse shouldBe Seq(cluster1).map(x => LeoLenses.createRuntimeAPIRespToListRuntimeResp.get(x))

      //connect
      val proxyRequest = HttpRequest(GET, Uri(s"/notebooks/$googleProject/$clusterName"))
      proxy.proxyRequest(userInfo, GoogleProject(googleProject), RuntimeName(clusterName), proxyRequest).unsafeRunSync()

      //sync
      val syncRequest = HttpRequest(POST, Uri(s"/notebooks/$googleProject/$clusterName/api/localize"))
      proxy.proxyLocalize(userInfo, GoogleProject(googleProject), RuntimeName(clusterName), syncRequest).unsafeRunSync()

      val updateAsyncClusterCreationFields = UpdateAsyncClusterCreationFields(
        Some(GcsPath(initBucketPath, GcsObjectName(""))),
        Some(serviceAccountKey),
        cluster1.id,
        Some(makeDataprocInfo(1)),
        Instant.now
      )
      // change cluster status to Running so that it can be deleted
      dbFutureValue {
        clusterQuery.updateAsyncClusterCreationFields(updateAsyncClusterCreationFields)
      }
      dbFutureValue { clusterQuery.setToRunning(cluster1.id, IP("numbers.and.dots"), Instant.now) }

      //delete
      leo.deleteCluster(userInfo, project, cluster1Name).unsafeRunSync()

      //verify we correctly notified the auth provider
      verify(spyProvider).notifyClusterCreated(any[String].asInstanceOf[ClusterInternalId],
                                               any[WorkbenchEmail],
                                               any[GoogleProject],
                                               any[ClusterName])(any[ApplicativeAsk[IO, TraceId]])

      // notification of deletion happens only after it has been fully deleted
      verify(spyProvider, never).notifyClusterDeleted(any[String].asInstanceOf[ClusterInternalId],
                                                      any[WorkbenchEmail],
                                                      any[WorkbenchEmail],
                                                      any[GoogleProject],
                                                      any[ClusterName])(any[ApplicativeAsk[IO, TraceId]])
    }

    "should not let you do things if the auth provider says no" in {
      val spyProvider = spy(alwaysNoProvider)
      val leo = leoWithAuthProvider(spyProvider)
      val proxy = proxyWithAuthProvider(spyProvider)

      //can't make a cluster
      val clusterCreateException =
        leo.createCluster(userInfo, project, cluster1Name, testClusterRequest).unsafeToFuture.failed.futureValue
      clusterCreateException shouldBe a[AuthorizationError]
      clusterCreateException.asInstanceOf[AuthorizationError].statusCode shouldBe StatusCodes.Forbidden

      //can't get details on an existing cluster
      //poke a cluster into the database so we actually have something to look for
      cluster1.save(None)

      val clusterGetResponseException =
        leo.getActiveClusterDetails(userInfo, cluster1.googleProject, cluster1Name).unsafeToFuture.failed.futureValue
      clusterGetResponseException shouldBe a[RuntimeNotFoundException]
      clusterGetResponseException.asInstanceOf[RuntimeNotFoundException].statusCode shouldBe StatusCodes.NotFound

      //connect to cluster
      val httpRequest = HttpRequest(GET, Uri(s"/notebooks/$googleProject/$clusterName"))
      val clusterNotFoundException = proxy
        .proxyRequest(userInfo, GoogleProject(googleProject), RuntimeName(clusterName), httpRequest)
        .attempt
        .unsafeRunSync()
        .swap
        .toOption
        .get
      clusterNotFoundException shouldBe a[RuntimeNotFoundException]

      //sync
      val syncRequest = HttpRequest(POST, Uri(s"/notebooks/$googleProject/$clusterName/api/localize"))
      val syncNotFoundException = proxy
        .proxyLocalize(userInfo, GoogleProject(googleProject), RuntimeName(clusterName), syncRequest)
        .attempt
        .unsafeRunSync()
        .swap
        .toOption
        .get
      syncNotFoundException shouldBe a[RuntimeNotFoundException]

      //destroy cluster
      val clusterNotFoundAgain = leo.deleteCluster(userInfo, project, cluster1Name).unsafeToFuture.failed.futureValue
      clusterNotFoundAgain shouldBe a[RuntimeNotFoundException]

      //verify we never notified the auth provider of clusters happening because they didn't
      verify(spyProvider, Mockito.never).notifyClusterCreated(any[String].asInstanceOf[ClusterInternalId],
                                                              any[WorkbenchEmail],
                                                              any[GoogleProject],
                                                              any[ClusterName])(any[ApplicativeAsk[IO, TraceId]])
      verify(spyProvider, Mockito.never).notifyClusterDeleted(any[String].asInstanceOf[ClusterInternalId],
                                                              any[WorkbenchEmail],
                                                              any[WorkbenchEmail],
                                                              any[GoogleProject],
                                                              any[ClusterName])(any[ApplicativeAsk[IO, TraceId]])
    }

    "should give you a 401 if you can see a cluster's details but can't do the more specific action" in isolatedDbTest {
      val readOnlyProvider =
        new MockLeoAuthProvider(config.getConfig("auth.readOnlyProviderConfig"), serviceAccountProvider)
      val spyProvider = spy(readOnlyProvider)
      val leo = leoWithAuthProvider(spyProvider)
      val proxy = proxyWithAuthProvider(spyProvider)

      //poke a cluster into the database so we actually have something to look for
      val savedCluster = cluster1.save(None)
      // status should work for this user
      leo
        .getActiveClusterDetails(userInfo, project, cluster1.runtimeName)
        .unsafeToFuture
        .futureValue
        .internalId shouldEqual cluster1.internalId

      // list should work for this user
      //list all clusters should be fine, but empty
      leo.listClusters(userInfo, Map()).unsafeToFuture.futureValue.toSet shouldEqual Set(savedCluster).map(
        r =>
          ListRuntimeResponse
            .fromRuntime(r, dbFutureValue(RuntimeConfigQueries.getRuntimeConfig(savedCluster.runtimeConfigId)))
      )

      //connect should 401
      val httpRequest = HttpRequest(GET, Uri(s"/notebooks/$googleProject/$clusterName"))
      val clusterAuthException = proxy
        .proxyRequest(userInfo, GoogleProject(googleProject), RuntimeName(clusterName), httpRequest)
        .attempt
        .unsafeRunSync()
        .swap
        .toOption
        .get
      clusterAuthException shouldBe a[AuthorizationError]

      //sync
      val syncRequest = HttpRequest(POST, Uri(s"/notebooks/$googleProject/$clusterName/api/localize"))
      val syncNotFoundException = proxy
        .proxyLocalize(userInfo, GoogleProject(googleProject), RuntimeName(clusterName), syncRequest)
        .attempt
        .unsafeRunSync()
        .swap
        .toOption
        .get
      syncNotFoundException shouldBe a[AuthorizationError]

      //destroy should 401 too
      val clusterDestroyException = leo.deleteCluster(userInfo, project, cluster1Name).unsafeToFuture.failed.futureValue
      clusterDestroyException shouldBe a[AuthorizationError]

      //verify we never notified the auth provider of clusters happening because they didn't
      verify(spyProvider, Mockito.never).notifyClusterCreated(any[String].asInstanceOf[ClusterInternalId],
                                                              any[WorkbenchEmail],
                                                              any[GoogleProject],
                                                              any[ClusterName])(any[ApplicativeAsk[IO, TraceId]])
      verify(spyProvider, Mockito.never).notifyClusterDeleted(any[String].asInstanceOf[ClusterInternalId],
                                                              any[WorkbenchEmail],
                                                              any[WorkbenchEmail],
                                                              any[GoogleProject],
                                                              any[ClusterName])(any[ApplicativeAsk[IO, TraceId]])
    }

    "should not create a cluster if auth provider notifyClusterCreated returns failure" in isolatedDbTest {
      val badNotifyProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"),
                                                      serviceAccountProvider,
                                                      notifySucceeds = false)
      val spyProvider = spy(badNotifyProvider)
      val leo = leoWithAuthProvider(spyProvider)

      val clusterCreateExc =
        leo.createCluster(userInfo, project, cluster1Name, testClusterRequest).unsafeToFuture.failed.futureValue
      clusterCreateExc shouldBe a[RuntimeException]

      // no cluster should have been made
      val clusterLookup = dbFutureValue { clusterQuery.getActiveClusterByName(project, cluster1Name) }
      clusterLookup shouldBe 'empty

      // check that the cluster does not exist
      mockGoogleDataprocDAO.clusters should not contain key(cluster1Name)

      // creation notifications should have been fired
      verify(spyProvider).notifyClusterCreated(any[String].asInstanceOf[ClusterInternalId],
                                               any[WorkbenchEmail],
                                               any[GoogleProject],
                                               any[ClusterName])(any[ApplicativeAsk[IO, TraceId]])
    }

    "should return clusters the user created even if the auth provider doesn't" in isolatedDbTest {
      val leo = leoWithAuthProvider(noVisibleClustersProvider)

      // create
      val cluster1 = leo.createCluster(userInfo, project, cluster1Name, testClusterRequest).unsafeToFuture.futureValue

      // list
      val listResponse = leo.listClusters(userInfo, Map()).unsafeToFuture.futureValue
      listResponse shouldBe Seq(cluster1).map(LeoLenses.createRuntimeAPIRespToListRuntimeResp.get)
    }

    "should return clusters the user didn't create if the auth provider says yes" in isolatedDbTest {
      val leo = leoWithAuthProvider(alwaysYesProvider)

      // create
      val cluster1 = leo.createCluster(userInfo, project, cluster1Name, testClusterRequest).unsafeToFuture.futureValue

      val newEmail = WorkbenchEmail("new-user@example.com")
      val newUserInfo = UserInfo(OAuth2BearerToken("accessToken"), WorkbenchUserId("new-user"), newEmail, 0)

      // list
      val listResponse = leo.listClusters(newUserInfo, Map()).unsafeToFuture().futureValue
      listResponse shouldBe Seq(cluster1).map(LeoLenses.createRuntimeAPIRespToListRuntimeResp.get)
    }
  }
}
