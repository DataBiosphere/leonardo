package org.broadinstitute.dsde.workbench.leonardo
package service

import java.util.UUID

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.effect.IO
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleIamDAO, MockGoogleProjectDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.{clusterEq, clusterSetEq, stripFieldsForListCluster}
import org.broadinstitute.dsde.workbench.leonardo.auth.MockLeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.dao.MockWelderDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, _}
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers, OptionValues}

import scala.concurrent.{ExecutionContext, Future}

class AuthProviderSpec extends FreeSpec with ScalatestRouteTest with Matchers with MockitoSugar with TestComponent with ScalaFutures with OptionValues with GcsPathUtils with TestProxy with BeforeAndAfterAll with CommonTestData{

  val cluster1 = makeCluster(1)
  val cluster1Name = cluster1.clusterName

  val clusterName = cluster1Name.value
  val googleProject = project.value

  val routeTest = this

  implicit val cs = IO.contextShift(executor)
  implicit val timer = IO.timer(executor)

  private val alwaysYesProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider)
  private val alwaysNoProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysNoProviderConfig"), serviceAccountProvider)
  private val noVisibleClustersProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider) {
    override def filterUserVisibleClusters(userInfo: UserInfo, clusters: List[(GoogleProject, ClusterName)])(implicit executionContext: ExecutionContext): Future[List[(GoogleProject, ClusterName)]] = {
      Future.successful(List.empty)
    }
  }

  val mockGoogleIamDAO = new LeoMockGoogleIamDAO
  val mockGoogleStorageDAO = new MockGoogleStorageDAO
  val mockGoogleProjectDAO = new MockGoogleProjectDAO
  val bucketHelper = new BucketHelper(dataprocConfig, mockGoogleDataprocDAO, mockGoogleComputeDAO, mockGoogleStorageDAO, serviceAccountProvider)
  val clusterHelper = new ClusterHelper(DbSingleton.ref, dataprocConfig, mockGoogleDataprocDAO, mockGoogleComputeDAO, mockGoogleIamDAO)
  val clusterDnsCache =  new ClusterDnsCache(proxyConfig, DbSingleton.ref, dnsCacheConfig)
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
    new LeonardoService(dataprocConfig, MockWelderDAO, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, mockGoogleDataprocDAO, mockGoogleComputeDAO, mockGoogleProjectDAO, mockGoogleStorageDAO ,mockPetGoogleStorageDAO, DbSingleton.ref, authProvider, serviceAccountProvider, bucketHelper, clusterHelper, contentSecurityPolicy)
  }

  def proxyWithAuthProvider(authProvider: LeoAuthProvider): ProxyService = {
    new MockProxyService(proxyConfig, mockGoogleDataprocDAO, DbSingleton.ref, authProvider, clusterDnsCache)
  }

  "Leo with an AuthProvider" - {
    "should let you do things if the auth provider says yes" in isolatedDbTest {
      val spyProvider = spy(alwaysYesProvider)
      val leo = leoWithAuthProvider(spyProvider)
      val proxy = proxyWithAuthProvider(spyProvider)

      // create
      val cluster1 = leo.createCluster(userInfo, project, cluster1Name, testClusterRequest).futureValue

      // get status
      val clusterStatus = leo.getActiveClusterDetails(userInfo, project, cluster1Name).futureValue

      cluster1 shouldEqual clusterStatus

      // list
      val listResponse = leo.listClusters(userInfo, Map()).futureValue
      listResponse shouldBe Seq(cluster1).map(stripFieldsForListCluster)

      //connect
      val proxyRequest = HttpRequest(GET, Uri(s"/notebooks/$googleProject/$clusterName"))
      proxy.proxyRequest(userInfo, GoogleProject(googleProject), ClusterName(clusterName), proxyRequest).futureValue

      //sync
      val syncRequest = HttpRequest(POST, Uri(s"/notebooks/$googleProject/$clusterName/api/localize"))
      proxy.proxyLocalize(userInfo, GoogleProject(googleProject), ClusterName(clusterName), syncRequest).futureValue

      // change cluster status to Running so that it can be deleted
      dbFutureValue { _.clusterQuery.setToRunning(cluster1.id, IP("numbers.and.dots")) }

      //delete
      leo.deleteCluster(userInfo, project, cluster1Name).futureValue

      //verify we correctly notified the auth provider
      verify(spyProvider).notifyClusterCreated(any[ClusterInternalId], any[WorkbenchEmail], any[GoogleProject], any[ClusterName], TraceId(any[UUID]))(any[ExecutionContext])

      // notification of deletion happens only after it has been fully deleted
      verify(spyProvider, never).notifyClusterDeleted(any[ClusterInternalId], any[WorkbenchEmail], any[WorkbenchEmail], any[GoogleProject], any[ClusterName])(any[ExecutionContext])
    }

    "should not let you do things if the auth provider says no" in isolatedDbTest {
      val spyProvider = spy(alwaysNoProvider)
      val leo = leoWithAuthProvider(spyProvider)
      val proxy = proxyWithAuthProvider(spyProvider)

      //can't make a cluster
      val clusterCreateException = leo.createCluster(userInfo, project, cluster1Name, testClusterRequest).failed.futureValue
      clusterCreateException shouldBe a [AuthorizationError]
      clusterCreateException.asInstanceOf[AuthorizationError].statusCode shouldBe StatusCodes.Forbidden

      //can't get details on an existing cluster
      //poke a cluster into the database so we actually have something to look for
      cluster1.save(None)

      val clusterGetResponseException = leo.getActiveClusterDetails(userInfo, cluster1.googleProject, cluster1Name).failed.futureValue
      clusterGetResponseException shouldBe a [ClusterNotFoundException]
      clusterGetResponseException.asInstanceOf[ClusterNotFoundException].statusCode shouldBe StatusCodes.NotFound

      //connect to cluster
      val httpRequest = HttpRequest(GET, Uri(s"/notebooks/$googleProject/$clusterName"))
      val clusterNotFoundException = proxy.proxyRequest(userInfo, GoogleProject(googleProject), ClusterName(clusterName), httpRequest).failed.futureValue
      clusterNotFoundException shouldBe a [ClusterNotFoundException]

      //sync
      val syncRequest = HttpRequest(POST, Uri(s"/notebooks/$googleProject/$clusterName/api/localize"))
      val syncNotFoundException = proxy.proxyLocalize(userInfo, GoogleProject(googleProject), ClusterName(clusterName), syncRequest).failed.futureValue
      syncNotFoundException shouldBe a [ClusterNotFoundException]

      //destroy cluster
      val clusterNotFoundAgain = leo.deleteCluster(userInfo, project, cluster1Name).failed.futureValue
      clusterNotFoundAgain shouldBe a [ClusterNotFoundException]

      //verify we never notified the auth provider of clusters happening because they didn't
      verify(spyProvider, Mockito.never).notifyClusterCreated(any[ClusterInternalId], any[WorkbenchEmail], any[GoogleProject], any[ClusterName], TraceId(any[UUID]))(any[ExecutionContext])
      verify(spyProvider, Mockito.never).notifyClusterDeleted(any[ClusterInternalId], any[WorkbenchEmail], any[WorkbenchEmail], any[GoogleProject], any[ClusterName])(any[ExecutionContext])
    }

    "should give you a 401 if you can see a cluster's details but can't do the more specific action" in isolatedDbTest {
      val readOnlyProvider = new MockLeoAuthProvider(config.getConfig("auth.readOnlyProviderConfig"), serviceAccountProvider)
      val spyProvider = spy(readOnlyProvider)
      val leo = leoWithAuthProvider(spyProvider)
      val proxy = proxyWithAuthProvider(spyProvider)

      //poke a cluster into the database so we actually have something to look for
      cluster1.save(None)
      // status should work for this user
      leo.getActiveClusterDetails(userInfo, project, cluster1.clusterName).futureValue shouldEqual cluster1

      // list should work for this user
      //list all clusters should be fine, but empty
      leo.listClusters(userInfo, Map()).futureValue.toSet shouldEqual Set(cluster1).map(stripFieldsForListCluster)

      //connect should 401
      val httpRequest = HttpRequest(GET, Uri(s"/notebooks/$googleProject/$clusterName"))
      val clusterAuthException = proxy.proxyRequest(userInfo, GoogleProject(googleProject), ClusterName(clusterName), httpRequest).failed.futureValue
      clusterAuthException shouldBe a [AuthorizationError]

      //sync
      val syncRequest = HttpRequest(POST, Uri(s"/notebooks/$googleProject/$clusterName/api/localize"))
      val syncNotFoundException = proxy.proxyLocalize(userInfo, GoogleProject(googleProject), ClusterName(clusterName), syncRequest).failed.futureValue
      syncNotFoundException shouldBe a [AuthorizationError]

      //destroy should 401 too
      val clusterDestroyException = leo.deleteCluster(userInfo, project, cluster1Name).failed.futureValue
      clusterDestroyException shouldBe a [AuthorizationError]

      //verify we never notified the auth provider of clusters happening because they didn't
      verify(spyProvider, Mockito.never).notifyClusterCreated(any[ClusterInternalId], any[WorkbenchEmail], any[GoogleProject], any[ClusterName], TraceId(any[UUID]))(any[ExecutionContext])
      verify(spyProvider, Mockito.never).notifyClusterDeleted(any[ClusterInternalId], any[WorkbenchEmail], any[WorkbenchEmail], any[GoogleProject], any[ClusterName])(any[ExecutionContext])
    }

    "should not create a cluster if auth provider notifyClusterCreated returns failure" in isolatedDbTest {
      val badNotifyProvider = new MockLeoAuthProvider(config.getConfig("auth.alwaysYesProviderConfig"), serviceAccountProvider, notifySucceeds = false)
      val spyProvider = spy(badNotifyProvider)
      val leo = leoWithAuthProvider(spyProvider)

      val clusterCreateExc = leo.createCluster(userInfo, project, cluster1Name, testClusterRequest).failed.futureValue
      clusterCreateExc shouldBe a [RuntimeException]

      // no cluster should have been made
      val clusterLookup = dbFutureValue { _.clusterQuery.getActiveClusterByName(project, cluster1Name) }
      clusterLookup shouldBe 'empty

      // check that the cluster does not exist
      mockGoogleDataprocDAO.clusters should not contain key (cluster1Name)

      // creation and deletion notifications should have been fired
      verify(spyProvider).notifyClusterCreated(any[ClusterInternalId], any[WorkbenchEmail], any[GoogleProject], any[ClusterName], TraceId(any[UUID]))(any[ExecutionContext])
      verify(spyProvider).notifyClusterDeleted(any[ClusterInternalId], any[WorkbenchEmail], any[WorkbenchEmail], any[GoogleProject], any[ClusterName])(any[ExecutionContext])
    }

    "should return clusters the user created even if the auth provider doesn't" in isolatedDbTest {
      val leo = leoWithAuthProvider(noVisibleClustersProvider)

      // create
      val cluster1 = leo.createCluster(userInfo, project, cluster1Name, testClusterRequest).futureValue

      // list
      val listResponse = leo.listClusters(userInfo, Map()).futureValue
      listResponse shouldBe Seq(cluster1).map(stripFieldsForListCluster)
    }
  }
}
