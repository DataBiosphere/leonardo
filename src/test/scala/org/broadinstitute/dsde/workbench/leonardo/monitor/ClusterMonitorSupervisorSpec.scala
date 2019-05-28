package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockJupyterDAO, MockRStudioDAO, ToolDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, ClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService
import org.broadinstitute.dsde.workbench.leonardo.util.BucketHelper
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Future

class ClusterMonitorSupervisorSpec extends TestKit(ActorSystem("leonardotest"))
  with FlatSpecLike with Matchers with MockitoSugar with BeforeAndAfterAll
  with TestComponent with CommonTestData with GcsPathUtils { testKit =>

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ClusterMonitorSupervisor" should "auto freeze the cluster" in isolatedDbTest {
    val runningCluster = makeCluster(1).copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(45, ChronoUnit.SECONDS)),
      autopauseThreshold = 1).save()

    val gdDAO = mock[GoogleDataprocDAO]

    val computeDAO = mock[GoogleComputeDAO]

    val storageDAO = mock[GoogleStorageDAO]

    val iamDAO = mock[GoogleIamDAO]

    val authProvider = mock[LeoAuthProvider]

    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }

    val bucketHelper = new BucketHelper(dataprocConfig, gdDAO, computeDAO, storageDAO, serviceAccountProvider)

    val leoService = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig,
      clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, gdDAO, computeDAO, iamDAO,
      storageDAO, mockPetGoogleStorageDAO, DbSingleton.ref, whitelistAuthProvider, serviceAccountProvider, whitelist,
      bucketHelper, contentSecurityPolicy)

    val clusterSupervisorActor = system.actorOf(ClusterMonitorSupervisor.props(monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO,
      computeDAO, iamDAO, storageDAO, DbSingleton.ref, authProvider, autoFreezeConfig, MockJupyterDAO, MockRStudioDAO, leoService))

    eventually(timeout(Span(30, Seconds))) {
      val c1 = dbFutureValue { _.clusterQuery.getClusterById(runningCluster.id) }
      c1.map(_.status) shouldBe Some(ClusterStatus.Stopping)
    }
  }

  it should "not auto freeze the cluster if jupyter kernel is still running" in isolatedDbTest {
    val runningCluster = makeCluster(2).copy(status = ClusterStatus.Running, auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(45, ChronoUnit.SECONDS)),
      autopauseThreshold = 1).save()

    val clusterRes = dbFutureValue { _.clusterQuery.getClusterById(runningCluster.id) }

    val gdDAO = mock[GoogleDataprocDAO]

    val computeDAO = mock[GoogleComputeDAO]

    val storageDAO = mock[GoogleStorageDAO]

    val iamDAO = mock[GoogleIamDAO]

    val authProvider = mock[LeoAuthProvider]

    val jupyterProxyDAO = new ToolDAO{
      override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.successful(true)
      override def isAllKernalsIdle(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.successful(false)
    }

    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }

    val bucketHelper = new BucketHelper(dataprocConfig, gdDAO, computeDAO, storageDAO, serviceAccountProvider)

    val leoService = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig,
      clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, gdDAO, computeDAO, iamDAO,
      storageDAO, mockPetGoogleStorageDAO, DbSingleton.ref, whitelistAuthProvider, serviceAccountProvider, whitelist,
      bucketHelper, contentSecurityPolicy)

    val clusterSupervisorActor = system.actorOf(ClusterMonitorSupervisor.props(monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO,
      computeDAO, iamDAO, storageDAO, DbSingleton.ref, authProvider, autoFreezeConfig, jupyterProxyDAO, MockRStudioDAO, leoService))

    eventually(timeout(Span(30, Seconds))) {
      val c1 = dbFutureValue { _.clusterQuery.getClusterById(runningCluster.id) }
      c1.map(_.status) shouldBe (Some(ClusterStatus.Running))
    }
  }

  it should "auto freeze the cluster if we fail to get jupyter kernel status" in isolatedDbTest {
    val runningCluster = makeCluster(2).copy(status = ClusterStatus.Running, auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(45, ChronoUnit.SECONDS)),
      autopauseThreshold = 1).save()

    val clusterRes = dbFutureValue { _.clusterQuery.getClusterById(runningCluster.id) }
    val gdDAO = mock[GoogleDataprocDAO]
    val computeDAO = mock[GoogleComputeDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val iamDAO = mock[GoogleIamDAO]
    val authProvider = mock[LeoAuthProvider]

    val jupyterProxyDAO = new ToolDAO{
      override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.successful(true)
      override def isAllKernalsIdle(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.failed(new Exception)
    }

    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }

    val bucketHelper = new BucketHelper(dataprocConfig, gdDAO, computeDAO, storageDAO, serviceAccountProvider)

    val leoService = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig,
      clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, gdDAO, computeDAO, iamDAO,
      storageDAO, mockPetGoogleStorageDAO, DbSingleton.ref, whitelistAuthProvider, serviceAccountProvider, whitelist,
      bucketHelper, contentSecurityPolicy)

    val clusterSupervisorActor = system.actorOf(ClusterMonitorSupervisor.props(monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO,
      computeDAO, iamDAO, storageDAO, DbSingleton.ref, authProvider, autoFreezeConfig, jupyterProxyDAO, MockRStudioDAO, leoService))

    eventually(timeout(Span(30, Seconds))) {
      val c1 = dbFutureValue { _.clusterQuery.getClusterById(runningCluster.id) }
      c1.map(_.status) shouldBe (Some(ClusterStatus.Stopping))
    }
  }

  it should "auto freeze the cluster if the max kernel busy time is exceeded" in isolatedDbTest {
    val runningCluster = makeCluster(2).copy(status = ClusterStatus.Running, auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(25, ChronoUnit.HOURS)), kernelFoundBusyDate = Some(Instant.now().minus(25, ChronoUnit.HOURS))).save()

    val clusterRes = dbFutureValue { _.clusterQuery.getClusterById(runningCluster.id) }
    val gdDAO = mock[GoogleDataprocDAO]
    val computeDAO = mock[GoogleComputeDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val iamDAO = mock[GoogleIamDAO]
    val authProvider = mock[LeoAuthProvider]

    val jupyterProxyDAO = new ToolDAO{
      override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.successful(true)
      override def isAllKernalsIdle(googleProject: GoogleProject, clusterName: ClusterName): Future[Boolean] = Future.successful(false)
    }

    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }

    val bucketHelper = new BucketHelper(dataprocConfig, gdDAO, computeDAO, storageDAO, serviceAccountProvider)

    val leoService = new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig,
      clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, gdDAO, computeDAO, iamDAO,
      storageDAO, mockPetGoogleStorageDAO, DbSingleton.ref, whitelistAuthProvider, serviceAccountProvider, whitelist,
      bucketHelper, contentSecurityPolicy)

    val clusterSupervisorActor = system.actorOf(ClusterMonitorSupervisor.props(monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO,
      computeDAO, iamDAO, storageDAO, DbSingleton.ref, authProvider, autoFreezeConfig, jupyterProxyDAO, MockRStudioDAO, leoService))

    eventually(timeout(Span(30, Seconds))) {
      val c1 = dbFutureValue { _.clusterQuery.getClusterById(runningCluster.id) }
      c1.map(_.status) shouldBe (Some(ClusterStatus.Stopping))
    }
  }

}
