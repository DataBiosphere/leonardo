package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDirectoryDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleProjectDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, ClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper, QueueFactory}
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import CommonTestData._
import org.broadinstitute.dsde.workbench.newrelic.mock.FakeNewRelicMetricsInterpreter

class ClusterMonitorSupervisorSpec
    extends TestKit(ActorSystem("leonardotest"))
    with FlatSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll
    with TestComponent
    with GcsPathUtils { testKit =>

  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO()
  implicit val nr = FakeNewRelicMetricsInterpreter

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ClusterMonitorSupervisor" should "auto freeze the cluster" in isolatedDbTest {
    val runningCluster = makeCluster(1)
      .copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(45, ChronoUnit.SECONDS)),
            autopauseThreshold = 1)
      .save()

    val gdDAO = mock[GoogleDataprocDAO]

    val computeDAO = mock[GoogleComputeDAO]

    val storageDAO = mock[GoogleStorageDAO]

    val iamDAO = mock[GoogleIamDAO]

    val projectDAO = mock[GoogleProjectDAO]

    val authProvider = mock[LeoAuthProvider[IO]]

    val bucketHelper = new BucketHelper(computeDAO, storageDAO, FakeGoogleStorageService, serviceAccountProvider)

    val clusterHelper = new ClusterHelper(dataprocConfig,
                                          imageConfig,
                                          googleGroupsConfig,
                                          proxyConfig,
                                          clusterResourcesConfig,
                                          clusterFilesConfig,
                                          monitorConfig,
                                          welderConfig,
                                          bucketHelper,
                                          gdDAO,
                                          computeDAO,
                                          mockGoogleDirectoryDAO,
                                          iamDAO,
                                          projectDAO,
                                          MockWelderDAO,
                                          blocker)

    implicit def clusterToolToToolDao = ToolDAO.clusterToolToToolDao(MockJupyterDAO, MockWelderDAO, MockRStudioDAO)
    system.actorOf(
      ClusterMonitorSupervisor.props(
        monitorConfig,
        dataprocConfig,
        imageConfig,
        clusterBucketConfig,
        gdDAO,
        computeDAO,
        storageDAO,
        FakeGoogleStorageService,
        authProvider,
        autoFreezeConfig,
        MockJupyterDAO,
        MockRStudioDAO,
        MockWelderDAO,
        clusterHelper,
        QueueFactory.makePublisherQueue()
      )
    )

    eventually(timeout(Span(30, Seconds))) {
      val c1 = dbFutureValue { clusterQuery.getClusterById(runningCluster.id) }
      c1.map(_.status) shouldBe Some(ClusterStatus.Stopping)
    }
  }

  it should "not auto freeze the cluster if jupyter kernel is still running" in isolatedDbTest {
    val runningCluster = makeCluster(2)
      .copy(status = ClusterStatus.Running,
            auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(45, ChronoUnit.SECONDS)),
            autopauseThreshold = 1)
      .save()

    val gdDAO = mock[GoogleDataprocDAO]

    val computeDAO = mock[GoogleComputeDAO]

    val storageDAO = mock[GoogleStorageDAO]

    val iamDAO = mock[GoogleIamDAO]

    val projectDAO = mock[GoogleProjectDAO]

    val authProvider = mock[LeoAuthProvider[IO]]

    val jupyterProxyDAO = new JupyterDAO[IO] {
      override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): IO[Boolean] =
        IO.pure(true)
      override def isAllKernelsIdle(googleProject: GoogleProject, clusterName: ClusterName): IO[Boolean] =
        IO.pure(false)
    }

    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }

    val bucketHelper = new BucketHelper(computeDAO, storageDAO, FakeGoogleStorageService, serviceAccountProvider)

    val clusterHelper = new ClusterHelper(dataprocConfig,
                                          imageConfig,
                                          googleGroupsConfig,
                                          proxyConfig,
                                          clusterResourcesConfig,
                                          clusterFilesConfig,
                                          monitorConfig,
                                          welderConfig,
                                          bucketHelper,
                                          gdDAO,
                                          computeDAO,
                                          mockGoogleDirectoryDAO,
                                          iamDAO,
                                          projectDAO,
                                          MockWelderDAO,
                                          blocker)

    implicit def clusterToolToToolDao = ToolDAO.clusterToolToToolDao(jupyterProxyDAO, MockWelderDAO, MockRStudioDAO)
    val clusterSupervisorActor = system.actorOf(
      ClusterMonitorSupervisor.props(
        monitorConfig,
        dataprocConfig,
        imageConfig,
        clusterBucketConfig,
        gdDAO,
        computeDAO,
        storageDAO,
        FakeGoogleStorageService,
        authProvider,
        autoFreezeConfig,
        jupyterProxyDAO,
        MockRStudioDAO,
        MockWelderDAO,
        clusterHelper,
        QueueFactory.makePublisherQueue()
      )
    )

    eventually(timeout(Span(30, Seconds))) {
      val c1 = dbFutureValue { clusterQuery.getClusterById(runningCluster.id) }
      c1.map(_.status) shouldBe (Some(ClusterStatus.Running))
    }
  }

  it should "auto freeze the cluster if we fail to get jupyter kernel status" in isolatedDbTest {
    val runningCluster = makeCluster(2)
      .copy(status = ClusterStatus.Running,
            auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(45, ChronoUnit.SECONDS)),
            autopauseThreshold = 1)
      .save()

    val gdDAO = mock[GoogleDataprocDAO]
    val computeDAO = mock[GoogleComputeDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val iamDAO = mock[GoogleIamDAO]
    val projectDAO = mock[GoogleProjectDAO]
    val authProvider = mock[LeoAuthProvider[IO]]

    val jupyterProxyDAO = new JupyterDAO[IO] {
      override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): IO[Boolean] =
        IO.pure(true)
      override def isAllKernelsIdle(googleProject: GoogleProject, clusterName: ClusterName): IO[Boolean] =
        IO.raiseError(new Exception)
    }

    val bucketHelper = new BucketHelper(computeDAO, storageDAO, FakeGoogleStorageService, serviceAccountProvider)

    val clusterHelper = new ClusterHelper(dataprocConfig,
                                          imageConfig,
                                          googleGroupsConfig,
                                          proxyConfig,
                                          clusterResourcesConfig,
                                          clusterFilesConfig,
                                          monitorConfig,
                                          welderConfig,
                                          bucketHelper,
                                          gdDAO,
                                          computeDAO,
                                          mockGoogleDirectoryDAO,
                                          iamDAO,
                                          projectDAO,
                                          MockWelderDAO,
                                          blocker)

    implicit def clusterToolToToolDao = ToolDAO.clusterToolToToolDao(jupyterProxyDAO, MockWelderDAO, MockRStudioDAO)
    val clusterSupervisorActor = system.actorOf(
      ClusterMonitorSupervisor.props(
        monitorConfig,
        dataprocConfig,
        imageConfig,
        clusterBucketConfig,
        gdDAO,
        computeDAO,
        storageDAO,
        FakeGoogleStorageService,
        authProvider,
        autoFreezeConfig,
        jupyterProxyDAO,
        MockRStudioDAO,
        MockWelderDAO,
        clusterHelper,
        QueueFactory.makePublisherQueue()
      )
    )

    eventually(timeout(Span(30, Seconds))) {
      val c1 = dbFutureValue { clusterQuery.getClusterById(runningCluster.id) }
      c1.map(_.status) shouldBe (Some(ClusterStatus.Stopping))
    }
  }

  it should "auto freeze the cluster if the max kernel busy time is exceeded" in isolatedDbTest {
    val runningCluster = makeCluster(2)
      .copy(
        status = ClusterStatus.Running,
        auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(25, ChronoUnit.HOURS),
                                   kernelFoundBusyDate = Some(Instant.now().minus(25, ChronoUnit.HOURS)))
      )
      .save()

    val gdDAO = mock[GoogleDataprocDAO]
    val computeDAO = mock[GoogleComputeDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val iamDAO = mock[GoogleIamDAO]
    val projectDAO = mock[GoogleProjectDAO]
    val authProvider = mock[LeoAuthProvider[IO]]

    val jupyterProxyDAO = new JupyterDAO[IO] {
      override def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): IO[Boolean] =
        IO.pure(true)
      override def isAllKernelsIdle(googleProject: GoogleProject, clusterName: ClusterName): IO[Boolean] =
        IO.pure(false)
    }

    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }

    val bucketHelper = new BucketHelper(computeDAO, storageDAO, FakeGoogleStorageService, serviceAccountProvider)

    val clusterHelper = new ClusterHelper(dataprocConfig,
                                          imageConfig,
                                          googleGroupsConfig,
                                          proxyConfig,
                                          clusterResourcesConfig,
                                          clusterFilesConfig,
                                          monitorConfig,
                                          welderConfig,
                                          bucketHelper,
                                          gdDAO,
                                          computeDAO,
                                          mockGoogleDirectoryDAO,
                                          iamDAO,
                                          projectDAO,
                                          MockWelderDAO,
                                          blocker)

    implicit def clusterToolToToolDao = ToolDAO.clusterToolToToolDao(jupyterProxyDAO, MockWelderDAO, MockRStudioDAO)

    system.actorOf(
      ClusterMonitorSupervisor.props(
        monitorConfig,
        dataprocConfig,
        imageConfig,
        clusterBucketConfig,
        gdDAO,
        computeDAO,
        storageDAO,
        FakeGoogleStorageService,
        authProvider,
        autoFreezeConfig,
        jupyterProxyDAO,
        MockRStudioDAO,
        MockWelderDAO,
        clusterHelper,
        QueueFactory.makePublisherQueue()
      )
    )

    eventually(timeout(Span(30, Seconds))) {
      val c1 = dbFutureValue { clusterQuery.getClusterById(runningCluster.id) }
      c1.map(_.status) shouldBe (Some(ClusterStatus.Stopping))
    }
  }
}
