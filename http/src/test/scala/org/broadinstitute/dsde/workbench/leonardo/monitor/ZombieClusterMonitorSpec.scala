package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{ActorRef, ActorSystem, Terminated}
import akka.testkit.TestKit
import com.google.api.client.googleapis.testing.json.GoogleJsonResponseExceptionFactoryTesting
import com.google.api.client.testing.json.MockJsonFactory
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleProjectDAO}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.{GcsPathUtils, RuntimeConfigId, RuntimeName, RuntimeStatus}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

class ZombieClusterMonitorSpec
    extends TestKit(ActorSystem("leonardotest"))
    with FlatSpecLike
    with BeforeAndAfterAll
    with TestComponent
    with GcsPathUtils { testKit =>

  val testCluster1 = makeCluster(1).copy(status = RuntimeStatus.Running)
  val testCluster2 = makeCluster(2).copy(status = RuntimeStatus.Running)
  val testCluster3 = makeCluster(3).copy(status = RuntimeStatus.Running)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ZombieClusterMonitor" should "detect zombie clusters when the project is inactive" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create 2 running clusters in the same project
    val savedTestCluster1 = testCluster1.save()
    savedTestCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster1

    val savedTestCluster2 = testCluster2.save()
    savedTestCluster2.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster2

    // stub GoogleProjectDAO to make the project inactive
    val googleProjectDAO = new MockGoogleProjectDAO {
      override def isProjectActive(projectName: String): Future[Boolean] = Future.successful(false)
    }

    // zombie actor should flag both clusters as inactive
    withZombieActor(googleProjectDAO = googleProjectDAO) { _ =>
      eventually(timeout(Span(10, Seconds))) {
        val c1 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster1.id) }.get
        val c2 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster2.id) }.get

        List(c1, c2).foreach { c =>
          c.status shouldBe RuntimeStatus.Deleted
          c.auditInfo.destroyedDate shouldBe 'defined
          c.errors.size shouldBe 1
          c.errors.head.errorCode shouldBe -1
          c.errors.head.errorMessage should include("An underlying resource was removed in Google")
        }
      }
    }
  }

  "ZombieClusterMonitor" should "detect zombie clusters when we get a 403 from google" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create a running cluster
    val savedTestCluster1 = testCluster1.save()
    savedTestCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster1

    // stub GoogleProjectDAO to make the project throw an error
    val googleProjectDAO = new MockGoogleProjectDAO {
      val jsonFactory = new MockJsonFactory
      val testException = GoogleJsonResponseExceptionFactoryTesting.newMock(jsonFactory, 403, "you shall not pass")

      override def isProjectActive(projectName: String): Future[Boolean] =
        Future.failed(testException)
    }

    // zombie actor should flag the cluster as inactive
    withZombieActor(googleProjectDAO = googleProjectDAO) { _ =>
      eventually(timeout(Span(10, Seconds))) {
        val c1 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster1.id) }.get

        c1.status shouldBe RuntimeStatus.Deleted
        c1.auditInfo.destroyedDate shouldBe 'defined
        c1.errors.size shouldBe 1
        c1.errors.head.errorCode shouldBe -1
        c1.errors.head.errorMessage should include("An underlying resource was removed in Google")
      }
    }
  }

  it should "detect zombie clusters when the project's billing is inactive" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create 2 running clusters in the same project
    val savedTestCluster1 = testCluster1.save()
    savedTestCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster1

    val savedTestCluster2 = testCluster2.save()
    savedTestCluster2.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster2

    // stub GoogleProjectDAO to make the project inactive
    val googleProjectDAO = new MockGoogleProjectDAO {
      override def isBillingActive(projectName: String): Future[Boolean] = Future.successful(false)
    }

    // zombie actor should flag both clusters as inactive
    withZombieActor(googleProjectDAO = googleProjectDAO) { _ =>
      eventually(timeout(Span(10, Seconds))) {
        val c1 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster1.id) }.get
        val c2 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster2.id) }.get

        List(c1, c2).foreach { c =>
          c.status shouldBe RuntimeStatus.Deleted
          c.auditInfo.destroyedDate shouldBe 'defined
          c.errors.size shouldBe 1
          c.errors.head.errorCode shouldBe -1
          c.errors.head.errorMessage should include("An underlying resource was removed in Google")
        }
      }
    }
  }

  it should "detect zombie clusters when the cluster is inactive" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create 2 running clusters in the same project
    val savedTestCluster1 = testCluster1.save()
    savedTestCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster1

    val savedTestCluster2 = testCluster2.save()
    savedTestCluster2.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster2

    // stub GoogleDataprocDAO to flag cluster2 as deleted
    val gdDAO = new MockGoogleDataprocDAO {
      override def getClusterStatus(googleProject: GoogleProject, clusterName: RuntimeName): Future[RuntimeStatus] =
        Future.successful {
          if (clusterName == savedTestCluster2.runtimeName) {
            RuntimeStatus.Deleted
          } else {
            RuntimeStatus.Running
          }
        }
    }

    // c2 should be flagged as a zombie but not c1
    withZombieActor(gdDAO = gdDAO) { _ =>
      eventually(timeout(Span(10, Seconds))) {
        val c1 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster1.id) }.get
        val c2 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster2.id) }.get

        c2.status shouldBe RuntimeStatus.Deleted
        c2.auditInfo.destroyedDate shouldBe 'defined
        c2.errors.size shouldBe 1
        c2.errors.head.errorCode shouldBe -1
        c2.errors.head.errorMessage should include("An underlying resource was removed in Google")

        c1.status shouldBe RuntimeStatus.Running
        c1.errors shouldBe 'empty
      }
    }
  }

  it should "zombify creating cluster after hang period" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create a Running and a Creating cluster in the same project
    val savedTestCluster1 = testCluster1.save()
    savedTestCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster1

    val creatingTestCluster = testCluster2.copy(status = RuntimeStatus.Creating)
    val savedTestCluster2 = creatingTestCluster.save()
    savedTestCluster2.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual creatingTestCluster

    // stub GoogleDataprocDAO to flag both clusters as deleted
    val gdDAO = new MockGoogleDataprocDAO {
      override def getClusterStatus(googleProject: GoogleProject, clusterName: RuntimeName): Future[RuntimeStatus] =
        Future.successful(RuntimeStatus.Deleted)
    }

    val shouldHangAfter: Span = zombieClusterConfig.creationHangTolerance.plus(zombieClusterConfig.zombieCheckPeriod)

    // cluster2 should be active when it's still within hang tolerance
    withZombieActor(gdDAO = gdDAO) { _ =>
      eventually(timeout(3 seconds)) { //This timeout can be anything smaller than hang tolerance
        val c2 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster2.id) }.get

        c2.status shouldBe RuntimeStatus.Creating
        c2.auditInfo.destroyedDate shouldBe None
      }
    }
    // the Running cluster should be a zombie but the Creating one shouldn't
    withZombieActor(gdDAO = gdDAO) { _ =>
      eventually(timeout(shouldHangAfter)) {
        val c1 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster1.id) }.get
        val c2 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster2.id) }.get

        c1.status shouldBe RuntimeStatus.Deleted
        c1.auditInfo.destroyedDate shouldBe 'defined
        c1.errors.size shouldBe 1
        c1.errors.head.errorCode shouldBe -1
        c1.errors.head.errorMessage should include("An underlying resource was removed in Google")

        c2.status shouldBe RuntimeStatus.Deleted
        c2.auditInfo.destroyedDate shouldBe 'defined
        c2.errors.size shouldBe 1
        c2.errors.head.errorCode shouldBe -1
        c2.errors.head.errorMessage should include("An underlying resource was removed in Google")
      }
    }
  }

  it should "not zombify cluster before hang period" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    val creatingTestCluster = testCluster2.copy(status = RuntimeStatus.Creating)
    val savedTestCluster2 = creatingTestCluster.save()
    savedTestCluster2.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual creatingTestCluster

    val shouldNotHangBefore = zombieClusterConfig.creationHangTolerance.minus(zombieClusterConfig.zombieCheckPeriod)
    // stub GoogleDataprocDAO to flag both clusters as deleted
    val gdDAO = new MockGoogleDataprocDAO {
      override def getClusterStatus(googleProject: GoogleProject, clusterName: RuntimeName): Future[RuntimeStatus] =
        Future.successful(RuntimeStatus.Deleted)
    }

    // the Running cluster should be a zombie but the Creating one shouldn't
    withZombieActor(gdDAO = gdDAO) { _ =>
      Thread.sleep(shouldNotHangBefore.toSeconds)

      val c1 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster2.id) }.get
      c1.status shouldBe RuntimeStatus.Creating
      c1.errors.size shouldBe 0
    }
  }

  it should "not zombify upon non-403 errors from Google" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // running cluster in "bad" project - should not get zombified
    val clusterBadProject = testCluster1.copy(googleProject = GoogleProject("bad-project"))
    val savedClusterBadProject = clusterBadProject.save()
    savedClusterBadProject.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual clusterBadProject

    // running "bad" cluster - should not get zombified
    val badCluster = testCluster2.copy(runtimeName = RuntimeName("bad-cluster"))
    val savedBadCluster = badCluster.save()
    savedBadCluster.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual badCluster

    // running "good" cluster - should get zombified
    val goodCluster = testCluster3.copy(runtimeName = RuntimeName("good-cluster"))
    val savedGoodCluster = goodCluster.save()
    savedGoodCluster.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual goodCluster

    // stub GoogleProjectDAO to return an error for the bad project
    val googleProjectDAO = new MockGoogleProjectDAO {
      override def isProjectActive(projectName: String): Future[Boolean] =
        if (projectName == clusterBadProject.googleProject.value) {
          Future.failed(new Exception)
        } else Future.successful(true)
    }

    // stub GoogleDataprocDAO to return an error for the bad cluster
    val gdDAO = new MockGoogleDataprocDAO {
      override def getClusterStatus(googleProject: GoogleProject, clusterName: RuntimeName): Future[RuntimeStatus] =
        if (googleProject == clusterBadProject.googleProject && clusterName == clusterBadProject.runtimeName) {
          Future.successful(RuntimeStatus.Running)
        } else if (googleProject == badCluster.googleProject && clusterName == badCluster.runtimeName) {
          Future.failed(new Exception)
        } else {
          Future.successful(RuntimeStatus.Deleted)
        }
    }

    // only the "good" cluster should be zombified
    withZombieActor(gdDAO = gdDAO) { _ =>
      eventually(timeout(Span(10, Seconds))) {
        val c1 = dbFutureValue { clusterQuery.getClusterById(savedClusterBadProject.id) }.get
        val c2 = dbFutureValue { clusterQuery.getClusterById(savedBadCluster.id) }.get
        val c3 = dbFutureValue { clusterQuery.getClusterById(savedGoodCluster.id) }.get

        c1.status shouldBe RuntimeStatus.Running
        c1.errors shouldBe 'empty

        c2.status shouldBe RuntimeStatus.Running
        c2.errors shouldBe 'empty

        c3.status shouldBe RuntimeStatus.Deleted
        c3.auditInfo.destroyedDate shouldBe 'defined
        c3.errors.size shouldBe 1
        c3.errors.head.errorCode shouldBe -1
        c3.errors.head.errorMessage should include("An underlying resource was removed in Google")
      }
    }
  }

  private def withZombieActor[T](
    gdDAO: GoogleDataprocDAO = new MockGoogleDataprocDAO,
    googleProjectDAO: GoogleProjectDAO = new MockGoogleProjectDAO
  )(testCode: ActorRef => T): T = {
    val actor =
      system.actorOf(ZombieClusterMonitor.props(zombieClusterConfig, gdDAO, googleProjectDAO))
    val testResult = Try(testCode(actor))
    // shut down the actor and wait for it to terminate
    testKit watch actor
    system.stop(actor)
    expectMsgClass(5 seconds, classOf[Terminated])
    testResult.get
  }

}
