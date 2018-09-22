package org.broadinstitute.dsde.workbench.leonardo.monitor

import akka.actor.{ActorRef, ActorSystem, Terminated}
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleProjectDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, ClusterStatus}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

class ZombieClusterMonitorSpec extends TestKit(ActorSystem("leonardotest")) with
  FlatSpecLike with BeforeAndAfterAll with TestComponent with CommonTestData with GcsPathUtils { testKit =>

  val testCluster1 = makeCluster(1).copy(status = ClusterStatus.Running)
  val testCluster2 = makeCluster(2).copy(status = ClusterStatus.Running)
  val testCluster3 = makeCluster(3).copy(status = ClusterStatus.Running)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ZombieClusterMonitor" should "should detect zombie clusters when the project is inactive" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create 2 running clusters in the same project
    val savedTestCluster1 = testCluster1.save()
    savedTestCluster1 shouldEqual testCluster1

    val savedTestCluster2 = testCluster2.save()
    savedTestCluster2 shouldEqual testCluster2

    // stub GoogleProjectDAO to make the project inactive
    val googleProjectDAO = new MockGoogleProjectDAO {
      override def isProjectActive(projectName: String): Future[Boolean] = Future.successful(false)
    }

    // zombie actor should flag both clusters as inactive
    withZombieActor(googleProjectDAO = googleProjectDAO) { _ =>
      eventually(timeout(Span(10, Seconds))) {
        val c1 = dbFutureValue { _.clusterQuery.getClusterById(savedTestCluster1.id) }.get
        val c2 = dbFutureValue { _.clusterQuery.getClusterById(savedTestCluster2.id) }.get

        List(c1, c2).foreach { c =>
          c.status shouldBe ClusterStatus.Error
          c.errors.size shouldBe 1
          c.errors.head.errorCode shouldBe -1
          c.errors.head.errorMessage should include ("An underlying resource was removed in Google")
        }
      }
    }
  }

  it should "should detect zombie clusters when the project's billing is inactive" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create 2 running clusters in the same project
    val savedTestCluster1 = testCluster1.save()
    savedTestCluster1 shouldEqual testCluster1

    val savedTestCluster2 = testCluster2.save()
    savedTestCluster2 shouldEqual testCluster2

    // stub GoogleProjectDAO to make the project inactive
    val googleProjectDAO = new MockGoogleProjectDAO {
      override def isBillingActive(projectName: String): Future[Boolean] = Future.successful(false)
    }

    // zombie actor should flag both clusters as inactive
    withZombieActor(googleProjectDAO = googleProjectDAO) { _ =>
      eventually(timeout(Span(10, Seconds))) {
        val c1 = dbFutureValue { _.clusterQuery.getClusterById(savedTestCluster1.id) }.get
        val c2 = dbFutureValue { _.clusterQuery.getClusterById(savedTestCluster2.id) }.get

        List(c1, c2).foreach { c =>
          c.status shouldBe ClusterStatus.Error
          c.errors.size shouldBe 1
          c.errors.head.errorCode shouldBe -1
          c.errors.head.errorMessage should include ("An underlying resource was removed in Google")
        }
      }
    }
  }

  it should "should detect zombie clusters when the cluster is inactive" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create 2 running clusters in the same project
    val savedTestCluster1 = testCluster1.save()
    savedTestCluster1 shouldEqual testCluster1

    val savedTestCluster2 = testCluster2.save()
    savedTestCluster2 shouldEqual testCluster2

    // stub GoogleDataprocDAO to flag cluster2 as deleted
    val gdDAO = new MockGoogleDataprocDAO {
      override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[ClusterStatus] = {
        Future.successful {
          if (clusterName == savedTestCluster2.clusterName) {
            ClusterStatus.Deleted
          } else {
            ClusterStatus.Running
          }
        }
      }
    }

    // c2 should be flagged as a zombie but not c1
    withZombieActor(gdDAO = gdDAO) { _ =>
      eventually(timeout(Span(10, Seconds))) {
        val c1 = dbFutureValue { _.clusterQuery.getClusterById(savedTestCluster1.id) }.get
        val c2 = dbFutureValue { _.clusterQuery.getClusterById(savedTestCluster2.id) }.get

        c2.status shouldBe ClusterStatus.Error
        c2.errors.size shouldBe 1
        c2.errors.head.errorCode shouldBe -1
        c2.errors.head.errorMessage should include ("An underlying resource was removed in Google")

        c1.status shouldBe ClusterStatus.Running
        c1.errors shouldBe 'empty
      }
    }
  }

  it should "should not zombify Creating clusters" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create a Running and a Creating cluster in the same project
    val savedTestCluster1 = testCluster1.save()
    savedTestCluster1 shouldEqual testCluster1

    val creatingTestCluster = testCluster2.copy(status = ClusterStatus.Creating)
    val savedTestCluster2 = creatingTestCluster.save()
    savedTestCluster2 shouldEqual creatingTestCluster

    // stub GoogleDataprocDAO to flag both clusters as deleted
    val gdDAO = new MockGoogleDataprocDAO {
      override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[ClusterStatus] = {
        Future.successful(ClusterStatus.Deleted)
      }
    }

    // the Running cluster should be a zombie but the Creating one shouldn't
    withZombieActor(gdDAO = gdDAO) { _ =>
      eventually(timeout(Span(10, Seconds))) {
        val c1 = dbFutureValue { _.clusterQuery.getClusterById(savedTestCluster1.id) }.get
        val c2 = dbFutureValue { _.clusterQuery.getClusterById(savedTestCluster2.id) }.get

        c1.status shouldBe ClusterStatus.Error
        c1.errors.size shouldBe 1
        c1.errors.head.errorCode shouldBe -1
        c1.errors.head.errorMessage should include ("An underlying resource was removed in Google")

        c2.status shouldBe ClusterStatus.Creating
        c2.errors shouldBe 'empty
      }
    }
  }

  it should "should not zombify upon errors from Google" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // running cluster in "bad" project - should not get zombified
    val clusterBadProject = testCluster1.copy(googleProject = GoogleProject("bad-project"))
    val savedClusterBadProject = clusterBadProject.save()
    savedClusterBadProject shouldEqual clusterBadProject

    // running "bad" cluster - should not get zombified
    val badCluster = testCluster2.copy(clusterName = ClusterName("bad-cluster"))
    val savedBadCluster = badCluster.save()
    savedBadCluster shouldEqual badCluster

    // running "good" cluster - should get zombified
    val goodCluster = testCluster3.copy(clusterName = ClusterName("good-cluster"))
    val savedGoodCluster = goodCluster.save()
    savedGoodCluster shouldEqual goodCluster

    // stub GoogleProjectDAO to return an error for the bad project
    val googleProjectDAO = new MockGoogleProjectDAO {
      override def isProjectActive(projectName: String): Future[Boolean] = {
        if (projectName == clusterBadProject.googleProject.value) {
          Future.failed(new Exception)
        } else Future.successful(true)
      }
    }

    // stub GoogleDataprocDAO to return an error for the bad cluster
    val gdDAO = new MockGoogleDataprocDAO {
      override def getClusterStatus(googleProject: GoogleProject, clusterName: ClusterName): Future[ClusterStatus] = {
        if (googleProject == clusterBadProject.googleProject && clusterName == clusterBadProject.clusterName) {
          Future.successful(ClusterStatus.Running)
        } else if (googleProject == badCluster.googleProject && clusterName == badCluster.clusterName) {
          Future.failed(new Exception)
        } else {
          Future.successful(ClusterStatus.Deleted)
        }
      }
    }

    // only the "good" cluster should be zombified
    withZombieActor(gdDAO = gdDAO) { _ =>
      eventually(timeout(Span(10, Seconds))) {
        val c1 = dbFutureValue { _.clusterQuery.getClusterById(savedClusterBadProject.id) }.get
        val c2 = dbFutureValue { _.clusterQuery.getClusterById(savedBadCluster.id) }.get
        val c3 = dbFutureValue { _.clusterQuery.getClusterById(savedGoodCluster.id) }.get

        c1.status shouldBe ClusterStatus.Running
        c1.errors shouldBe 'empty

        c2.status shouldBe ClusterStatus.Running
        c2.errors shouldBe 'empty

        c3.status shouldBe ClusterStatus.Error
        c3.errors.size shouldBe 1
        c3.errors.head.errorCode shouldBe -1
        c3.errors.head.errorMessage should include ("An underlying resource was removed in Google")
      }
    }
  }

  private def withZombieActor[T](gdDAO: GoogleDataprocDAO = new MockGoogleDataprocDAO, googleProjectDAO: GoogleProjectDAO = new MockGoogleProjectDAO)
                             (testCode: ActorRef => T): T = {
    val actor = system.actorOf(ZombieClusterMonitor.props(zombieClusterConfig, gdDAO, googleProjectDAO, DbSingleton.ref))
    val testResult = Try(testCode(actor))
    // shut down the actor and wait for it to terminate
    testKit watch actor
    system.stop(actor)
    expectMsgClass(5 seconds, classOf[Terminated])
    testResult.get
  }

}
