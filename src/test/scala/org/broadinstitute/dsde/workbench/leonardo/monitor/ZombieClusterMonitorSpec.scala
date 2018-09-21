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
