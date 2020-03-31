package org.broadinstitute.dsde.workbench.leonardo
package monitor

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.effect.concurrent.Deferred
import cats.mtl.ApplicativeAsk
import com.google.api.client.googleapis.testing.json.GoogleJsonResponseExceptionFactoryTesting
import com.google.api.client.testing.json.MockJsonFactory
import com.google.cloud.compute.v1.Instance
import fs2.Stream
import org.broadinstitute.dsde.workbench.google.GoogleProjectDAO
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleDataprocDAO, MockGoogleProjectDAO}
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, InstanceName, MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.Config.{dataprocInterpreterConfig, gceInterpreterConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleDataprocDAO, MockGoogleComputeService}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.util.{DataprocInterpreter, GceInterpreter, RuntimeInstances}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class ZombieRuntimeMonitorSpec
    extends TestKit(ActorSystem("leonardotest"))
    with FlatSpecLike
    with BeforeAndAfterAll
    with LeonardoTestSuite
    with TestComponent
    with GcsPathUtils { testKit =>

  val testCluster1 = makeCluster(1).copy(status = RuntimeStatus.Running)
  val testCluster2 = makeCluster(2).copy(status = RuntimeStatus.Running)
  val testCluster3 = makeCluster(3).copy(status = RuntimeStatus.Running)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ZombieRuntimeMonitor" should "detect zombie clusters when the project is inactive" in isolatedDbTest {
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
    withZombieMonitor(googleProjectDAO = googleProjectDAO) { () =>
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

  it should "detect zombie clusters when we get a 403 from google" in isolatedDbTest {
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
    withZombieMonitor(googleProjectDAO = googleProjectDAO) { () =>
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
    withZombieMonitor(googleProjectDAO = googleProjectDAO) { () =>
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

  it should "detect zombie dataptoc cluster when the cluster is inactive" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create 2 running clusters in the same project
    val savedTestCluster1 = testCluster1.save()
    savedTestCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster1

    val savedTestCluster2 = testCluster2.save()
    savedTestCluster2.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster2

    // stub GoogleDataprocDAO to flag cluster2 as deleted
    val gdDAO = new MockGoogleDataprocDAO {
      override def getClusterStatus(googleProject: GoogleProject,
                                    clusterName: RuntimeName): Future[Option[DataprocClusterStatus]] =
        Future.successful {
          if (clusterName == savedTestCluster2.runtimeName) {
            None
          } else {
            Some(DataprocClusterStatus.Running)
          }
        }
    }

    // c2 should be flagged as a zombie but not c1
    withZombieMonitor(gdDAO = gdDAO) { () =>
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

  it should "detect zombie gce instance when the cluster is inactive" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    // create a running clusters in the same project
    val runtimeConfig = RuntimeConfig.GceConfig(
      MachineTypeName("n1-standard-4"),
      DiskSize(50)
    )
    val savedTestCluster = testCluster1.saveWithRuntimeConfig(runtimeConfig)
    savedTestCluster.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual testCluster1

    // stub GoogleComputeService to flag runtime as deleted
    val gce = new MockGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: ApplicativeAsk[IO, TraceId]
      ): IO[Option[Instance]] =
        IO.pure(None)
    }

    withZombieMonitor(gce = gce) { () =>
      eventually(timeout(Span(10, Seconds))) {
        val c1 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster.id) }.get

        c1.status shouldBe RuntimeStatus.Deleted
        c1.auditInfo.destroyedDate shouldBe 'defined
        c1.errors.size shouldBe 1
        c1.errors.head.errorCode shouldBe -1
        c1.errors.head.errorMessage should include("An underlying resource was removed in Google")
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
      override def getClusterStatus(googleProject: GoogleProject,
                                    clusterName: RuntimeName): Future[Option[DataprocClusterStatus]] =
        Future.successful(None)
    }

    val shouldHangAfter: Span = zombieClusterConfig.creationHangTolerance.plus(zombieClusterConfig.zombieCheckPeriod)

    // cluster2 should be active when it's still within hang tolerance
    withZombieMonitor(gdDAO = gdDAO) { () =>
      eventually(timeout(3 seconds)) { //This timeout can be anything smaller than hang tolerance
        val c2 = dbFutureValue { clusterQuery.getClusterById(savedTestCluster2.id) }.get

        c2.status shouldBe RuntimeStatus.Creating
        c2.auditInfo.destroyedDate shouldBe None
      }
    }

    // the Running cluster should be a zombie but the Creating one shouldn't
    withZombieMonitor(gdDAO = gdDAO) { () =>
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
      override def getClusterStatus(googleProject: GoogleProject,
                                    clusterName: RuntimeName): Future[Option[DataprocClusterStatus]] =
        Future.successful(None)
    }

    // the Running cluster should be a zombie but the Creating one shouldn't
    withZombieMonitor(gdDAO = gdDAO) { () =>
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

    // stub GoogleDataprocDAO to return an error for the bad cluster
    val gdDAO = new MockGoogleDataprocDAO {
      override def getClusterStatus(googleProject: GoogleProject,
                                    clusterName: RuntimeName): Future[Option[DataprocClusterStatus]] =
        if (googleProject == clusterBadProject.googleProject && clusterName == clusterBadProject.runtimeName) {
          Future.successful(Some(DataprocClusterStatus.Running))
        } else if (googleProject == badCluster.googleProject && clusterName == badCluster.runtimeName) {
          Future.failed(new Exception("boom"))
        } else {
          Future.successful(None)
        }
    }

    // only the "good" cluster should be zombified
    withZombieMonitor(gdDAO = gdDAO) { () =>
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

  private def withZombieMonitor[A](
    gdDAO: GoogleDataprocDAO = new MockGoogleDataprocDAO,
    gce: GoogleComputeService[IO] = new MockGoogleComputeService,
    googleProjectDAO: GoogleProjectDAO = new MockGoogleProjectDAO
  )(validations: () => A): A = {
    val dataprocInterp =
      new DataprocInterpreter(dataprocInterpreterConfig, null, null, gdDAO, gce, null, null, null, null, blocker)
    val gceInterp = new GceInterpreter(gceInterpreterConfig, null, null, gce, null, blocker)

    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    val zombieClusterMonitor = ZombieRuntimeMonitor[IO](zombieClusterConfig, googleProjectDAO)
    val process = Stream.eval(Deferred[IO, A]).flatMap { signalToStop =>
      val signal = Stream.eval(IO(validations())).evalMap(r => signalToStop.complete(r))
      val p = Stream(zombieClusterMonitor.process.interruptWhen(signalToStop.get.attempt.map(_.map(_ => ()))), signal)
        .parJoin(2)
      p ++ Stream.eval(signalToStop.get)
    }
    process.compile.lastOrError.unsafeRunSync().asInstanceOf[A]
  }
}
