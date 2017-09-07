package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, Props, Terminated}
import akka.testkit.TestKit
import com.google.api.services.compute.model.{AccessConfig, Instance, NetworkInterface}
import com.google.api.services.dataproc.model.{Cluster => GoogleCluster, ClusterStatus => GoogleClusterStatus, _}
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.leonardo.config.MonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterRequest, ClusterResponse, ClusterStatus}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterCreated, ClusterDeleted}
import org.mockito.ArgumentMatchers.{eq => eqq, _}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 9/6/17.
  */
class ClusterMonitorSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with MockitoSugar with BeforeAndAfterAll with TestComponent { testKit =>

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val creatingCluster = Cluster(
    clusterName = "name1",
    googleId = UUID.randomUUID(),
    googleProject = "dsp-leo-test",
    googleServiceAccount = "not-a-service-acct@google.com",
    googleBucket = "bucket1",
    clusterUrl = Cluster.getClusterUrl("dsp-leo-test", "name1"),
    operationName = "op1",
    status = ClusterStatus.Creating,
    hostIp = None,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"))

  val deletingCluster = Cluster(
    clusterName = "name2",
    googleId = UUID.randomUUID(),
    googleProject = "dsp-leo-test",
    googleServiceAccount = "not-a-service-acct@google.com",
    googleBucket = "bucket1",
    clusterUrl = Cluster.getClusterUrl("dsp-leo-test", "name1"),
    operationName = "op1",
    status = ClusterStatus.Deleting,
    hostIp = Some("numbers.and.dots"),
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"))

  /**
    * Extends ClusterMonitorSupervisor so the akka TestKit can watch the child ClusterMontitorActor's.
    */
  class TestSupervisor(gdDAO: DataprocDAO, dbRef: DbReference) extends ClusterMonitorSupervisor(MonitorConfig(1 second), gdDAO, dbRef) {
    override def createChildActor(cluster: Cluster): ActorRef = {
      val child = super.createChildActor(cluster)
      testKit watch child
      child
    }
  }

  def createClusterSupervisor(dao: DataprocDAO): ActorRef = {
    system.actorOf(Props(new TestSupervisor(dao, DbSingleton.ref)))
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status RUNNING
  // Post:
  // - cluster is updated in the DB with status Running and the host IP
  // - monitor actor shuts down
  "ClusterMonitorActor" should "monitor until RUNNING state" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(creatingCluster) } shouldEqual creatingCluster

    val dao = mock[DataprocDAO]
    when {
      dao.getCluster(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful {
      new GoogleCluster().setStatus(new GoogleClusterStatus().setState("RUNNING")).setProjectId(creatingCluster.googleProject)
        .setConfig(new ClusterConfig()
          .setMasterConfig(new InstanceGroupConfig().setInstanceNames(List("masterInstance").asJava))
          .setGceClusterConfig(new GceClusterConfig().setZoneUri("us-central1-c")))
    }
    when {
      dao.getInstance(eqq(creatingCluster.googleProject), anyString, eqq("masterInstance"))(any[ExecutionContext])
    } thenReturn Future.successful {
      new Instance().setNetworkInterfaces(List(new NetworkInterface().setAccessConfigs(List(new AccessConfig().setNatIP("1.2.3.4")).asJava)).asJava)
    }

    createClusterSupervisor(dao) ! ClusterCreated(creatingCluster)

    expectMsgClass(5 seconds, classOf[Terminated])
    val updatedCluster = dbFutureValue { _.clusterQuery.getByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
    updatedCluster.flatMap(_.hostIp) shouldBe Some("1.2.3.4")
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status CREATING, UNKNOWN, or UPDATING
  // Post:
  // - cluster is not changed in the DB
  // - monitor actor does not shut down
  Seq("CREATING", "UNKNOWN", "UPDATING").map { state =>
    it should s"monitor $state state" in isolatedDbTest {
      dbFutureValue { _.clusterQuery.save(creatingCluster) } shouldEqual creatingCluster

      val dao = mock[DataprocDAO]
      when {
        dao.getCluster(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
      } thenReturn Future.successful {
        new GoogleCluster().setStatus(new GoogleClusterStatus().setState(state))
      }

      createClusterSupervisor(dao) ! ClusterCreated(creatingCluster)

      expectNoMsg(5 seconds)

      val updatedCluster = dbFutureValue { _.clusterQuery.getByName(creatingCluster.googleProject, creatingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster shouldBe Some(creatingCluster)
    }
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status ERROR and error code CANCELLED
  // Post:
  // - cluster status is set to Error in the DB
  // - monitor actor shuts down
  it should "monitor until ERROR state with no restart" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(creatingCluster) } shouldEqual creatingCluster

    val dao = mock[DataprocDAO]
    when {
      dao.getCluster(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful {
      new GoogleCluster().setStatus(new GoogleClusterStatus().setState("ERROR"))
    }
    when {
      dao.getOperation(eqq("op1"))(any[ExecutionContext])
    } thenReturn Future.successful {
      new Operation().setDone(true).setError(new Status().setCode(Code.CANCELLED.value()).setMessage("test message"))
    }

    createClusterSupervisor(dao) ! ClusterCreated(creatingCluster)

    expectMsgClass(5 seconds, classOf[Terminated])
    val updatedCluster = dbFutureValue { _.clusterQuery.getByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Error)
    updatedCluster.flatMap(_.hostIp) shouldBe None
  }

  // Pre:
  // - cluster exists in the DB with status Deleting
  // - dataproc DAO returns status DELETED
  // Post:
  // - cluster status is set to Deleted in the DB
  // - monitor actor shuts down
  it should "monitor until DELETED state" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(deletingCluster) } shouldEqual deletingCluster

    val dao = mock[DataprocDAO]
    when {
      dao.getCluster(eqq(deletingCluster.googleProject), eqq(deletingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful {
      new GoogleCluster().setStatus(new GoogleClusterStatus().setState("DELETED"))
    }

    createClusterSupervisor(dao) ! ClusterDeleted(deletingCluster)

    expectMsgClass(5 seconds, classOf[Terminated])
    val updatedCluster = dbFutureValue { _.clusterQuery.getByGoogleId(deletingCluster.googleId) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Deleted)
    updatedCluster.flatMap(_.hostIp) shouldBe deletingCluster.hostIp
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status ERROR and error code UNKNOWN
  // Post:
  // - old cluster status is set to Deleted in the DB
  // - new cluster exists with the original cluster name
  // - new cluster has status Running and the host IP
  // - monitor actor shuts down
  it should "monitor until ERROR state with restart" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(creatingCluster) } shouldEqual creatingCluster

    val dao = mock[DataprocDAO]
    when {
      dao.getCluster(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful {
      new GoogleCluster().setStatus(new GoogleClusterStatus().setState("ERROR"))
    } thenReturn Future.successful {
      new GoogleCluster().setStatus(new GoogleClusterStatus().setState("DELETED"))
    } thenReturn Future.successful {
      new GoogleCluster().setStatus(new GoogleClusterStatus().setState("CREATING"))
    } thenReturn Future.successful {
      new GoogleCluster().setStatus(new GoogleClusterStatus().setState("RUNNING")).setProjectId(creatingCluster.googleProject)
        .setConfig(new ClusterConfig().setMasterConfig(new InstanceGroupConfig().setInstanceNames(List("masterInstance").asJava))
        .setGceClusterConfig(new GceClusterConfig().setZoneUri("us-central1-c")))
    }

    when {
      dao.getOperation(eqq("op1"))(any[ExecutionContext])
    } thenReturn Future.successful {
      new Operation().setDone(true).setError(new Status().setCode(Code.UNKNOWN.value()).setMessage("test message"))
    }

    when {
      dao.deleteCluster(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(Some(new Operation().setName("new op")))

    val newClusterId = UUID.randomUUID()
    when {
      dao.createCluster(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName), any[ClusterRequest])(any[ExecutionContext])
    } thenReturn Future.successful {
      ClusterResponse(creatingCluster.clusterName, creatingCluster.googleProject, newClusterId.toString, "CREATING", "description", "create op")
    }

    when {
      dao.getInstance(eqq(creatingCluster.googleProject), anyString, eqq("masterInstance"))(any[ExecutionContext])
    } thenReturn Future.successful {
      new Instance().setNetworkInterfaces(List(new NetworkInterface().setAccessConfigs(List(new AccessConfig().setNatIP("1.2.3.4")).asJava)).asJava)
    }

    createClusterSupervisor(dao) ! ClusterCreated(creatingCluster)

    // Expect 3 Terminated messages:
    // 1. original cluster monitor, terminates at Error status
    // 2. deletion monitor, terminates at Deleted status
    // 3. new Cluster creating monitor, terminates at Running status
    expectMsgClass(5 seconds, classOf[Terminated])
    expectMsgClass(5 seconds, classOf[Terminated])
    expectMsgClass(5 seconds, classOf[Terminated])

    val oldCluster = dbFutureValue { _.clusterQuery.getByGoogleId(creatingCluster.googleId) }
    oldCluster shouldBe 'defined
    oldCluster.map(_.status) shouldBe Some(ClusterStatus.Deleted)
    oldCluster.flatMap(_.hostIp) shouldBe None

    val newCluster = dbFutureValue { _.clusterQuery.getByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    newCluster shouldBe 'defined
    newCluster.map(_.googleId) shouldBe Some(newClusterId)
    newCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
    newCluster.flatMap(_.hostIp) shouldBe Some("1.2.3.4")
  }

}
