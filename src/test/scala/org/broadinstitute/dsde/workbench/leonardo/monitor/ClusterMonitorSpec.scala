package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.io.File
import java.time.Instant
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, Terminated}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import io.grpc.Status.Code
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterCreated, ClusterDeleted}
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService
import org.mockito.ArgumentMatchers.{eq => eqq, _}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 9/6/17.
  */
class ClusterMonitorSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with MockitoSugar with BeforeAndAfterAll with TestComponent { testKit =>

  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val dataprocConfig = config.as[DataprocConfig]("dataproc")

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
    clusterUrl = Cluster.getClusterUrl("dsp-leo-test", "name2"),
    operationName = "op1",
    status = ClusterStatus.Deleting,
    hostIp = None,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def createClusterSupervisor(dao: DataprocDAO): ActorRef = {
    val actor = system.actorOf(TestClusterSupervisorActor.props(dao, DbSingleton.ref, testKit))
    new LeonardoService(dataprocConfig, dao, DbSingleton.ref, actor)
    actor
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
      dao.getClusterStatus(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Running)

    when {
      dao.getClusterMasterInstanceIp(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(Some("1.2.3.4"))

    createClusterSupervisor(dao) ! ClusterCreated(creatingCluster)

    expectMsgClass(1 second, classOf[Terminated])
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
  Seq(ClusterStatus.Creating, ClusterStatus.Updating, ClusterStatus.Unknown).map { status =>
    it should s"monitor $status status" in isolatedDbTest {
      dbFutureValue { _.clusterQuery.save(creatingCluster) } shouldEqual creatingCluster

      val dao = mock[DataprocDAO]
      when {
        dao.getClusterStatus(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
      } thenReturn Future.successful(status)

      createClusterSupervisor(dao) ! ClusterCreated(creatingCluster)

      expectNoMsg(1 second)

      val updatedCluster = dbFutureValue { _.clusterQuery.getByName(creatingCluster.googleProject, creatingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster shouldBe Some(creatingCluster)
    }
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status RUNNING, but no IP address
  // Post:
  // - cluster is not changed in the DB
  // - monitor actor does not shut down
  it should "keep monitoring in RUNNING state with no IP" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(creatingCluster) } shouldEqual creatingCluster

    val dao = mock[DataprocDAO]
    when {
      dao.getClusterStatus(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Running)

    when {
      dao.getClusterMasterInstanceIp(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(None)

    createClusterSupervisor(dao) ! ClusterCreated(creatingCluster)

    expectNoMsg(1 second)

    val updatedCluster = dbFutureValue { _.clusterQuery.getByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster shouldBe Some(creatingCluster)
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status ERROR, but no error code
  // Post:
  // - cluster is not changed in the DB
  // - monitor actor does not shut down
  it should "keep monitoring in ERROR state with no error code" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(creatingCluster) } shouldEqual creatingCluster

    val dao = mock[DataprocDAO]
    when {
      dao.getClusterStatus(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Error)

    when {
      dao.getClusterErrorDetails(eqq("op1"))(any[ExecutionContext])
    } thenReturn Future.successful(None)

    createClusterSupervisor(dao) ! ClusterCreated(creatingCluster)

    expectNoMsg(1 second)

    val updatedCluster = dbFutureValue { _.clusterQuery.getByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster shouldBe Some(creatingCluster)
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
      dao.getClusterStatus(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Error)

    when {
      dao.getClusterErrorDetails(eqq("op1"))(any[ExecutionContext])
    } thenReturn Future.successful(Some(ClusterErrorDetails(Code.CANCELLED.value, Some("test message"))))

    when {
      dao.deleteCluster(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    createClusterSupervisor(dao) ! ClusterCreated(creatingCluster)

    expectMsgClass(1 second, classOf[Terminated])
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
      dao.getClusterStatus(eqq(deletingCluster.googleProject), eqq(deletingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Deleted)

    createClusterSupervisor(dao) ! ClusterDeleted(deletingCluster)

    expectMsgClass(1 second, classOf[Terminated])
    val updatedCluster = dbFutureValue { _.clusterQuery.getByGoogleId(deletingCluster.googleId) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Deleted)
    updatedCluster.flatMap(_.hostIp) shouldBe None
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
      dao.getClusterStatus(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn {
      Future.successful(ClusterStatus.Error)
    } thenReturn {
      Future.successful(ClusterStatus.Deleted)
    } thenReturn {
      Future.successful(ClusterStatus.Creating)
    } thenReturn {
      Future.successful(ClusterStatus.Running)
    }

    when {
      dao.getClusterErrorDetails(eqq("op1"))(any[ExecutionContext])
    } thenReturn Future.successful(Some(ClusterErrorDetails(Code.UNKNOWN.value, Some("Test message"))))

    when {
      dao.deleteCluster(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    val newClusterId = UUID.randomUUID()
    when {
      dao.createCluster(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName), any[ClusterRequest], anyString)(any[ExecutionContext])
    } thenReturn Future.successful {
      ClusterResponse(creatingCluster.clusterName, creatingCluster.googleProject, newClusterId.toString, "CREATING", "description", "create op")
    }

    when {
      dao.updateFirewallRule(eqq(creatingCluster.googleProject))
    } thenReturn Future.successful(())

    when {
      dao.createBucket(anyString, anyString)
    } thenReturn Future.successful(())

    when {
      dao.uploadToBucket(anyString, anyString, anyString, any[File])
    } thenReturn Future.successful(())

    when {
      dao.uploadToBucket(anyString, anyString, anyString, anyString)
    } thenReturn Future.successful(())

    when {
      dao.getClusterMasterInstanceIp(eqq(creatingCluster.googleProject), eqq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(Some("1.2.3.4"))

    createClusterSupervisor(dao) ! ClusterCreated(creatingCluster)

    // Expect 3 Terminated messages:
    // 1. original cluster monitor, terminates at Error status
    // 2. deletion monitor, terminates at Deleted status
    // 3. new Cluster creating monitor, terminates at Running status
    expectMsgClass(1 second, classOf[Terminated])
    expectMsgClass(1 second, classOf[Terminated])
    expectMsgClass(1 second, classOf[Terminated])

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

  // Pre:
  // - cluster exists in the DB with status Deleting
  // - dataproc DAO returns status RUNNING
  // Post:
  // - cluster is not changed in the DB
  // - monitor actor does not shut down
  it should "not restart a deleting cluster" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(deletingCluster) } shouldEqual deletingCluster

    val dao = mock[DataprocDAO]
    when {
      dao.getClusterStatus(eqq(deletingCluster.googleProject), eqq(deletingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Running)

    createClusterSupervisor(dao) ! ClusterCreated(deletingCluster)

    expectNoMsg(1 second)

    val updatedCluster = dbFutureValue { _.clusterQuery.getByName(deletingCluster.googleProject, deletingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster shouldBe Some(deletingCluster)
  }

}
