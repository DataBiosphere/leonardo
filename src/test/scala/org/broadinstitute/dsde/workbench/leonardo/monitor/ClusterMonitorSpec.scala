package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.io.{ByteArrayInputStream, File}
import java.time.Instant
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Terminated}
import akka.testkit.TestKit
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}

import org.broadinstitute.dsde.workbench.leonardo.dao.JupyterDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.{Master, Worker}
import org.broadinstitute.dsde.workbench.leonardo.model.google.InstanceStatus
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterCreated, ClusterDeleted}
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService
import org.broadinstitute.dsde.workbench.leonardo.util.BucketHelper
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.GcsRole
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsEntity, GcsObjectName, GcsPath, GoogleProject, ServiceAccountKeyId}
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Try}

/**
  * Created by rtitle on 9/6/17.
  */
class ClusterMonitorSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with MockitoSugar with BeforeAndAfterAll with TestComponent with CommonTestData with GcsPathUtils { testKit =>

  val creatingCluster = makeCluster(1).copy(serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
                                            dataprocInfo = makeDataprocInfo(1).copy(hostIp = None),
                                            status = ClusterStatus.Creating,
                                            userJupyterExtensionConfig = Some(userExtConfig))

  val deletingCluster = makeCluster(2).copy(serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
                                            status = ClusterStatus.Deleting,
                                            instances = Set(masterInstance, workerInstance1, workerInstance2))

  val stoppingCluster = makeCluster(3).copy(serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
                                            dataprocInfo = makeDataprocInfo(1).copy(hostIp = None),
                                            status = ClusterStatus.Stopping)

  val startingCluster = makeCluster(4).copy(serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
                                            status = ClusterStatus.Starting)

  val errorCluster = makeCluster(5).copy(
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
    status = ClusterStatus.Error)

  val clusterInstances = Map(Master -> Set(masterInstance.key),
                             Worker -> Set(workerInstance1.key, workerInstance2.key))

  val clusterInstances2 = clusterInstances.mapValues(_.map(modifyInstanceKey))

  val clusterMonitorPatience = 10 seconds

  def stubComputeDAO(status: InstanceStatus): GoogleComputeDAO = {
    val dao = mock[GoogleComputeDAO]
    when {
      dao.getInstance(mockitoEq(masterInstance.key))
    } thenReturn Future.successful(Some(masterInstance.copy(status = status)))
    when {
      dao.getInstance(mockitoEq(workerInstance1.key))
    } thenReturn Future.successful(Some(workerInstance1.copy(status = status)))
    when {
      dao.getInstance(mockitoEq(workerInstance2.key))
    } thenReturn Future.successful(Some(workerInstance2.copy(status = status)))

    when {
      dao.getInstance(mockitoEq(modifyInstanceKey(masterInstance.key)))
    } thenReturn Future.successful(Some(modifyInstance(masterInstance).copy(status = status)))
    when {
      dao.getInstance(mockitoEq(modifyInstanceKey(workerInstance1.key)))
    } thenReturn Future.successful(Some(modifyInstance(workerInstance1).copy(status = status)))
    when {
      dao.getInstance(mockitoEq(modifyInstanceKey(workerInstance2.key)))
    } thenReturn Future.successful(Some(modifyInstance(workerInstance2).copy(status = status)))
    when {
      dao.getProjectNumber(any[GoogleProject])
    } thenReturn Future.successful(Some((new Random).nextLong()))
    dao
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def createClusterSupervisor(gdDAO: GoogleDataprocDAO, computeDAO: GoogleComputeDAO, iamDAO: GoogleIamDAO, storageDAO: GoogleStorageDAO, authProvider: LeoAuthProvider, jupyterDAO: JupyterDAO): ActorRef = {
    val cacheActor = system.actorOf(ClusterDnsCache.props(proxyConfig, DbSingleton.ref))
    val bucketHelper = new BucketHelper(dataprocConfig, gdDAO, computeDAO, storageDAO, serviceAccountProvider)
    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }
    val supervisorActor = system.actorOf(TestClusterSupervisorActor.props(dataprocConfig, gdDAO, computeDAO, iamDAO, storageDAO, DbSingleton.ref, cacheActor, testKit, authProvider, autoFreezeConfig, jupyterDAO))
    new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, gdDAO, computeDAO, iamDAO, storageDAO, mockPetGoogleStorageDAO, DbSingleton.ref, supervisorActor, whitelistAuthProvider, serviceAccountProvider, whitelist, bucketHelper, contentSecurityPolicy)
    supervisorActor
  }

  def withClusterSupervisor[T](gdDAO: GoogleDataprocDAO, computeDAO: GoogleComputeDAO, iamDAO: GoogleIamDAO, storageDAO: GoogleStorageDAO, authProvider: LeoAuthProvider, jupyterDAO: JupyterDAO = mockJupyterDAO, runningChild: Boolean = true)(testCode: ActorRef => T): T = {
    val supervisor = createClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, jupyterDAO)
    val testResult = Try(testCode(supervisor))
    testKit watch supervisor
    supervisor ! TearDown
    // Should receive a Terminated msg for each running child, plus one for the supervisor itself
    expectMsgAllClassOf(clusterMonitorPatience, Seq.fill(if (runningChild) 2 else 1)(classOf[Terminated]):_*)
    testResult.get
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status RUNNING
  // - compute DAO returns status RUNNING
  // Post:
  // - cluster is updated in the DB with status Running and the host IP
  // - instances are populated in the DB
  // - monitor actor shuts down
  "ClusterMonitorActor" should "monitor until RUNNING state" in isolatedDbTest {
    val savedCreatingCluster = creatingCluster.save()
    creatingCluster shouldEqual savedCreatingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Running)

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)

    when {
      gdDAO.getClusterMasterInstance(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(Some(masterInstance.key))

    when {
      gdDAO.getClusterStagingBucket(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket")))

    val computeDAO = stubComputeDAO(InstanceStatus.Running)

    val storageDAO = mock[GoogleStorageDAO]
    when {
      storageDAO.deleteBucket(any[GcsBucketName], any[Boolean])
    } thenReturn Future.successful(())

    when {
      storageDAO.setBucketAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    when {
      storageDAO.setDefaultObjectAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, false) { actor =>
      actor ! ClusterCreated(savedCreatingCluster)
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])

      val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
      updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe Some(IP("1.2.3.4"))
      updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))

      verify(storageDAO, never()).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status CREATING, UNKNOWN, or UPDATING
  // - compute DAO return status RUNNING
  // Post:
  // - cluster is not changed in the DB
  // - instances are populated in the DB
  // - monitor actor does not shut down
  Seq(ClusterStatus.Creating, ClusterStatus.Updating, ClusterStatus.Unknown).foreach { status =>
    it should s"monitor $status status" in isolatedDbTest {
      val savedCreatingCluster = creatingCluster.save()
      creatingCluster shouldEqual savedCreatingCluster

      val gdDAO = mock[GoogleDataprocDAO]
      when {
        gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
      } thenReturn Future.successful(status)

      when {
        gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
      } thenReturn Future.successful(clusterInstances)

      val computeDAO = stubComputeDAO(InstanceStatus.Running)
      val iamDAO = mock[GoogleIamDAO]
      val storageDAO = mock[GoogleStorageDAO]
      val authProvider = mock[LeoAuthProvider]

      withClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, true) { actor =>
        actor ! ClusterCreated(savedCreatingCluster)
        expectNoMessage(clusterMonitorPatience)

        val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
        updatedCluster shouldBe 'defined
        updatedCluster shouldBe Some(savedCreatingCluster.copy(instances = Set(masterInstance, workerInstance1, workerInstance2)))

        verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
        verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
        verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
      }
    }
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status RUNNING, but no IP address
  // - compute DAO returns RUNNING
  // Post:
  // - cluster is not changed in the DB
  // - instances are populated in the DB
  // - monitor actor does not shut down
  it should "keep monitoring in RUNNING state with no IP" in isolatedDbTest {
    val savedCreatingCluster = creatingCluster.save()
    creatingCluster shouldEqual savedCreatingCluster

    val dao = mock[GoogleDataprocDAO]
    when {
      dao.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Running)

    when {
      dao.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)

    when {
      dao.getClusterMasterInstance(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(None)

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val iamDAO = mock[GoogleIamDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(dao, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, true) { actor =>
      actor ! ClusterCreated(savedCreatingCluster)
      expectNoMessage(clusterMonitorPatience)

      val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster shouldBe Some(savedCreatingCluster.copy(instances = Set(masterInstance, workerInstance1, workerInstance2)))

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status ERROR, but no error code
  // - compute DAO returns RUNNING
  // Post:
  // - cluster is not changed in the DB
  // - instances are populated in the DB
  // - monitor actor does not shut down
  it should "keep monitoring in ERROR state with no error code" in isolatedDbTest {
    val savedCreatingCluster = creatingCluster.save()
    creatingCluster shouldEqual savedCreatingCluster
    
    val dao = mock[GoogleDataprocDAO]
    when {
      dao.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Error)

    when {
      dao.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)

    when {
      dao.getClusterErrorDetails(mockitoEq(creatingCluster.dataprocInfo.operationName))
    } thenReturn Future.successful(None)

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val storageDAO = mock[GoogleStorageDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(dao, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, true) { actor =>
      actor ! ClusterCreated(savedCreatingCluster)
      expectNoMessage(clusterMonitorPatience)

      val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster shouldBe Some(savedCreatingCluster.copy(instances = Set(masterInstance, workerInstance1, workerInstance2)))

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status ERROR and error code CANCELLED
  // - compute DAO returns RUNNING
  // Post:
  // - cluster status is set to Error in the DB
  // - instances are populated in the DB
  // - monitor actor shuts down
  it should "monitor until ERROR state with no restart" in isolatedDbTest {
    val savedCreatingCluster = creatingCluster.save()
    creatingCluster shouldEqual savedCreatingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Error)

    when {
      gdDAO.getClusterErrorDetails(mockitoEq(creatingCluster.dataprocInfo.operationName))
    } thenReturn Future.successful(Some(ClusterErrorDetails(Code.CANCELLED.value, Some("test message"))))

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(())

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    when {
      iamDAO.removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    } thenReturn Future.successful(())

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val storageDAO = mock[GoogleStorageDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, false) { actor =>
      actor ! ClusterCreated(savedCreatingCluster)
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])

      val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Error)
      updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe None
      updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, if (notebookServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Deleting
  // - dataproc DAO returns status DELETED
  // - compute DAO returns None
  // Post:
  // - cluster status is set to Deleted in the DB
  // - instances are deleted in the DB
  // - init bucket is deleted
  // - monitor actor shuts down
  it should "monitor until DELETED state" in isolatedDbTest {
    val savedDeletingCluster = deletingCluster.save()
    deletingCluster shouldEqual savedDeletingCluster

    val dao = mock[GoogleDataprocDAO]
    when {
      dao.getClusterStatus(mockitoEq(deletingCluster.googleProject), mockitoEq(deletingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Deleted)

    when {
      dao.getClusterInstances(mockitoEq(deletingCluster.googleProject), mockitoEq(deletingCluster.clusterName))
    } thenReturn Future.successful(Map.empty[DataprocRole, Set[InstanceKey]])

    val computeDAO = mock[GoogleComputeDAO]
    val iamDAO = mock[GoogleIamDAO]
    val storageDAO = mock[GoogleStorageDAO]

    when {
      storageDAO.deleteBucket(any[GcsBucketName], any[Boolean])
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    when {
      authProvider.notifyClusterDeleted(mockitoEq(deletingCluster.auditInfo.creator), mockitoEq(deletingCluster.auditInfo.creator), mockitoEq(deletingCluster.googleProject), mockitoEq(deletingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    withClusterSupervisor(dao, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, false) { actor =>
      actor ! ClusterDeleted(savedDeletingCluster)
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])

      val updatedCluster = dbFutureValue { _.clusterQuery.getClusterById(savedDeletingCluster.id) }
      updatedCluster shouldBe 'defined
      updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Deleted)
      updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe None
      updatedCluster.map(_.instances) shouldBe Some(Set.empty)

      verify(storageDAO, times(1)).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
      verify(authProvider).notifyClusterDeleted(mockitoEq(deletingCluster.auditInfo.creator), mockitoEq(deletingCluster.auditInfo.creator), mockitoEq(deletingCluster.googleProject), mockitoEq(deletingCluster.clusterName))(any[ExecutionContext])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status ERROR and error code UNKNOWN
  // - compute DAO returns status Running
  // Post:
  // - old cluster status is set to Deleted in the DB
  // - new cluster exists with the original cluster name
  // - new cluster has status Running and the host IP
  // - monitor actor shuts down
  it should "monitor until ERROR state with restart" in isolatedDbTest {
    val savedCreatingCluster = creatingCluster.save()
    creatingCluster shouldEqual savedCreatingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
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
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn {
      Future.successful(clusterInstances)
    } thenReturn {
      Future.successful(clusterInstances2)
    }

    when {
      gdDAO.getClusterErrorDetails(mockitoEq(creatingCluster.dataprocInfo.operationName))
    } thenReturn Future.successful(Some(ClusterErrorDetails(Code.UNKNOWN.value, Some("Test message"))))

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(())

    val newClusterId = UUID.randomUUID()
    when {
      gdDAO.createCluster(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName), any[MachineConfig], any[GcsPath], any[Option[WorkbenchEmail]], any[Option[String]], any[GcsBucketName])
    } thenReturn Future.successful {
      Operation(creatingCluster.dataprocInfo.operationName.get, newClusterId)
    }

    when {
      storageDAO.setBucketAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    when {
      storageDAO.setDefaultObjectAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    when {
      storageDAO.createBucket(any[GoogleProject], any[GcsBucketName], any[List[GcsEntity]], any[List[GcsEntity]])
    } thenReturn Future.successful(GcsBucketName("my-bucket"))

    when {
      storageDAO.storeObject(any[GcsBucketName], any[GcsObjectName], any[File], any[String])
    } thenReturn Future.successful(())

    when {
      storageDAO.storeObject(any[GcsBucketName], any[GcsObjectName], any[String], any[String])
    } thenReturn Future.successful(())

    when {
      storageDAO.storeObject(any[GcsBucketName], any[GcsObjectName], any[ByteArrayInputStream], any[String])
    } thenReturn Future.successful(())

    when {
      gdDAO.getClusterMasterInstance(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(Some(InstanceKey(creatingCluster.googleProject, ZoneUri("my-zone"), InstanceName("master-instance"))))

    when {
      gdDAO.getClusterStagingBucket(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket")))

    when {
      storageDAO.deleteBucket(any[GcsBucketName], mockitoEq(true))
    } thenReturn Future.successful(())

    when {
      computeDAO.updateFirewallRule(mockitoEq(creatingCluster.googleProject), any[FirewallRule])
    } thenReturn Future.successful(())

    when {
      computeDAO.getComputeEngineDefaultServiceAccount(mockitoEq(creatingCluster.googleProject))
    } thenReturn Future.successful(Some(serviceAccountEmail))

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.addIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    when {
      iamDAO.removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    } thenReturn Future.successful(())

    when {
      iamDAO.createServiceAccountKey(any[GoogleProject], any[WorkbenchEmail])
    } thenReturn Future.successful(serviceAccountKey)

    val authProvider = mock[LeoAuthProvider]

    when {
      authProvider.notifyClusterDeleted(mockitoEq(creatingCluster.auditInfo.creator), mockitoEq(creatingCluster.auditInfo.creator), mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, false) { actor =>
      actor ! ClusterCreated(savedCreatingCluster)

      // Expect 3 Terminated messages:
      // 1. original cluster monitor, terminates at Error status
      // 2. deletion monitor, terminates at Deleted status
      // 3. new Cluster creating monitor, terminates at Running status
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])

      val oldCluster = dbFutureValue { _.clusterQuery.getClusterById(savedCreatingCluster.id) }
      oldCluster shouldBe 'defined
      oldCluster.map(_.status) shouldBe Some(ClusterStatus.Deleted)
      oldCluster.flatMap(_.dataprocInfo.hostIp) shouldBe None
      oldCluster.map(_.instances) shouldBe Some(Set.empty)

      val newCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
      val newClusterBucket = dbFutureValue { _.clusterQuery.getInitBucket(creatingCluster.googleProject, creatingCluster.clusterName) }
      newCluster shouldBe 'defined
      newClusterBucket shouldBe 'defined

      newCluster.flatMap(_.dataprocInfo.googleId) shouldBe Some(newClusterId)
      newCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
      newCluster.flatMap(_.dataprocInfo.hostIp) shouldBe Some(IP("1.2.3.4"))
      newCluster.map(_.instances.count(_.status == InstanceStatus.Running)) shouldBe Some(3)
      newCluster.flatMap(_.userJupyterExtensionConfig) shouldBe Some(userExtConfig)

      verify(storageDAO, never).deleteBucket(mockitoEq(newClusterBucket.get.bucketName), any[Boolean])
      // should only add/remove the dataproc.worker role 1 time
      verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).addIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, if (notebookServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
      verify(authProvider).notifyClusterDeleted(mockitoEq(creatingCluster.auditInfo.creator), mockitoEq(creatingCluster.auditInfo.creator), mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))(any[ExecutionContext])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Deleting
  // - dataproc DAO returns status RUNNING
  // Post:
  // - cluster is not changed in the DB
  // - monitor actor does not shut down
  it should "not restart a deleting cluster" in isolatedDbTest {
    val savedDeletingCluster = deletingCluster.save()
    deletingCluster shouldEqual savedDeletingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(deletingCluster.googleProject), mockitoEq(deletingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Running)

    when {
      gdDAO.getClusterInstances(mockitoEq(deletingCluster.googleProject), mockitoEq(deletingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val iamDAO = mock[GoogleIamDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, true) { actor =>
      actor ! ClusterCreated(savedDeletingCluster)
      expectNoMessage(clusterMonitorPatience)

      val updatedCluster = dbFutureValue { _.clusterQuery.getDeletingClusterByName(deletingCluster.googleProject, deletingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster shouldBe Some(savedDeletingCluster)

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }

  it should "create two clusters for the same user" in isolatedDbTest {
    val savedCreatingCluster = creatingCluster.save()
    creatingCluster shouldEqual savedCreatingCluster

    val creatingCluster2 = creatingCluster.copy(
      clusterName = ClusterName(creatingCluster.clusterName.value + "_2"),
      dataprocInfo = creatingCluster.dataprocInfo.copy(googleId = Option(UUID.randomUUID()),
                                                       hostIp = Option(IP("5.6.7.8")))
    )
    val savedCreatingCluster2 = creatingCluster2.save()
    creatingCluster2 shouldEqual savedCreatingCluster2

    val gdDAO = mock[GoogleDataprocDAO]
    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Running)
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster2.googleProject), mockitoEq(creatingCluster2.clusterName))
    } thenReturn Future.successful(ClusterStatus.Running)

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)
    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster2.googleProject), mockitoEq(creatingCluster2.clusterName))
    } thenReturn Future.successful(clusterInstances2)

    when {
      gdDAO.getClusterMasterInstance(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(Some(InstanceKey(creatingCluster.googleProject, ZoneUri("my-zone"), InstanceName("master-instance"))))
    when {
      gdDAO.getClusterMasterInstance(mockitoEq(creatingCluster2.googleProject), mockitoEq(creatingCluster2.clusterName))
    } thenReturn Future.successful(Some(InstanceKey(creatingCluster.googleProject, ZoneUri("my-zone"), InstanceName("master-instance"))))

    when {
      gdDAO.getClusterStagingBucket(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket")))
    when {
      gdDAO.getClusterStagingBucket(mockitoEq(creatingCluster2.googleProject), mockitoEq(creatingCluster2.clusterName))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket2")))

    val storageDAO = mock[GoogleStorageDAO]
    when {
      storageDAO.deleteBucket(any[GcsBucketName], any[Boolean])
    } thenReturn Future.successful(())

    when {
      storageDAO.setBucketAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    when {
      storageDAO.setDefaultObjectAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    // Create the first cluster
    val supervisor = withClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, false) { actor =>
      actor ! ClusterCreated(savedCreatingCluster)
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])

      val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
      updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe Some(IP("1.2.3.4"))
      updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      // removeIamRolesForUser should not have been called because there is still a creating cluster in the DB
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])

      // Create the second cluster
      actor ! ClusterCreated(savedCreatingCluster2)
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])

      val updatedCluster2 = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster2.googleProject, creatingCluster2.clusterName) }
      updatedCluster2 shouldBe 'defined
      updatedCluster2.map(_.status) shouldBe Some(ClusterStatus.Running)
      updatedCluster2.flatMap(_.dataprocInfo.hostIp) shouldBe Some(IP("1.2.3.4")) // same ip because we're using the same set of instances
      updatedCluster2.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2).map(modifyInstance))

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      // removeIamRolesForUser should have been called once now
      verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Stopping
  // - dataproc DAO returns status RUNNING
  // - compute DAO returns all instances stopped
  // Post:
  // - cluster is updated in the DB with status Stopped
  // - instances are populated in the DB
  // - monitor actor shuts down
  it should "monitor until STOPPED state" in isolatedDbTest {
    val savedStoppingCluster = stoppingCluster.save()
    stoppingCluster shouldEqual savedStoppingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(stoppingCluster.googleProject), mockitoEq(stoppingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Running)
    when {
      gdDAO.getClusterInstances(mockitoEq(stoppingCluster.googleProject), mockitoEq(stoppingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)

    val computeDAO = stubComputeDAO(InstanceStatus.Stopped)
    val storageDAO = mock[GoogleStorageDAO]
    val iamDAO = mock[GoogleIamDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, false) { actor =>
      actor ! ClusterCreated(savedStoppingCluster)
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])

      val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(stoppingCluster.googleProject, stoppingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Stopped)
      updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe None
      updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2).map(_.copy(status = InstanceStatus.Stopped)))

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Starting
  // - dataproc DAO returns status RUNNING
  // - compute DAO returns all instances running
  // Post:
  // - cluster is updated in the DB with status Running with IP
  // - instances are populated in the DB
  // - monitor actor shuts down
  it should "monitor from STARTING to RUNNING state" in isolatedDbTest {
    val savedStartingCluster = startingCluster.save()
    startingCluster shouldEqual savedStartingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Running)
    when {
      gdDAO.getClusterInstances(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)
    when {
      gdDAO.getClusterMasterInstance(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn Future.successful(Some(masterInstance.key))
    when {
      gdDAO.getClusterStagingBucket(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket")))

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val storageDAO = mock[GoogleStorageDAO]
    when {
      storageDAO.deleteBucket(any[GcsBucketName], any[Boolean])
    } thenReturn Future.successful(())

    when {
      storageDAO.setBucketAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    when {
      storageDAO.setDefaultObjectAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())
    val authProvider = mock[LeoAuthProvider]

    val jupyterDAO = mock[JupyterDAO]
    when {
      jupyterDAO.getStatus(any[GoogleProject], any[ClusterName])
    } thenReturn Future.successful(true)

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, jupyterDAO, false) { actor =>
      actor ! ClusterCreated(savedStartingCluster)
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])

      val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(startingCluster.googleProject, startingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
      updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe Some(IP("1.2.3.4"))
      updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      // starting a cluster should not touch IAM roles
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Starting
  // - dataproc DAO returns status RUNNING
  // - Jupyter DAO returns status as False
  // - compute DAO returns all instances running
  // Post:
  // - cluster is not updated in the DB with status Running
  it should "cluster should not got from STARTING to RUNNING if Jupyter is not ready" in isolatedDbTest {
    val savedStartingCluster = startingCluster.save()
    savedStartingCluster shouldEqual startingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Running)
    when {
      gdDAO.getClusterInstances(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)
    when {
      gdDAO.getClusterMasterInstance(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn Future.successful(Some(masterInstance.key))
    when {
      gdDAO.getClusterStagingBucket(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket")))

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val storageDAO = mock[GoogleStorageDAO]
    when {
      storageDAO.deleteBucket(any[GcsBucketName], any[Boolean])
    } thenReturn Future.successful(())

    when {
      storageDAO.setBucketAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    when {
      storageDAO.setDefaultObjectAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())
    val authProvider = mock[LeoAuthProvider]


    val jupyterDAO = mock[JupyterDAO]
    when {
      jupyterDAO.getStatus(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn Future.successful(false)

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, jupyterDAO, false) { actor =>
      actor ! ClusterCreated(savedStartingCluster)
      expectNoMessage(clusterMonitorPatience)

      val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(startingCluster.googleProject, startingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Starting)
      actor ! PoisonPill
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])
    }
  }
  
  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status RUNNING
  // - compute DAO returns status RUNNING
  // Post:
  // - cluster is updated in the DB with status Stopped
  // - instances are populated in the DB
  // - monitor actor shuts down
  it should "stop a cluster after creation" in isolatedDbTest {
    val savedCreatingCluster = creatingCluster.save()
    creatingCluster shouldEqual savedCreatingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Running)

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(Map((Master: DataprocRole) -> Set(masterInstance.key)))

    when {
      gdDAO.getClusterMasterInstance(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(Some(masterInstance.key))

    when {
      gdDAO.getClusterStagingBucket(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket")))

    val computeDAO = mock[GoogleComputeDAO]
    when {
      computeDAO.getInstance(mockitoEq(masterInstance.key))
    } thenReturn {
      Future.successful(Some(masterInstance.copy(status = InstanceStatus.Running)))
    } thenReturn {
      Future.successful(Some(masterInstance.copy(status = InstanceStatus.Stopped)))
    }

    when {
      computeDAO.addInstanceMetadata(mockitoEq(masterInstance.key), any[Map[String, String]])
    } thenReturn Future.successful(())

    when {
      computeDAO.stopInstance(mockitoEq(masterInstance.key))
    } thenReturn Future.successful(())

    val storageDAO = mock[GoogleStorageDAO]
    when {
      storageDAO.deleteBucket(any[GcsBucketName], any[Boolean])
    } thenReturn Future.successful(())

    when {
      storageDAO.setBucketAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    when {
      storageDAO.setDefaultObjectAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, false) { actor =>
      actor ! ClusterCreated(savedCreatingCluster, stopAfterCreate = true)

      // expect 2 shutdowns: 1 after cluster creation and 1 after cluster stop
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])
      expectMsgClass(clusterMonitorPatience, classOf[Terminated])

      val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Stopped)
      updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe None
      updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance.copy(status = InstanceStatus.Stopped)))

      verify(storageDAO, never()).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Error
  // - dataproc DAO throws exception
  // Post:
  // - cluster is not updated in the DB
  // - monitor actor shuts down
  // - dataproc DAO should not have been called
  it should "stop monitoring a cluster in Error status" in isolatedDbTest {
    val savedErrorCluster = errorCluster.save()
    savedErrorCluster shouldEqual errorCluster

    val gdDAO = mock[GoogleDataprocDAO]
    val computeDAO = mock[GoogleComputeDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val iamDAO = mock[GoogleIamDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider, mockJupyterDAO, false) { actor =>
      actor ! ClusterCreated(savedErrorCluster, stopAfterCreate = true)

      expectMsgClass(clusterMonitorPatience, classOf[Terminated])

      val dbCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(errorCluster.googleProject, errorCluster.clusterName) }
      dbCluster shouldBe 'defined
      dbCluster.get shouldEqual errorCluster

      verify(gdDAO, never()).getClusterStatus(any[GoogleProject], any[ClusterName])
    }
  }
}
