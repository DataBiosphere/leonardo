package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.io.{ByteArrayInputStream, File}
import java.time.Instant
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, Terminated}
import akka.testkit.TestKit
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.{Master, Worker}
import org.broadinstitute.dsde.workbench.leonardo.model.google.InstanceStatus
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterCreated, ClusterDeleted}
import org.broadinstitute.dsde.workbench.leonardo.service.{BucketHelper, LeonardoService}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.GcsRole
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsEntity, GcsObjectName, GcsPath, GoogleProject, ServiceAccountKeyId}
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 9/6/17.
  */
class ClusterMonitorSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with MockitoSugar with BeforeAndAfterAll with TestComponent with CommonTestData with GcsPathUtils { testKit =>
  val masterInstance = Instance(
    InstanceKey(
      project,
      ZoneUri("my-zone"),
      InstanceName("master-instance")),
    googleId = BigInt(12345),
    status = InstanceStatus.Running,
    ip = Some(IP("1.2.3.4")),
    dataprocRole = Some(DataprocRole.Master),
    createdDate = Instant.now(),
    destroyedDate = None)

  val workerInstance1 = Instance(
    InstanceKey(
      project,
      ZoneUri("my-zone"),
      InstanceName("worker-instance-1")),
    googleId = BigInt(23456),
    status = InstanceStatus.Running,
    ip = Some(IP("1.2.3.5")),
    dataprocRole = Some(DataprocRole.Worker),
    createdDate = Instant.now(),
    destroyedDate = None)

  val workerInstance2 = Instance(
    InstanceKey(
      project,
      ZoneUri("my-zone"),
      InstanceName("worker-instance-2")),
    googleId = BigInt(34567),
    status = InstanceStatus.Running,
    ip = Some(IP("1.2.3.6")),
    dataprocRole = Some(DataprocRole.Worker),
    createdDate = Instant.now(),
    destroyedDate = None)

  val creatingCluster = Cluster(
    clusterName = name1,
    googleId = UUID.randomUUID(),
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
    operationName = OperationName("op1"),
    status = ClusterStatus.Creating,
    hostIp = None,
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    None,
    None,
    Some(GcsBucketName("testStagingBucket1")),
    Set.empty
  )

  val deletingCluster = Cluster(
    clusterName = name2,
    googleId = UUID.randomUUID(),
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name2, clusterUrlBase),
    operationName = OperationName("op1"),
    status = ClusterStatus.Deleting,
    hostIp = None,
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = Some(jupyterExtensionUri),
    jupyterUserScriptUri = Some(jupyterUserScriptUri),
    Some(GcsBucketName("testStagingBucket1")),
    Set(masterInstance, workerInstance1, workerInstance2)
  )

  val clusterInstances = Map(Master -> Set(masterInstance.key),
                             Worker -> Set(workerInstance1.key, workerInstance2.key))

  def stubComputeDAO(status: InstanceStatus = InstanceStatus.Running): GoogleComputeDAO = {
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
    dao
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def createClusterSupervisor(gdDAO: GoogleDataprocDAO, computeDAO: GoogleComputeDAO, iamDAO: GoogleIamDAO, storageDAO: GoogleStorageDAO, authProvider: LeoAuthProvider): ActorRef = {
    val cacheActor = system.actorOf(ClusterDnsCache.props(proxyConfig, DbSingleton.ref))
    val bucketHelper = new BucketHelper(dataprocConfig, gdDAO, computeDAO, storageDAO, serviceAccountProvider)
    val supervisorActor = system.actorOf(TestClusterSupervisorActor.props(dataprocConfig, gdDAO, computeDAO, iamDAO, storageDAO, DbSingleton.ref, cacheActor, testKit, authProvider))
    new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, gdDAO, computeDAO, iamDAO, storageDAO, DbSingleton.ref, supervisorActor, whitelistAuthProvider, serviceAccountProvider, whitelist, bucketHelper)
    supervisorActor
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
    dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster

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

    createClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider) ! ClusterCreated(creatingCluster)

    expectMsgClass(1 second, classOf[Terminated])
    val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
    updatedCluster.flatMap(_.hostIp) shouldBe Some(IP("1.2.3.4"))
    updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))

    verify(storageDAO).deleteBucket(any[GcsBucketName], mockitoEq(true))
    verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
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
      dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster

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

      createClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider) ! ClusterCreated(creatingCluster)

      expectNoMsg(1 second)

      val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster shouldBe Some(creatingCluster.copy(instances = Set(masterInstance, workerInstance1, workerInstance2)))

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
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
    dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster

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

    createClusterSupervisor(dao, computeDAO, iamDAO, storageDAO, authProvider) ! ClusterCreated(creatingCluster)

    expectNoMsg(1 second)

    val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster shouldBe Some(creatingCluster.copy(instances = Set(masterInstance, workerInstance1, workerInstance2)))

    verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
    verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
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
    dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster

    val dao = mock[GoogleDataprocDAO]
    when {
      dao.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Error)

    when {
      dao.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)

    when {
      dao.getClusterErrorDetails(mockitoEq(OperationName("op1")))
    } thenReturn Future.successful(None)

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val storageDAO = mock[GoogleStorageDAO]
    val authProvider = mock[LeoAuthProvider]

    createClusterSupervisor(dao, computeDAO, iamDAO, storageDAO, authProvider) ! ClusterCreated(creatingCluster)

    expectNoMsg(1 second)

    val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster shouldBe Some(creatingCluster.copy(instances = Set(masterInstance, workerInstance1, workerInstance2)))

    verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
    verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
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
    dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Error)

    when {
      gdDAO.getClusterErrorDetails(mockitoEq(OperationName("op1")))
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

    createClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider) ! ClusterCreated(creatingCluster)

    expectMsgClass(1 second, classOf[Terminated])
    val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Error)
    updatedCluster.flatMap(_.hostIp) shouldBe None
    updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))

    verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
    verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, if (notebookServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
  }

  // Pre:
  // - cluster exists in the DB with status Deleting
  // - dataproc DAO returns status DELETED
  // - compute DAO returns None
  // Post:
  // - cluster status is set to Deleted in the DB
  // - instances are deleted in the DB
  // - monitor actor shuts down
  it should "monitor until DELETED state" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(deletingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual deletingCluster

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

    val authProvider = mock[LeoAuthProvider]

    when {
      authProvider.notifyClusterDeleted(mockitoEq(deletingCluster.creator), mockitoEq(deletingCluster.creator), mockitoEq(deletingCluster.googleProject), mockitoEq(deletingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    createClusterSupervisor(dao, computeDAO, iamDAO, storageDAO, authProvider) ! ClusterDeleted(deletingCluster)

    expectMsgClass(1 second, classOf[Terminated])
    val updatedCluster = dbFutureValue { _.clusterQuery.getByGoogleId(deletingCluster.googleId) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Deleted)
    updatedCluster.flatMap(_.hostIp) shouldBe None
    updatedCluster.map(_.instances.map(_.status)) shouldBe Some(Set(InstanceStatus.Deleted))
    updatedCluster.map(_.instances.size) shouldBe Some(3)

    verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
    verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    verify(authProvider).notifyClusterDeleted(mockitoEq(deletingCluster.creator), mockitoEq(deletingCluster.creator), mockitoEq(deletingCluster.googleProject), mockitoEq(deletingCluster.clusterName))(any[ExecutionContext])
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
    dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val storageDAO = mock[GoogleStorageDAO]
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
    } thenReturn Future.successful(clusterInstances)

    when {
      gdDAO.getClusterErrorDetails(mockitoEq(OperationName("op1")))
    } thenReturn Future.successful(Some(ClusterErrorDetails(Code.UNKNOWN.value, Some("Test message"))))

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(())

    when {
      gdDAO.getComputeEngineDefaultServiceAccount(mockitoEq(creatingCluster.googleProject))
    } thenReturn Future.successful(Some(serviceAccountEmail))

    val newClusterId = UUID.randomUUID()
    when {
      gdDAO.createCluster(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName), any[MachineConfig], any[GcsPath], any[Option[WorkbenchEmail]], any[Option[String]], any[GcsBucketName])
    } thenReturn Future.successful {
      Operation(creatingCluster.operationName, newClusterId)
    }

    when {
      storageDAO.setBucketAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    when {
      storageDAO.setDefaultObjectAccessControl(any[GcsBucketName], any[GcsEntity], any[GcsRole])
    } thenReturn Future.successful(())

    when {
      computeDAO.updateFirewallRule(mockitoEq(creatingCluster.googleProject), any[FirewallRule])
    } thenReturn Future.successful(())

    when {
      storageDAO.createBucket(any[GoogleProject], any[GcsBucketName])
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
      authProvider.notifyClusterDeleted(mockitoEq(creatingCluster.creator), mockitoEq(creatingCluster.creator), mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    createClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider) ! ClusterCreated(creatingCluster)

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
    // TODO these are doubled for some reason
    // oldCluster.map(_.instances.map(_.status)) shouldBe Some(Set(InstanceStatus.Deleted))
    // oldCluster.map(_.instances.size) shouldBe Some(3)

    val newCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    val newClusterBucket = dbFutureValue { _.clusterQuery.getInitBucket(creatingCluster.googleProject, creatingCluster.clusterName) }
    newCluster shouldBe 'defined
    newClusterBucket shouldBe 'defined

    newCluster.map(_.googleId) shouldBe Some(newClusterId)
    newCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
    newCluster.flatMap(_.hostIp) shouldBe Some(IP("1.2.3.4"))
    // TODO newCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))

    verify(storageDAO).deleteBucket(mockitoEq(newClusterBucket.get.bucketName), mockitoEq(true))
    // should only add/remove the dataproc.worker role 1 time
    verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).addIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, if (notebookServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    verify(authProvider).notifyClusterDeleted(mockitoEq(creatingCluster.creator), mockitoEq(creatingCluster.creator), mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))(any[ExecutionContext])
  }

  // Pre:
  // - cluster exists in the DB with status Deleting
  // - dataproc DAO returns status RUNNING
  // Post:
  // - cluster is not changed in the DB
  // - monitor actor does not shut down
  it should "not restart a deleting cluster" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(deletingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual deletingCluster

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

    createClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider) ! ClusterCreated(deletingCluster)

    expectNoMsg(1 second)

    val updatedCluster = dbFutureValue { _.clusterQuery.getDeletingClusterByName(deletingCluster.googleProject, deletingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster shouldBe Some(deletingCluster)

    verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
    verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
  }

  it should "foo create two clusters for the same user" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster
    val creatingCluster2 = creatingCluster.copy(
      clusterName = ClusterName(creatingCluster.clusterName.value + "_2"),
      googleId = UUID.randomUUID(),
      hostIp = Option(IP("5.6.7.8"))
    )
    dbFutureValue { _.clusterQuery.save(creatingCluster2, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster2

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
    } thenReturn Future.successful(clusterInstances)

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
    val supervisor = createClusterSupervisor(gdDAO, computeDAO, iamDAO, storageDAO, authProvider)
    supervisor ! ClusterCreated(creatingCluster)
    expectMsgClass(1 second, classOf[Terminated])

    val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
    updatedCluster.flatMap(_.hostIp) shouldBe Some(IP("1.2.3.4"))
    updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))

    verify(storageDAO, times(1)).deleteBucket(any[GcsBucketName], any[Boolean])
    // removeIamRolesForUser should not have been called because there is still a creating cluster in the DB
    verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])

    // Create the second cluster
    supervisor ! ClusterCreated(creatingCluster2)
    expectMsgClass(1 second, classOf[Terminated])

    val updatedCluster2 = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster2.googleProject, creatingCluster2.clusterName) }
    updatedCluster2 shouldBe 'defined
    updatedCluster2.map(_.status) shouldBe Some(ClusterStatus.Running)
    updatedCluster2.flatMap(_.hostIp) shouldBe Some(IP("1.2.3.4"))  // same ip because we're using the same set of instances
    // TODO updatedCluster2.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))

    verify(storageDAO, times(2)).deleteBucket(any[GcsBucketName], any[Boolean])
    // removeIamRolesForUser should have been called once now
    verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
  }
}
