package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.io.File
import java.time.Instant
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, Terminated}
import akka.testkit.TestKit
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils, VCMockitoMatchers}
import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath}
import org.broadinstitute.dsde.workbench.leonardo.dao.DataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterCreated, ClusterDeleted}
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, ServiceAccountKeyId}
import org.mockito.ArgumentMatchers.{any, anyString, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 9/6/17.
  */
class ClusterMonitorSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with MockitoSugar with BeforeAndAfterAll with TestComponent with VCMockitoMatchers with CommonTestData with GcsPathUtils { testKit =>
  val creatingCluster = Cluster(
    clusterName = name1,
    googleId = UUID.randomUUID(),
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1),
    operationName = OperationName("op1"),
    status = ClusterStatus.Creating,
    hostIp = None,
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    None,
    None,
    Some(GcsBucketName("testStagingBucket1")))

  val deletingCluster = Cluster(
    clusterName = name2,
    googleId = UUID.randomUUID(),
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name2),
    operationName = OperationName("op1"),
    status = ClusterStatus.Deleting,
    hostIp = None,
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = jupyterExtensionUri,
    jupyterUserScriptUri = jupyterUserScript,
    Some(GcsBucketName("testStagingBucket1"))
  )

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def createClusterSupervisor(gdDAO: DataprocDAO, iamDAO: GoogleIamDAO, authProvider: LeoAuthProvider): ActorRef = {
    val cacheActor = system.actorOf(ClusterDnsCache.props(proxyConfig, DbSingleton.ref))
    val supervisorActor = system.actorOf(TestClusterSupervisorActor.props(dataprocConfig, gdDAO, iamDAO, DbSingleton.ref, cacheActor, testKit, authProvider))
    new LeonardoService(dataprocConfig, clusterFilesConfig, clusterResourcesConfig, proxyConfig, swaggerConfig, gdDAO, iamDAO, DbSingleton.ref, supervisorActor, whitelistAuthProvider, serviceAccountProvider, whitelist)
    supervisorActor
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status RUNNING
  // Post:
  // - cluster is updated in the DB with status Running and the host IP
  // - monitor actor shuts down
  "ClusterMonitorActor" should "monitor until RUNNING state" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster

    val gdDAO = mock[DataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Running)

    when {
      gdDAO.getClusterMasterInstanceIp(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(Some(IP("1.2.3.4")))

    when {
      gdDAO.deleteBucket(mockitoEq(dataprocConfig.leoGoogleProject), vcAny(GcsBucketName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    when {
      gdDAO.setStagingBucketOwnership(mockitoEq(creatingCluster))
    } thenReturn Future.successful(())

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    createClusterSupervisor(gdDAO, iamDAO, authProvider) ! ClusterCreated(creatingCluster)

    expectMsgClass(1 second, classOf[Terminated])
    val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
    updatedCluster.flatMap(_.hostIp) shouldBe Some(IP("1.2.3.4"))

    verify(gdDAO).deleteBucket(mockitoEq(dataprocConfig.leoGoogleProject), vcAny(GcsBucketName))(any[ExecutionContext])
    verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status CREATING, UNKNOWN, or UPDATING
  // Post:
  // - cluster is not changed in the DB
  // - monitor actor does not shut down
  Seq(ClusterStatus.Creating, ClusterStatus.Updating, ClusterStatus.Unknown).foreach { status =>
    it should s"monitor $status status" in isolatedDbTest {
      dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster

      val gdDAO = mock[DataprocDAO]
      when {
        gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
      } thenReturn Future.successful(status)

      val iamDAO = mock[GoogleIamDAO]
      val authProvider = mock[LeoAuthProvider]

      createClusterSupervisor(gdDAO, iamDAO, authProvider) ! ClusterCreated(creatingCluster)

      expectNoMsg(1 second)

      val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
      updatedCluster shouldBe 'defined
      updatedCluster shouldBe Some(creatingCluster)

      verify(gdDAO, never).deleteBucket(any[GoogleProject], GcsBucketName(anyString))(any[ExecutionContext])
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status RUNNING, but no IP address
  // Post:
  // - cluster is not changed in the DB
  // - monitor actor does not shut down
  it should "keep monitoring in RUNNING state with no IP" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster

    val dao = mock[DataprocDAO]
    when {
      dao.getClusterStatus(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Running)

    when {
      dao.getClusterMasterInstanceIp(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(None)

    val iamDAO = mock[GoogleIamDAO]
    val authProvider = mock[LeoAuthProvider]

    createClusterSupervisor(dao, iamDAO, authProvider) ! ClusterCreated(creatingCluster)

    expectNoMsg(1 second)

    val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster shouldBe Some(creatingCluster)

    verify(dao, never).deleteBucket(any[GoogleProject], GcsBucketName(anyString))(any[ExecutionContext])
    verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status ERROR, but no error code
  // Post:
  // - cluster is not changed in the DB
  // - monitor actor does not shut down
  it should "keep monitoring in ERROR state with no error code" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster

    val dao = mock[DataprocDAO]
    when {
      dao.getClusterStatus(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Error)

    when {
      dao.getClusterErrorDetails(vcEq(OperationName("op1")))(any[ExecutionContext])
    } thenReturn Future.successful(None)

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    createClusterSupervisor(dao, iamDAO, authProvider) ! ClusterCreated(creatingCluster)

    expectNoMsg(1 second)

    val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster shouldBe Some(creatingCluster)

    verify(dao, never).deleteBucket(any[GoogleProject], GcsBucketName(anyString))(any[ExecutionContext])
    verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
  }

  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status ERROR and error code CANCELLED
  // Post:
  // - cluster status is set to Error in the DB
  // - monitor actor shuts down
  it should "monitor until ERROR state with no restart" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster

    val gdDAO = mock[DataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Error)

    when {
      gdDAO.getClusterErrorDetails(vcEq(OperationName("op1")))(any[ExecutionContext])
    } thenReturn Future.successful(Some(ClusterErrorDetails(Code.CANCELLED.value, Some("test message"))))

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    when {
      iamDAO.removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    createClusterSupervisor(gdDAO, iamDAO, authProvider) ! ClusterCreated(creatingCluster)

    expectMsgClass(1 second, classOf[Terminated])
    val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Error)
    updatedCluster.flatMap(_.hostIp) shouldBe None

    verify(gdDAO, never).deleteBucket(any[GoogleProject], GcsBucketName(anyString))(any[ExecutionContext])
    verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, if (notebookServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
  }

  // Pre:
  // - cluster exists in the DB with status Deleting
  // - dataproc DAO returns status DELETED
  // Post:
  // - cluster status is set to Deleted in the DB
  // - monitor actor shuts down
  it should "monitor until DELETED state" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(deletingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual deletingCluster

    val dao = mock[DataprocDAO]
    when {
      dao.getClusterStatus(mockitoEq(deletingCluster.googleProject), vcEq(deletingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Deleted)

    val iamDAO = mock[GoogleIamDAO]

    val authProvider = mock[LeoAuthProvider]

    when {
      authProvider.notifyClusterDeleted(mockitoEq(deletingCluster.creator), mockitoEq(deletingCluster.creator), mockitoEq(deletingCluster.googleProject), vcEq(deletingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    createClusterSupervisor(dao, iamDAO, authProvider) ! ClusterDeleted(deletingCluster)

    expectMsgClass(1 second, classOf[Terminated])
    val updatedCluster = dbFutureValue { _.clusterQuery.getByGoogleId(deletingCluster.googleId) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Deleted)
    updatedCluster.flatMap(_.hostIp) shouldBe None

    verify(dao, never).deleteBucket(any[GoogleProject], GcsBucketName(anyString))(any[ExecutionContext])
    verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    verify(authProvider).notifyClusterDeleted(mockitoEq(deletingCluster.creator), mockitoEq(deletingCluster.creator), mockitoEq(deletingCluster.googleProject), vcEq(deletingCluster.clusterName))(any[ExecutionContext])
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

    val gdDAO = mock[DataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
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
      gdDAO.getClusterErrorDetails(vcEq(OperationName("op1")))(any[ExecutionContext])
    } thenReturn Future.successful(Some(ClusterErrorDetails(Code.UNKNOWN.value, Some("Test message"))))

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    val newClusterId = UUID.randomUUID()
    when {
      gdDAO.createCluster(mockitoEq(creatingCluster.creator), mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName), any[ClusterRequest], vcAny[GcsBucketName], any[ServiceAccountInfo], vcAny[GcsBucketName])(any[ExecutionContext])
    } thenReturn Future.successful {
      creatingCluster.copy(googleId=newClusterId)
    }

    when {
      gdDAO.setStagingBucketOwnership(mockitoEq(creatingCluster.copy(googleId=newClusterId)))
    } thenReturn Future.successful(())

    when {
      gdDAO.updateFirewallRule(mockitoEq(creatingCluster.googleProject))
    } thenReturn Future.successful(())

    when {
      gdDAO.createInitBucket(any[GoogleProject], any[GoogleProject], vcAny[GcsBucketName], any[ServiceAccountInfo])
    } thenReturn Future.successful(GcsBucketName("my-bucket"))

    when {
      gdDAO.createStagingBucket(any[GoogleProject], any[GoogleProject], vcAny[GcsBucketName], any[ServiceAccountInfo], any[List[WorkbenchEmail]], any[List[WorkbenchEmail]])
    } thenReturn Future.successful(GcsBucketName("my-bucket"))


    when {
      gdDAO.uploadToBucket(any[GoogleProject], any[GcsPath], any[File])
    } thenReturn Future.successful(())

    when {
      gdDAO.uploadToBucket(any[GoogleProject], any[GcsPath], anyString)
    } thenReturn Future.successful(())

    when {
      gdDAO.getClusterMasterInstanceIp(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(Some(IP("1.2.3.4")))

    when {
      gdDAO.deleteBucket(mockitoEq(dataprocConfig.leoGoogleProject), vcAny(GcsBucketName))(any[ExecutionContext])
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
      authProvider.notifyClusterDeleted(mockitoEq(creatingCluster.creator), mockitoEq(creatingCluster.creator), mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    createClusterSupervisor(gdDAO, iamDAO, authProvider) ! ClusterCreated(creatingCluster)

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

    val newCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    val newClusterBucket = dbFutureValue { _.clusterQuery.getInitBucket(creatingCluster.googleProject, creatingCluster.clusterName.string) }
    newCluster shouldBe 'defined
    newClusterBucket shouldBe 'defined

    newCluster.map(_.googleId) shouldBe Some(newClusterId)
    newCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
    newCluster.flatMap(_.hostIp) shouldBe Some(IP("1.2.3.4"))

    verify(gdDAO).deleteBucket(mockitoEq(dataprocConfig.leoGoogleProject), vcEq(newClusterBucket.get.bucketName))(any[ExecutionContext])
    // should only add/remove the dataproc.worker role 1 time
    verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).addIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, if (notebookServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    verify(authProvider).notifyClusterDeleted(mockitoEq(creatingCluster.creator), mockitoEq(creatingCluster.creator), mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
  }

  // Pre:
  // - cluster exists in the DB with status Deleting
  // - dataproc DAO returns status RUNNING
  // Post:
  // - cluster is not changed in the DB
  // - monitor actor does not shut down
  it should "not restart a deleting cluster" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(deletingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual deletingCluster

    val gdDAO = mock[DataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(deletingCluster.googleProject), vcEq(deletingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Running)

    val iamDAO = mock[GoogleIamDAO]
    val authProvider = mock[LeoAuthProvider]

    createClusterSupervisor(gdDAO, iamDAO, authProvider) ! ClusterCreated(deletingCluster)

    expectNoMsg(1 second)

    val updatedCluster = dbFutureValue { _.clusterQuery.getDeletingClusterByName(deletingCluster.googleProject, deletingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster shouldBe Some(deletingCluster)

    verify(gdDAO, never).deleteBucket(any[GoogleProject], GcsBucketName(anyString))(any[ExecutionContext])
    verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
  }

  it should "create two clusters for the same user" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(creatingCluster, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster
    val creatingCluster2 = creatingCluster.copy(
      clusterName = creatingCluster.clusterName + "_2",
      googleId = UUID.randomUUID(),
      hostIp = Option(IP("5.6.7.8"))
    )
    dbFutureValue { _.clusterQuery.save(creatingCluster2, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual creatingCluster2

    val gdDAO = mock[DataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Running)
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster2.googleProject), vcEq(creatingCluster2.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(ClusterStatus.Running)

    when {
      gdDAO.getClusterMasterInstanceIp(mockitoEq(creatingCluster.googleProject), vcEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(Some(IP("1.2.3.4")))
    when {
      gdDAO.getClusterMasterInstanceIp(mockitoEq(creatingCluster2.googleProject), vcEq(creatingCluster2.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(Some(IP("5.6.7.8")))

    when {
      gdDAO.deleteBucket(mockitoEq(dataprocConfig.leoGoogleProject), vcAny(GcsBucketName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    when {
      gdDAO.setStagingBucketOwnership(mockitoEq(creatingCluster))
    } thenReturn Future.successful(())
    when {
      gdDAO.setStagingBucketOwnership(mockitoEq(creatingCluster2))
    } thenReturn Future.successful(())

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    // Create the first cluster
    val supervisor = createClusterSupervisor(gdDAO, iamDAO, authProvider)
    supervisor ! ClusterCreated(creatingCluster)
    expectMsgClass(1 second, classOf[Terminated])

    val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
    updatedCluster shouldBe 'defined
    updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
    updatedCluster.flatMap(_.hostIp) shouldBe Some(IP("1.2.3.4"))

    verify(gdDAO, times(1)).deleteBucket(mockitoEq(dataprocConfig.leoGoogleProject), vcAny(GcsBucketName))(any[ExecutionContext])
    // removeIamRolesForUser should not have been called because there is still a creating cluster in the DB
    verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])

    // Create the second cluster
    supervisor ! ClusterCreated(creatingCluster2)
    expectMsgClass(1 second, classOf[Terminated])

    val updatedCluster2 = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster2.googleProject, creatingCluster2.clusterName) }
    updatedCluster2 shouldBe 'defined
    updatedCluster2.map(_.status) shouldBe Some(ClusterStatus.Running)
    updatedCluster2.flatMap(_.hostIp) shouldBe Some(IP("5.6.7.8"))

    verify(gdDAO, times(2)).deleteBucket(mockitoEq(dataprocConfig.leoGoogleProject), vcAny(GcsBucketName))(any[ExecutionContext])
    // removeIamRolesForUser should have been called once now
    verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
    verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
  }
}
