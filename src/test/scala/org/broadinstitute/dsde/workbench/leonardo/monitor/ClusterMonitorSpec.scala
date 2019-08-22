package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.io.{ByteArrayInputStream, File}
import java.time.Instant
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, Terminated}
import akka.testkit.TestKit
import cats.effect.IO
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.{GoogleIamDAO, GoogleProjectDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.google2.mock.FakeGoogleStorageInterpreter
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.{Master, Worker}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{InstanceStatus, _}
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper}
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsLifecycleTypes.GcsLifecycleType
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.GcsRole
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsEntity, GcsObjectName, GoogleProject, ServiceAccountKeyId}
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Random, Try}

/**
  * Created by rtitle on 9/6/17.
  */
class ClusterMonitorSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with MockitoSugar with BeforeAndAfterAll with TestComponent with CommonTestData with GcsPathUtils with Eventually { testKit =>

  val creatingCluster = makeCluster(1).copy(serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
                                            dataprocInfo = makeDataprocInfo(1).copy(hostIp = None),
                                            status = ClusterStatus.Creating,
                                            userJupyterExtensionConfig = Some(userExtConfig),
                                            stopAfterCreation = false)

  val deletingCluster = makeCluster(2).copy(serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
                                            status = ClusterStatus.Deleting,
                                            instances = Set(masterInstance, workerInstance1, workerInstance2))

  val stoppingCluster = makeCluster(3).copy(serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
                                            dataprocInfo = makeDataprocInfo(1).copy(hostIp = None),
                                            status = ClusterStatus.Stopping)

  val startingCluster = makeCluster(4).copy(serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
                                            status = ClusterStatus.Starting,
                                            clusterImages = Set(ClusterImage(ClusterTool.RStudio, "rstudio_image", Instant.now()), ClusterImage(ClusterTool.Jupyter, "jupyter_image", Instant.now())))

  val errorCluster = makeCluster(5).copy(serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
                                         status = ClusterStatus.Error)

  val stoppedCluster = makeCluster(6).copy(serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
                                           dataprocInfo = makeDataprocInfo(1).copy(hostIp = None),
                                           status = ClusterStatus.Stopped,
                                           clusterImages = Set(ClusterImage(ClusterTool.RStudio, "rstudio_image", Instant.now())))

  val clusterInstances = Map(Master -> Set(masterInstance.key),
                             Worker -> Set(workerInstance1.key, workerInstance2.key))

  val clusterInstances2 = clusterInstances.mapValues(_.map(modifyInstanceKey))

  val clusterMonitorPatience = 10 seconds

  implicit val monitorPat = PatienceConfig(timeout = scaled(Span(30, Seconds)), interval = scaled(Span(2, Seconds)))

  implicit val cs = IO.contextShift(system.dispatcher)


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
    when {
      dao.getGoogleApiServiceAccount(any[GoogleProject])
    } thenReturn Future.successful(Some(WorkbenchEmail("test@example.com")))
    dao
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def createClusterSupervisor(gdDAO: GoogleDataprocDAO, computeDAO: GoogleComputeDAO, iamDAO: GoogleIamDAO, projectDAO: GoogleProjectDAO, storageDAO: GoogleStorageDAO, storage2DAO: GoogleStorageService[IO], authProvider: LeoAuthProvider, jupyterDAO: JupyterDAO, rstudioDAO: RStudioDAO, welderDAO: WelderDAO): ActorRef = {
    val bucketHelper = new BucketHelper(dataprocConfig, gdDAO, computeDAO, storageDAO, serviceAccountProvider)
    val clusterHelper = new ClusterHelper(dataprocConfig, gdDAO, computeDAO, iamDAO)
    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => new MockGoogleStorageDAO
    val leoService = new LeonardoService(dataprocConfig, MockWelderDAO, clusterFilesConfig, clusterResourcesConfig, clusterDefaultsConfig, proxyConfig, swaggerConfig, autoFreezeConfig, gdDAO, computeDAO, projectDAO, storageDAO, mockPetGoogleStorageDAO, DbSingleton.ref, whitelistAuthProvider, serviceAccountProvider, bucketHelper, clusterHelper, contentSecurityPolicy)
    val supervisorActor = system.actorOf(TestClusterSupervisorActor.props(monitorConfig, dataprocConfig, clusterBucketConfig, gdDAO, computeDAO, storageDAO, storage2DAO, DbSingleton.ref, testKit, authProvider, autoFreezeConfig, jupyterDAO, rstudioDAO, welderDAO, leoService, clusterHelper))

    supervisorActor
  }

  def withClusterSupervisor[T](gdDAO: GoogleDataprocDAO, computeDAO: GoogleComputeDAO, iamDAO: GoogleIamDAO, projectDAO: GoogleProjectDAO, storageDAO: GoogleStorageDAO, storage2DAO: GoogleStorageService[IO], authProvider: LeoAuthProvider, jupyterDAO: JupyterDAO = MockJupyterDAO, rstudioDAO: RStudioDAO = MockRStudioDAO, welderDAO: WelderDAO = MockWelderDAO, runningChild: Boolean = true)(testCode: ActorRef => T): T = {
    val supervisor = createClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, storage2DAO, authProvider, jupyterDAO, rstudioDAO, welderDAO)
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

    val projectDAO = mock[GoogleProjectDAO]
    when {
      projectDAO.getLabels(any[String])
    } thenReturn Future.successful(Map("key" -> "value"))

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
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO, false) { actor =>

      eventually {
        val updatedCluster = dbFutureValue {
          _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
        updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe Some(IP("1.2.3.4"))
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))
      }
      verify(storageDAO, never()).deleteBucket(any[GcsBucketName], any[Boolean])
      val dpWorkerTimes = if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()
      verify(iamDAO, dpWorkerTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      val imageUserTimes = if (dataprocConfig.customDataprocImage.isDefined) times(1) else never()
      verify(iamDAO, imageUserTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/compute.imageUser")))
      verify(iamDAO, if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeIamRolesForUser(mockitoEq(creatingCluster.googleProject), any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
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
      val projectDAO = mock[GoogleProjectDAO]
      val iamDAO = mock[GoogleIamDAO]
      val storageDAO = mock[GoogleStorageDAO]
      val authProvider = mock[LeoAuthProvider]

      withClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO, true) { actor =>

        eventually {
          val updatedCluster = dbFutureValue {
            _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
          }
          updatedCluster shouldBe 'defined
          updatedCluster shouldBe Some(savedCreatingCluster.copy(instances = Set(masterInstance, workerInstance1, workerInstance2)))
        }
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
    val projectDAO = mock[GoogleProjectDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(dao, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO,true) { actor =>

      eventually {
        val updatedCluster = dbFutureValue {
          _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster shouldBe Some(savedCreatingCluster.copy(instances = Set(masterInstance, workerInstance1, workerInstance2)))
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
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
  it should "monitor until ERROR state with no restart and getClusterStatus returns Some(ErrorDetail) " in isolatedDbTest {
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
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
    } thenReturn Future.successful(())

    when {
      iamDAO.removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    } thenReturn Future.successful(())

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val storageDAO = mock[GoogleStorageDAO]
    val projectDAO = mock[GoogleProjectDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO, false) { actor =>

      eventually {
        val updatedCluster = dbFutureValue {
          _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Error)
        updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe None
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      val dpWorkerTimes = if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()
      verify(iamDAO, dpWorkerTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      val imageUserTimes = if (dataprocConfig.customDataprocImage.isDefined) times(1) else never()
      verify(iamDAO, imageUserTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/compute.imageUser")))
      verify(iamDAO, if (notebookServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }


  // Pre:
  // - cluster exists in the DB with status Creating
  // - dataproc DAO returns status ERROR and no ERROR CODE
  // - compute DAO returns RUNNING
  // Post:
  // - cluster status is set to Error in the DB
  // - instances are populated in the DB
  // - monitor actor shuts down
  it should "monitor until ERROR state with no restart and getClusterErrorDetails returns None " in isolatedDbTest {
    val savedCreatingCluster = creatingCluster.save()
    creatingCluster shouldEqual savedCreatingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Error)

    when {
      gdDAO.getClusterErrorDetails(mockitoEq(creatingCluster.dataprocInfo.operationName))
    } thenReturn Future.successful(None)

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(())

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
    } thenReturn Future.successful(())

    when {
      iamDAO.removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    } thenReturn Future.successful(())

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val storageDAO = mock[GoogleStorageDAO]
    val projectDAO = mock[GoogleProjectDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO, false) { actor =>

      eventually {
        val updatedCluster = dbFutureValue {
          _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Error)
        updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe None
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      val dpWorkerTimes = if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()
      verify(iamDAO, dpWorkerTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      val imageUserTimes = if (dataprocConfig.customDataprocImage.isDefined) times(1) else never()
      verify(iamDAO, imageUserTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/compute.imageUser")))
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
    val projectDAO = mock[GoogleProjectDAO]
    val iamDAO = mock[GoogleIamDAO]

    val storageDAO = mock[GoogleStorageDAO]
    when {
      storageDAO.setBucketLifecycle(any[GcsBucketName], any[Int], any[GcsLifecycleType])
    } thenReturn Future.successful(())

    when {
      storageDAO.deleteBucket(any[GcsBucketName], any[Boolean])
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    when {
      authProvider.notifyClusterDeleted(mockitoEq(deletingCluster.internalId), mockitoEq(deletingCluster.auditInfo.creator), mockitoEq(deletingCluster.auditInfo.creator), mockitoEq(deletingCluster.googleProject), mockitoEq(deletingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    withClusterSupervisor(dao, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO, false) { actor =>

      eventually {
        val updatedCluster = dbFutureValue {
          _.clusterQuery.getClusterById(savedDeletingCluster.id)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Deleted)
        updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe None
        updatedCluster.map(_.instances) shouldBe Some(Set.empty)
      }
      verify(storageDAO, times(1)).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(storageDAO, times(1)).setBucketLifecycle(any[GcsBucketName], any[Int], any[GcsLifecycleType])
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
      verify(authProvider).notifyClusterDeleted(mockitoEq(deletingCluster.internalId), mockitoEq(deletingCluster.auditInfo.creator), mockitoEq(deletingCluster.auditInfo.creator), mockitoEq(deletingCluster.googleProject), mockitoEq(deletingCluster.clusterName))(any[ExecutionContext])
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
    val projectDAO = mock[GoogleProjectDAO]
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
      gdDAO.createCluster(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName), any[CreateClusterConfig])
    } thenReturn Future.successful {
      Operation(creatingCluster.dataprocInfo.operationName.get, newClusterId)
    }

    when {
      projectDAO.getLabels(any[String])
    } thenReturn Future.successful(Map("key" -> "value"))

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
      storageDAO.setBucketLifecycle(any[GcsBucketName], any[Int], any[GcsLifecycleType])
    } thenReturn Future.successful(())

    when {
      computeDAO.updateFirewallRule(mockitoEq(creatingCluster.googleProject), any[FirewallRule])
    } thenReturn Future.successful(())

    when {
      computeDAO.getComputeEngineDefaultServiceAccount(mockitoEq(creatingCluster.googleProject))
    } thenReturn Future.successful(Some(serviceAccountEmail))

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.addIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
    } thenReturn Future.successful(())

    when {
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
    } thenReturn Future.successful(())

    when {
      iamDAO.removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    } thenReturn Future.successful(())

    when {
      iamDAO.createServiceAccountKey(any[GoogleProject], any[WorkbenchEmail])
    } thenReturn Future.successful(serviceAccountKey)

    val authProvider = mock[LeoAuthProvider]

    when {
      authProvider.notifyClusterDeleted(mockitoEq(creatingCluster.internalId), mockitoEq(creatingCluster.auditInfo.creator), mockitoEq(creatingCluster.auditInfo.creator), mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))(any[ExecutionContext])
    } thenReturn Future.successful(())

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO, false) { actor =>

      eventually {
        val oldCluster = dbFutureValue {
          _.clusterQuery.getClusterById(savedCreatingCluster.id)
        }

        oldCluster shouldBe 'defined
        oldCluster.map(_.status) shouldBe Some(ClusterStatus.Deleted)
        oldCluster.flatMap(_.dataprocInfo.hostIp) shouldBe None
        oldCluster.map(_.instances) shouldBe Some(Set.empty)
        oldCluster.flatMap(_.userJupyterExtensionConfig) shouldBe Some(userExtConfig)

        val newCluster = dbFutureValue {
          _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        val newClusterBucket = dbFutureValue {
          _.clusterQuery.getInitBucket(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        newCluster shouldBe 'defined
        newClusterBucket shouldBe 'defined
        newCluster.flatMap(_.dataprocInfo.googleId) shouldBe Some(newClusterId)
        newCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
        newCluster.flatMap(_.dataprocInfo.hostIp) shouldBe Some(IP("1.2.3.4"))
        newCluster.map(_.instances.count(_.status == InstanceStatus.Running)) shouldBe Some(3)
        newCluster.flatMap(_.userJupyterExtensionConfig) shouldBe Some(userExtConfig)

        verify(storageDAO, never).deleteBucket(mockitoEq(newClusterBucket.get.bucketName), any[Boolean])

      }
      // should only add/remove the dataproc.worker role 1 time
      val dpWorkerTimes = if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()
      verify(iamDAO, dpWorkerTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, dpWorkerTimes).addIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      val imageUserTimes = if (dataprocConfig.customDataprocImage.isDefined) times(1) else never()
      verify(iamDAO, imageUserTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/compute.imageUser")))
      verify(iamDAO, imageUserTimes).addIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/compute.imageUser")))
      verify(iamDAO, if (notebookServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
      verify(authProvider).notifyClusterDeleted(mockitoEq(creatingCluster.internalId), mockitoEq(creatingCluster.auditInfo.creator), mockitoEq(creatingCluster.auditInfo.creator), mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))(any[ExecutionContext])
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
    val projectDAO = mock[GoogleProjectDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO, true) { actor =>

      eventually {
        val updatedCluster = dbFutureValue {
          _.clusterQuery.getDeletingClusterByName(deletingCluster.googleProject, deletingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster shouldBe Some(savedDeletingCluster)
      }

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
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
    val projectDAO = mock[GoogleProjectDAO]
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
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    // Create the first cluster
    val supervisor = withClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO, false) { actor =>

      eventually {
        val updatedCluster = dbFutureValue {
          _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
        updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe Some(IP("1.2.3.4"))
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])

      // Create the second cluster

      eventually {
        val updatedCluster2 = dbFutureValue {
          _.clusterQuery.getActiveClusterByName(creatingCluster2.googleProject, creatingCluster2.clusterName)
        }
        updatedCluster2 shouldBe 'defined
        updatedCluster2.map(_.status) shouldBe Some(ClusterStatus.Running)
        updatedCluster2.flatMap(_.dataprocInfo.hostIp) shouldBe Some(IP("1.2.3.4")) // same ip because we're using the same set of instances
        updatedCluster2.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2).map(modifyInstance))
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      // Changing to atleast once since based on the timing of the monitor this method can be called once or twice
      val dpWorkerTimes = if (clusterServiceAccount(creatingCluster.googleProject).isDefined) atLeastOnce() else never()
      verify(iamDAO, dpWorkerTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      val imageUserTimes = if (dataprocConfig.customDataprocImage.isDefined) atLeastOnce() else never()
      verify(iamDAO, imageUserTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/compute.imageUser")))
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
    val projectDAO = mock[GoogleProjectDAO]
    val iamDAO = mock[GoogleIamDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO, false) { actor =>

      eventually {
        val updatedCluster = dbFutureValue {
          _.clusterQuery.getActiveClusterByName(stoppingCluster.googleProject, stoppingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Stopped)
        updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe None
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2).map(_.copy(status = InstanceStatus.Stopped)))
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
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
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
    } thenReturn Future.successful(())
    val authProvider = mock[LeoAuthProvider]

    val jupyterDAO = mock[JupyterDAO]
    when {
      jupyterDAO.isProxyAvailable(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn Future.successful(true)

    val rstudioDAO = mock[RStudioDAO]
    when {
      rstudioDAO.isProxyAvailable(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn Future.successful(true)

    val projectDAO = mock[GoogleProjectDAO]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, jupyterDAO, rstudioDAO, MockWelderDAO, false) { actor =>

      eventually {
        val updatedCluster = dbFutureValue {
          _.clusterQuery.getActiveClusterByName(startingCluster.googleProject, startingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
        updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe Some(IP("1.2.3.4"))
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      // starting a cluster should not touch IAM roles
      verify(iamDAO, never()).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
      verify(jupyterDAO, times(1)).isProxyAvailable(any[GoogleProject], any[ClusterName])
      verify(rstudioDAO, times(1)).isProxyAvailable(any[GoogleProject], any[ClusterName])
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
    val stopAfterCreationCluster = creatingCluster.copy(stopAfterCreation = true)
    stopAfterCreationCluster.save() shouldEqual stopAfterCreationCluster

    val projectDAO = mock[GoogleProjectDAO]
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

    when {
      computeDAO.getGoogleApiServiceAccount(mockitoEq(creatingCluster.googleProject))
    } thenReturn Future.successful(Some(WorkbenchEmail("api-service-account")))

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
      iamDAO.removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], any[Set[String]])
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO, false) { actor =>

      eventually {
        val updatedCluster = dbFutureValue { _.clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName) }
        updatedCluster shouldBe 'defined

        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Stopped)
        updatedCluster.flatMap(_.dataprocInfo.hostIp) shouldBe None
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance.copy(status = InstanceStatus.Stopped)))
      }
      verify(storageDAO, never()).deleteBucket(any[GcsBucketName], any[Boolean])
      val dpWorkerTimes = if (clusterServiceAccount(creatingCluster.googleProject).isDefined) times(1) else never()
      verify(iamDAO, dpWorkerTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/dataproc.worker")))
      val imageUserTimes = if (dataprocConfig.customDataprocImage.isDefined) times(1) else never()
      verify(iamDAO, imageUserTimes).removeIamRolesForUser(any[GoogleProject], any[WorkbenchEmail], mockitoEq(Set("roles/compute.imageUser")))
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
    val projectDAO = mock[GoogleProjectDAO]
    val iamDAO = mock[GoogleIamDAO]
    val authProvider = mock[LeoAuthProvider]

    withClusterSupervisor(gdDAO, computeDAO, iamDAO, projectDAO, storageDAO, FakeGoogleStorageInterpreter, authProvider, MockJupyterDAO, MockRStudioDAO, MockWelderDAO, false) { actor =>

      eventually {
        val dbCluster = dbFutureValue {
          _.clusterQuery.getActiveClusterByName(errorCluster.googleProject, errorCluster.clusterName)
        }
        dbCluster shouldBe 'defined
        dbCluster.get shouldEqual errorCluster
      }
      verify(gdDAO, never()).getClusterStatus(any[GoogleProject], any[ClusterName])
    }
  }
}
