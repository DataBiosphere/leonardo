package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.io.{ByteArrayInputStream, File}
import java.time.Instant
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, Terminated}
import akka.testkit.TestKit
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.google.api.services.compute.model
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleDirectoryDAO
import org.broadinstitute.dsde.workbench.google.{GoogleDirectoryDAO, GoogleIamDAO, GoogleProjectDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.mock.BaseFakeGoogleStorage
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GetMetadataResponse, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{GoogleComputeDAO, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent, clusterQuery}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocRole.{Master, Worker}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{InstanceStatus, _}
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper, QueueFactory}
import org.broadinstitute.dsde.workbench.model.google.GcsLifecycleTypes.GcsLifecycleType
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.GcsRole
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GcsEntity, GcsObjectName, GoogleProject, ServiceAccountKeyId}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Random, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import CommonTestData._
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.newrelic.mock.FakeNewRelicMetricsInterpreter

/**
 * Created by rtitle on 9/6/17.
 */
class ClusterMonitorSpec
    extends TestKit(ActorSystem("leonardotest"))
    with FlatSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll
    with TestComponent
    with GcsPathUtils
    with Eventually { testKit =>

  val creatingCluster = makeCluster(1).copy(
    serviceAccountInfo =
      ServiceAccountInfo(clusterServiceAccountFromProject(project), notebookServiceAccountFromProject(project)),
    dataprocInfo = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = ClusterStatus.Creating,
    userJupyterExtensionConfig = Some(userExtConfig),
    stopAfterCreation = false
  )

  val deletingCluster = makeCluster(2).copy(
    serviceAccountInfo =
      ServiceAccountInfo(clusterServiceAccountFromProject(project), notebookServiceAccountFromProject(project)),
    status = ClusterStatus.Deleting,
    instances = Set(masterInstance, workerInstance1, workerInstance2)
  )

  val stoppingCluster = makeCluster(3).copy(
    serviceAccountInfo =
      ServiceAccountInfo(clusterServiceAccountFromProject(project), notebookServiceAccountFromProject(project)),
    dataprocInfo = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = ClusterStatus.Stopping
  )

  val startingCluster = makeCluster(4).copy(
    serviceAccountInfo =
      ServiceAccountInfo(clusterServiceAccountFromProject(project), notebookServiceAccountFromProject(project)),
    status = ClusterStatus.Starting,
    clusterImages = Set(ClusterImage(ClusterImageType.RStudio, "rstudio_image", Instant.now()),
                        ClusterImage(ClusterImageType.Jupyter, "jupyter_image", Instant.now()))
  )
  val runningCluster = makeCluster(1).copy(
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount, notebookServiceAccount),
    dataprocInfo = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = ClusterStatus.Running,
    instances = Set(masterInstance, workerInstance1, workerInstance2)
  )

  val errorCluster = makeCluster(5).copy(
    serviceAccountInfo =
      ServiceAccountInfo(clusterServiceAccountFromProject(project), notebookServiceAccountFromProject(project)),
    status = ClusterStatus.Error
  )

  val stoppedCluster = makeCluster(6).copy(
    serviceAccountInfo =
      ServiceAccountInfo(clusterServiceAccountFromProject(project), notebookServiceAccountFromProject(project)),
    dataprocInfo = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = ClusterStatus.Stopped,
    clusterImages = Set(ClusterImage(ClusterImageType.RStudio, "rstudio_image", Instant.now()))
  )

  val clusterInstances = Map(Master -> Set(masterInstance.key), Worker -> Set(workerInstance1.key, workerInstance2.key))

  val clusterInstances2 = clusterInstances.mapValues(_.map(modifyInstanceKey))

  val clusterMonitorPatience = 10 seconds

  implicit val monitorPat = PatienceConfig(timeout = scaled(Span(30, Seconds)), interval = scaled(Span(2, Seconds)))
  implicit val nr = FakeNewRelicMetricsInterpreter

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
      dao.getZones(mockitoEq(masterInstance.key.project), any[String])
    } thenReturn Future.successful(List(ZoneUri("us-central1-a")))
    when {
      dao.getMachineType(mockitoEq(masterInstance.key.project), any[ZoneUri], any[MachineType])
    } thenReturn Future.successful(Some(new model.MachineType().setMemoryMb(7680)))
    dao
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def createClusterSupervisor(gdDAO: GoogleDataprocDAO,
                              computeDAO: GoogleComputeDAO,
                              directoryDAO: GoogleDirectoryDAO,
                              iamDAO: GoogleIamDAO,
                              projectDAO: GoogleProjectDAO,
                              storageDAO: GoogleStorageDAO,
                              storage2DAO: GoogleStorageService[IO],
                              authProvider: LeoAuthProvider[IO],
                              jupyterDAO: JupyterDAO[IO],
                              rstudioDAO: RStudioDAO[IO],
                              welderDAO: WelderDAO[IO],
                             queue: InspectableQueue[IO, LeoPubsubMessage]): ActorRef = {
    val bucketHelper = new BucketHelper(computeDAO, storageDAO, storage2DAO, serviceAccountProvider)
    val clusterHelper = new ClusterHelper(DbSingleton.dbRef,
                                          dataprocConfig,
                                          imageConfig,
                                          googleGroupsConfig,
                                          proxyConfig,
                                          clusterResourcesConfig,
                                          clusterFilesConfig,
                                          monitorConfig,
                                          welderConfig,
                                          bucketHelper,
                                          gdDAO,
                                          computeDAO,
                                          directoryDAO,
                                          iamDAO,
                                          projectDAO, MockWelderDAO,
                                          blocker)
    val supervisorActor = system.actorOf(
      TestClusterSupervisorActor.props(
        monitorConfig,
        dataprocConfig,
        imageConfig,
        clusterBucketConfig,
        gdDAO,
        computeDAO,
        storageDAO,
        storage2DAO,
        DbSingleton.dbRef,
        testKit,
        authProvider,
        autoFreezeConfig,
        jupyterDAO,
        rstudioDAO,
        welderDAO,
        clusterHelper,
        queue
      )
    )

    supervisorActor
  }

  def withClusterSupervisor[T](
    gdDAO: GoogleDataprocDAO,
    computeDAO: GoogleComputeDAO,
    iamDAO: GoogleIamDAO,
    projectDAO: GoogleProjectDAO,
    storageDAO: GoogleStorageDAO,
    storage2DAO: GoogleStorageService[IO],
    authProvider: LeoAuthProvider[IO],
    jupyterDAO: JupyterDAO[IO] = MockJupyterDAO,
    rstudioDAO: RStudioDAO[IO] = MockRStudioDAO,
    welderDAO: WelderDAO[IO] = MockWelderDAO,
    runningChild: Boolean = true,
    directoryDAO: GoogleDirectoryDAO = new MockGoogleDirectoryDAO(),
                              queue: InspectableQueue[IO, LeoPubsubMessage] = QueueFactory.makePublisherQueue()
  )(testCode: ActorRef => T): T = {
    // Set up the mock directoryDAO to have the Google group used to grant permission to users to pull the custom dataproc image
    directoryDAO
      .createGroup(dataprocImageProjectGroupName,
                   dataprocImageProjectGroupEmail,
                   Option(directoryDAO.lockedDownGroupSettings))
      .futureValue
    val supervisor = createClusterSupervisor(gdDAO,
                                             computeDAO,
                                             directoryDAO,
                                             iamDAO,
                                             projectDAO,
                                             storageDAO,
                                             storage2DAO,
                                             authProvider,
                                             jupyterDAO,
                                             rstudioDAO,
                                             welderDAO,
      queue)
    val testResult = Try(testCode(supervisor))
    testKit watch supervisor
    supervisor ! TearDown
    // Should receive a Terminated msg for each running child, plus one for the supervisor itself
    expectMsgAllClassOf(clusterMonitorPatience, Seq.fill(if (runningChild) 2 else 1)(classOf[Terminated]): _*)
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

    val authProvider = mock[LeoAuthProvider[IO]]

    val jupyterDAO = mock[JupyterDAO[IO]]
    when {
      jupyterDAO.isProxyAvailable(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn IO.pure(true)

    withClusterSupervisor(gdDAO,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          jupyterDAO,
                          MockRStudioDAO,
                          MockWelderDAO,
                          false) { actor =>
      eventually {
        val updatedCluster = dbFutureValue {
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
        updatedCluster.flatMap(_.dataprocInfo.flatMap(_.hostIp)) shouldBe Some(IP("1.2.3.4"))
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))
      }
      verify(storageDAO, never()).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(jupyterDAO, times(1)).isProxyAvailable(any[GoogleProject], any[ClusterName])
    }
  }

  //Pre:
  // - cluster exists in DB with status Creating
  // - dataproc DAO returns creating and compute DAO returns status running
  // - the poll period for creationTimeLimit passes
  //Post:
  // - cluster status is set to Error in the DB
  it should "Delete a cluster that is stuck creating for too long" in isolatedDbTest {
    val savedCreatingCluster = creatingCluster.save()
    creatingCluster shouldEqual savedCreatingCluster

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(ClusterStatus.Creating)

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)

    when {
      gdDAO.getClusterErrorDetails(mockitoEq(creatingCluster.dataprocInfo.map(_.operationName)))
    } thenReturn Future.successful(Some(ClusterErrorDetails(Code.DEADLINE_EXCEEDED.value, Some("Test message"))))

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(())

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val projectDAO = mock[GoogleProjectDAO]
    val iamDAO = mock[GoogleIamDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val authProvider = mock[LeoAuthProvider[IO]]

    withClusterSupervisor(gdDAO,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          MockJupyterDAO,
                          MockRStudioDAO,
                          MockWelderDAO,
                          true) { actor =>
      eventually(
        timeout(
          Span(
            (monitorConfig.monitorStatusTimeouts.getOrElse(
              ClusterStatus.Creating,
              throw new Exception("config does not have proper params for monitor status timeouts")
            ) * 3).toSeconds,
            Seconds
          )
        )
      ) {
        val updatedCluster = dbFutureValue {
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Error)

        verify(gdDAO, times(1)).deleteCluster(mockitoEq(creatingCluster.googleProject),
                                              mockitoEq(creatingCluster.clusterName))
      }
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
      val authProvider = mock[LeoAuthProvider[IO]]

      withClusterSupervisor(gdDAO,
                            computeDAO,
                            iamDAO,
                            projectDAO,
                            storageDAO,
                            FakeGoogleStorageService,
                            authProvider,
                            MockJupyterDAO,
                            MockRStudioDAO,
                            MockWelderDAO,
                            true) { actor =>
        eventually {
          val updatedCluster = dbFutureValue {
            clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
          }
          updatedCluster shouldBe 'defined
          updatedCluster shouldBe Some(
            savedCreatingCluster.copy(instances = Set(masterInstance, workerInstance1, workerInstance2))
          )
        }
        verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
        verify(iamDAO, never()).removeIamRoles(any[GoogleProject],
                                               any[WorkbenchEmail],
                                               any[MemberType],
                                               any[Set[String]])
        verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject],
                                                        any[WorkbenchEmail],
                                                        any[ServiceAccountKeyId])
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
    val authProvider = mock[LeoAuthProvider[IO]]

    withClusterSupervisor(dao,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          MockJupyterDAO,
                          MockRStudioDAO,
                          MockWelderDAO,
                          true) { actor =>
      eventually {
        val updatedCluster = dbFutureValue {
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster shouldBe Some(
          savedCreatingCluster.copy(instances = Set(masterInstance, workerInstance1, workerInstance2))
        )
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRoles(any[GoogleProject],
                                             any[WorkbenchEmail],
                                             mockitoEq(MemberType.ServiceAccount),
                                             any[Set[String]])
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
      gdDAO.getClusterErrorDetails(mockitoEq(creatingCluster.dataprocInfo.map(_.operationName)))
    } thenReturn Future.successful(Some(ClusterErrorDetails(Code.CANCELLED.value, Some("test message"))))

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(())

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRoles(any[GoogleProject],
                            any[WorkbenchEmail],
                            mockitoEq(MemberType.ServiceAccount),
                            any[Set[String]])
    } thenReturn Future.successful(true)

    when {
      iamDAO.removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    } thenReturn Future.successful(())

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val storageDAO = mock[GoogleStorageDAO]
    val projectDAO = mock[GoogleProjectDAO]
    val authProvider = mock[LeoAuthProvider[IO]]

    withClusterSupervisor(gdDAO,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          MockJupyterDAO,
                          MockRStudioDAO,
                          MockWelderDAO,
                          false) { actor =>
      eventually {
        val updatedCluster = dbFutureValue {
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Error)
        updatedCluster.flatMap(_.dataprocInfo.flatMap(_.hostIp)) shouldBe None
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))
      }

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO,
             if (notebookServiceAccountFromProject(creatingCluster.googleProject).isDefined) times(1) else never())
        .removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
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
      gdDAO.getClusterErrorDetails(mockitoEq(creatingCluster.dataprocInfo.map(_.operationName)))
    } thenReturn Future.successful(None)

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(())

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.successful(clusterInstances)

    val iamDAO = mock[GoogleIamDAO]
    when {
      iamDAO.removeIamRoles(any[GoogleProject],
                            any[WorkbenchEmail],
                            mockitoEq(MemberType.ServiceAccount),
                            any[Set[String]])
    } thenReturn Future.successful(true)

    when {
      iamDAO.removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    } thenReturn Future.successful(())

    val computeDAO = stubComputeDAO(InstanceStatus.Running)
    val storageDAO = mock[GoogleStorageDAO]
    val projectDAO = mock[GoogleProjectDAO]
    val authProvider = mock[LeoAuthProvider[IO]]

    withClusterSupervisor(gdDAO,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          MockJupyterDAO,
                          MockRStudioDAO,
                          MockWelderDAO,
                          false) { actor =>
      eventually {
        val updatedCluster = dbFutureValue {
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Error)
        updatedCluster.flatMap(_.dataprocInfo.flatMap(_.hostIp)) shouldBe None
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO,
             if (notebookServiceAccountFromProject(creatingCluster.googleProject).isDefined) times(1) else never())
        .removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
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
    when {
      iamDAO.removeIamRoles(any[GoogleProject],
                            any[WorkbenchEmail],
                            mockitoEq(MemberType.ServiceAccount),
                            any[Set[String]])
    } thenReturn Future.successful(true)

    val storageDAO = mock[GoogleStorageDAO]
    when {
      storageDAO.setBucketLifecycle(any[GcsBucketName], any[Int], any[GcsLifecycleType])
    } thenReturn Future.successful(())

    when {
      storageDAO.deleteBucket(any[GcsBucketName], any[Boolean])
    } thenReturn Future.successful(())

    val authProvider = mock[LeoAuthProvider[IO]]

    when {
      authProvider.notifyClusterDeleted(
        mockitoEq(deletingCluster.internalId),
        mockitoEq(deletingCluster.auditInfo.creator),
        mockitoEq(deletingCluster.auditInfo.creator),
        mockitoEq(deletingCluster.googleProject),
        mockitoEq(deletingCluster.clusterName)
      )(any[ApplicativeAsk[IO, TraceId]])
    } thenReturn IO.unit

    withClusterSupervisor(dao,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          MockJupyterDAO,
                          MockRStudioDAO,
                          MockWelderDAO,
                          false) { actor =>
      eventually {
        val updatedCluster = dbFutureValue {
          clusterQuery.getClusterById(savedDeletingCluster.id)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Deleted)
        updatedCluster.flatMap(_.dataprocInfo.flatMap(_.hostIp)) shouldBe None
        updatedCluster.map(_.instances) shouldBe Some(Set.empty)
      }
      verify(storageDAO, times(1)).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(storageDAO, times(1)).setBucketLifecycle(any[GcsBucketName], any[Int], any[GcsLifecycleType])
      verify(authProvider).notifyClusterDeleted(
        mockitoEq(deletingCluster.internalId),
        mockitoEq(deletingCluster.auditInfo.creator),
        mockitoEq(deletingCluster.auditInfo.creator),
        mockitoEq(deletingCluster.googleProject),
        mockitoEq(deletingCluster.clusterName)
      )(any[ApplicativeAsk[IO, TraceId]])
      verify(iamDAO, times(1)).removeIamRoles(any[GoogleProject],
                                              any[WorkbenchEmail],
                                              mockitoEq(MemberType.ServiceAccount),
                                              any[Set[String]])
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
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
      gdDAO.getClusterErrorDetails(mockitoEq(creatingCluster.dataprocInfo.map(_.operationName)))
    } thenReturn Future.successful(Some(ClusterErrorDetails(Code.UNKNOWN.value, Some("Test message"))))

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject), mockitoEq(creatingCluster.clusterName))
    } thenReturn Future.unit

    val newClusterId = UUID.randomUUID()
    when {
      gdDAO.createCluster(mockitoEq(creatingCluster.googleProject),
                          mockitoEq(creatingCluster.clusterName),
                          any[CreateClusterConfig])
    } thenReturn Future.successful {
      Operation(creatingCluster.dataprocInfo.map(_.operationName).get, newClusterId)
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
    } thenReturn Future.successful(
      Some(InstanceKey(creatingCluster.googleProject, ZoneUri("my-zone"), InstanceName("master-instance")))
    )

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
      iamDAO.addIamRoles(any[GoogleProject],
                         any[WorkbenchEmail],
                         mockitoEq(MemberType.ServiceAccount),
                         any[Set[String]])
    } thenReturn Future.successful(true)

    when {
      iamDAO.removeIamRoles(any[GoogleProject],
                            any[WorkbenchEmail],
                            mockitoEq(MemberType.ServiceAccount),
                            any[Set[String]])
    } thenReturn Future.successful(true)

    when {
      iamDAO.removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    } thenReturn Future.successful(())

    when {
      iamDAO.createServiceAccountKey(any[GoogleProject], any[WorkbenchEmail])
    } thenReturn Future.successful(serviceAccountKey)

    val authProvider = mock[LeoAuthProvider[IO]]

    when {
      authProvider.notifyClusterDeleted(
        mockitoEq(creatingCluster.internalId),
        mockitoEq(creatingCluster.auditInfo.creator),
        mockitoEq(creatingCluster.auditInfo.creator),
        mockitoEq(creatingCluster.googleProject),
        mockitoEq(creatingCluster.clusterName)
      )(any[ApplicativeAsk[IO, TraceId]])
    } thenReturn IO.unit

    withClusterSupervisor(gdDAO,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          MockJupyterDAO,
                          MockRStudioDAO,
                          MockWelderDAO,
                          false) { _ =>
      eventually {
        val newCluster = dbFutureValue {
          clusterQuery.getClusterById(savedCreatingCluster.id)
        }
        val newClusterBucket = dbFutureValue {
          clusterQuery.getInitBucket(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        newCluster shouldBe 'defined
        newClusterBucket shouldBe 'defined
        newCluster.flatMap(_.dataprocInfo.map(_.googleId)) shouldBe Some(newClusterId)
        newCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
        newCluster.flatMap(_.dataprocInfo.flatMap(_.hostIp)) shouldBe Some(IP("1.2.3.4"))
        newCluster.map(_.instances.count(_.status == InstanceStatus.Running)) shouldBe Some(3)
        newCluster.flatMap(_.userJupyterExtensionConfig) shouldBe Some(userExtConfig)

        verify(storageDAO, never).deleteBucket(mockitoEq(newClusterBucket.get.bucketName), any[Boolean])
      }
      // should only add/remove the dataproc.worker role 1 time
      val dpWorkerTimes =
        if (clusterServiceAccountFromProject(creatingCluster.googleProject).isDefined) times(1) else never()
      verify(iamDAO, dpWorkerTimes).addIamRoles(any[GoogleProject],
                                                any[WorkbenchEmail],
                                                mockitoEq(MemberType.ServiceAccount),
                                                mockitoEq(Set("roles/dataproc.worker")))
      verify(iamDAO, dpWorkerTimes).removeIamRoles(any[GoogleProject],
                                                   any[WorkbenchEmail],
                                                   mockitoEq(MemberType.ServiceAccount),
                                                   mockitoEq(Set("roles/dataproc.worker")))
      // no longer adding/removing compute.imageUser role
      verify(iamDAO, never()).addIamRoles(any[GoogleProject],
                                          any[WorkbenchEmail],
                                          mockitoEq(MemberType.User),
                                          any[Set[String]])
      verify(iamDAO, never()).removeIamRoles(any[GoogleProject],
                                             any[WorkbenchEmail],
                                             mockitoEq(MemberType.User),
                                             any[Set[String]])
      verify(iamDAO,
             if (notebookServiceAccountFromProject(creatingCluster.googleProject).isDefined) times(1) else never())
        .removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
      verify(authProvider).notifyClusterDeleted(
        mockitoEq(creatingCluster.internalId),
        mockitoEq(creatingCluster.auditInfo.creator),
        mockitoEq(creatingCluster.auditInfo.creator),
        mockitoEq(creatingCluster.googleProject),
        mockitoEq(creatingCluster.clusterName)
      )(any[ApplicativeAsk[IO, TraceId]])
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
    val authProvider = mock[LeoAuthProvider[IO]]

    withClusterSupervisor(gdDAO,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          MockJupyterDAO,
                          MockRStudioDAO,
                          MockWelderDAO,
                          true) { actor =>
      eventually {
        val updatedCluster = dbFutureValue {
          clusterQuery.getDeletingClusterByName(deletingCluster.googleProject, deletingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster shouldBe Some(savedDeletingCluster)
      }

      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRoles(any[GoogleProject], any[WorkbenchEmail], any[MemberType], any[Set[String]])
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
    }
  }

  it should "create two clusters for the same user" in isolatedDbTest {
    val savedCreatingCluster = creatingCluster.save()
    creatingCluster shouldEqual savedCreatingCluster

    val creatingCluster2 = creatingCluster.copy(
      clusterName = ClusterName(creatingCluster.clusterName.value + "_2"),
      dataprocInfo =
        creatingCluster.dataprocInfo.map(_.copy(googleId = UUID.randomUUID(), hostIp = Some(IP("5.6.7.8"))))
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
    } thenReturn Future.successful(
      Some(InstanceKey(creatingCluster.googleProject, ZoneUri("my-zone"), InstanceName("master-instance")))
    )
    when {
      gdDAO.getClusterMasterInstance(mockitoEq(creatingCluster2.googleProject), mockitoEq(creatingCluster2.clusterName))
    } thenReturn Future.successful(
      Some(InstanceKey(creatingCluster.googleProject, ZoneUri("my-zone"), InstanceName("master-instance")))
    )

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

    val authProvider = mock[LeoAuthProvider[IO]]

    // Create the first cluster
    val supervisor = withClusterSupervisor(gdDAO,
                                           computeDAO,
                                           iamDAO,
                                           projectDAO,
                                           storageDAO,
                                           FakeGoogleStorageService,
                                           authProvider,
                                           MockJupyterDAO,
                                           MockRStudioDAO,
                                           MockWelderDAO,
                                           false) { actor =>
      eventually {
        val updatedCluster = dbFutureValue {
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
        updatedCluster.flatMap(_.dataprocInfo.flatMap(_.hostIp)) shouldBe Some(IP("1.2.3.4"))
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])

      // Create the second cluster

      eventually {
        val updatedCluster2 = dbFutureValue {
          clusterQuery.getActiveClusterByName(creatingCluster2.googleProject, creatingCluster2.clusterName)
        }
        updatedCluster2 shouldBe 'defined
        updatedCluster2.map(_.status) shouldBe Some(ClusterStatus.Running)
        updatedCluster2.flatMap(_.dataprocInfo.flatMap(_.hostIp)) shouldBe Some(IP("1.2.3.4")) // same ip because we're using the same set of instances
        updatedCluster2.map(_.instances) shouldBe Some(
          Set(masterInstance, workerInstance1, workerInstance2).map(modifyInstance)
        )
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
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
    val authProvider = mock[LeoAuthProvider[IO]]

    withClusterSupervisor(gdDAO,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          MockJupyterDAO,
                          MockRStudioDAO,
                          MockWelderDAO,
                          false) { actor =>
      eventually {
        val updatedCluster = dbFutureValue {
          clusterQuery.getActiveClusterByName(stoppingCluster.googleProject, stoppingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Stopped)
        updatedCluster.flatMap(_.dataprocInfo.flatMap(_.hostIp)) shouldBe None
        updatedCluster.map(_.instances) shouldBe Some(
          Set(masterInstance, workerInstance1, workerInstance2).map(_.copy(status = InstanceStatus.Stopped))
        )
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeIamRoles(any[GoogleProject], any[WorkbenchEmail], any[MemberType], any[Set[String]])
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
      iamDAO.removeIamRoles(any[GoogleProject],
                            any[WorkbenchEmail],
                            mockitoEq(MemberType.ServiceAccount),
                            any[Set[String]])
    } thenReturn Future.successful(true)

    val authProvider = mock[LeoAuthProvider[IO]]

    val jupyterDAO = mock[JupyterDAO[IO]]
    when {
      jupyterDAO.isProxyAvailable(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn IO.pure(true)

    val rstudioDAO = mock[RStudioDAO[IO]]
    when {
      rstudioDAO.isProxyAvailable(mockitoEq(startingCluster.googleProject), mockitoEq(startingCluster.clusterName))
    } thenReturn IO.pure(true)

    val projectDAO = mock[GoogleProjectDAO]

    withClusterSupervisor(gdDAO,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          jupyterDAO,
                          rstudioDAO,
                          MockWelderDAO,
                          false) { actor =>
      eventually {
        val updatedCluster = dbFutureValue {
          clusterQuery.getActiveClusterByName(startingCluster.googleProject, startingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Running)
        updatedCluster.flatMap(_.dataprocInfo.flatMap(_.hostIp)) shouldBe Some(IP("1.2.3.4"))
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance, workerInstance1, workerInstance2))
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      // starting a cluster should not touch IAM roles
      verify(iamDAO, never()).removeIamRoles(any[GoogleProject], any[WorkbenchEmail], any[MemberType], any[Set[String]])
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

    val authProvider = mock[LeoAuthProvider[IO]]

    withClusterSupervisor(gdDAO,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          MockJupyterDAO,
                          MockRStudioDAO,
                          MockWelderDAO,
                          false) { actor =>
      eventually {
        val updatedCluster = dbFutureValue {
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.clusterName)
        }
        updatedCluster shouldBe 'defined

        updatedCluster.map(_.status) shouldBe Some(ClusterStatus.Stopped)
        updatedCluster.flatMap(_.dataprocInfo.flatMap(_.hostIp)) shouldBe None
        updatedCluster.map(_.instances) shouldBe Some(Set(masterInstance.copy(status = InstanceStatus.Stopped)))
      }
      verify(storageDAO, never()).deleteBucket(any[GcsBucketName], any[Boolean])
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
    val authProvider = mock[LeoAuthProvider[IO]]

    withClusterSupervisor(gdDAO,
                          computeDAO,
                          iamDAO,
                          projectDAO,
                          storageDAO,
                          FakeGoogleStorageService,
                          authProvider,
                          MockJupyterDAO,
                          MockRStudioDAO,
                          MockWelderDAO,
                          false) { _ =>
      eventually {
        val dbCluster = dbFutureValue {
          clusterQuery.getActiveClusterByName(errorCluster.googleProject, errorCluster.clusterName)
        }
        dbCluster shouldBe 'defined
        dbCluster.get shouldEqual errorCluster
      }
      verify(gdDAO, never()).getClusterStatus(any[GoogleProject], any[ClusterName])
    }
  }

  // Pre:
  // - cluster exists in the DB with status Stopping
  // Post:
  // - cluster is not updated in the DB
  // - monitor actor shuts down
  // - dataproc DAO should not have been called
  it should "put a message in the queue for a stopping cluster" in isolatedDbTest {
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
    val authProvider = mock[LeoAuthProvider[IO]]

    val queue = QueueFactory.makePublisherQueue()

    queue.getSize.unsafeRunSync() shouldBe 0

    withClusterSupervisor(gdDAO,
      computeDAO,
      iamDAO,
      projectDAO,
      storageDAO,
      FakeGoogleStorageService,
      authProvider,
      MockJupyterDAO,
      MockRStudioDAO,
      MockWelderDAO,
      false,
        queue = queue
    ) { actor =>
      eventually {
        val size = queue.getSize.unsafeRunSync()
        size shouldBe 1
      }
    }


  }
}

object FakeGoogleStorageService extends BaseFakeGoogleStorage {
  override def getObjectMetadata(bucketName: GcsBucketName,
                                 blobName: GcsBlobName,
                                 traceId: Option[TraceId],
                                 retryConfig: RetryConfig): fs2.Stream[IO, GetMetadataResponse] =
    fs2.Stream.empty.covary[IO]
}
