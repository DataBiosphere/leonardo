package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.io.{ByteArrayInputStream, File}
import java.time.Instant
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, Terminated}
import akka.testkit.TestKit
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.{Operation => GoogleOperation, _}
import fs2.concurrent.InspectableQueue
import io.grpc.Status.Code
import org.broadinstitute.dsde.workbench.RetryConfig
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleDirectoryDAO
import org.broadinstitute.dsde.workbench.google.{GoogleDirectoryDAO, GoogleIamDAO, GoogleProjectDAO, GoogleStorageDAO}
import org.broadinstitute.dsde.workbench.google2.mock.BaseFakeGoogleStorage
import org.broadinstitute.dsde.workbench.google2.{
  FirewallRuleName,
  GcsBlobName,
  GetMetadataResponse,
  GoogleComputeService,
  GoogleStorageService,
  InstanceName,
  MachineTypeName,
  RegionName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.DataprocRole.{Master, Worker}
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{CreateClusterConfig, GoogleDataprocDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateRuntimeMessage
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.GcsLifecycleTypes.GcsLifecycleType
import org.broadinstitute.dsde.workbench.model.google.GcsRoles.GcsRole
import org.broadinstitute.dsde.workbench.model.google.{
  GcsBucketName,
  GcsEntity,
  GcsObjectName,
  GoogleProject,
  ServiceAccountKeyId
}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.newrelic.mock.FakeNewRelicMetricsInterpreter
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

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
    asyncRuntimeFields = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = RuntimeStatus.Creating,
    userJupyterExtensionConfig = Some(userExtConfig),
    stopAfterCreation = false
  )

  val deletingCluster = makeCluster(2).copy(
    serviceAccountInfo =
      ServiceAccountInfo(clusterServiceAccountFromProject(project), notebookServiceAccountFromProject(project)),
    status = RuntimeStatus.Deleting,
    dataprocInstances = Set(masterInstance, workerInstance1, workerInstance2)
  )

  val stoppingCluster = makeCluster(3).copy(
    serviceAccountInfo =
      ServiceAccountInfo(clusterServiceAccountFromProject(project), notebookServiceAccountFromProject(project)),
    asyncRuntimeFields = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = RuntimeStatus.Stopping
  )

  val startingCluster = makeCluster(4).copy(
    serviceAccountInfo =
      ServiceAccountInfo(clusterServiceAccountFromProject(project), notebookServiceAccountFromProject(project)),
    status = RuntimeStatus.Starting,
    runtimeImages = Set(RuntimeImage(RuntimeImageType.RStudio, "rstudio_image", Instant.now()),
                        RuntimeImage(RuntimeImageType.Jupyter, "jupyter_image", Instant.now()))
  )
  val runningCluster = makeCluster(1).copy(
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount, notebookServiceAccount),
    asyncRuntimeFields = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = RuntimeStatus.Running,
    dataprocInstances = Set(masterInstance, workerInstance1, workerInstance2)
  )

  val errorCluster = makeCluster(5).copy(
    serviceAccountInfo =
      ServiceAccountInfo(clusterServiceAccountFromProject(project), notebookServiceAccountFromProject(project)),
    status = RuntimeStatus.Error
  )

  val stoppedCluster = makeCluster(6).copy(
    serviceAccountInfo =
      ServiceAccountInfo(clusterServiceAccountFromProject(project), notebookServiceAccountFromProject(project)),
    asyncRuntimeFields = Some(makeDataprocInfo(1).copy(hostIp = None)),
    status = RuntimeStatus.Stopped,
    runtimeImages = Set(RuntimeImage(RuntimeImageType.RStudio, "rstudio_image", Instant.now()))
  )

  val clusterInstances = Map(Master -> Set(masterInstance.key), Worker -> Set(workerInstance1.key, workerInstance2.key))

  val clusterInstances2 = clusterInstances.mapValues(_.map(modifyInstanceKey))

  val clusterMonitorPatience = 10 seconds

  implicit val monitorPat = PatienceConfig(timeout = scaled(Span(30, Seconds)), interval = scaled(Span(2, Seconds)))
  implicit val nr = FakeNewRelicMetricsInterpreter

  def stubComputeService(status: GceInstanceStatus): GoogleComputeService[IO] = {
    val service = mock[GoogleComputeService[IO]]

    List(masterInstance,
         workerInstance1,
         workerInstance2,
         modifyInstance(masterInstance),
         modifyInstance(workerInstance1),
         modifyInstance(workerInstance2)).foreach { instance =>
      when {
        service.getInstance(mockitoEq(instance.key.project),
                            ZoneName(mockitoEq(instance.key.zone.value)),
                            InstanceName(mockitoEq(instance.key.name.value)))(any[ApplicativeAsk[IO, TraceId]])
      } thenReturn IO.pure(
        Some(
          Instance
            .newBuilder()
            .setId(instance.googleId.toString)
            .setStatus(status.toString)
            .addNetworkInterfaces(
              NetworkInterface
                .newBuilder()
                .addAccessConfigs(AccessConfig.newBuilder().setNatIP("1.2.3.4").build())
                .build()
            )
            .build
        )
      )
    }

    when {
      service.getZones(mockitoEq(masterInstance.key.project), RegionName(any[String]))(any[ApplicativeAsk[IO, TraceId]])
    } thenReturn IO.pure(List(Zone.newBuilder().setName("us-central1-a").build))

    when {
      service.getMachineType(mockitoEq(masterInstance.key.project),
                             ZoneName(any[String]),
                             MachineTypeName(any[String]))(any[ApplicativeAsk[IO, TraceId]])
    } thenReturn IO.pure(Some(MachineType.newBuilder().setMemoryMb(7680).build))

    service
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def createClusterSupervisor(gdDAO: GoogleDataprocDAO,
                              computeService: GoogleComputeService[IO],
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
    val bucketHelperConfig =
      BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig, clusterResourcesConfig)
    val bucketHelper =
      new BucketHelper[IO](bucketHelperConfig,
                           computeService,
                           storageDAO,
                           storage2DAO,
                           projectDAO,
                           serviceAccountProvider,
                           blocker)
    val vpcInterp = new VPCInterpreter[IO](vpcInterpreterConfig, projectDAO, computeService)
    val dataprocInterp = new DataprocInterpreter[IO](dataprocInterpreterConfig,
                                                     bucketHelper,
                                                     vpcInterp,
                                                     gdDAO,
                                                     computeService,
                                                     directoryDAO,
                                                     iamDAO,
                                                     projectDAO,
                                                     MockWelderDAO,
                                                     blocker)
    val gceInterp =
      new GceInterpreter[IO](gceInterpreterConfig, bucketHelper, vpcInterp, computeService, MockWelderDAO, blocker)
    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    system.actorOf(
      TestClusterSupervisorActor.props(
        monitorConfig,
        dataprocConfig,
        gceConfig,
        imageConfig,
        clusterBucketConfig,
        gdDAO,
        computeService,
        storageDAO,
        storage2DAO,
        dbRef,
        testKit,
        authProvider,
        autoFreezeConfig,
        jupyterDAO,
        rstudioDAO,
        welderDAO,
        queue
      )
    )
  }

  def withClusterSupervisor[T](
    gdDAO: GoogleDataprocDAO,
    computeService: GoogleComputeService[IO],
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
                                             computeService,
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
    creatingCluster shouldEqual savedCreatingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject),
                             RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Running))

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject),
                                RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(clusterInstances)

    when {
      gdDAO.getClusterMasterInstance(mockitoEq(creatingCluster.googleProject),
                                     RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(masterInstance.key))

    when {
      gdDAO.getClusterStagingBucket(mockitoEq(creatingCluster.googleProject),
                                    RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket")))

    val computeService = stubComputeService(GceInstanceStatus.Running)

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
      jupyterDAO.isProxyAvailable(mockitoEq(creatingCluster.googleProject),
                                  RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn IO.pure(true)

    withClusterSupervisor(gdDAO,
                          computeService,
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
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.runtimeName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.get.status shouldBe RuntimeStatus.Running
        updatedCluster.flatMap(_.asyncRuntimeFields.flatMap(_.hostIp)) shouldBe Some(IP("1.2.3.4"))
        updatedCluster.get.dataprocInstances.map(_.key) shouldBe Set(masterInstance, workerInstance1, workerInstance2)
          .map(_.key)
      }
      verify(storageDAO, never()).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(jupyterDAO, times(1)).isProxyAvailable(any[GoogleProject], RuntimeName(any[String]))
    }
  }

  //Pre:
  // - cluster exists in DB with status Creating
  // - dataproc DAO returns creating and compute DAO returns status running
  // - the poll period for creationTimeLimit passes
  //Post:
  // - cluster status is set to Error in the DB
  it should "delete a cluster that is stuck creating for too long" in isolatedDbTest {
    val savedCreatingCluster = creatingCluster.save()
    creatingCluster shouldEqual savedCreatingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject),
                             RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Creating))

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject),
                                RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(clusterInstances)

    when {
      gdDAO.getClusterErrorDetails(mockitoEq(creatingCluster.asyncRuntimeFields.map(_.operationName)))
    } thenReturn Future.successful(Some(RuntimeErrorDetails(Code.DEADLINE_EXCEEDED.value, Some("Test message"))))

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject),
                          RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(())

    val computeDAO = stubComputeService(GceInstanceStatus.Running)
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
              RuntimeStatus.Creating,
              throw new Exception("config does not have proper params for monitor status timeouts")
            ) * 3).toSeconds,
            Seconds
          )
        )
      ) {
        val updatedCluster = dbFutureValue {
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.runtimeName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.get.status shouldBe RuntimeStatus.Error

        verify(gdDAO, times(1)).deleteCluster(mockitoEq(creatingCluster.googleProject),
                                              RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
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
  Seq(DataprocClusterStatus.Creating, DataprocClusterStatus.Updating, DataprocClusterStatus.Unknown).foreach { status =>
    it should s"monitor $status status" in isolatedDbTest {
      val savedCreatingCluster = creatingCluster.save()
      creatingCluster shouldEqual savedCreatingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

      val gdDAO = mock[GoogleDataprocDAO]
      when {
        gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject),
                               RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
      } thenReturn Future.successful(Some(status))

      when {
        gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject),
                                  RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
      } thenReturn Future.successful(clusterInstances)

      val computeDAO = stubComputeService(GceInstanceStatus.Running)
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
            clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.runtimeName)
          }
          updatedCluster shouldBe 'defined
          updatedCluster.get.status shouldBe creatingCluster.status
          updatedCluster.get.dataprocInstances.map(_.key) shouldBe Set(masterInstance, workerInstance1, workerInstance2)
            .map(_.key)
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
    creatingCluster shouldEqual savedCreatingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

    val dao = mock[GoogleDataprocDAO]
    when {
      dao.getClusterStatus(mockitoEq(creatingCluster.googleProject),
                           RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Running))

    when {
      dao.getClusterInstances(mockitoEq(creatingCluster.googleProject),
                              RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(clusterInstances)

    when {
      dao.getClusterMasterInstance(mockitoEq(creatingCluster.googleProject),
                                   RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(None)

    val computeDAO = stubComputeService(GceInstanceStatus.Running)
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
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.runtimeName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.get.status shouldBe creatingCluster.status
        updatedCluster.get.dataprocInstances.map(_.key) shouldBe Set(masterInstance, workerInstance1, workerInstance2)
          .map(_.key)
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
    creatingCluster shouldEqual savedCreatingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject),
                             RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Error))

    when {
      gdDAO.getClusterErrorDetails(mockitoEq(creatingCluster.asyncRuntimeFields.map(_.operationName)))
    } thenReturn Future.successful(Some(RuntimeErrorDetails(Code.CANCELLED.value, Some("test message"))))

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject),
                          RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(())

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject),
                                RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
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

    val computeDAO = stubComputeService(GceInstanceStatus.Running)
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
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.runtimeName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.get.status shouldBe RuntimeStatus.Error
        updatedCluster.flatMap(_.asyncRuntimeFields.flatMap(_.hostIp)) shouldBe None
        updatedCluster.get.dataprocInstances.map(_.key) shouldBe Set(masterInstance, workerInstance1, workerInstance2)
          .map(_.key)
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
    creatingCluster shouldEqual savedCreatingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject),
                             RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Error))

    when {
      gdDAO.getClusterErrorDetails(mockitoEq(creatingCluster.asyncRuntimeFields.map(_.operationName)))
    } thenReturn Future.successful(None)

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject),
                          RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(())

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject),
                                RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
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

    val computeDAO = stubComputeService(GceInstanceStatus.Running)
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
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.runtimeName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.get.status shouldBe RuntimeStatus.Error
        updatedCluster.flatMap(_.asyncRuntimeFields.flatMap(_.hostIp)) shouldBe None
        updatedCluster.get.dataprocInstances.map(_.key) shouldBe Set(masterInstance, workerInstance1, workerInstance2)
          .map(_.key)
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
    deletingCluster shouldEqual savedDeletingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

    val dao = mock[GoogleDataprocDAO]
    when {
      dao.getClusterStatus(mockitoEq(deletingCluster.googleProject),
                           RuntimeName(mockitoEq(deletingCluster.runtimeName.asString)))
    } thenReturn Future.successful(None)

    when {
      dao.getClusterInstances(mockitoEq(deletingCluster.googleProject),
                              RuntimeName(mockitoEq(deletingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Map.empty[DataprocRole, Set[DataprocInstanceKey]])

    val computeDAO = mock[GoogleComputeService[IO]]
    val projectDAO = mock[GoogleProjectDAO]
    when {
      projectDAO.getProjectNumber(mockitoEq(deletingCluster.googleProject.value))
    } thenReturn Future.successful(Some(42L))

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
        RuntimeInternalId(mockitoEq(deletingCluster.internalId.asString)),
        mockitoEq(deletingCluster.auditInfo.creator),
        mockitoEq(deletingCluster.auditInfo.creator),
        mockitoEq(deletingCluster.googleProject),
        RuntimeName(mockitoEq(deletingCluster.runtimeName.asString))
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
        updatedCluster.get.status shouldBe RuntimeStatus.Deleted
        updatedCluster.flatMap(_.asyncRuntimeFields.flatMap(_.hostIp)) shouldBe None
        updatedCluster.get.dataprocInstances shouldBe Set.empty
      }
      verify(storageDAO, times(1)).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(storageDAO, times(1)).setBucketLifecycle(any[GcsBucketName], any[Int], any[GcsLifecycleType])
      verify(authProvider).notifyClusterDeleted(
        RuntimeInternalId(mockitoEq(deletingCluster.internalId.asString)),
        mockitoEq(deletingCluster.auditInfo.creator),
        mockitoEq(deletingCluster.auditInfo.creator),
        mockitoEq(deletingCluster.googleProject),
        RuntimeName(mockitoEq(deletingCluster.runtimeName.asString))
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
    creatingCluster shouldEqual savedCreatingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))
    val gdDAO = mock[GoogleDataprocDAO]
    val projectDAO = mock[GoogleProjectDAO]
    val storageDAO = mock[GoogleStorageDAO]
    val computeDAO = stubComputeService(GceInstanceStatus.Running)
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject),
                             RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn {
      Future.successful(Some(DataprocClusterStatus.Error))
    } thenReturn {
      Future.successful(None)
    } thenReturn {
      Future.successful(Some(DataprocClusterStatus.Creating))
    } thenReturn {
      Future.successful(Some(DataprocClusterStatus.Running))
    }

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject),
                                RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn {
      Future.successful(clusterInstances)
    } thenReturn {
      Future.successful(clusterInstances2)
    }

    when {
      gdDAO.getClusterErrorDetails(mockitoEq(creatingCluster.asyncRuntimeFields.map(_.operationName)))
    } thenReturn Future.successful(Some(RuntimeErrorDetails(Code.UNKNOWN.value, Some("Test message"))))

    when {
      gdDAO.deleteCluster(mockitoEq(creatingCluster.googleProject),
                          RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.unit

    val newClusterId = GoogleId(UUID.randomUUID().toString)
    when {
      gdDAO.createCluster(mockitoEq(creatingCluster.googleProject),
                          RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)),
                          any[CreateClusterConfig])
    } thenReturn Future.successful {
      Operation(creatingCluster.asyncRuntimeFields.map(_.operationName).get, newClusterId)
    }

    when {
      projectDAO.getLabels(any[String])
    } thenReturn Future.successful(Map("key" -> "value"))

    when {
      projectDAO.getProjectNumber(mockitoEq(creatingCluster.googleProject.value))
    } thenReturn Future.successful(Some(42L))

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
      gdDAO.getClusterMasterInstance(mockitoEq(creatingCluster.googleProject),
                                     RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(
      Some(DataprocInstanceKey(creatingCluster.googleProject, ZoneName("my-zone"), InstanceName("master-instance")))
    )

    when {
      gdDAO.getClusterStagingBucket(mockitoEq(creatingCluster.googleProject),
                                    RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket")))

    when {
      storageDAO.deleteBucket(any[GcsBucketName], mockitoEq(true))
    } thenReturn Future.successful(())

    when {
      storageDAO.setBucketLifecycle(any[GcsBucketName], any[Int], any[GcsLifecycleType])
    } thenReturn Future.successful(())

    when {
      computeDAO.getFirewallRule(mockitoEq(creatingCluster.googleProject), FirewallRuleName(any[String]))(
        (any[ApplicativeAsk[IO, TraceId]])
      )
    } thenReturn IO.pure(Some(Firewall.newBuilder().setId("fw").build()))

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
        RuntimeInternalId(mockitoEq(creatingCluster.internalId.asString)),
        mockitoEq(creatingCluster.auditInfo.creator),
        mockitoEq(creatingCluster.auditInfo.creator),
        mockitoEq(creatingCluster.googleProject),
        RuntimeName(mockitoEq(creatingCluster.runtimeName.asString))
      )(any[ApplicativeAsk[IO, TraceId]])
    } thenReturn IO.unit

    val publisherQueue = QueueFactory.makePublisherQueue()
    withClusterSupervisor(
      gdDAO,
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
      queue = publisherQueue
    ) { _ =>
      eventually {
        val newCluster = dbFutureValue {
          clusterQuery.getClusterById(savedCreatingCluster.id)
        }
        newCluster.get.status shouldBe RuntimeStatus.Creating
        // Since creating cluster is now initiated by pubsub message, we're only validating that we've published the right message
        val createClusterMsg =
          publisherQueue.dequeue1.unsafeRunSync().asInstanceOf[LeoPubsubMessage.CreateRuntimeMessage]
        val expectedMsg = CreateRuntimeMessage.fromRuntime(creatingCluster, CommonTestData.defaultRuntimeConfig, None)
        createClusterMsg.copy(traceId = None, scopes = Set.empty, id = -1) shouldBe (expectedMsg.copy(
          scopes = Set.empty
        ))
        createClusterMsg.scopes should contain theSameElementsAs (expectedMsg.scopes)
      }
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
        RuntimeInternalId(mockitoEq(creatingCluster.internalId.asString)),
        mockitoEq(creatingCluster.auditInfo.creator),
        mockitoEq(creatingCluster.auditInfo.creator),
        mockitoEq(creatingCluster.googleProject),
        RuntimeName(mockitoEq(creatingCluster.runtimeName.asString))
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
    deletingCluster shouldEqual savedDeletingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(deletingCluster.googleProject),
                             RuntimeName(mockitoEq(deletingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Running))

    when {
      gdDAO.getClusterInstances(mockitoEq(deletingCluster.googleProject),
                                RuntimeName(mockitoEq(deletingCluster.runtimeName.asString)))
    } thenReturn Future.successful(clusterInstances)

    val computeDAO = stubComputeService(GceInstanceStatus.Running)
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
          clusterQuery.getDeletingClusterByName(deletingCluster.googleProject, deletingCluster.runtimeName)
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
    creatingCluster shouldEqual savedCreatingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

    val creatingCluster2 = creatingCluster.copy(
      runtimeName = RuntimeName(creatingCluster.runtimeName.asString + "_2"),
      asyncRuntimeFields = creatingCluster.asyncRuntimeFields.map(
        _.copy(googleId = GoogleId(UUID.randomUUID().toString), hostIp = Some(IP("5.6.7.8")))
      )
    )
    val savedCreatingCluster2 = creatingCluster2.save()
    creatingCluster2 shouldEqual savedCreatingCluster2.copy(runtimeConfigId = RuntimeConfigId(-1))

    val gdDAO = mock[GoogleDataprocDAO]
    val projectDAO = mock[GoogleProjectDAO]
    val computeDAO = stubComputeService(GceInstanceStatus.Running)
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject),
                             RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Running))
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster2.googleProject),
                             RuntimeName(mockitoEq(creatingCluster2.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Running))

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject),
                                RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(clusterInstances)
    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster2.googleProject),
                                RuntimeName(mockitoEq(creatingCluster2.runtimeName.asString)))
    } thenReturn Future.successful(clusterInstances2)

    when {
      gdDAO.getClusterMasterInstance(mockitoEq(creatingCluster.googleProject),
                                     RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(
      Some(DataprocInstanceKey(creatingCluster.googleProject, ZoneName("my-zone"), InstanceName("master-instance")))
    )
    when {
      gdDAO.getClusterMasterInstance(mockitoEq(creatingCluster2.googleProject),
                                     RuntimeName(mockitoEq(creatingCluster2.runtimeName.asString)))
    } thenReturn Future.successful(
      Some(DataprocInstanceKey(creatingCluster.googleProject, ZoneName("my-zone"), InstanceName("master-instance")))
    )

    when {
      gdDAO.getClusterStagingBucket(mockitoEq(creatingCluster.googleProject),
                                    RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket")))
    when {
      gdDAO.getClusterStagingBucket(mockitoEq(creatingCluster2.googleProject),
                                    RuntimeName(mockitoEq(creatingCluster2.runtimeName.asString)))
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
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.runtimeName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.get.status shouldBe RuntimeStatus.Running
        updatedCluster.flatMap(_.asyncRuntimeFields.flatMap(_.hostIp)) shouldBe Some(IP("1.2.3.4"))
        updatedCluster.get.dataprocInstances.map(_.key) shouldBe Set(masterInstance, workerInstance1, workerInstance2)
          .map(_.key)
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])

      // Create the second cluster

      eventually {
        val updatedCluster2 = dbFutureValue {
          clusterQuery.getActiveClusterByName(creatingCluster2.googleProject, creatingCluster2.runtimeName)
        }
        updatedCluster2 shouldBe 'defined
        updatedCluster2.get.status shouldBe RuntimeStatus.Running
        updatedCluster2.flatMap(_.asyncRuntimeFields.flatMap(_.hostIp)) shouldBe Some(IP("1.2.3.4")) // same ip because we're using the same set of instances
        updatedCluster2.get.dataprocInstances
          .map(_.key) shouldBe Set(masterInstance, workerInstance1, workerInstance2).map(modifyInstance).map(_.key)

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
    stoppingCluster shouldEqual savedStoppingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(stoppingCluster.googleProject),
                             RuntimeName(mockitoEq(stoppingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Running))
    when {
      gdDAO.getClusterInstances(mockitoEq(stoppingCluster.googleProject),
                                RuntimeName(mockitoEq(stoppingCluster.runtimeName.asString)))
    } thenReturn Future.successful(clusterInstances)

    val computeDAO = stubComputeService(GceInstanceStatus.Stopped)
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
          clusterQuery.getActiveClusterByName(stoppingCluster.googleProject, stoppingCluster.runtimeName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.get.status shouldBe RuntimeStatus.Stopped
        updatedCluster.flatMap(_.asyncRuntimeFields.flatMap(_.hostIp)) shouldBe None
        updatedCluster.get.dataprocInstances.map(_.key) shouldBe Set(masterInstance, workerInstance1, workerInstance2)
          .map(_.key)
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
    startingCluster shouldEqual savedStartingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(startingCluster.googleProject),
                             RuntimeName(mockitoEq(startingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Running))
    when {
      gdDAO.getClusterInstances(mockitoEq(startingCluster.googleProject),
                                RuntimeName(mockitoEq(startingCluster.runtimeName.asString)))
    } thenReturn Future.successful(clusterInstances)
    when {
      gdDAO.getClusterMasterInstance(mockitoEq(startingCluster.googleProject),
                                     RuntimeName(mockitoEq(startingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(masterInstance.key))
    when {
      gdDAO.getClusterStagingBucket(mockitoEq(startingCluster.googleProject),
                                    RuntimeName(mockitoEq(startingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket")))

    val computeDAO = stubComputeService(GceInstanceStatus.Running)
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
      jupyterDAO.isProxyAvailable(mockitoEq(startingCluster.googleProject),
                                  RuntimeName(mockitoEq(startingCluster.runtimeName.asString)))
    } thenReturn IO.pure(true)

    val rstudioDAO = mock[RStudioDAO[IO]]
    when {
      rstudioDAO.isProxyAvailable(mockitoEq(startingCluster.googleProject),
                                  RuntimeName(mockitoEq(startingCluster.runtimeName.asString)))
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
          clusterQuery.getActiveClusterByName(startingCluster.googleProject, startingCluster.runtimeName)
        }
        updatedCluster shouldBe 'defined
        updatedCluster.get.status shouldBe RuntimeStatus.Running
        updatedCluster.flatMap(_.asyncRuntimeFields.flatMap(_.hostIp)) shouldBe Some(IP("1.2.3.4"))
        updatedCluster.get.dataprocInstances.map(_.key) shouldBe Set(masterInstance, workerInstance1, workerInstance2)
          .map(_.key)
      }
      verify(storageDAO, never).deleteBucket(any[GcsBucketName], any[Boolean])
      // starting a cluster should not touch IAM roles
      verify(iamDAO, never()).removeIamRoles(any[GoogleProject], any[WorkbenchEmail], any[MemberType], any[Set[String]])
      verify(iamDAO, never()).removeServiceAccountKey(any[GoogleProject], any[WorkbenchEmail], any[ServiceAccountKeyId])
      verify(jupyterDAO, times(1)).isProxyAvailable(any[GoogleProject], RuntimeName(any[String]))
      verify(rstudioDAO, times(1)).isProxyAvailable(any[GoogleProject], RuntimeName(any[String]))
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
    stopAfterCreationCluster.save().copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual stopAfterCreationCluster

    val projectDAO = mock[GoogleProjectDAO]
    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(creatingCluster.googleProject),
                             RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Running))

    when {
      gdDAO.getClusterInstances(mockitoEq(creatingCluster.googleProject),
                                RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Map((Master: DataprocRole) -> Set(masterInstance.key)))

    when {
      gdDAO.getClusterMasterInstance(mockitoEq(creatingCluster.googleProject),
                                     RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(masterInstance.key))

    when {
      gdDAO.getClusterStagingBucket(mockitoEq(creatingCluster.googleProject),
                                    RuntimeName(mockitoEq(creatingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(GcsBucketName("staging-bucket")))

    val computeDAO = mock[GoogleComputeService[IO]]
    when {
      computeDAO.getInstance(mockitoEq(masterInstance.key.project),
                             ZoneName(mockitoEq(masterInstance.key.zone.value)),
                             InstanceName(mockitoEq(masterInstance.key.name.value)))(any[ApplicativeAsk[IO, TraceId]])
    } thenReturn {
      IO.pure(
        Some(
          Instance
            .newBuilder()
            .setId(masterInstance.googleId.toString)
            .setStatus(GceInstanceStatus.Running.toString)
            .addNetworkInterfaces(
              NetworkInterface
                .newBuilder()
                .addAccessConfigs(AccessConfig.newBuilder().setNatIP("1.2.3.4").build())
                .build()
            )
            .build
        )
      )
    } thenReturn {
      IO.pure(
        Some(
          Instance
            .newBuilder()
            .setId(masterInstance.googleId.toString)
            .setStatus(GceInstanceStatus.Stopped.toString)
            .addNetworkInterfaces(
              NetworkInterface
                .newBuilder()
                .addAccessConfigs(AccessConfig.newBuilder().setNatIP("1.2.3.4").build())
                .build()
            )
            .build
        )
      )
    }

    when {
      computeDAO.addInstanceMetadata(
        mockitoEq(masterInstance.key.project),
        ZoneName(mockitoEq(masterInstance.key.zone.value)),
        InstanceName(mockitoEq(masterInstance.key.name.value)),
        any[Map[String, String]]
      )(any[ApplicativeAsk[IO, TraceId]])
    } thenReturn IO.unit

    when {
      computeDAO.stopInstance(mockitoEq(masterInstance.key.project),
                              ZoneName(mockitoEq(masterInstance.key.zone.value)),
                              InstanceName(mockitoEq(masterInstance.key.name.value)))(any[ApplicativeAsk[IO, TraceId]])
    } thenReturn IO.pure(GoogleOperation.newBuilder().setId("op").build())

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
          clusterQuery.getActiveClusterByName(creatingCluster.googleProject, creatingCluster.runtimeName)
        }
        updatedCluster shouldBe 'defined

        updatedCluster.get.status shouldBe RuntimeStatus.Stopped
        updatedCluster.flatMap(_.asyncRuntimeFields.flatMap(_.hostIp)) shouldBe None
        updatedCluster.get.dataprocInstances.map(_.key) shouldBe Set(masterInstance.key)
        updatedCluster.get.dataprocInstances.map(_.status) shouldBe Set(GceInstanceStatus.Stopped)
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
    savedErrorCluster.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual errorCluster

    val gdDAO = mock[GoogleDataprocDAO]
    val computeDAO = mock[GoogleComputeService[IO]]
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
          clusterQuery.getActiveClusterByName(errorCluster.googleProject, errorCluster.runtimeName)
        }
        dbCluster shouldBe 'defined
        dbCluster.get.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual errorCluster
      }
      verify(gdDAO, never()).getClusterStatus(any[GoogleProject], RuntimeName(any[String]))
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
    stoppingCluster shouldEqual savedStoppingCluster.copy(runtimeConfigId = RuntimeConfigId(-1))

    val gdDAO = mock[GoogleDataprocDAO]
    when {
      gdDAO.getClusterStatus(mockitoEq(stoppingCluster.googleProject),
                             RuntimeName(mockitoEq(stoppingCluster.runtimeName.asString)))
    } thenReturn Future.successful(Some(DataprocClusterStatus.Running))
    when {
      gdDAO.getClusterInstances(mockitoEq(stoppingCluster.googleProject),
                                RuntimeName(mockitoEq(stoppingCluster.runtimeName.asString)))
    } thenReturn Future.successful(clusterInstances)

    val computeDAO = stubComputeService(GceInstanceStatus.Stopped)
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
                          queue = queue) { actor =>
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
