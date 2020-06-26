package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import java.io.ByteArrayInputStream
import java.net.URL
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.mtl.ApplicativeAsk
import com.typesafe.scalalogging.LazyLogging
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, MockGoogleDiskService}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.ContainerImage.GCR
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, Proxy, RStudio, Welder}
import org.broadinstitute.dsde.workbench.leonardo.SamResource.RuntimeSamResource
import org.broadinstitute.dsde.workbench.leonardo.auth.WhitelistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeService
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockDockerDAO, MockSamDAO, MockWelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.util.Retry
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{never, verify, _}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class LeonardoServiceSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterAll
    with TestComponent
    with ScalaFutures
    with OptionValues
    with Retry
    with LazyLogging {

  private var gdDAO: MockGoogleDataprocDAO = _
  private var directoryDAO: MockGoogleDirectoryDAO = _
  private var iamDAO: MockGoogleIamDAO = _
  private var projectDAO: MockGoogleProjectDAO = _
  private var storageDAO: MockGoogleStorageDAO = _
  private var samDao: MockSamDAO = _
  private var bucketHelper: BucketHelper[IO] = _
  private var dataprocInterp: RuntimeAlgebra[IO] = _
  private var gceInterp: RuntimeAlgebra[IO] = _
  private var leo: LeonardoService = _
  private var authProvider: LeoAuthProvider[IO] = _

  val mockPetGoogleDAO: String => GoogleStorageDAO = _ => {
    new MockGoogleStorageDAO
  }

  before {
    gdDAO = new MockGoogleDataprocDAO
    directoryDAO = new MockGoogleDirectoryDAO()
    iamDAO = new MockGoogleIamDAO
    projectDAO = new MockGoogleProjectDAO
    storageDAO = new MockGoogleStorageDAO
    // Pre-populate the juptyer extension bucket in the mock storage DAO, as it is passed in some requests
    // Value should match userJupyterExtensionConfig variable
    storageDAO.buckets += GcsBucketName("bucket-name") -> Set(
      (GcsObjectName("extension"), new ByteArrayInputStream("foo".getBytes()))
    )

    // Set up the mock directoryDAO to have the Google group used to grant permission to users to pull the custom dataproc image
    directoryDAO
      .createGroup(
        Config.googleGroupsConfig.dataprocImageProjectGroupName,
        Config.googleGroupsConfig.dataprocImageProjectGroupEmail,
        Option(directoryDAO.lockedDownGroupSettings)
      )
      .futureValue

    samDao = serviceAccountProvider.samDao
    authProvider = new WhitelistAuthProvider(whitelistAuthConfig, serviceAccountProvider)

    val bucketHelperConfig =
      BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig, clusterResourcesConfig)
    val bucketHelper =
      new BucketHelper[IO](bucketHelperConfig, FakeGoogleStorageService, serviceAccountProvider, blocker)
    val vpcInterp = new VPCInterpreter[IO](Config.vpcInterpreterConfig, projectDAO, MockGoogleComputeService)
    dataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                 bucketHelper,
                                                 vpcInterp,
                                                 gdDAO,
                                                 MockGoogleComputeService,
                                                 MockGoogleDiskService,
                                                 directoryDAO,
                                                 iamDAO,
                                                 projectDAO,
                                                 MockWelderDAO,
                                                 blocker)
    gceInterp = new GceInterpreter[IO](Config.gceInterpreterConfig,
                                       bucketHelper,
                                       vpcInterp,
                                       MockGoogleComputeService,
                                       MockGoogleDiskService,
                                       MockWelderDAO,
                                       blocker)
    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    leo = new LeonardoService(dataprocConfig,
                              imageConfig,
                              MockWelderDAO,
                              proxyConfig,
                              swaggerConfig,
                              autoFreezeConfig,
                              Config.zombieRuntimeMonitorConfig,
                              welderConfig,
                              mockPetGoogleDAO,
                              authProvider,
                              serviceAccountProvider,
                              bucketHelper,
                              new MockDockerDAO,
                              QueueFactory.makePublisherQueue())
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "LeonardoService" should "create a single node cluster with default machine configs" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeRunSync()

    // check the create response has the correct info
    clusterCreateResponse.clusterName shouldBe name0
    clusterCreateResponse.googleProject shouldBe project
    clusterCreateResponse.serviceAccountInfo shouldEqual clusterServiceAccountFromProject(project).get
    clusterCreateResponse.asyncRuntimeFields shouldBe None
    clusterCreateResponse.auditInfo.creator shouldBe userInfo.userEmail
    clusterCreateResponse.auditInfo.destroyedDate shouldBe None
    clusterCreateResponse.runtimeConfig shouldEqual singleNodeDefaultMachineConfig
    clusterCreateResponse.status shouldBe RuntimeStatus.Creating
    clusterCreateResponse.jupyterExtensionUri shouldBe None
    clusterCreateResponse.jupyterUserScriptUri shouldBe testClusterRequest.jupyterUserScriptUri
    clusterCreateResponse.jupyterStartUserScriptUri shouldBe testClusterRequest.jupyterStartUserScriptUri
    clusterCreateResponse.errors shouldBe List.empty
    clusterCreateResponse.dataprocInstances shouldBe Set.empty
    clusterCreateResponse.userJupyterExtensionConfig shouldBe testClusterRequest.userJupyterExtensionConfig
    clusterCreateResponse.autopauseThreshold shouldBe testClusterRequest.autopauseThreshold.getOrElse(0)
    clusterCreateResponse.defaultClientId shouldBe testClusterRequest.defaultClientId
    clusterCreateResponse.stopAfterCreation shouldBe testClusterRequest.stopAfterCreation.getOrElse(false)
    clusterCreateResponse.clusterImages.map(_.imageType) shouldBe Set(Jupyter, Proxy)
    clusterCreateResponse.scopes shouldBe dataprocConfig.defaultScopes
    clusterCreateResponse.welderEnabled shouldBe false
    clusterCreateResponse.labels.get("tool") shouldBe Some("Jupyter")
    clusterCreateResponse.clusterUrl shouldBe new URL(s"https://leo/proxy/${project.value}/${name0.asString}/jupyter")

    // check the cluster persisted to the database matches the create response
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(clusterCreateResponse.id))
    TestUtils.compareClusterAndCreateClusterAPIResponse(dbCluster.get, clusterCreateResponse)

    // check that no state in Google changed
    // TODO
//    computeDAO.firewallRules shouldBe 'empty
    storageDAO.buckets.keySet shouldBe Set(GcsBucketName("bucket-name"))

    // init bucket should not have been persisted to the database
    val dbInitBucketOpt = dbFutureValue(clusterQuery.getInitBucket(project, name0))
    dbInitBucketOpt shouldBe None
  }

  it should "create and get a cluster" in isolatedDbTest {
    // create a cluster
    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // get the cluster detail
    val clusterGetResponse = leo.getActiveClusterDetails(userInfo, project, name1).unsafeToFuture.futureValue
    // check the create response and get response are the same
    TestUtils.compareClusterAndCreateClusterAPIResponse(clusterGetResponse, clusterCreateResponse)
  }

  it should "create a cluster with the default welder image" in isolatedDbTest {
    // create the cluster
    val clusterRequest = testClusterRequest.copy(
      runtimeConfig = Some(singleNodeDefaultMachineConfigRequest),
      stopAfterCreation = Some(true),
      enableWelder = Some(true)
    )

    val clusterResponse = leo.createCluster(userInfo, project, name1, clusterRequest).unsafeToFuture.futureValue

    // check the cluster persisted to the database matches the create response
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(clusterResponse.id))
    TestUtils.compareClusterAndCreateClusterAPIResponse(dbCluster.get, clusterResponse)

    // cluster images should contain welder and Jupyter
    clusterResponse.clusterImages.find(_.imageType == Jupyter).map(_.imageUrl) shouldBe Some(
      imageConfig.jupyterImage.imageUrl
    )
    clusterResponse.clusterImages.find(_.imageType == RStudio) shouldBe None
    clusterResponse.clusterImages.find(_.imageType == Welder).map(_.imageUrl) shouldBe Some(
      imageConfig.welderImage.imageUrl
    )
  }

  it should "create a cluster with a client-supplied welder image" in isolatedDbTest {
    val customWelderImage = GCR("my-custom-welder-image-link")

    // create the cluster
    val clusterRequest = testClusterRequest.copy(
      runtimeConfig = Some(singleNodeDefaultMachineConfigRequest),
      stopAfterCreation = Some(true),
      welderDockerImage = Some(customWelderImage),
      enableWelder = Some(true)
    )

    val clusterResponse = leo.createCluster(userInfo, project, name1, clusterRequest).unsafeToFuture.futureValue

    // check the cluster persisted to the database matches the create response
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(clusterResponse.id))
    TestUtils.compareClusterAndCreateClusterAPIResponse(dbCluster.get, clusterResponse)

    // cluster images should contain welder and Jupyter
    clusterResponse.clusterImages.find(_.imageType == Jupyter).map(_.imageUrl) shouldBe Some(
      imageConfig.jupyterImage.imageUrl
    )
    clusterResponse.clusterImages.find(_.imageType == RStudio) shouldBe None
    clusterResponse.clusterImages.find(_.imageType == Welder).map(_.imageUrl) shouldBe Some(customWelderImage.imageUrl)
  }

  it should "create a cluster with an rstudio image" in isolatedDbTest {
    val rstudioImage = GCR("some-rstudio-image")

    // create the cluster
    val clusterRequest = testClusterRequest.copy(toolDockerImage = Some(rstudioImage), enableWelder = Some(true))
    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    val leoForTest = new LeonardoService(dataprocConfig,
                                         imageConfig,
                                         MockWelderDAO,
                                         proxyConfig,
                                         swaggerConfig,
                                         autoFreezeConfig,
                                         Config.zombieRuntimeMonitorConfig,
                                         welderConfig,
                                         mockPetGoogleDAO,
                                         authProvider,
                                         serviceAccountProvider,
                                         bucketHelper,
                                         new MockDockerDAO(RStudio),
                                         QueueFactory.makePublisherQueue())

    val clusterResponse = leoForTest.createCluster(userInfo, project, name1, clusterRequest).unsafeToFuture.futureValue

    // check the cluster persisted to the database matches the create response
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(clusterResponse.id))
    TestUtils.compareClusterAndCreateClusterAPIResponse(dbCluster.get, clusterResponse)

    // cluster images should contain welder and RStudio
    clusterResponse.clusterImages.find(_.imageType == RStudio).map(_.imageUrl) shouldBe Some(rstudioImage.imageUrl)
    clusterResponse.clusterImages.find(_.imageType == Jupyter) shouldBe None
    clusterResponse.clusterImages.find(_.imageType == Welder).map(_.imageUrl) shouldBe Some(
      imageConfig.welderImage.imageUrl
    )
    clusterResponse.labels.get("tool") shouldBe Some("RStudio")
    clusterResponse.clusterUrl shouldBe new URL(s"https://leo/proxy/${project.value}/${name1.asString}/rstudio")

    // list clusters should return RStudio information
    val clusterList = leoForTest.listClusters(userInfo, Map.empty, Some(project)).unsafeToFuture.futureValue
    clusterList.size shouldBe 1
    clusterList.toSet shouldBe Set(clusterResponse).map(x => LeoLenses.createRuntimeRespToListRuntimeResp.get(x))
    clusterList.head.labels.get("tool") shouldBe Some("RStudio")
    clusterList.head.clusterUrl shouldBe new URL(s"https://leo/proxy/${project.value}/${name1.asString}/rstudio")
  }

  it should "create a single node with default machine config cluster when no machine config is set" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(runtimeConfig = None)

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual dataprocConfig.runtimeConfigDefaults
  }

  it should "create a single node cluster with zero workers explicitly defined in machine config" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.DataprocConfig(
          numberOfWorkers = Some(0),
          masterMachineType = Some(dataprocConfig.runtimeConfigDefaults.masterMachineType),
          masterDiskSize = Some(dataprocConfig.runtimeConfigDefaults.masterDiskSize),
          None,
          None,
          None,
          None,
          Map.empty
        )
      )
    )
    val expectedRuntimeConfig = RuntimeConfig.DataprocConfig(
      numberOfWorkers = 0,
      masterMachineType = dataprocConfig.runtimeConfigDefaults.masterMachineType,
      masterDiskSize = dataprocConfig.runtimeConfigDefaults.masterDiskSize,
      None,
      None,
      None,
      None,
      Map.empty
    )

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual expectedRuntimeConfig
  }

  it should "create a single node cluster with master configs defined" in isolatedDbTest {
    val singleNodeDefinedMachineConfigReq =
      RuntimeConfigRequest.DataprocConfig(Some(0),
                                          Some(MachineTypeName("test-master-machine-type2")),
                                          Some(DiskSize(200)),
                                          None,
                                          None,
                                          None,
                                          None,
                                          Map.empty)

    val expectedRuntimeConfig = RuntimeConfig.DataprocConfig(
      numberOfWorkers = 0,
      masterMachineType = MachineTypeName("test-master-machine-type2"),
      masterDiskSize = DiskSize(200),
      workerMachineType = None,
      workerDiskSize = None,
      numberOfWorkerLocalSSDs = None,
      numberOfPreemptibleWorkers = None,
      properties = Map.empty
    )

    val clusterRequestWithMachineConfig =
      testClusterRequest.copy(runtimeConfig = Some(singleNodeDefinedMachineConfigReq))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual expectedRuntimeConfig
  }

  it should "create a single node cluster and override worker configs" in isolatedDbTest {
    // machine config is creating a single node cluster, but has worker configs defined
    val machineConfig = Some(
      RuntimeConfigRequest.DataprocConfig(
        Some(0),
        Some(MachineTypeName("test-master-machine-type3")),
        Some(DiskSize(200)),
        Some(MachineTypeName("test-worker-machine-type")),
        Some(DiskSize(10)),
        Some(3),
        Some(4),
        Map.empty
      )
    )
    val clusterRequestWithMachineConfig = testClusterRequest.copy(runtimeConfig = machineConfig)

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual RuntimeConfig.DataprocConfig(
      0,
      MachineTypeName("test-master-machine-type3"),
      DiskSize(200),
      None,
      None,
      None,
      None,
      Map.empty
    )
  }

  it should "create a standard cluster with 2 workers with default worker configs" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(
      runtimeConfig = Some(
        RuntimeConfigRequest.DataprocConfig(
          numberOfWorkers = Some(2),
          masterDiskSize = Some(singleNodeDefaultMachineConfig.masterDiskSize),
          masterMachineType = Some(singleNodeDefaultMachineConfig.masterMachineType),
          workerMachineType = None,
          workerDiskSize = None,
          numberOfWorkerLocalSSDs = None,
          numberOfPreemptibleWorkers = None,
          properties = Map.empty
        )
      )
    )
    val machineConfigResponse = RuntimeConfig.DataprocConfig(
      2,
      singleNodeDefaultMachineConfig.masterMachineType,
      singleNodeDefaultMachineConfig.masterDiskSize,
      singleNodeDefaultMachineConfig.workerMachineType,
      singleNodeDefaultMachineConfig.workerDiskSize,
      singleNodeDefaultMachineConfig.numberOfWorkerLocalSSDs,
      singleNodeDefaultMachineConfig.numberOfPreemptibleWorkers,
      Map.empty
    )

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual machineConfigResponse
  }

  it should "create a standard cluster with 10 workers with defined config" in isolatedDbTest {
    val machineConfig = RuntimeConfigRequest.DataprocConfig(
      Some(10),
      Some(MachineTypeName("test-master-machine-type")),
      Some(DiskSize(200)),
      Some(MachineTypeName("test-worker-machine-type")),
      Some(DiskSize(300)),
      Some(3),
      Some(4),
      Map.empty
    )
    val expectedRuntimeConfig = RuntimeConfig.DataprocConfig(
      numberOfWorkers = 10,
      masterMachineType = MachineTypeName("test-master-machine-type"),
      masterDiskSize = DiskSize(200),
      workerMachineType = Some(MachineTypeName("test-worker-machine-type")),
      workerDiskSize = Some(DiskSize(300)),
      numberOfWorkerLocalSSDs = Some(3),
      numberOfPreemptibleWorkers = Some(4),
      properties = Map.empty
    )
    val clusterRequestWithMachineConfig = testClusterRequest.copy(runtimeConfig = Some(machineConfig))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual expectedRuntimeConfig
  }

  it should "create a standard cluster with 2 workers and override too-small disk sizes with minimum disk size" in isolatedDbTest {
    val machineConfig = RuntimeConfigRequest.DataprocConfig(
      Some(2),
      Some(MachineTypeName("test-master-machine-type")),
      Some(DiskSize(5)),
      Some(MachineTypeName("test-worker-machine-type")),
      Some(DiskSize(5)),
      Some(3),
      Some(4),
      Map.empty
    )
    val clusterRequestWithMachineConfig = testClusterRequest.copy(runtimeConfig = Some(machineConfig))
    val expectedMachineConfig = RuntimeConfig.DataprocConfig(
      2,
      MachineTypeName("test-master-machine-type"),
      DiskSize(10),
      Some(MachineTypeName("test-worker-machine-type")),
      Some(DiskSize(10)),
      Some(3),
      Some(4),
      Map.empty
    )

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.runtimeConfig shouldEqual expectedMachineConfig
  }

  it should "throw ClusterNotFoundException for nonexistent clusters" in isolatedDbTest {
    val exc = leo
      .getActiveClusterDetails(userInfo, GoogleProject("nonexistent"), RuntimeName("cluster"))
      .unsafeToFuture
      .failed
      .futureValue
    exc shouldBe a[RuntimeNotFoundException]
  }

  it should "throw ClusterAlreadyExistsException when creating a cluster with same name and project as an existing cluster" in isolatedDbTest {
    val cluster1 = leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeToFuture.futureValue
    val exc = leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeToFuture.failed.futureValue
    exc shouldBe a[RuntimeAlreadyExistsException]
  }

  it should "create two clusters with same name with only one active" in isolatedDbTest {
    // create first cluster
    val cluster = leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(cluster.id))
    dbCluster.map(_.status) shouldBe Some(RuntimeStatus.Creating)

    // change cluster status to Running so that it can be deleted
    dbFutureValue(clusterQuery.setToRunning(cluster.id, IP("numbers.and.dots"), Instant.now))

    // delete the cluster
    leo.deleteCluster(userInfo, project, name0).unsafeToFuture.futureValue

    // check that the cluster was deleted
    val dbDeletingCluster = dbFutureValue(clusterQuery.getClusterById(cluster.id))
    dbDeletingCluster.map(_.status) shouldBe Some(RuntimeStatus.Deleted)

    // recreate cluster with same project and cluster name
    val cluster2 = leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster2 = dbFutureValue(clusterQuery.getClusterById(cluster2.id))
    dbCluster2.map(_.status) shouldBe Some(RuntimeStatus.Creating)
  }

  it should "delete a Running cluster" in isolatedDbTest {
    // need a specialized LeonardoService for this test, so we can spy on its authProvider
    val spyProvider: LeoAuthProvider[IO] = spy(authProvider)
    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }
    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    val publisherQueue = QueueFactory.makePublisherQueue()
    val leoForTest = new LeonardoService(dataprocConfig,
                                         imageConfig,
                                         MockWelderDAO,
                                         proxyConfig,
                                         swaggerConfig,
                                         autoFreezeConfig,
                                         Config.zombieRuntimeMonitorConfig,
                                         welderConfig,
                                         mockPetGoogleStorageDAO,
                                         spyProvider,
                                         serviceAccountProvider,
                                         bucketHelper,
                                         new MockDockerDAO,
                                         publisherQueue)

    val cluster = leoForTest.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(cluster.id))
    dbCluster.map(_.status) shouldBe Some(RuntimeStatus.Creating)

    // change cluster status to Running so that it can be deleted
    val updateAsyncClusterCreationFields = UpdateAsyncClusterCreationFields(
      Some(GcsPath(initBucketName, GcsObjectName(""))),
      Some(serviceAccountKey),
      cluster.id,
      Some(makeAsyncRuntimeFields(1)),
      Instant.now
    )
    dbFutureValue {
      clusterQuery.updateAsyncClusterCreationFields(updateAsyncClusterCreationFields)
    }
    dbFutureValue(clusterQuery.setToRunning(cluster.id, IP("numbers.and.dots"), Instant.now))

    // delete the cluster
    leoForTest.deleteCluster(userInfo, project, name1).unsafeToFuture.futureValue

    val validateStatus = withLeoPublisher(publisherQueue) {
      for {
        status <- clusterQuery.getClusterStatus(cluster.id).transaction
        message <- publisherQueue.tryDequeue1
      } yield {
        status shouldBe Some(RuntimeStatus.Deleting)
        message shouldBe (None)
      }
    }

    validateStatus.unsafeRunSync()
    // the auth provider should have not yet been notified of deletion
    verify(spyProvider, never).notifyResourceDeleted(
      RuntimeSamResource(mockitoEq(cluster.samResource.resourceId)),
      mockitoEq(userInfo.userEmail),
      mockitoEq(userInfo.userEmail),
      mockitoEq(project)
    )(any[ApplicativeAsk[IO, TraceId]])
  }

  it should "delete a cluster that has status Error" in isolatedDbTest {
    // need a specialized LeonardoService for this test, so we can spy on its authProvider
    val spyProvider: LeoAuthProvider[IO] = spy(authProvider)
    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }
    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    val publisherQueue = QueueFactory.makePublisherQueue()
    val leoForTest = new LeonardoService(dataprocConfig,
                                         imageConfig,
                                         MockWelderDAO,
                                         proxyConfig,
                                         swaggerConfig,
                                         autoFreezeConfig,
                                         Config.zombieRuntimeMonitorConfig,
                                         welderConfig,
                                         mockPetGoogleStorageDAO,
                                         spyProvider,
                                         serviceAccountProvider,
                                         bucketHelper,
                                         new MockDockerDAO,
                                         publisherQueue)

    // create the cluster
    val cluster = leoForTest.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(cluster.id))
    dbCluster.map(_.status) shouldBe Some(RuntimeStatus.Creating)

    // change the cluster status to Error
    val updateAsyncClusterCreationFields = UpdateAsyncClusterCreationFields(
      Some(GcsPath(initBucketName, GcsObjectName(""))),
      Some(serviceAccountKey),
      cluster.id,
      Some(makeAsyncRuntimeFields(1)),
      Instant.now
    )
    dbFutureValue {
      clusterQuery.updateAsyncClusterCreationFields(updateAsyncClusterCreationFields)
    }
    dbFutureValue(clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.Error, Instant.now))

    // delete the cluster
    leoForTest.deleteCluster(userInfo, project, name1).unsafeToFuture.futureValue

    val validateStatus = withLeoPublisher(publisherQueue) {
      for {
        status <- clusterQuery.getClusterStatus(cluster.id).transaction
        message <- publisherQueue.tryDequeue1
      } yield {
        status shouldBe Some(RuntimeStatus.Deleting)
        message shouldBe (None)
      }
    }

    validateStatus.unsafeRunSync()

    // the auth provider should have not yet been notified of deletion
    verify(spyProvider, never).notifyResourceDeleted(
      RuntimeSamResource(mockitoEq(cluster.samResource.resourceId)),
      mockitoEq(userInfo.userEmail),
      mockitoEq(userInfo.userEmail),
      mockitoEq(project)
    )(any[ApplicativeAsk[IO, TraceId]])
  }

  it should "delete a cluster's instances" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    val leo = makeLeo(publisherQueue)
    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(cluster.id))
    dbCluster.map(_.status) shouldBe Some(RuntimeStatus.Creating)

    // populate some instances for the cluster
    val getClusterKey = LeoLenses.createRuntimeRespToGetClusterKey.get(cluster)
    dbFutureValue {
      instanceQuery.saveAllForCluster(getClusterId(getClusterKey),
                                      Seq(masterInstance, workerInstance1, workerInstance2))
    }

    // change cluster status to Running so that it can be deleted
    val updateAsyncClusterCreationFields = UpdateAsyncClusterCreationFields(
      Some(GcsPath(initBucketName, GcsObjectName(""))),
      Some(serviceAccountKey),
      cluster.id,
      Some(makeAsyncRuntimeFields(1)),
      Instant.now
    )
    dbFutureValue {
      clusterQuery.updateAsyncClusterCreationFields(updateAsyncClusterCreationFields)
    }
    dbFutureValue(clusterQuery.setToRunning(cluster.id, IP("numbers.and.dots"), Instant.now))

    // delete the cluster
    leo.deleteCluster(userInfo, project, name1).unsafeToFuture.futureValue

    val validateStatus = withLeoPublisher(publisherQueue) {
      for {
        status <- clusterQuery.getClusterStatus(cluster.id).transaction
        message <- publisherQueue.tryDequeue1
      } yield {
        status shouldBe Some(RuntimeStatus.Deleting)
        message shouldBe (None)
      }
    }

    validateStatus.unsafeRunSync()
    // check that the instances are still in the DB (they get removed by the ClusterMonitorActor)
    val getClusterIdKey = LeoLenses.createRuntimeRespToGetClusterKey.get(cluster)
    val instances = dbFutureValue(instanceQuery.getAllForCluster(getClusterId(getClusterIdKey)))
    instances.toSet shouldBe Set(masterInstance, workerInstance1, workerInstance2)
  }

  it should "throw a JupyterExtensionException when the extensionUri is too long" in isolatedDbTest {
    // create the cluster
    val clusterRequest = testClusterRequest.copy(userJupyterExtensionConfig =
      Some(
        UserJupyterExtensionConfig(nbExtensions =
          Map("notebookExtension" -> s"gs://bucket/${Stream.continually('a').take(1025).mkString}")
        )
      )
    )
    val response = leo.createCluster(userInfo, project, name0, clusterRequest).unsafeToFuture.failed.futureValue

    response shouldBe a[BucketObjectException]
  }

  it should "throw a JupyterExtensionException when the jupyterExtensionUri does not point to a GCS object" in isolatedDbTest {
    // create the cluster
    val response =
      leo
        .createCluster(
          userInfo,
          project,
          name0,
          testClusterRequest.copy(userJupyterExtensionConfig =
            Some(UserJupyterExtensionConfig(nbExtensions = Map("notebookExtension" -> "gs://bogus/object.tar.gz")))
          )
        )
        .unsafeToFuture
        .failed
        .futureValue

    response shouldBe a[BucketObjectException]
  }

  it should "list no clusters" in isolatedDbTest {
    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue shouldBe 'empty
    leo.listClusters(userInfo, Map("foo" -> "bar", "baz" -> "biz")).unsafeToFuture.futureValue shouldBe 'empty
  }

  it should "list all clusters" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).unsafeToFuture.futureValue

    val clusterName2 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo
      .createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeToFuture
      .futureValue

    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
  }

  it should "error when trying to delete a creating cluster" in isolatedDbTest {
    // create cluster
    leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeToFuture.futureValue

    // should fail to delete because cluster is in Creating status
    leo
      .deleteCluster(userInfo, project, name0)
      .unsafeToFuture
      .failed
      .futureValue shouldBe a[RuntimeCannotBeDeletedException]
  }

  it should "list all active clusters" in isolatedDbTest {
    // create a couple of clusters
    val cluster1 = leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    val clusterName2 = RuntimeName("test-cluster-2")
    val cluster2 = leo
      .createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeToFuture
      .futureValue

    leo.listClusters(userInfo, Map("includeDeleted" -> "false")).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster1,
      cluster2
    ).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )

    val clusterName3 = RuntimeName("test-cluster-3")
    val cluster3 = leo
      .createCluster(userInfo, project, clusterName3, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeToFuture
      .futureValue

    dbFutureValue(clusterQuery.completeDeletion(cluster3.id, Instant.now))

    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map("includeDeleted" -> "false")).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster1,
      cluster2
    ).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map("includeDeleted" -> "true")).unsafeToFuture.futureValue.toSet.size shouldBe 3
  }

  it should "list clusters with labels" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).unsafeToFuture.futureValue

    val clusterName2 = RuntimeName(s"test-cluster-2")
    val cluster2 = leo
      .createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeToFuture
      .futureValue

    leo.listClusters(userInfo, Map("foo" -> "bar")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2)
      .map(
        LeoLenses.createRuntimeRespToListRuntimeResp.get
      )
    leo.listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes")).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster1
    ).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo
      .listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes", "vcf" -> "no"))
      .unsafeToFuture
      .futureValue
      .toSet shouldBe Set(
      cluster1
    ).map(LeoLenses.createRuntimeRespToListRuntimeResp.get)
    leo.listClusters(userInfo, Map("a" -> "b")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster2).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map("foo" -> "bar", "baz" -> "biz")).unsafeToFuture.futureValue.toSet shouldBe Set.empty
    leo
      .listClusters(userInfo, Map("A" -> "B"))
      .unsafeToFuture
      .futureValue
      .toSet shouldBe Set(cluster2).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    ) // labels are not case sensitive because MySQL
    //Assert that extensions were added as labels as well
    leo
      .listClusters(userInfo, Map("abc" -> "def", "pqr" -> "pqr", "xyz" -> "xyz"))
      .unsafeToFuture
      .futureValue
      .toSet shouldBe Set(
      cluster1,
      cluster2
    ).map(LeoLenses.createRuntimeRespToListRuntimeResp.get)
  }

  it should "throw IllegalLabelKeyException when using a forbidden label" in isolatedDbTest {
    // cluster should not be allowed to have a label with key of "includeDeleted"
    val modifiedTestClusterRequest = testClusterRequest.copy(labels = Map("includeDeleted" -> "val"))
    val includeDeletedResponse =
      leo.createCluster(userInfo, project, name0, modifiedTestClusterRequest).unsafeToFuture.failed.futureValue

    includeDeletedResponse shouldBe a[IllegalLabelKeyException]
  }

  it should "list clusters with swagger-style labels" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).unsafeToFuture.futureValue

    val clusterName2 = RuntimeName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo
      .createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeToFuture
      .futureValue

    leo.listClusters(userInfo, Map("_labels" -> "foo=bar")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1,
                                                                                                          cluster2).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map("_labels" -> "foo=bar,bam=yes")).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster1
    ).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo
      .listClusters(userInfo, Map("_labels" -> "foo=bar,bam=yes,vcf=no"))
      .unsafeToFuture
      .futureValue
      .toSet shouldBe Set(cluster1).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map("_labels" -> "a=b")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster2).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map("_labels" -> "baz=biz")).unsafeToFuture.futureValue.toSet shouldBe Set.empty
    leo.listClusters(userInfo, Map("_labels" -> "A=B")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster2).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    ) // labels are not case sensitive because MySQL
    leo
      .listClusters(userInfo, Map("_labels" -> "foo%3Dbar"))
      .unsafeToFuture
      .failed
      .futureValue shouldBe a[ParseLabelsException]
    leo
      .listClusters(userInfo, Map("_labels" -> "foo=bar;bam=yes"))
      .unsafeToFuture
      .failed
      .futureValue shouldBe a[ParseLabelsException]
    leo
      .listClusters(userInfo, Map("_labels" -> "foo=bar,bam"))
      .unsafeToFuture
      .failed
      .futureValue shouldBe a[ParseLabelsException]
    leo
      .listClusters(userInfo, Map("_labels" -> "bogus"))
      .unsafeToFuture
      .failed
      .futureValue shouldBe a[ParseLabelsException]
    leo
      .listClusters(userInfo, Map("_labels" -> "a,b"))
      .unsafeToFuture
      .failed
      .futureValue shouldBe a[ParseLabelsException]
  }

  it should "list clusters belonging to a project" in isolatedDbTest {
    // create a couple of clusters
    val cluster1 = leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue
    val cluster2 = leo
      .createCluster(userInfo, project2, name2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeToFuture
      .futureValue

    leo.listClusters(userInfo, Map.empty, Some(project)).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map.empty, Some(project2)).unsafeToFuture.futureValue.toSet shouldBe Set(cluster2).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map("foo" -> "bar"), Some(project)).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster1
    ).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map("foo" -> "bar"), Some(project2)).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster2
    ).map(
      LeoLenses.createRuntimeRespToListRuntimeResp.get
    )
    leo.listClusters(userInfo, Map("k" -> "v"), Some(project)).unsafeToFuture.futureValue.toSet shouldBe Set.empty
    leo.listClusters(userInfo, Map("k" -> "v"), Some(project2)).unsafeToFuture.futureValue.toSet shouldBe Set.empty
    leo
      .listClusters(userInfo, Map("foo" -> "bar"), Some(GoogleProject("non-existing-project")))
      .unsafeToFuture
      .futureValue
      .toSet shouldBe Set.empty
  }

  it should "stop a cluster" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    val leo = makeLeo(publisherQueue)
    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(cluster.id))
    dbCluster.map(_.status) shouldBe Some(RuntimeStatus.Creating)

    // populate some instances for the cluster
    val clusterInstances = Seq(masterInstance, workerInstance1, workerInstance2)
    val getClusterKey = LeoLenses.createRuntimeRespToGetClusterKey.get(cluster)
    dbFutureValue(instanceQuery.saveAllForCluster(getClusterId(getClusterKey), clusterInstances))
    // TODO
//    computeDAO.instances ++= clusterInstances.groupBy(_.key).mapValues(_.head)
//    computeDAO.instanceMetadata ++= clusterInstances.groupBy(_.key).mapValues(_ => Map.empty)

    // set the cluster to Running
    dbFutureValue(clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"), Instant.now))

    // stop the cluster
    leo.stopCluster(userInfo, project, name1).unsafeToFuture.futureValue

    val validateStatus = withLeoPublisher(publisherQueue) {
      for {
        status <- clusterQuery.getClusterStatus(cluster.id).transaction
        message <- publisherQueue.tryDequeue1
      } yield {
        status shouldBe Some(RuntimeStatus.Stopping)
        message shouldBe (None)
      }
    }

    validateStatus.unsafeRunSync()
    // instance status should still be Running in the DB
    // the ClusterMonitorActor is what updates instance status
    val instances = dbFutureValue(instanceQuery.getAllForCluster(getClusterId(getClusterKey)))
    instances.size shouldBe 3
    instances.map(_.status).toSet shouldBe Set(GceInstanceStatus.Running)

    // Google instances should be stopped
    // TODO
//    computeDAO.instances.keySet shouldBe clusterInstances.map(_.key).toSet
//    computeDAO.instanceMetadata.keySet shouldBe clusterInstances.map(_.key).toSet
//    computeDAO.instances.values.toSet shouldBe clusterInstances.map(_.copy(status = InstanceStatus.Stopped)).toSet
//    computeDAO.instanceMetadata.values.map(_.keys).flatten.toSet shouldBe Set("shutdown-script")
  }

  it should "calculate autopause threshold properly" in {
    LeonardoService.calculateAutopauseThreshold(None, None, autoFreezeConfig) shouldBe autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
    LeonardoService.calculateAutopauseThreshold(Some(false), None, autoFreezeConfig) shouldBe autoPauseOffValue
    LeonardoService.calculateAutopauseThreshold(Some(true), None, autoFreezeConfig) shouldBe autoFreezeConfig.autoFreezeAfter.toMinutes.toInt
    LeonardoService.calculateAutopauseThreshold(Some(true), Some(30), autoFreezeConfig) shouldBe 30
  }

  it should "start a cluster" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    val leo = makeLeo(publisherQueue)

    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue(clusterQuery.getClusterById(cluster.id))
    dbCluster.map(_.status) shouldBe Some(RuntimeStatus.Creating)

    val getClusterKey = LeoLenses.createRuntimeRespToGetClusterKey.get(cluster)

    // populate some instances for the cluster and set its status to Stopped
    val clusterInstances =
      Seq(masterInstance, workerInstance1, workerInstance2).map(_.copy(status = GceInstanceStatus.Stopped))
    dbFutureValue(instanceQuery.saveAllForCluster(getClusterId(getClusterKey), clusterInstances))
    dbFutureValue(clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.Stopped, Instant.now))
    // TODO
//    computeDAO.instances ++= clusterInstances.groupBy(_.key).mapValues(_.head)
//    computeDAO.instanceMetadata ++= clusterInstances.groupBy(_.key).mapValues(_ => Map.empty)

    // start the cluster
    leo.startCluster(userInfo, project, name1).unsafeToFuture.futureValue

    // cluster status should be Starting in the DB
    val validateStatus = withLeoPublisher(publisherQueue) {
      for {
        status <- clusterQuery.getClusterStatus(cluster.id).transaction
        message <- publisherQueue.tryDequeue1
      } yield {
        status shouldBe Some(RuntimeStatus.Starting)
        message shouldBe (None)
      }
    }

    validateStatus.unsafeRunSync()
    // instance status should still be Stopped in the DB
    // the ClusterMonitorActor is what updates instance status
    val instances = dbFutureValue {
      instanceQuery.getAllForCluster(getClusterId(getClusterKey))
    }
    instances.size shouldBe 3
    instances.map(_.status).toSet shouldBe Set(GceInstanceStatus.Stopped)

    // Google instances should be started
    // TODO
//    computeDAO.instances.keySet shouldBe clusterInstances.map(_.key).toSet
//    computeDAO.instanceMetadata.keySet shouldBe clusterInstances.map(_.key).toSet
//    computeDAO.instances.values.toSet shouldBe clusterInstances.map(_.copy(status = InstanceStatus.Running)).toSet
//    computeDAO.instanceMetadata.values.map(_.keys).flatten.toSet shouldBe Set("startup-script")
  }

  // TODO: remove this test once data syncing release is complete
  it should "label and start an outdated cluster" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    val leo = makeLeo(publisherQueue)
    // create the cluster
    val request = testClusterRequest.copy(labels = Map("TEST_ONLY_DEPLOY_WELDER" -> "yes"))
    val cluster = leo.createCluster(userInfo, project, name1, request)(traceId).unsafeToFuture.futureValue

    // check that the cluster was created
    dbFutureValue(clusterQuery.getClusterById(cluster.id)).map(_.status) shouldBe Some(RuntimeStatus.Creating)

    // set its status to Stopped and update its createdDate
    dbFutureValue(clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.Stopped, Instant.now))
    dbFutureValue {
      clusterQuery.updateClusterCreatedDate(cluster.id,
                                            new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-31").toInstant)
    }

    // start the cluster
    leo.startCluster(userInfo, project, name1).unsafeToFuture.futureValue

    // cluster status should Starting and have new label
    val getClusterKey = LeoLenses.createRuntimeRespToGetClusterKey.get(cluster)
    val dbCluster = dbFutureValue(clusterQuery.getClusterByUniqueKey(getClusterKey)).get
    val validateStatus = withLeoPublisher(publisherQueue) {
      for {
        status <- clusterQuery.getClusterStatus(cluster.id).transaction
        message <- publisherQueue.tryDequeue1
      } yield {
        status shouldBe Some(RuntimeStatus.Starting)
        message shouldBe (None)
      }
    }

    dbCluster.labels.exists(_ == "welderInstallFailed" -> "true")
  }

  it should "extract labels properly" in {
    val input1 = Map("_labels" -> "foo=bar,baz=biz")
    LeonardoService.processLabelMap(input1) shouldBe (Right(Map("foo" -> "bar", "baz" -> "biz")))

    val failureInput = Map("_labels" -> "foo=bar,,baz=biz")
    LeonardoService.processLabelMap(failureInput).isLeft shouldBe (true)

    val duplicateLabel = Map("_labels" -> "foo=bar,foo=biz")
    LeonardoService.processLabelMap(duplicateLabel) shouldBe (Right(Map("foo" -> "biz")))
  }

  private def withLeoPublisher(
    publisherQueue: InspectableQueue[IO, LeoPubsubMessage]
  )(validations: IO[Assertion]): IO[Assertion] = {
    val leoPublisher = new LeoPublisher[IO](publisherQueue, FakeGooglePublisher)
    withInfiniteStream(leoPublisher.process, validations)
  }

  private def makeLeo(
    queue: InspectableQueue[IO, LeoPubsubMessage] = QueueFactory.makePublisherQueue()
  )(implicit system: ActorSystem, runtimeInstances: RuntimeInstances[IO]): LeonardoService =
    new LeonardoService(dataprocConfig,
                        imageConfig,
                        MockWelderDAO,
                        proxyConfig,
                        swaggerConfig,
                        autoFreezeConfig,
                        Config.zombieRuntimeMonitorConfig,
                        welderConfig,
                        mockPetGoogleDAO,
                        authProvider,
                        serviceAccountProvider,
                        bucketHelper,
                        new MockDockerDAO,
                        queue)
}
