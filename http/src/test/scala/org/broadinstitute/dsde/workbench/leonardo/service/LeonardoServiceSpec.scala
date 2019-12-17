package org.broadinstitute.dsde.workbench.leonardo
package service

import java.io.ByteArrayInputStream
import java.net.URL
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.{Blocker, IO}
import cats.mtl.ApplicativeAsk
import com.typesafe.scalalogging.LazyLogging
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.google.GoogleStorageDAO
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.auth.WhitelistAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleComputeDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockDockerDAO, MockSamDAO, MockWelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, LeoComponent, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterImageType.{Jupyter, RStudio, Welder}
import org.broadinstitute.dsde.workbench.leonardo.model.ContainerImage.GCR
import org.broadinstitute.dsde.workbench.leonardo.model.MachineConfigOps.{
  NegativeIntegerArgumentInClusterRequestException,
  OneWorkerSpecifiedInClusterRequestException
}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.Stopped
import org.broadinstitute.dsde.workbench.leonardo.model.google.VPCConfig.{VPCNetwork, VPCSubnet}
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.monitor.FakeGoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.util.{BucketHelper, ClusterHelper, TemplateHelper}
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.util.Retry
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{never, verify, _}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Await
import scala.concurrent.duration._

class LeonardoServiceSpec
    extends TestKit(ActorSystem("leonardotest"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterAll
    with TestComponent
    with ScalaFutures
    with OptionValues
    with CommonTestData
    with LeoComponent
    with Retry
    with LazyLogging {

  private var gdDAO: MockGoogleDataprocDAO = _
  private var computeDAO: MockGoogleComputeDAO = _
  private var directoryDAO: MockGoogleDirectoryDAO = _
  private var iamDAO: MockGoogleIamDAO = _
  private var projectDAO: MockGoogleProjectDAO = _
  private var storageDAO: MockGoogleStorageDAO = _
  private var samDao: MockSamDAO = _
  private var bucketHelper: BucketHelper = _
  private var clusterHelper: ClusterHelper = _
  private var leo: LeonardoService = _
  private var authProvider: LeoAuthProvider[IO] = _

  val mockPetGoogleDAO: String => GoogleStorageDAO = _ => {
    new MockGoogleStorageDAO
  }

  implicit val cs = IO.contextShift(system.dispatcher)
  implicit val timer = IO.timer(system.dispatcher)
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
  val blocker = Blocker.liftExecutionContext(system.dispatcher)

  before {
    gdDAO = new MockGoogleDataprocDAO
    computeDAO = new MockGoogleComputeDAO
    directoryDAO = new MockGoogleDirectoryDAO()
    iamDAO = new MockGoogleIamDAO
    projectDAO = new MockGoogleProjectDAO
    storageDAO = new MockGoogleStorageDAO
    // Pre-populate the juptyer extension bucket in the mock storage DAO, as it is passed in some requests
    storageDAO.buckets += jupyterExtensionUri.bucketName -> Set(
      (jupyterExtensionUri.objectName, new ByteArrayInputStream("foo".getBytes()))
    )

    // Set up the mock directoryDAO to have the Google group used to grant permission to users to pull the custom dataproc image
    directoryDAO
      .createGroup(dataprocImageProjectGroupName,
                   dataprocImageProjectGroupEmail,
                   Option(directoryDAO.lockedDownGroupSettings))
      .futureValue

    samDao = serviceAccountProvider.samDao
    authProvider = new WhitelistAuthProvider(whitelistAuthConfig, serviceAccountProvider)

    bucketHelper = new BucketHelper(computeDAO, storageDAO, FakeGoogleStorageService, serviceAccountProvider)
    clusterHelper = new ClusterHelper(DbSingleton.ref,
                                      dataprocConfig,
                                      googleGroupsConfig,
                                      proxyConfig,
                                      clusterResourcesConfig,
                                      clusterFilesConfig,
                                      monitorConfig,
                                      bucketHelper,
                                      gdDAO,
                                      computeDAO,
                                      directoryDAO,
                                      iamDAO,
                                      projectDAO,
                                      blocker)

    leo = new LeonardoService(dataprocConfig,
                              MockWelderDAO,
                              clusterDefaultsConfig,
                              proxyConfig,
                              swaggerConfig,
                              autoFreezeConfig,
                              mockPetGoogleDAO,
                              DbSingleton.ref,
                              authProvider,
                              serviceAccountProvider,
                              bucketHelper,
                              clusterHelper,
                              new MockDockerDAO)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  lazy val serviceAccountCredentialFile = notebookServiceAccount(project)
    .map(_ => List(ClusterTemplateValues.serviceAccountCredentialsFilename))
    .getOrElse(List.empty)

  lazy val configFiles = List(
    clusterResourcesConfig.jupyterDockerCompose.value,
    clusterResourcesConfig.rstudioDockerCompose.value,
    clusterResourcesConfig.proxyDockerCompose.value,
    clusterResourcesConfig.welderDockerCompose.value,
    clusterResourcesConfig.initActionsScript.value,
    clusterFilesConfig.jupyterServerCrt.getName,
    clusterFilesConfig.jupyterServerKey.getName,
    clusterFilesConfig.jupyterRootCaPem.getName,
    clusterResourcesConfig.proxySiteConf.value,
    clusterResourcesConfig.jupyterNotebookConfigUri.value,
    clusterResourcesConfig.jupyterNotebookFrontendConfigUri.value,
    clusterResourcesConfig.customEnvVarsConfigUri.value
  )

  lazy val initFiles = (configFiles ++ serviceAccountCredentialFile).map(GcsObjectName(_))

  "LeonardoService" should "create a single node cluster with default machine configs" in isolatedDbTest {
    // create the cluster
    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeToFuture.futureValue

    // check the create response has the correct info
    clusterCreateResponse.clusterName shouldBe name0
    clusterCreateResponse.googleProject shouldBe project
    clusterCreateResponse.serviceAccountInfo.clusterServiceAccount shouldEqual clusterServiceAccount(project)
    clusterCreateResponse.serviceAccountInfo.notebookServiceAccount shouldEqual notebookServiceAccount(project)
    clusterCreateResponse.dataprocInfo shouldBe None
    clusterCreateResponse.auditInfo.creator shouldBe userInfo.userEmail
    clusterCreateResponse.auditInfo.destroyedDate shouldBe None
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig
    clusterCreateResponse.properties shouldBe Map.empty
    clusterCreateResponse.status shouldBe ClusterStatus.Creating
    clusterCreateResponse.jupyterExtensionUri shouldBe None
    clusterCreateResponse.jupyterUserScriptUri shouldBe testClusterRequest.jupyterUserScriptUri
    clusterCreateResponse.jupyterStartUserScriptUri shouldBe testClusterRequest.jupyterStartUserScriptUri
    clusterCreateResponse.errors shouldBe List.empty
    clusterCreateResponse.instances shouldBe Set.empty
    clusterCreateResponse.userJupyterExtensionConfig shouldBe testClusterRequest.userJupyterExtensionConfig
    clusterCreateResponse.autopauseThreshold shouldBe testClusterRequest.autopauseThreshold.getOrElse(0)
    clusterCreateResponse.defaultClientId shouldBe testClusterRequest.defaultClientId
    clusterCreateResponse.stopAfterCreation shouldBe testClusterRequest.stopAfterCreation.getOrElse(false)
    clusterCreateResponse.clusterImages.map(_.imageType) shouldBe Set(Jupyter)
    clusterCreateResponse.scopes shouldBe dataprocConfig.defaultScopes
    clusterCreateResponse.welderEnabled shouldBe false
    clusterCreateResponse.labels.get("tool") shouldBe Some("Jupyter")
    clusterCreateResponse.clusterUrl shouldBe new URL(s"http://leonardo/proxy/$project/$name0/jupyter")

    // check the cluster persisted to the database matches the create response
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(clusterCreateResponse.id) }
    dbCluster shouldBe Some(clusterCreateResponse)

    // check that no state in Google changed
    computeDAO.firewallRules shouldBe 'empty
    storageDAO.buckets.keySet shouldBe Set(jupyterExtensionUri.bucketName)

    // init bucket should not have been persisted to the database
    val dbInitBucketOpt = dbFutureValue { _.clusterQuery.getInitBucket(project, name0) }
    dbInitBucketOpt shouldBe None
  }

  it should "create and get a cluster" in isolatedDbTest {
    // create a cluster
    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // get the cluster detail
    val clusterGetResponse = leo.getActiveClusterDetails(userInfo, project, name1).unsafeToFuture.futureValue
    // check the create response and get response are the same
    clusterCreateResponse shouldEqual clusterGetResponse
  }

  it should "create a cluster with the default welder image" in isolatedDbTest {
    // create the cluster
    val clusterRequest = testClusterRequest.copy(
      machineConfig = Some(singleNodeDefaultMachineConfig),
      stopAfterCreation = Some(true),
      enableWelder = Some(true)
    )

    val clusterResponse = leo.createCluster(userInfo, project, name1, clusterRequest).unsafeToFuture.futureValue

    // check the cluster persisted to the database matches the create response
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(clusterResponse.id) }
    dbCluster shouldBe Some(clusterResponse)

    // cluster images should contain welder and Jupyter
    clusterResponse.clusterImages.find(_.imageType == Jupyter).map(_.imageUrl) shouldBe Some(
      dataprocConfig.jupyterImage
    )
    clusterResponse.clusterImages.find(_.imageType == RStudio) shouldBe None
    clusterResponse.clusterImages.find(_.imageType == Welder).map(_.imageUrl) shouldBe Some(
      dataprocConfig.welderDockerImage
    )
  }

  it should "create a cluster with a client-supplied welder image" in isolatedDbTest {
    val customWelderImage = GCR("my-custom-welder-image-link")

    // create the cluster
    val clusterRequest = testClusterRequest.copy(
      machineConfig = Some(singleNodeDefaultMachineConfig),
      stopAfterCreation = Some(true),
      welderDockerImage = Some(customWelderImage),
      enableWelder = Some(true)
    )

    val clusterResponse = leo.createCluster(userInfo, project, name1, clusterRequest).unsafeToFuture.futureValue

    // check the cluster persisted to the database matches the create response
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(clusterResponse.id) }
    dbCluster shouldBe Some(clusterResponse)

    // cluster images should contain welder and Jupyter
    clusterResponse.clusterImages.find(_.imageType == Jupyter).map(_.imageUrl) shouldBe Some(
      dataprocConfig.jupyterImage
    )
    clusterResponse.clusterImages.find(_.imageType == RStudio) shouldBe None
    clusterResponse.clusterImages.find(_.imageType == Welder).map(_.imageUrl) shouldBe Some(customWelderImage.imageUrl)
  }

  it should "create a cluster with an rstudio image" in isolatedDbTest {
    val rstudioImage = GCR("some-rstudio-image")

    // create the cluster
    val clusterRequest = testClusterRequest.copy(toolDockerImage = Some(rstudioImage), enableWelder = Some(true))
    val leoForTest = new LeonardoService(dataprocConfig,
                                         MockWelderDAO,
                                         clusterDefaultsConfig,
                                         proxyConfig,
                                         swaggerConfig,
                                         autoFreezeConfig,
                                         mockPetGoogleDAO,
                                         DbSingleton.ref,
                                         authProvider,
                                         serviceAccountProvider,
                                         bucketHelper,
                                         clusterHelper,
                                         new MockDockerDAO(RStudio))
    val clusterResponse = leoForTest.createCluster(userInfo, project, name1, clusterRequest).unsafeToFuture.futureValue

    // check the cluster persisted to the database matches the create response
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(clusterResponse.id) }
    dbCluster shouldBe Some(clusterResponse)

    // cluster images should contain welder and RStudio
    clusterResponse.clusterImages.find(_.imageType == RStudio).map(_.imageUrl) shouldBe Some(rstudioImage.imageUrl)
    clusterResponse.clusterImages.find(_.imageType == Jupyter) shouldBe None
    clusterResponse.clusterImages.find(_.imageType == Welder).map(_.imageUrl) shouldBe Some(
      dataprocConfig.welderDockerImage
    )
    clusterResponse.labels.get("tool") shouldBe Some("RStudio")
    clusterResponse.clusterUrl shouldBe new URL(s"http://leonardo/proxy/$project/$name1/rstudio")

    // list clusters should return RStudio information
    val clusterList = leoForTest.listClusters(userInfo, Map.empty, Some(project)).unsafeToFuture.futureValue
    clusterList.size shouldBe 1
    clusterList.toSet shouldBe Set(clusterResponse).map(_.toListClusterResp)
    clusterList.head.labels.get("tool") shouldBe Some("RStudio")
    clusterList.head.clusterUrl shouldBe new URL(s"http://leonardo/proxy/$project/$name1/rstudio")
  }

  it should "create a single node cluster with an empty machine config" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig()))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig
  }

  it should "create a single node cluster with zero workers explicitly defined in machine config" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(0))))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefaultMachineConfig
  }

  it should "create a single node cluster with master configs defined" in isolatedDbTest {
    val singleNodeDefinedMachineConfig = MachineConfig(Some(0), Some("test-master-machine-type2"), Some(200))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(singleNodeDefinedMachineConfig))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.machineConfig shouldEqual singleNodeDefinedMachineConfig
  }

  it should "create a single node cluster and override worker configs" in isolatedDbTest {
    // machine config is creating a single node cluster, but has worker configs defined
    val machineConfig = Some(
      MachineConfig(Some(0),
                    Some("test-master-machine-type3"),
                    Some(200),
                    Some("test-worker-machine-type"),
                    Some(10),
                    Some(3),
                    Some(4))
    )
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = machineConfig)

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.machineConfig shouldEqual MachineConfig(Some(0), Some("test-master-machine-type3"), Some(200))
  }

  it should "create a standard cluster with 2 workers with default worker configs" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(2))))
    val machineConfigResponse = MachineConfig(
      Some(2),
      Some(clusterDefaultsConfig.masterMachineType),
      Some(clusterDefaultsConfig.masterDiskSize),
      Some(clusterDefaultsConfig.workerMachineType),
      Some(clusterDefaultsConfig.workerDiskSize),
      Some(clusterDefaultsConfig.numberOfWorkerLocalSSDs),
      Some(clusterDefaultsConfig.numberOfPreemptibleWorkers)
    )

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.machineConfig shouldEqual machineConfigResponse
  }

  it should "create a standard cluster with 10 workers with defined config" in isolatedDbTest {
    val machineConfig = MachineConfig(Some(10),
                                      Some("test-master-machine-type"),
                                      Some(200),
                                      Some("test-worker-machine-type"),
                                      Some(300),
                                      Some(3),
                                      Some(4))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(machineConfig))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.machineConfig shouldEqual machineConfig
  }

  it should "create a standard cluster with 2 workers and override too-small disk sizes with minimum disk size" in isolatedDbTest {
    val machineConfig = MachineConfig(Some(2),
                                      Some("test-master-machine-type"),
                                      Some(5),
                                      Some("test-worker-machine-type"),
                                      Some(5),
                                      Some(3),
                                      Some(4))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(machineConfig))
    val expectedMachineConfig = MachineConfig(Some(2),
                                              Some("test-master-machine-type"),
                                              Some(10),
                                              Some("test-worker-machine-type"),
                                              Some(10),
                                              Some(3),
                                              Some(4))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.futureValue
    clusterCreateResponse.machineConfig shouldEqual expectedMachineConfig
  }

  it should "throw OneWorkerSpecifiedInClusterRequestException when create a 1 worker cluster" in isolatedDbTest {
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(1))))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.failed.futureValue
    clusterCreateResponse shouldBe a[OneWorkerSpecifiedInClusterRequestException]
  }

  it should "throw NegativeIntegerArgumentInClusterRequestException when master disk size in single node cluster request is a negative integer" in isolatedDbTest {
    val clusterRequestWithMachineConfig =
      testClusterRequest.copy(machineConfig = Some(MachineConfig(Some(0), Some("test-worker-machine-type"), Some(-30))))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.failed.futureValue
    clusterCreateResponse shouldBe a[NegativeIntegerArgumentInClusterRequestException]
  }

  it should "throw NegativeIntegerArgumentInClusterRequestException when number of preemptible workers in a 2 worker cluster request is a negative integer" in isolatedDbTest {
    val machineConfig = MachineConfig(Some(10),
                                      Some("test-master-machine-type"),
                                      Some(200),
                                      Some("test-worker-machine-type"),
                                      Some(300),
                                      Some(3),
                                      Some(-1))
    val clusterRequestWithMachineConfig = testClusterRequest.copy(machineConfig = Option(machineConfig))

    val clusterCreateResponse =
      leo.createCluster(userInfo, project, name0, clusterRequestWithMachineConfig).unsafeToFuture.failed.futureValue
    clusterCreateResponse shouldBe a[NegativeIntegerArgumentInClusterRequestException]
  }

  it should "throw ClusterNotFoundException for nonexistent clusters" in isolatedDbTest {
    val exc = leo
      .getActiveClusterDetails(userInfo, GoogleProject("nonexistent"), ClusterName("cluster"))
      .unsafeToFuture
      .failed
      .futureValue
    exc shouldBe a[ClusterNotFoundException]
  }

  it should "throw ClusterAlreadyExistsException when creating a cluster with same name and project as an existing cluster" in isolatedDbTest {
    val cluster1 = leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeToFuture.futureValue
    val exc = leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeToFuture.failed.futureValue
    exc shouldBe a[ClusterAlreadyExistsException]
  }

  it should "create two clusters with same name with only one active" in isolatedDbTest {
    // create first cluster
    val cluster = leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // change cluster status to Running so that it can be deleted
    dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("numbers.and.dots"), Instant.now) }

    // delete the cluster
    leo.deleteCluster(userInfo, project, name0).unsafeToFuture.futureValue

    // check that the cluster was deleted
    val dbDeletingCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbDeletingCluster.map(_.status) shouldBe Some(ClusterStatus.Deleted)

    // recreate cluster with same project and cluster name
    val cluster2 = leo.createCluster(userInfo, project, name0, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster2 = dbFutureValue { _.clusterQuery.getClusterById(cluster2.id) }
    dbCluster2.map(_.status) shouldBe Some(ClusterStatus.Creating)
  }

  // TODO move to a ClusterHelperSpec
  it should "choose the correct VPC subnet and network settings" in isolatedDbTest {
    //if config isn't set up to look at labels (which the default one isn't), the labels don't matter
    //and we should fall back to the config
    val decoySubnetMap = Map("subnet-label" -> "incorrectSubnet", "network-label" -> "incorrectNetwork")
    clusterHelper.getClusterVPCSettings(decoySubnetMap) shouldBe Some(VPCSubnet("test-subnet"))

    //label behaviour should be: project-subnet, project-network, config-subnet, config-network
    val configWithProjectLabels =
      dataprocConfig.copy(projectVPCSubnetLabel = Some("subnet-label"), projectVPCNetworkLabel = Some("network-label"))
    val clusterHelperWithLabels = new ClusterHelper(DbSingleton.ref,
                                                    configWithProjectLabels,
                                                    googleGroupsConfig,
                                                    proxyConfig,
                                                    clusterResourcesConfig,
                                                    clusterFilesConfig,
                                                    monitorConfig,
                                                    bucketHelper,
                                                    gdDAO,
                                                    computeDAO,
                                                    directoryDAO,
                                                    iamDAO,
                                                    projectDAO,
                                                    blocker)

    val subnetMap = Map("subnet-label" -> "correctSubnet", "network-label" -> "incorrectNetwork")
    clusterHelperWithLabels.getClusterVPCSettings(subnetMap) shouldBe Some(VPCSubnet("correctSubnet"))

    val networkMap = Map("network-label" -> "correctNetwork")
    clusterHelperWithLabels.getClusterVPCSettings(networkMap) shouldBe Some(VPCNetwork("correctNetwork"))

    clusterHelperWithLabels.getClusterVPCSettings(Map()) shouldBe Some(VPCSubnet("test-subnet"))

    val configWithNoSubnet = dataprocConfig.copy(vpcSubnet = None)
    val clusterHelperWithNoSubnet = new ClusterHelper(DbSingleton.ref,
                                                      configWithNoSubnet,
                                                      googleGroupsConfig,
                                                      proxyConfig,
                                                      clusterResourcesConfig,
                                                      clusterFilesConfig,
                                                      monitorConfig,
                                                      bucketHelper,
                                                      gdDAO,
                                                      computeDAO,
                                                      directoryDAO,
                                                      iamDAO,
                                                      projectDAO,
                                                      blocker)
    clusterHelperWithNoSubnet.getClusterVPCSettings(Map()) shouldBe Some(VPCNetwork("test-network"))
  }

  it should "delete a Running cluster" in isolatedDbTest {
    // need a specialized LeonardoService for this test, so we can spy on its authProvider
    val spyProvider: LeoAuthProvider[IO] = spy(authProvider)
    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }
    val leoForTest = new LeonardoService(dataprocConfig,
                                         MockWelderDAO,
                                         clusterDefaultsConfig,
                                         proxyConfig,
                                         swaggerConfig,
                                         autoFreezeConfig,
                                         mockPetGoogleStorageDAO,
                                         DbSingleton.ref,
                                         spyProvider,
                                         serviceAccountProvider,
                                         bucketHelper,
                                         clusterHelper,
                                         new MockDockerDAO)

    val cluster = leoForTest.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // change cluster status to Running so that it can be deleted
    dbFutureValue {
      _.clusterQuery.updateAsyncClusterCreationFields(
        Some(GcsPath(initBucketPath, GcsObjectName(""))),
        Some(serviceAccountKey),
        cluster.copy(dataprocInfo = Some(makeDataprocInfo(1))),
        Instant.now
      )
    }
    dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("numbers.and.dots"), Instant.now) }

    // delete the cluster
    leoForTest.deleteCluster(userInfo, project, name1).unsafeToFuture.futureValue

    // the cluster has transitioned to the Deleting state (Cluster Monitor will later transition it to Deleted)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(project, name1) }
      .map(_.status) shouldBe Some(ClusterStatus.Deleting)

    // the auth provider should have not yet been notified of deletion
    verify(spyProvider, never).notifyClusterDeleted(mockitoEq(cluster.internalId),
                                                    mockitoEq(userInfo.userEmail),
                                                    mockitoEq(userInfo.userEmail),
                                                    mockitoEq(project),
                                                    mockitoEq(name1))(any[ApplicativeAsk[IO, TraceId]])
  }

  it should "delete a cluster that has status Error" in isolatedDbTest {
    // need a specialized LeonardoService for this test, so we can spy on its authProvider
    val spyProvider: LeoAuthProvider[IO] = spy(authProvider)
    val mockPetGoogleStorageDAO: String => GoogleStorageDAO = _ => {
      new MockGoogleStorageDAO
    }
    val leoForTest = new LeonardoService(dataprocConfig,
                                         MockWelderDAO,
                                         clusterDefaultsConfig,
                                         proxyConfig,
                                         swaggerConfig,
                                         autoFreezeConfig,
                                         mockPetGoogleStorageDAO,
                                         DbSingleton.ref,
                                         spyProvider,
                                         serviceAccountProvider,
                                         bucketHelper,
                                         clusterHelper,
                                         new MockDockerDAO)

    // create the cluster
    val cluster = leoForTest.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // change the cluster status to Error
    dbFutureValue {
      _.clusterQuery.updateAsyncClusterCreationFields(
        Some(GcsPath(initBucketPath, GcsObjectName(""))),
        Some(serviceAccountKey),
        cluster.copy(dataprocInfo = Some(makeDataprocInfo(1))),
        Instant.now
      )
    }
    dbFutureValue { _.clusterQuery.updateClusterStatus(cluster.id, ClusterStatus.Error, Instant.now) }

    // delete the cluster
    leoForTest.deleteCluster(userInfo, project, name1).unsafeToFuture.futureValue

    // the cluster has transitioned to the Deleting state (Cluster Monitor will later transition it to Deleted)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(project, name1) }
      .map(_.status) shouldBe Some(ClusterStatus.Deleting)

    // the auth provider should have not yet been notified of deletion
    verify(spyProvider, never).notifyClusterDeleted(mockitoEq(cluster.internalId),
                                                    mockitoEq(userInfo.userEmail),
                                                    mockitoEq(userInfo.userEmail),
                                                    mockitoEq(project),
                                                    mockitoEq(name1))(any[ApplicativeAsk[IO, TraceId]])
  }

  it should "delete a cluster's instances" in isolatedDbTest {
    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // populate some instances for the cluster
    dbFutureValue {
      _.instanceQuery.saveAllForCluster(getClusterId(cluster), Seq(masterInstance, workerInstance1, workerInstance2))
    }

    // change cluster status to Running so that it can be deleted
    dbFutureValue {
      _.clusterQuery.updateAsyncClusterCreationFields(
        Some(GcsPath(initBucketPath, GcsObjectName(""))),
        Some(serviceAccountKey),
        cluster.copy(dataprocInfo = Some(makeDataprocInfo(1))),
        Instant.now
      )
    }
    dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("numbers.and.dots"), Instant.now) }

    // delete the cluster
    leo.deleteCluster(userInfo, project, name1).unsafeToFuture.futureValue

    // the cluster has transitioned to the Deleting state (Cluster Monitor will later transition it to Deleted)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(project, name1) }
      .map(_.status) shouldBe Some(ClusterStatus.Deleting)

    // check that the instances are still in the DB (they get removed by the ClusterMonitorActor)
    val instances = dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(cluster)) }
    instances.toSet shouldBe Set(masterInstance, workerInstance1, workerInstance2)
  }

  // TODO move to TemplateHelperSpec
  it should "template a script using config values" in isolatedDbTest {
    // Create replacements map
    val clusterInit = ClusterTemplateValues(
      testCluster,
      Some(initBucketPath),
      Some(stagingBucketName),
      Some(serviceAccountKey),
      dataprocConfig,
      proxyConfig,
      clusterFilesConfig,
      clusterResourcesConfig
    )
    val replacements: Map[String, String] = clusterInit.toMap

    // Each value in the replacement map will replace its key in the file being processed
    val result = TemplateHelper
      .templateResource(replacements, clusterResourcesConfig.initActionsScript, blocker)
      .compile
      .to[Array]
      .unsafeToFuture
      .futureValue

    // Check that the values in the bash script file were correctly replaced
    val expected =
      s"""|#!/usr/bin/env bash
          |
          |"${name1.value}"
          |"${project.value}"
          |"${jupyterImage.imageUrl}"
          |""
          |"${proxyConfig.jupyterProxyDockerImage}"
          |"${testCluster.jupyterUserScriptUri.get.toUri}"
          |"${testCluster.jupyterStartUserScriptUri.get.toUri}"
          |"${GcsPath(initBucketPath, GcsObjectName(ClusterTemplateValues.serviceAccountCredentialsFilename)).toUri}"
          |""
          |""
          |""
          |"${GcsPath(stagingBucketName, GcsObjectName("userscript_output.txt")).toUri}"
          |"${GcsPath(initBucketPath, GcsObjectName("jupyter_notebook_config.py")).toUri}"
          |"${GcsPath(initBucketPath, GcsObjectName("notebook.json")).toUri}"
          |"${GcsPath(initBucketPath, GcsObjectName("custom_env_vars.env")).toUri}"""".stripMargin

    new String(result, StandardCharsets.UTF_8) shouldEqual expected
  }

  it should "throw a JupyterExtensionException when the extensionUri is too long" in isolatedDbTest {
    val jupyterExtensionUri =
      GcsPath(GcsBucketName("bucket"), GcsObjectName(Stream.continually('a').take(1025).mkString))

    // create the cluster
    val clusterRequest = testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri))
    val response = leo.createCluster(userInfo, project, name0, clusterRequest).unsafeToFuture.failed.futureValue

    response shouldBe a[BucketObjectException]
  }

  it should "throw a JupyterExtensionException when the jupyterExtensionUri does not point to a GCS object" in isolatedDbTest {
    val jupyterExtensionUri = parseGcsPath("gs://bogus/object.tar.gz").right.get

    // create the cluster
    val response =
      leo
        .createCluster(userInfo,
                       project,
                       name0,
                       testClusterRequest.copy(jupyterExtensionUri = Some(jupyterExtensionUri)))
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
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).unsafeToFuture.futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo
      .createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeToFuture
      .futureValue

    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
      _.toListClusterResp
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
      .futureValue shouldBe a[ClusterCannotBeDeletedException]
  }

  it should "list all active clusters" in isolatedDbTest {
    // create a couple of clusters
    val cluster1 = leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    val clusterName2 = ClusterName("test-cluster-2")
    val cluster2 = leo
      .createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeToFuture
      .futureValue

    leo.listClusters(userInfo, Map("includeDeleted" -> "false")).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster1,
      cluster2
    ).map(
      _.toListClusterResp
    )
    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
      _.toListClusterResp
    )
    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
      _.toListClusterResp
    )

    val clusterName3 = ClusterName("test-cluster-3")
    val cluster3 = leo
      .createCluster(userInfo, project, clusterName3, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeToFuture
      .futureValue

    dbFutureValue { _.clusterQuery.completeDeletion(cluster3.id, Instant.now) }

    leo.listClusters(userInfo, Map.empty).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2).map(
      _.toListClusterResp
    )
    leo.listClusters(userInfo, Map("includeDeleted" -> "false")).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster1,
      cluster2
    ).map(
      _.toListClusterResp
    )
    leo.listClusters(userInfo, Map("includeDeleted" -> "true")).unsafeToFuture.futureValue.toSet.size shouldBe 3
  }

  it should "list clusters with labels" in isolatedDbTest {
    // create a couple of clusters
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).unsafeToFuture.futureValue

    val clusterName2 = ClusterName(s"test-cluster-2")
    val cluster2 = leo
      .createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeToFuture
      .futureValue

    leo.listClusters(userInfo, Map("foo" -> "bar")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1, cluster2)
      .map(
        _.toListClusterResp
      )
    leo.listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes")).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster1
    ).map(
      _.toListClusterResp
    )
    leo
      .listClusters(userInfo, Map("foo" -> "bar", "bam" -> "yes", "vcf" -> "no"))
      .unsafeToFuture
      .futureValue
      .toSet shouldBe Set(
      cluster1
    ).map(_.toListClusterResp)
    leo.listClusters(userInfo, Map("a" -> "b")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster2).map(
      _.toListClusterResp
    )
    leo.listClusters(userInfo, Map("foo" -> "bar", "baz" -> "biz")).unsafeToFuture.futureValue.toSet shouldBe Set.empty
    leo
      .listClusters(userInfo, Map("A" -> "B"))
      .unsafeToFuture
      .futureValue
      .toSet shouldBe Set(cluster2).map(_.toListClusterResp) // labels are not case sensitive because MySQL
    //Assert that extensions were added as labels as well
    leo
      .listClusters(userInfo, Map("abc" -> "def", "pqr" -> "pqr", "xyz" -> "xyz"))
      .unsafeToFuture
      .futureValue
      .toSet shouldBe Set(
      cluster1,
      cluster2
    ).map(_.toListClusterResp)
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
    val clusterName1 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster1 = leo.createCluster(userInfo, project, clusterName1, testClusterRequest).unsafeToFuture.futureValue

    val clusterName2 = ClusterName(s"cluster-${UUID.randomUUID.toString}")
    val cluster2 = leo
      .createCluster(userInfo, project, clusterName2, testClusterRequest.copy(labels = Map("a" -> "b", "foo" -> "bar")))
      .unsafeToFuture
      .futureValue

    leo.listClusters(userInfo, Map("_labels" -> "foo=bar")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster1,
                                                                                                          cluster2).map(
      _.toListClusterResp
    )
    leo.listClusters(userInfo, Map("_labels" -> "foo=bar,bam=yes")).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster1
    ).map(
      _.toListClusterResp
    )
    leo
      .listClusters(userInfo, Map("_labels" -> "foo=bar,bam=yes,vcf=no"))
      .unsafeToFuture
      .futureValue
      .toSet shouldBe Set(cluster1).map(
      _.toListClusterResp
    )
    leo.listClusters(userInfo, Map("_labels" -> "a=b")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster2).map(
      _.toListClusterResp
    )
    leo.listClusters(userInfo, Map("_labels" -> "baz=biz")).unsafeToFuture.futureValue.toSet shouldBe Set.empty
    leo.listClusters(userInfo, Map("_labels" -> "A=B")).unsafeToFuture.futureValue.toSet shouldBe Set(cluster2).map(
      _.toListClusterResp
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
      _.toListClusterResp
    )
    leo.listClusters(userInfo, Map.empty, Some(project2)).unsafeToFuture.futureValue.toSet shouldBe Set(cluster2).map(
      _.toListClusterResp
    )
    leo.listClusters(userInfo, Map("foo" -> "bar"), Some(project)).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster1
    ).map(
      _.toListClusterResp
    )
    leo.listClusters(userInfo, Map("foo" -> "bar"), Some(project2)).unsafeToFuture.futureValue.toSet shouldBe Set(
      cluster2
    ).map(
      _.toListClusterResp
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
    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // populate some instances for the cluster
    val clusterInstances = Seq(masterInstance, workerInstance1, workerInstance2)
    dbFutureValue { _.instanceQuery.saveAllForCluster(getClusterId(cluster), clusterInstances) }
    computeDAO.instances ++= clusterInstances.groupBy(_.key).mapValues(_.head)
    computeDAO.instanceMetadata ++= clusterInstances.groupBy(_.key).mapValues(_ => Map.empty)

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"), Instant.now) }

    // stop the cluster
    leo.stopCluster(userInfo, project, name1).unsafeToFuture.futureValue

    // cluster status should be Stopping in the DB
    dbFutureValue { _.clusterQuery.getClusterByUniqueKey(cluster) }.get.status shouldBe ClusterStatus.Stopping

    // instance status should still be Running in the DB
    // the ClusterMonitorActor is what updates instance status
    val instances = dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(cluster)) }
    instances.size shouldBe 3
    instances.map(_.status).toSet shouldBe Set(InstanceStatus.Running)

    // Google instances should be stopped
    computeDAO.instances.keySet shouldBe clusterInstances.map(_.key).toSet
    computeDAO.instanceMetadata.keySet shouldBe clusterInstances.map(_.key).toSet
    computeDAO.instances.values.toSet shouldBe clusterInstances.map(_.copy(status = InstanceStatus.Stopped)).toSet
    computeDAO.instanceMetadata.values.map(_.keys).flatten.toSet shouldBe Set("shutdown-script")
  }

  it should "resize a cluster" in isolatedDbTest {
    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"), Instant.now) }

    leo
      .updateCluster(userInfo,
                     project,
                     name1,
                     testClusterRequest.copy(machineConfig = Some(MachineConfig(numberOfWorkers = Some(2)))))
      .unsafeToFuture
      .futureValue

    //unfortunately we can't actually check that the new instances were added because the monitor
    //handles that but we will check as much as we can

    //check that status of cluster is Updating
    dbFutureValue { _.clusterQuery.getClusterStatus(cluster.id) } shouldBe Some(ClusterStatus.Updating)

    //check that the machine config has been updated
    dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }.get.machineConfig.numberOfWorkers shouldBe Some(
      2
    )
  }

  it should "update the autopause threshold for a cluster" in isolatedDbTest {
    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"), Instant.now) }

    leo
      .updateCluster(userInfo,
                     project,
                     name1,
                     testClusterRequest.copy(autopause = Some(true), autopauseThreshold = Some(7)))
      .unsafeToFuture
      .futureValue

    //check that status of cluster is still Running
    dbFutureValue { _.clusterQuery.getClusterStatus(cluster.id) } shouldBe Some(ClusterStatus.Running)

    //check that the autopause threshold has been updated
    dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }.get.autopauseThreshold shouldBe 7
  }

  it should "update the master machine type for a cluster" in isolatedDbTest {
    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // set the cluster to Stopped
    dbFutureValue { _.clusterQuery.updateClusterStatus(cluster.id, Stopped, Instant.now) }

    val newMachineType = "n1-micro-1"
    leo
      .updateCluster(
        userInfo,
        project,
        name1,
        testClusterRequest.copy(machineConfig = Some(MachineConfig(masterMachineType = Some(newMachineType))))
      )
      .unsafeToFuture
      .futureValue

    //check that status of cluster is still Stopped
    dbFutureValue { _.clusterQuery.getClusterStatus(cluster.id) } shouldBe Some(ClusterStatus.Stopped)

    //check that the machine config has been updated
    dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }.get.machineConfig.masterMachineType shouldBe Some(
      newMachineType
    )
  }

  it should "not allow changing the master machine type for a cluster in RUNNING state" in isolatedDbTest {
    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"), Instant.now) }

    val newMachineType = "n1-micro-1"
    val failure = leo
      .updateCluster(
        userInfo,
        project,
        name1,
        testClusterRequest.copy(machineConfig = Some(MachineConfig(masterMachineType = Some(newMachineType))))
      )
      .unsafeToFuture
      .failed
      .futureValue

    //check that status of cluster is still Running
    dbFutureValue { _.clusterQuery.getClusterStatus(cluster.id) } shouldBe Some(ClusterStatus.Running)

    failure shouldBe a[ClusterMachineTypeCannotBeChangedException]
  }

  it should "update the master disk size for a cluster" in isolatedDbTest {
    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"), Instant.now) }

    val newDiskSize = 1000
    leo
      .updateCluster(userInfo,
                     project,
                     name1,
                     testClusterRequest.copy(machineConfig = Some(MachineConfig(masterDiskSize = Some(newDiskSize)))))
      .unsafeToFuture
      .futureValue

    //check that status of cluster is still Running
    dbFutureValue { _.clusterQuery.getClusterStatus(cluster.id) } shouldBe Some(ClusterStatus.Running)

    //check that the machine config has been updated
    dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }.get.machineConfig.masterDiskSize shouldBe Some(
      newDiskSize
    )
  }

  it should "not allow decreasing the master disk size for a cluster" in isolatedDbTest {
    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // set the cluster to Running
    dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"), Instant.now) }

    val newDiskSize = 10
    val failure = leo
      .updateCluster(userInfo,
                     project,
                     name1,
                     testClusterRequest.copy(machineConfig = Some(MachineConfig(masterDiskSize = Some(newDiskSize)))))
      .unsafeToFuture
      .failed
      .futureValue

    //check that status of cluster is still Running
    dbFutureValue { _.clusterQuery.getClusterStatus(cluster.id) } shouldBe Some(ClusterStatus.Running)

    failure shouldBe a[ClusterDiskSizeCannotBeDecreasedException]
  }

  ClusterStatus.monitoredStatuses foreach { status =>
    it should s"not allow updating a cluster in ${status.toString} state" in isolatedDbTest {
      // create the cluster
      val cluster =
        leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

      // check that the cluster was created
      val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
      dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

      // set the cluster to Running
      dbFutureValue {
        _.clusterQuery.updateClusterStatusAndHostIp(cluster.id, status, Some(IP("1.2.3.4")), Instant.now)
      }

      intercept[ClusterCannotBeUpdatedException] {
        Await.result(
          leo
            .updateCluster(userInfo,
                           project,
                           name1,
                           testClusterRequest.copy(machineConfig = Some(MachineConfig(numberOfWorkers = Some(2)))))
            .unsafeToFuture,
          Duration.Inf
        )
      }

      //unfortunately we can't actually check that the new instances were added because the monitor
      //handles that but we will check as much as we can

      //check that status of cluster is Updating
      dbFutureValue { _.clusterQuery.getClusterStatus(cluster.id) } shouldBe Some(status)
    }
  }

  it should "start a cluster" in isolatedDbTest {
    // create the cluster
    val cluster =
      leo.createCluster(userInfo, project, name1, testClusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // populate some instances for the cluster and set its status to Stopped
    val clusterInstances =
      Seq(masterInstance, workerInstance1, workerInstance2).map(_.copy(status = InstanceStatus.Stopped))
    dbFutureValue { _.instanceQuery.saveAllForCluster(getClusterId(cluster), clusterInstances) }
    dbFutureValue { _.clusterQuery.updateClusterStatus(cluster.id, ClusterStatus.Stopped, Instant.now) }
    computeDAO.instances ++= clusterInstances.groupBy(_.key).mapValues(_.head)
    computeDAO.instanceMetadata ++= clusterInstances.groupBy(_.key).mapValues(_ => Map.empty)

    // start the cluster
    leo.startCluster(userInfo, project, name1).unsafeToFuture.futureValue

    // cluster status should be Starting in the DB
    dbFutureValue { _.clusterQuery.getClusterByUniqueKey(cluster) }.get.status shouldBe ClusterStatus.Starting

    // instance status should still be Stopped in the DB
    // the ClusterMonitorActor is what updates instance status
    val instances = dbFutureValue {
      _.instanceQuery.getAllForCluster(getClusterId(cluster))
    }
    instances.size shouldBe 3
    instances.map(_.status).toSet shouldBe Set(InstanceStatus.Stopped)

    // Google instances should be started
    computeDAO.instances.keySet shouldBe clusterInstances.map(_.key).toSet
    computeDAO.instanceMetadata.keySet shouldBe clusterInstances.map(_.key).toSet
    computeDAO.instances.values.toSet shouldBe clusterInstances.map(_.copy(status = InstanceStatus.Running)).toSet
    computeDAO.instanceMetadata.values.map(_.keys).flatten.toSet shouldBe Set("startup-script")
  }

  // TODO: remove this test once data syncing release is complete
  it should "label and start an outdated cluster" in isolatedDbTest {
    // create the cluster
    val request = testClusterRequest.copy(labels = Map("TEST_ONLY_DEPLOY_WELDER" -> "yes"))
    val cluster = leo.createCluster(userInfo, project, name1, request).unsafeToFuture.futureValue

    // check that the cluster was created
    dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }.map(_.status) shouldBe Some(ClusterStatus.Creating)

    // set its status to Stopped and update its createdDate
    dbFutureValue { _.clusterQuery.updateClusterStatus(cluster.id, ClusterStatus.Stopped, Instant.now) }
    dbFutureValue {
      _.clusterQuery.updateClusterCreatedDate(cluster.id,
                                              new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-31").toInstant)
    }

    // start the cluster
    leo.startCluster(userInfo, project, name1).unsafeToFuture.futureValue

    // cluster status should Starting and have new label
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterByUniqueKey(cluster) }.get
    dbCluster.status shouldBe ClusterStatus.Starting
    dbCluster.labels.exists(_ == "welderInstallFailed" -> "true")
  }

  it should "update disk size for 0 workers when a consumer specifies numberOfPreemptibleWorkers" in isolatedDbTest {
    val clusterRequest =
      testClusterRequest.copy(machineConfig = Some(singleNodeDefaultMachineConfig), stopAfterCreation = Some(true))

    val cluster = leo.createCluster(userInfo, project, name1, clusterRequest).unsafeToFuture.futureValue

    // check that the cluster was created
    val dbCluster = dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }
    dbCluster.map(_.status) shouldBe Some(ClusterStatus.Creating)

    dbFutureValue { _.clusterQuery.setToRunning(cluster.id, IP("1.2.3.4"), Instant.now) }

    val newDiskSize = 1000
    leo
      .updateCluster(
        userInfo,
        project,
        name1,
        testClusterRequest.copy(
          machineConfig = Some(MachineConfig(masterDiskSize = Some(newDiskSize), numberOfPreemptibleWorkers = Some(0)))
        )
      )
      .unsafeToFuture
      .futureValue

    //check that status of cluster is still Running
    dbFutureValue { _.clusterQuery.getClusterStatus(cluster.id) } shouldBe Some(ClusterStatus.Running)

    //check that the machine config has been updated
    dbFutureValue { _.clusterQuery.getClusterById(cluster.id) }.get.machineConfig.masterDiskSize shouldBe Some(
      newDiskSize
    )
  }
}
