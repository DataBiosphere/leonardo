package org.broadinstitute.dsde.workbench.leonardo.util

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.testing.json.GoogleJsonResponseExceptionFactoryTesting
import com.google.api.client.testing.json.MockJsonFactory
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.Creating
import org.broadinstitute.dsde.workbench.leonardo.model.google.VPCConfig.{VPCNetwork, VPCSubnet}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterName, CreateClusterConfig, Operation}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterImage, ClusterImageType, MemorySize}
import org.broadinstitute.dsde.workbench.leonardo.monitor.FakeGoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.LeoLenses
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockWelderDAO
import org.broadinstitute.dsde.workbench.newrelic.mock.FakeNewRelicMetricsInterpreter

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class ClusterHelperSpec
    extends TestKit(ActorSystem("leonardotest"))
    with TestComponent
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit val nr = FakeNewRelicMetricsInterpreter

  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO
  val mockGoogleStorageDAO = new MockGoogleStorageDAO
  val mockGoogleProjectDAO = new MockGoogleProjectDAO

  val testCluster = makeCluster(1)
    .copy(status = Creating,
          dataprocInfo = None,
          serviceAccountInfo = serviceAccountInfo.copy(notebookServiceAccount = None))

  val bucketHelper =
    new BucketHelper(mockGoogleComputeDAO, mockGoogleStorageDAO, FakeGoogleStorageService, serviceAccountProvider)

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
                                        mockGoogleDataprocDAO,
                                        mockGoogleComputeDAO,
                                        mockGoogleDirectoryDAO,
                                        mockGoogleIamDAO,
                                        mockGoogleProjectDAO, MockWelderDAO,
                                        blocker)

  override def beforeAll(): Unit =
    // Set up the mock directoryDAO to have the Google group used to grant permission to users to pull the custom dataproc image
    mockGoogleDirectoryDAO
      .createGroup(dataprocImageProjectGroupName,
                   dataprocImageProjectGroupEmail,
                   Option(mockGoogleDirectoryDAO.lockedDownGroupSettings))
      .futureValue

  "ClusterHelper" should "create a google cluster" in isolatedDbTest {
    val clusterCreationRes = clusterHelper.createCluster(testCluster).unsafeToFuture().futureValue

    // verify the mock dataproc DAO
    mockGoogleDataprocDAO.clusters.size shouldBe 1
    mockGoogleDataprocDAO.clusters should contain key testCluster.clusterName
    val operation = mockGoogleDataprocDAO.clusters(testCluster.clusterName)

    // verify the mock compute DAO
    mockGoogleComputeDAO.firewallRules.size shouldBe 1
    mockGoogleComputeDAO.firewallRules should contain key testCluster.googleProject

    // verify the returned cluster
    clusterCreationRes.cluster.dataprocInfo shouldBe 'defined
    clusterCreationRes.cluster.copy(dataprocInfo = None) shouldBe testCluster
    val dpInfo = clusterCreationRes.cluster.dataprocInfo.get
    dpInfo.operationName shouldBe operation.name
    dpInfo.googleId shouldBe operation.uuid
    dpInfo.hostIp shouldBe None
    dpInfo.stagingBucket.value should startWith("leostaging")

    // verify the returned init bucket
    clusterCreationRes.initBucket.value should startWith("leoinit")

    // verify the returned service account key
    mockGoogleIamDAO.serviceAccountKeys shouldBe 'empty
    clusterCreationRes.serviceAccountKey shouldBe None
  }

  it should "be able to determine appropriate custom dataproc image" in isolatedDbTest {
    val cluster = LeoLenses.clusterToClusterImages
      .modify(
        _ =>
          Set(
            ClusterImage(ClusterImageType.Jupyter,
                         "us.gcr.io/broad-dsp-gcr-public/terra-jupyter-hail:0.0.1",
                         Instant.now)
          )
      )(testCluster)

    val res = clusterHelper.createCluster(cluster).unsafeToFuture().futureValue
    res.customDataprocImage shouldBe Config.dataprocConfig.customDataprocImage
    val clusterWithLegacyImage = LeoLenses.clusterToClusterImages
      .modify(
        _ =>
          Set(
            ClusterImage(ClusterImageType.Jupyter,
                         "us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter:5c51ce6935da",
                         Instant.now)
          )
      )(testCluster)

    val resForLegacyImage = clusterHelper.createCluster(clusterWithLegacyImage).unsafeToFuture().futureValue

    resForLegacyImage.customDataprocImage shouldBe Config.dataprocConfig.legacyCustomDataprocImage
  }

  it should "clean up Google resources on error" in isolatedDbTest {
    val erroredDataprocDAO = new ErroredMockGoogleDataprocDAO
    val erroredClusterHelper = new ClusterHelper(DbSingleton.dbRef,
                                                 dataprocConfig,
                                                 imageConfig,
                                                 googleGroupsConfig,
                                                 proxyConfig,
                                                 clusterResourcesConfig,
                                                 clusterFilesConfig,
                                                 monitorConfig,
                                                 welderConfig,
                                                 bucketHelper,
                                                 erroredDataprocDAO,
                                                 mockGoogleComputeDAO,
                                                 mockGoogleDirectoryDAO,
                                                 mockGoogleIamDAO,
                                                 mockGoogleProjectDAO, MockWelderDAO,
                                                 blocker)

    val exception = erroredClusterHelper.createCluster(testCluster).unsafeToFuture().failed.futureValue
    exception shouldBe a[GoogleJsonResponseException]

    // verify Google DAOs have been cleaned up
    erroredDataprocDAO.clusters shouldBe 'empty
    erroredDataprocDAO.invocationCount shouldBe 1
    mockGoogleComputeDAO.firewallRules.size shouldBe 1
    mockGoogleComputeDAO.firewallRules should contain key testCluster.googleProject
    mockGoogleIamDAO.serviceAccountKeys shouldBe 'empty
  }

  it should "retry zone capacity issues" in isolatedDbTest {
    implicit val patienceConfig = PatienceConfig(timeout = 5.minutes)
    val erroredDataprocDAO = new ErroredMockGoogleDataprocDAO(429)
    val erroredClusterHelper = new ClusterHelper(DbSingleton.dbRef,
                                                 dataprocConfig,
                                                 imageConfig,
                                                 googleGroupsConfig,
                                                 proxyConfig,
                                                 clusterResourcesConfig,
                                                 clusterFilesConfig,
                                                 monitorConfig,
                                                 welderConfig,
                                                 bucketHelper,
                                                 erroredDataprocDAO,
                                                 mockGoogleComputeDAO,
                                                 mockGoogleDirectoryDAO,
                                                 mockGoogleIamDAO,
                                                 mockGoogleProjectDAO, MockWelderDAO,
                                                 blocker)

    val exception = erroredClusterHelper.createCluster(testCluster).unsafeToFuture().failed.futureValue
    exception shouldBe a[GoogleJsonResponseException]

    erroredDataprocDAO.invocationCount shouldBe 7
  }

  it should "choose the correct VPC subnet and network settings" in isolatedDbTest {
    // if config isn't set up to look at labels (which the default one isn't), the labels don't matter
    // and we should fall back to the config
    val decoySubnetMap = Map("subnet-label" -> "incorrectSubnet", "network-label" -> "incorrectNetwork")
    clusterHelper.getClusterVPCSettings(decoySubnetMap) shouldBe Some(VPCSubnet("test-subnet"))

    // label behaviour should be: project-subnet, project-network, config-subnet, config-network
    val configWithProjectLabels =
      dataprocConfig.copy(projectVPCSubnetLabel = Some("subnet-label"), projectVPCNetworkLabel = Some("network-label"))
    val clusterHelperWithLabels = new ClusterHelper(DbSingleton.dbRef,
                                                    configWithProjectLabels,
                                                    imageConfig,
                                                    googleGroupsConfig,
                                                    proxyConfig,
                                                    clusterResourcesConfig,
                                                    clusterFilesConfig,
                                                    monitorConfig,
                                                    welderConfig,
                                                    bucketHelper,
                                                    mockGoogleDataprocDAO,
                                                    mockGoogleComputeDAO,
                                                    mockGoogleDirectoryDAO,
                                                    mockGoogleIamDAO,
                                                    mockGoogleProjectDAO, MockWelderDAO,
                                                    blocker)

    val subnetMap = Map("subnet-label" -> "correctSubnet", "network-label" -> "incorrectNetwork")
    clusterHelperWithLabels.getClusterVPCSettings(subnetMap) shouldBe Some(VPCSubnet("correctSubnet"))

    val networkMap = Map("network-label" -> "correctNetwork")
    clusterHelperWithLabels.getClusterVPCSettings(networkMap) shouldBe Some(VPCNetwork("correctNetwork"))

    clusterHelperWithLabels.getClusterVPCSettings(Map()) shouldBe Some(VPCSubnet("test-subnet"))

    val configWithNoSubnet = dataprocConfig.copy(vpcSubnet = None)
    val clusterHelperWithNoSubnet = new ClusterHelper(DbSingleton.dbRef,
                                                      configWithNoSubnet,
                                                      imageConfig,
                                                      googleGroupsConfig,
                                                      proxyConfig,
                                                      clusterResourcesConfig,
                                                      clusterFilesConfig,
                                                      monitorConfig,
                                                      welderConfig,
                                                      bucketHelper,
                                                      mockGoogleDataprocDAO,
                                                      mockGoogleComputeDAO,
                                                      mockGoogleDirectoryDAO,
                                                      mockGoogleIamDAO,
                                                      mockGoogleProjectDAO, MockWelderDAO,
                                                      blocker)
    clusterHelperWithNoSubnet.getClusterVPCSettings(Map()) shouldBe Some(VPCNetwork("test-network"))
  }

  it should "retry 409 errors when adding IAM roles" in isolatedDbTest {
    implicit val patienceConfig = PatienceConfig(timeout = 5.minutes)
    val erroredIamDAO = new ErroredMockGoogleIamDAO(409)
    val erroredClusterHelper = new ClusterHelper(DbSingleton.dbRef,
                                                 dataprocConfig,
                                                 imageConfig,
                                                 googleGroupsConfig,
                                                 proxyConfig,
                                                 clusterResourcesConfig,
                                                 clusterFilesConfig,
                                                 monitorConfig,
                                                 welderConfig,
                                                 bucketHelper,
                                                 mockGoogleDataprocDAO,
                                                 mockGoogleComputeDAO,
                                                 mockGoogleDirectoryDAO,
                                                 erroredIamDAO,
                                                 mockGoogleProjectDAO, MockWelderDAO,
                                                 blocker)

    val exception = erroredClusterHelper.createCluster(testCluster).unsafeToFuture().failed.futureValue
    exception shouldBe a[GoogleJsonResponseException]

    erroredIamDAO.invocationCount should be > 2
  }

  it should "calculate cluster resource constraints" in isolatedDbTest {
    val resourceConstraints = clusterHelper.getClusterResourceContraints(testCluster).unsafeRunSync()

    // 7680m (in mock compute dao) - 5g (dataproc allocated) - 512m (welder allocated) = 2048m
    resourceConstraints.memoryLimit shouldBe MemorySize.fromMb(2048)
  }

  private class ErroredMockGoogleDataprocDAO(statusCode: Int = 400) extends MockGoogleDataprocDAO {
    var invocationCount = 0
    override def createCluster(googleProject: GoogleProject,
                               clusterName: ClusterName,
                               config: CreateClusterConfig): Future[Operation] = {
      invocationCount += 1
      val jsonFactory = new MockJsonFactory
      val testException =
        GoogleJsonResponseExceptionFactoryTesting.newMock(jsonFactory, statusCode, "oh no i have failed")

      Future.failed(testException)
    }
  }

  private class ErroredMockGoogleIamDAO(statusCode: Int = 400) extends MockGoogleIamDAO {
    var invocationCount = 0
    override def addIamRoles(iamProject: GoogleProject,
                             email: WorkbenchEmail,
                             memberType: MemberType,
                             rolesToAdd: Set[String]): Future[Boolean] = {
      invocationCount += 1
      val jsonFactory = new MockJsonFactory
      val testException =
        GoogleJsonResponseExceptionFactoryTesting.newMock(jsonFactory, statusCode, "oh no i have failed")

      Future.failed(testException)
    }
  }

}
