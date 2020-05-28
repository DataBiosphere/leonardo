package org.broadinstitute.dsde.workbench.leonardo
package util

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.testing.json.GoogleJsonResponseExceptionFactoryTesting
import com.google.api.client.testing.json.MockJsonFactory
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.RuntimeStatus.Creating
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockWelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{CreateClusterConfig, MockGoogleComputeService}
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateRuntimeMessage
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class DataprocInterpreterSpec
    extends TestKit(ActorSystem("leonardotest"))
    with FlatSpecLike
    with TestComponent
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with LeonardoTestSuite {
  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO
  val mockGoogleStorageDAO = new MockGoogleStorageDAO
  val mockGoogleProjectDAO = new MockGoogleProjectDAO

  val testCluster = makeCluster(1)
    .copy(status = Creating, asyncRuntimeFields = None)
  val testClusterClusterProjectAndName = RuntimeProjectAndName(testCluster.googleProject, testCluster.runtimeName)

  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig, clusterResourcesConfig)
  val bucketHelper =
    new BucketHelper[IO](bucketHelperConfig, FakeGoogleStorageService, serviceAccountProvider, blocker)
  val vpcInterp = new VPCInterpreter[IO](Config.vpcInterpreterConfig, mockGoogleProjectDAO, MockGoogleComputeService)

  val dataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                   bucketHelper,
                                                   vpcInterp,
                                                   mockGoogleDataprocDAO,
                                                   MockGoogleComputeService,
                                                   mockGoogleDirectoryDAO,
                                                   mockGoogleIamDAO,
                                                   mockGoogleProjectDAO,
                                                   MockWelderDAO,
                                                   blocker)

  override def beforeAll(): Unit =
    // Set up the mock directoryDAO to have the Google group used to grant permission to users to pull the custom dataproc image
    mockGoogleDirectoryDAO
      .createGroup(
        Config.googleGroupsConfig.dataprocImageProjectGroupName,
        Config.googleGroupsConfig.dataprocImageProjectGroupEmail,
        Option(mockGoogleDirectoryDAO.lockedDownGroupSettings)
      )
      .futureValue

  "DataprocInterpreter" should "create a google cluster" in isolatedDbTest {
    val clusterCreationRes =
      dataprocInterp
        .createRuntime(
          CreateRuntimeParams
            .fromCreateRuntimeMessage(CreateRuntimeMessage.fromRuntime(testCluster, defaultDataprocRuntimeConfig, None))
        )
        .unsafeToFuture()
        .futureValue

    // verify the mock dataproc DAO
    mockGoogleDataprocDAO.clusters.size shouldBe 1
    mockGoogleDataprocDAO.clusters should contain key testCluster.runtimeName
    val operation = mockGoogleDataprocDAO.clusters(testCluster.runtimeName)

    // verify the mock compute DAO
    // TODO
//    mockGoogleComputeDAO.firewallRules.size shouldBe 1
//    mockGoogleComputeDAO.firewallRules should contain key testCluster.googleProject

    // verify the returned cluster
    val dpInfo = clusterCreationRes.asyncRuntimeFields
    dpInfo.operationName shouldBe operation.name
    dpInfo.googleId shouldBe operation.id
    dpInfo.hostIp shouldBe None
    dpInfo.stagingBucket.value should startWith("leostaging")

    // verify the returned init bucket
    clusterCreationRes.initBucket.value should startWith("leoinit")

    // verify the returned service account key
    mockGoogleIamDAO.serviceAccountKeys shouldBe 'empty
    clusterCreationRes.serviceAccountKey shouldBe None
  }

  it should "be able to determine appropriate custom dataproc image" in isolatedDbTest {
    val cluster = LeoLenses.runtimeToRuntimeImages
      .modify(_ =>
        Set(
          RuntimeImage(RuntimeImageType.Jupyter, "us.gcr.io/broad-dsp-gcr-public/terra-jupyter-hail:0.0.1", Instant.now)
        )
      )(testCluster)

    val res =
      dataprocInterp
        .createRuntime(
          CreateRuntimeParams
            .fromCreateRuntimeMessage(CreateRuntimeMessage.fromRuntime(cluster, defaultDataprocRuntimeConfig, None))
        )
        .unsafeToFuture()
        .futureValue
    res.customImage shouldBe Config.dataprocConfig.customDataprocImage
    val clusterWithLegacyImage = LeoLenses.runtimeToRuntimeImages
      .modify(_ =>
        Set(
          RuntimeImage(RuntimeImageType.Jupyter,
                       "us.gcr.io/broad-dsp-gcr-public/leonardo-jupyter:5c51ce6935da",
                       Instant.now)
        )
      )(testCluster)

    val resForLegacyImage =
      dataprocInterp
        .createRuntime(
          CreateRuntimeParams.fromCreateRuntimeMessage(
            CreateRuntimeMessage.fromRuntime(clusterWithLegacyImage, defaultDataprocRuntimeConfig, None)
          )
        )
        .unsafeToFuture()
        .futureValue

    resForLegacyImage.customImage shouldBe Config.dataprocConfig.legacyCustomDataprocImage
  }

  it should "clean up Google resources on error" in isolatedDbTest {
    val erroredDataprocDAO = new ErroredMockGoogleDataprocDAO
    val erroredDataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                            bucketHelper,
                                                            vpcInterp,
                                                            erroredDataprocDAO,
                                                            MockGoogleComputeService,
                                                            mockGoogleDirectoryDAO,
                                                            mockGoogleIamDAO,
                                                            mockGoogleProjectDAO,
                                                            MockWelderDAO,
                                                            blocker)

    val exception =
      erroredDataprocInterp
        .createRuntime(
          CreateRuntimeParams
            .fromCreateRuntimeMessage(CreateRuntimeMessage.fromRuntime(testCluster, defaultDataprocRuntimeConfig, None))
        )
        .unsafeToFuture()
        .failed
        .futureValue
    exception shouldBe a[GoogleJsonResponseException]

    // verify Google DAOs have been cleaned up
    erroredDataprocDAO.clusters shouldBe 'empty
    erroredDataprocDAO.invocationCount shouldBe 1
    // TODO
//    mockGoogleComputeDAO.firewallRules.size shouldBe 1
//    mockGoogleComputeDAO.firewallRules should contain key testCluster.googleProject
    mockGoogleIamDAO.serviceAccountKeys shouldBe 'empty
  }

  it should "retry zone capacity issues" in isolatedDbTest {
    implicit val patienceConfig = PatienceConfig(timeout = 5.minutes)
    val erroredDataprocDAO = new ErroredMockGoogleDataprocDAO(429)
    val erroredDataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                            bucketHelper,
                                                            vpcInterp,
                                                            erroredDataprocDAO,
                                                            MockGoogleComputeService,
                                                            mockGoogleDirectoryDAO,
                                                            mockGoogleIamDAO,
                                                            mockGoogleProjectDAO,
                                                            MockWelderDAO,
                                                            blocker)

    val exception =
      erroredDataprocInterp
        .createRuntime(
          CreateRuntimeParams
            .fromCreateRuntimeMessage(CreateRuntimeMessage.fromRuntime(testCluster, defaultDataprocRuntimeConfig, None))
        )
        .unsafeToFuture()
        .failed
        .futureValue
    exception shouldBe a[GoogleJsonResponseException]

    erroredDataprocDAO.invocationCount shouldBe 5
  }

  it should "retry 409 errors when adding IAM roles" in isolatedDbTest {
    implicit val patienceConfig = PatienceConfig(timeout = 5.minutes)
    val erroredIamDAO = new ErroredMockGoogleIamDAO(409)
    val erroredDataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                            bucketHelper,
                                                            vpcInterp,
                                                            mockGoogleDataprocDAO,
                                                            MockGoogleComputeService,
                                                            mockGoogleDirectoryDAO,
                                                            erroredIamDAO,
                                                            mockGoogleProjectDAO,
                                                            MockWelderDAO,
                                                            blocker)

    val exception =
      erroredDataprocInterp
        .createRuntime(
          CreateRuntimeParams
            .fromCreateRuntimeMessage(CreateRuntimeMessage.fromRuntime(testCluster, defaultDataprocRuntimeConfig, None))
        )
        .unsafeToFuture()
        .failed
        .futureValue
    exception shouldBe a[GoogleJsonResponseException]

    erroredIamDAO.invocationCount should be > 2
  }

  it should "calculate cluster resource constraints" in isolatedDbTest {
    val runtimeConfig = RuntimeConfig.DataprocConfig(0,
                                                     MachineTypeName("n1-standard-4"),
                                                     DiskSize(500),
                                                     None,
                                                     None,
                                                     None,
                                                     None,
                                                     Map.empty[String, String])
    val resourceConstraints = dataprocInterp
      .getClusterResourceContraints(testClusterClusterProjectAndName, runtimeConfig.machineType)
      .unsafeRunSync()

    // 7680m (in mock compute dao) - 6g (dataproc allocated) - 768m (welder allocated) = 768m
    resourceConstraints.memoryLimit shouldBe MemorySize.fromMb(768)
  }

  private class ErroredMockGoogleDataprocDAO(statusCode: Int = 400) extends MockGoogleDataprocDAO {
    var invocationCount = 0
    override def createCluster(googleProject: GoogleProject,
                               clusterName: RuntimeName,
                               config: CreateClusterConfig): Future[GoogleOperation] = {
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
