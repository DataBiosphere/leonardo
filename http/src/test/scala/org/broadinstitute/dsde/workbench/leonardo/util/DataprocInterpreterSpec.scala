package org.broadinstitute.dsde.workbench.leonardo
package util

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.mtl.Ask
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.testing.json.GoogleJsonResponseExceptionFactoryTesting
import com.google.api.client.testing.json.MockJsonFactory
import com.google.cloud.compute.v1.Instance
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.mock._
import org.broadinstitute.dsde.workbench.google2.{
  DataprocClusterName,
  DataprocRole,
  DataprocRoleZonePreemptibility,
  InstanceName,
  MachineTypeName,
  RegionName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.RuntimeStatus.Creating
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockWelderDAO
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateRuntimeMessage
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class DataprocInterpreterSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with TestComponent
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with LeonardoTestSuite {
  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO
  val mockGoogleStorageDAO = new MockGoogleStorageDAO

  val testCluster = makeCluster(1)
    .copy(asyncRuntimeFields = None, status = Creating)
  val testClusterClusterProjectAndName = RuntimeProjectAndName(testCluster.cloudContext, testCluster.runtimeName)

  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig)
  val bucketHelper =
    new BucketHelper[IO](bucketHelperConfig, FakeGoogleStorageService, serviceAccountProvider)

  val mockGoogleResourceService = new FakeGoogleResourceService {
    override def getProjectNumber(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Long]] =
      IO(Some(1L))
  }

  val vpcInterp =
    new VPCInterpreter[IO](Config.vpcInterpreterConfig, mockGoogleResourceService, FakeGoogleComputeService)

  val dataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                   bucketHelper,
                                                   vpcInterp,
                                                   MockGoogleDataprocService,
                                                   MockGoogleComputeService,
                                                   MockGoogleDiskService,
                                                   mockGoogleDirectoryDAO,
                                                   mockGoogleIamDAO,
                                                   mockGoogleResourceService,
                                                   MockWelderDAO)

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
            .fromCreateRuntimeMessage(
              CreateRuntimeMessage.fromRuntime(testCluster,
                                               LeoLenses.runtimeConfigPrism.getOption(defaultDataprocRuntimeConfig).get,
                                               None)
            )
        )
        .unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
        .futureValue

    // verify the returned cluster
    val dpInfo = clusterCreationRes.get.asyncRuntimeFields
    dpInfo.operationName.value shouldBe "opName"
    dpInfo.proxyHostName.value shouldBe "clusterUuid"
    dpInfo.hostIp shouldBe None
    dpInfo.stagingBucket.value should startWith("leostaging")

    // verify the returned init bucket
    clusterCreationRes.get.initBucket.value should startWith("leoinit")

    // verify the returned service account key
    mockGoogleIamDAO.serviceAccountKeys shouldBe 'empty
  }

  it should "retry 409 errors when adding IAM roles" in isolatedDbTest {
    implicit val patienceConfig = PatienceConfig(timeout = 5.minutes)
    val erroredIamDAO = new ErroredMockGoogleIamDAO(409)
    val erroredDataprocInterp = new DataprocInterpreter[IO](Config.dataprocInterpreterConfig,
                                                            bucketHelper,
                                                            vpcInterp,
                                                            FakeGoogleDataprocService,
                                                            FakeGoogleComputeService,
                                                            MockGoogleDiskService,
                                                            mockGoogleDirectoryDAO,
                                                            erroredIamDAO,
                                                            MockGoogleResourceService,
                                                            MockWelderDAO)

    val exception =
      erroredDataprocInterp
        .createRuntime(
          CreateRuntimeParams
            .fromCreateRuntimeMessage(
              CreateRuntimeMessage.fromRuntime(testCluster,
                                               LeoLenses.runtimeConfigPrism.getOption(defaultDataprocRuntimeConfig).get,
                                               None)
            )
        )
        .unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
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
                                                     Map.empty[String, String],
                                                     RegionName("us-central1"),
                                                     true,
                                                     false)
    val resourceConstraints = dataprocInterp
      .getClusterResourceContraints(testClusterClusterProjectAndName,
                                    runtimeConfig.machineType,
                                    RegionName("us-central1"))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // 7680m (in mock compute dao) - 6g (dataproc allocated) - 768m (welder allocated) = 768m
    resourceConstraints.memoryLimit shouldBe MemorySize.fromMb(768)
  }

  private class ErroredMockGoogleIamDAO(statusCode: Int = 400) extends MockGoogleIamDAO {
    var invocationCount = 0
    override def addIamRoles(googleProject: GoogleProject,
                             userEmail: WorkbenchEmail,
                             memberType: MemberType,
                             rolesToAdd: Set[String],
                             retryIfGroupDoesNotExist: Boolean): Future[Boolean] = {
      invocationCount += 1
      val jsonFactory = new MockJsonFactory
      val testException =
        GoogleJsonResponseExceptionFactoryTesting.newMock(jsonFactory, statusCode, "oh no i have failed")

      Future.failed(testException)
    }
  }

  private object MockGoogleResourceService extends FakeGoogleResourceService {
    override def getProjectNumber(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Long]] =
      IO(Some(1234567890))
  }

  private object MockGoogleDataprocService extends BaseFakeGoogleDataprocService {
    override def getClusterInstances(project: GoogleProject, region: RegionName, clusterName: DataprocClusterName)(
      implicit ev: Ask[IO, TraceId]
    ): IO[Map[DataprocRoleZonePreemptibility, Set[InstanceName]]] =
      IO.pure(
        Map(
          DataprocRoleZonePreemptibility(DataprocRole.Master, ZoneName("us-central1-a"), false) -> Set(
            InstanceName("masterInstance")
          )
        )
      )
  }

  private object MockGoogleComputeService extends FakeGoogleComputeService {
    override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
      implicit
      ev: Ask[IO, TraceId]
    ): IO[Option[Instance]] = IO.pure(Some(Instance.newBuilder().setFingerprint("abcd").build()))
  }

}
