package org.broadinstitute.dsde.workbench.leonardo
package util

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.implicits._
import cats.mtl.Ask
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.googleapis.testing.json.GoogleJsonResponseExceptionFactoryTesting
import com.google.api.client.testing.json.MockJsonFactory
import com.google.api.gax.longrunning.OperationFuture
import com.google.cloud.compute.v1.{Instance, MachineType, Operation}
import com.google.cloud.dataproc.v1.ClusterOperationMetadata
import com.google.protobuf.Empty
import org.broadinstitute.dsde.workbench.google.GoogleDirectoryDAO
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.mock._
import org.broadinstitute.dsde.workbench.google2.{
  DataprocClusterName,
  DataprocRole,
  DataprocRoleZonePreemptibility,
  GoogleComputeService,
  GoogleDataprocService,
  MachineTypeName,
  RegionName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.RuntimeStatus.Creating
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockWelderDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent, UpdateAsyncClusterCreationFields}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateRuntimeMessage
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.google.iam.Expr
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes.IamMemberType
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import org.scalatest.prop.TableDrivenPropertyChecks.Table
import org.scalatest.prop.TableDrivenPropertyChecks.forAll

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
    new BucketHelper[IO](bucketHelperConfig, FakeGoogleStorageService, MockSamService)

  val mockGoogleResourceService = new FakeGoogleResourceService {
    override def getProjectNumber(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Long]] =
      IO(Some(1L))
  }

  val vpcInterp =
    new VPCInterpreter[IO](Config.vpcInterpreterConfig, mockGoogleResourceService, FakeGoogleComputeService)

  def dataprocInterp(computeService: GoogleComputeService[IO] = MockGoogleComputeService,
                     dataprocCluster: GoogleDataprocService[IO] = MockGoogleDataprocService,
                     googleDirectoryDao: GoogleDirectoryDAO = mockGoogleDirectoryDAO
  ) =
    new DataprocInterpreter[IO](
      Config.dataprocInterpreterConfig,
      bucketHelper,
      vpcInterp,
      dataprocCluster,
      computeService,
      MockGoogleDiskService,
      googleDirectoryDao,
      mockGoogleIamDAO,
      mockGoogleResourceService,
      MockWelderDAO
    )

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
      dataprocInterp()
        .createRuntime(
          CreateRuntimeParams
            .fromCreateRuntimeMessage(
              CreateRuntimeMessage.fromRuntime(testCluster,
                                               LeoLenses.runtimeConfigPrism.getOption(defaultDataprocRuntimeConfig).get,
                                               None,
                                               None
              )
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
    val erroredDataprocInterp = new DataprocInterpreter[IO](
      Config.dataprocInterpreterConfig,
      bucketHelper,
      vpcInterp,
      FakeGoogleDataprocService,
      FakeGoogleComputeService,
      MockGoogleDiskService,
      mockGoogleDirectoryDAO,
      erroredIamDAO,
      MockGoogleResourceService,
      MockWelderDAO
    )

    val exception =
      erroredDataprocInterp
        .createRuntime(
          CreateRuntimeParams
            .fromCreateRuntimeMessage(
              CreateRuntimeMessage.fromRuntime(testCluster,
                                               LeoLenses.runtimeConfigPrism.getOption(defaultDataprocRuntimeConfig).get,
                                               None,
                                               None
              )
            )
        )
        .unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
        .failed
        .futureValue
    exception shouldBe a[GoogleJsonResponseException]

    erroredIamDAO.invocationCount should be > 2
  }

  it should "wait until member added properly" in {
    val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO {
      override def isGroupMember(groupEmail: WorkbenchEmail, memberEmail: WorkbenchEmail): Future[Boolean] =
        Future.successful(false)
    }
    val res =
      dataprocInterp(googleDirectoryDao = mockGoogleDirectoryDAO)
        .waitUntilMemberAdded(
          WorkbenchEmail("member")
        )
        .attempt
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res shouldBe Left(
      org.broadinstitute.dsde.workbench
        .StreamTimeoutError("fail to add member to dataproc-image-project-group@test.firecloud.org")
    )
  }

  it should "Fail cluster creation if machine type is n1-highcpu" in isolatedDbTest {
    def dataprocInterpHighMem(dataprocCluster: GoogleDataprocService[IO] = MockGoogleDataprocService,
                              googleDirectoryDao: GoogleDirectoryDAO = mockGoogleDirectoryDAO
    ) =
      new DataprocInterpreter[IO](
        Config.dataprocInterpreterConfig,
        bucketHelper,
        vpcInterp,
        dataprocCluster,
        FakeGoogleComputeService,
        MockGoogleDiskService,
        googleDirectoryDao,
        mockGoogleIamDAO,
        mockGoogleResourceService,
        MockWelderDAO
      )

    val runtimeConfig = RuntimeConfig.DataprocConfig(0,
                                                     MachineTypeName("n1-highcpu-4"),
                                                     DiskSize(500),
                                                     None,
                                                     None,
                                                     None,
                                                     None,
                                                     Map.empty[String, String],
                                                     RegionName("us-central1"),
                                                     true,
                                                     false
    )
    val res = dataprocInterpHighMem()
      .getDataprocRuntimeResourceContraints(testClusterClusterProjectAndName,
                                            runtimeConfig.machineType,
                                            RegionName("us-central1")
      )
      .attempt
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    res.leftMap(_.getMessage) shouldBe Left(
      s"Machine type (n1-highcpu-4) not supported by Hail. Consider use `n1-highmem` machine type"
    )
  }

  it should "calculate cluster resource constraints and software config correctly for n1-standard and n1-highmem machine types" in isolatedDbTest {
    val highMemGoogleComputeService = new FakeGoogleComputeService {
      override def getMachineType(project: GoogleProject, zone: ZoneName, machineTypeName: MachineTypeName)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[MachineType]] =
        IO.pure(Some(MachineType.newBuilder().setName("pass").setMemoryMb(104 * 1024).setGuestCpus(4).build()))
    }

    def dataprocInterpHighMem(computeService: GoogleComputeService[IO] = highMemGoogleComputeService,
                              dataprocCluster: GoogleDataprocService[IO] = MockGoogleDataprocService,
                              googleDirectoryDao: GoogleDirectoryDAO = mockGoogleDirectoryDAO
    ) =
      new DataprocInterpreter[IO](
        Config.dataprocInterpreterConfig,
        bucketHelper,
        vpcInterp,
        dataprocCluster,
        computeService,
        MockGoogleDiskService,
        googleDirectoryDao,
        mockGoogleIamDAO,
        mockGoogleResourceService,
        MockWelderDAO
      )

    val machineTypes = Table("machineType", MachineTypeName("n1-standard-4"), MachineTypeName("n1-highmem-64"))
    forAll(machineTypes) { machineType: MachineTypeName =>
      val runtimeConfig = RuntimeConfig.DataprocConfig(0,
                                                       machineType,
                                                       DiskSize(500),
                                                       None,
                                                       None,
                                                       None,
                                                       None,
                                                       Map.empty[String, String],
                                                       RegionName("us-central1"),
                                                       true,
                                                       false
      )
      val resourceConstraints = dataprocInterpHighMem()
        .getDataprocRuntimeResourceContraints(testClusterClusterProjectAndName,
                                              runtimeConfig.machineType,
                                              RegionName("us-central1")
        )
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

      val dataProcSoftwareConfig = dataprocInterp().getSoftwareConfig(
        GoogleProject("MyGoogleProject"),
        RuntimeName("MyRuntimeName"),
        runtimeConfig,
        resourceConstraints.driverMemory.get
      )

      val propertyMap = dataProcSoftwareConfig.getPropertiesMap()

      val expectedDriverMemory =
        if (machineType == MachineTypeName("n1-standard-4")) (104 - 7) * 0.9 * 1024 else (104 - 11) * 0.9 * 1024
      resourceConstraints.memoryLimit shouldBe MemorySizeBytes.fromMb(expectedDriverMemory + 4 * 1024)
      resourceConstraints.shmSize shouldBe MemorySizeMegaBytes((0.5 * (expectedDriverMemory + 4 * 1024)).toLong)

      propertyMap.get(
        "spark:spark.driver.memory"
      ) shouldBe s"${expectedDriverMemory.toInt}m"
    }
  }

  it should "don't error if runtime is already deleted" in isolatedDbTest {
    val computeService = new FakeGoogleComputeService {
      override def modifyInstanceMetadata(
        project: GoogleProject,
        zone: ZoneName,
        instanceName: InstanceName,
        metadataToAdd: Map[String, String],
        metadataToRemove: Set[String]
      )(implicit ev: Ask[IO, TraceId]): IO[Option[OperationFuture[Operation, Operation]]] =
        IO.raiseError(new org.broadinstitute.dsde.workbench.model.WorkbenchException("Instance not found: "))
    }

    val dataproc = dataprocInterp(computeService)
    val res = for {
      runtime <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Deleting)
          .save()
      )
      _ <- IO(
        dbFutureValue(
          clusterQuery.updateAsyncClusterCreationFields(UpdateAsyncClusterCreationFields(None, 1, None, Instant.now))
        )
      )
      updatedRuntme <- IO(dbFutureValue(clusterQuery.getClusterById(runtime.id)))
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(updatedRuntme.get, CommonTestData.defaultDataprocRuntimeConfig)
      res <- dataproc.deleteRuntime(DeleteRuntimeParams(runtimeAndRuntimeConfig, None))
    } yield res shouldBe None
    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "call googleDataprocService.deleteCluster even if there's no metadata to update" in isolatedDbTest {
    val computeService = new FakeGoogleComputeService {
      override def modifyInstanceMetadata(
        project: GoogleProject,
        zone: ZoneName,
        instanceName: InstanceName,
        metadataToAdd: Map[String, String],
        metadataToRemove: Set[String]
      )(implicit ev: Ask[IO, TraceId]): IO[Option[OperationFuture[Operation, Operation]]] =
        IO.pure(None)
    }
    val dataprocService = new BaseFakeGoogleDataprocService {
      override def deleteCluster(project: GoogleProject, region: RegionName, clusterName: DataprocClusterName)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[OperationFuture[Empty, ClusterOperationMetadata]]] = IO.raiseError(new NotImplementedError)
    }

    val res = for {
      runtime <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Deleting)
          .save()
      )
      _ <- IO(
        dbFutureValue(
          clusterQuery.updateAsyncClusterCreationFields(UpdateAsyncClusterCreationFields(None, 1, None, Instant.now))
        )
      )
      updatedRuntme <- IO(dbFutureValue(clusterQuery.getClusterById(runtime.id)))
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(updatedRuntme.get, CommonTestData.defaultDataprocRuntimeConfig)
      dataproc = dataprocInterp(computeService, dataprocService)

      res <- dataproc
        .deleteRuntime(DeleteRuntimeParams(runtimeAndRuntimeConfig, Some(CommonTestData.masterInstance)))
        .attempt
    } yield
    // this is really hacky way of verifying deleteCluster is being called once. But I tried a few different ways to mock out `GoogleDataprocService[IO]`
    // and for some reason it didn't work
    res.swap.toOption.get.isInstanceOf[NotImplementedError] shouldBe true
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  private class ErroredMockGoogleIamDAO(statusCode: Int = 400) extends MockGoogleIamDAO {
    var invocationCount = 0

    override def addRoles(googleProject: GoogleProject,
                          userEmail: WorkbenchEmail,
                          memberType: IamMemberType,
                          rolesToAdd: Set[String],
                          retryIfGroupDoesNotExist: Boolean,
                          condition: Option[Expr]
    ): Future[Boolean] = {
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
    override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(implicit
      ev: Ask[IO, TraceId]
    ): IO[Option[Instance]] = IO.pure(Some(Instance.newBuilder().setFingerprint("abcd").build()))
  }

}
