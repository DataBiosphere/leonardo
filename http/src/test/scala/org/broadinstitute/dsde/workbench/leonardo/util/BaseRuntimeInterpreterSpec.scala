package org.broadinstitute.dsde.workbench.leonardo.util

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.google.mock._
import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.google2.mock._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockWelderDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.SamService
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, RuntimeConfigQueries, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.{
  CommonTestData,
  FakeGoogleStorageService,
  LeonardoTestSuite,
  RuntimeAndRuntimeConfig
}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext.Implicits.global

class BaseRuntimeInterpreterSpec
    extends TestKit(ActorSystem("leonardotest"))
    with AnyFlatSpecLike
    with TestComponent
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with MockitoSugar
    with LeonardoTestSuite {
  val mockGoogleIamDAO = new MockGoogleIamDAO
  val mockGoogleDirectoryDAO = new MockGoogleDirectoryDAO
  val mockGoogleStorageDAO = new MockGoogleStorageDAO

  val mockGoogleResourceService = new FakeGoogleResourceService {
    override def getProjectNumber(project: GoogleProject)(implicit ev: Ask[IO, TraceId]): IO[Option[Long]] =
      IO(Some(1L))
  }
  val vpcInterp =
    new VPCInterpreter[IO](Config.vpcInterpreterConfig, mockGoogleResourceService, FakeGoogleComputeService)

  val bucketHelperConfig =
    BucketHelperConfig(imageConfig, welderConfig, proxyConfig, clusterFilesConfig)
  val bucketHelper =
    new BucketHelper[IO](bucketHelperConfig, FakeGoogleStorageService, mock[SamService[IO]])

  val gceInterp = new GceInterpreter[IO](Config.gceInterpreterConfig,
                                         bucketHelper,
                                         vpcInterp,
                                         FakeGoogleComputeService,
                                         MockGoogleDiskService,
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

  it should "create a google cluster" in isolatedDbTest {
    val runtime = makeCluster(1).saveWithRuntimeConfig(CommonTestData.defaultGceRuntimeConfig)
    val newMachineType = MachineTypeName("n1-highmem-64")
    for {
      now <- IO.realTimeInstant
      runtime <- clusterQuery.getClusterById(runtime.id).transaction
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.get.runtimeConfigId).transaction
      _ <- gceInterp.updateMachineType(
        UpdateMachineTypeParams(RuntimeAndRuntimeConfig(runtime.get, runtimeConfig), newMachineType, now)
      )
      newMachineType <- RuntimeConfigQueries.getRuntimeConfig(runtime.get.runtimeConfigId).transaction
    } yield newMachineType shouldBe newMachineType
  }
}
