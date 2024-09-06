package org.broadinstitute.dsde.workbench.leonardo.util

import cats.effect.IO
import cats.mtl.Ask
import com.google.api.gax.longrunning.OperationFuture
import com.google.cloud.compute.v1.{Instance, Operation}
import org.broadinstitute.dsde.workbench.google2.mock.{
  FakeComputeOperationFuture,
  FakeGoogleComputeService,
  FakeGoogleResourceService,
  MockGoogleDiskService
}
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.MockWelderDAO
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent, UpdateAsyncClusterCreationFields}
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeConfigInCreateRuntimeMessage
import org.broadinstitute.dsde.workbench.leonardo.{
  CommonTestData,
  DiskSize,
  FakeGoogleStorageService,
  LeonardoTestSuite,
  MockSamService,
  RuntimeAndRuntimeConfig,
  RuntimeProjectAndName,
  RuntimeStatus
}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.scalatest.flatspec.AnyFlatSpecLike

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global

class GceInterpreterSpec extends AnyFlatSpecLike with TestComponent with LeonardoTestSuite {
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

  def gceInterp(computeService: GoogleComputeService[IO] = FakeGoogleComputeService) =
    new GceInterpreter[IO](Config.gceInterpreterConfig,
                           bucketHelper,
                           vpcInterp,
                           computeService,
                           MockGoogleDiskService,
                           MockWelderDAO
    )

  it should "not error if create runtime has conflict" in isolatedDbTest {
    val response = new FakeComputeOperationFuture {
      override def getName(): String =
        throw new java.util.concurrent.ExecutionException("Conflict", new Exception("bad"))
    }
    val computeService = new FakeGoogleComputeService {
      override def createInstance(project: GoogleProject, zone: ZoneName, instance: Instance)(implicit
        ev: Ask[IO, TraceId]
      ): IO[Option[OperationFuture[Operation, Operation]]] = IO.pure(Some(response))

    }

    val gce = gceInterp(computeService)
    val res = for {
      runtime <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Deleting)
          .saveWithRuntimeConfig(CommonTestData.defaultGceRuntimeConfig)
      )
      runtimeConfig = RuntimeConfigInCreateRuntimeMessage.GceConfig(
        CommonTestData.defaultGceRuntimeConfig.machineType,
        CommonTestData.defaultGceRuntimeConfig.diskSize,
        CommonTestData.defaultGceRuntimeConfig.bootDiskSize.getOrElse(DiskSize(120)),
        CommonTestData.defaultGceRuntimeConfig.zone,
        None
      )
      res <- gce.createRuntime(
        CreateRuntimeParams(
          runtime.id,
          RuntimeProjectAndName(runtime.cloudContext, runtime.runtimeName),
          runtime.serviceAccount,
          None,
          runtime.auditInfo,
          None,
          None,
          None,
          None,
          runtime.runtimeImages,
          Set.empty,
          true,
          Map.empty,
          runtimeConfig
        )
      )
    } yield res shouldBe None
    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
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

    val gce = gceInterp(computeService)
    val res = for {
      runtime <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Deleting)
          .saveWithRuntimeConfig(CommonTestData.defaultGceRuntimeConfig)
      )
      _ <- IO(
        dbFutureValue(
          clusterQuery.updateAsyncClusterCreationFields(UpdateAsyncClusterCreationFields(None, 1, None, Instant.now))
        )
      )
      updatedRuntme <- IO(dbFutureValue(clusterQuery.getClusterById(runtime.id)))
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(updatedRuntme.get, CommonTestData.defaultGceRuntimeConfig)
      res <- gce.deleteRuntime(DeleteRuntimeParams(runtimeAndRuntimeConfig, None))
    } yield res shouldBe None
    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }
}
