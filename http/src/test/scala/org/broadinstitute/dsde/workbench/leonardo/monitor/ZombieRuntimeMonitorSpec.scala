package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.IO
import cats.effect.concurrent.Deferred
import cats.mtl.Ask
import com.google.cloud.compute.v1.{Instance, Operation}
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.mock.{FakeGoogleComputeService, FakeGoogleDataprocService}
import org.broadinstitute.dsde.workbench.google2.{
  GoogleComputeService,
  GoogleDataprocService,
  GoogleDiskService,
  InstanceName,
  MachineTypeName,
  MockGoogleDiskService,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.config.Config.{dataprocInterpreterConfig, gceInterpreterConfig}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.util.{
  DataprocInterpreter,
  GceInterpreter,
  RuntimeAlgebra,
  RuntimeInstances
}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext.Implicits.global

class ZombieRuntimeMonitorSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with LeonardoTestSuite
    with TestComponent
    with MockitoSugar
    with GcsPathUtils { testKit =>

  val testCluster1 = makeCluster(1).copy(status = RuntimeStatus.Running)
  val testCluster2 = makeCluster(2).copy(status = RuntimeStatus.Running)
  val testCluster3 = makeCluster(3).copy(status = RuntimeStatus.Running)

  val deletedRuntime1 = makeCluster(4).copy(
    status = RuntimeStatus.Deleted,
    auditInfo = olderRuntimeAuditInfo,
    labels = Map(Config.zombieRuntimeMonitorConfig.deletionConfirmationLabelKey -> "false")
  )
  val deletedRuntime2 = makeCluster(5).copy(
    status = RuntimeStatus.Deleted,
    auditInfo = olderRuntimeAuditInfo,
    labels = Map(Config.zombieRuntimeMonitorConfig.deletionConfirmationLabelKey -> "false")
  )

  it should "detect inactive zombie gce instance" in isolatedDbTest {
    // create a running gce instance
    val runtimeConfig = RuntimeConfig.GceConfig(
      MachineTypeName("n1-standard-4"),
      DiskSize(50),
      bootDiskSize = Some(DiskSize(50))
    )
    val savedTestInstance1 = deletedRuntime1.saveWithRuntimeConfig(runtimeConfig)
    savedTestInstance1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual deletedRuntime1

    val savedTestInstance2 = deletedRuntime2.saveWithRuntimeConfig(runtimeConfig)
    savedTestInstance2.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual deletedRuntime2

    // stub GoogleComputeService to flag instance2 as still running on google
    val gce = new FakeGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Instance]] =
        if (instanceName.value === deletedRuntime2.runtimeName.asString) {
          IO.pure(Some(readyInstance))
        } else {
          IO.pure(None)
        }
      override def deleteInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Operation]] = {
        instanceName.value shouldBe (deletedRuntime2.runtimeName.asString)
        project shouldBe (deletedRuntime2.googleProject)
        IO.pure(Some(Operation.newBuilder().setId("op").setName("opName").setTargetId("target").build()))
      }
    }

    withZombieMonitor(gce = gce) { () =>
      eventually(timeout(Span(10, Seconds))) {
        val r1 = dbFutureValue(clusterQuery.getClusterById(savedTestInstance1.id)).get
        val r2 = dbFutureValue(clusterQuery.getClusterById(savedTestInstance2.id)).get

        r1.labels.get(Config.zombieRuntimeMonitorConfig.deletionConfirmationLabelKey) shouldBe Some("true")
        r2.labels.get(Config.zombieRuntimeMonitorConfig.deletionConfirmationLabelKey) shouldBe Some("false")
      }
    }
  }

  private def withZombieMonitor[A](
    dataproc: GoogleDataprocService[IO] = FakeGoogleDataprocService,
    gce: GoogleComputeService[IO] = new FakeGoogleComputeService,
    disk: GoogleDiskService[IO] = new MockGoogleDiskService,
    dataprocInterpOpt: Option[RuntimeAlgebra[IO]] = None
  )(validations: () => A): A = {
    val dataprocInterp = dataprocInterpOpt.getOrElse(
      new DataprocInterpreter[IO](dataprocInterpreterConfig,
                                  null,
                                  null,
                                  dataproc,
                                  gce,
                                  disk,
                                  null,
                                  null,
                                  null,
                                  null,
                                  blocker)
    )
    val gceInterp =
      new GceInterpreter(gceInterpreterConfig, null, null, gce, disk, null, blocker)

    implicit val runtimeInstances = new RuntimeInstances[IO](dataprocInterp, gceInterp)
    val zombieClusterMonitor =
      ZombieRuntimeMonitor[IO](Config.zombieRuntimeMonitorConfig)
    val process = Stream.eval(Deferred[IO, A]).flatMap { signalToStop =>
      val signal = Stream.eval(IO(validations())).evalMap(r => signalToStop.complete(r))
      val p = Stream(zombieClusterMonitor.process.interruptWhen(signalToStop.get.attempt.map(_.map(_ => ()))), signal)
        .parJoin(2)
      p ++ Stream.eval(signalToStop.get)
    }
    process.compile.lastOrError.unsafeRunSync().asInstanceOf[A]
  }
}
