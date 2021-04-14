package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import cats.effect.IO
import cats.mtl.Ask
import com.google.cloud.compute.v1.{AccessConfig, Instance, NetworkInterface, Operation}
import com.google.cloud.dataproc.v1.ClusterStatus.State
import com.google.cloud.dataproc.v1._
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.mock.{
  BaseFakeGoogleDataprocService,
  FakeGoogleComputeService,
  FakeGoogleDataprocService
}
import org.broadinstitute.dsde.workbench.google2.{
  ClusterError,
  GoogleComputeService,
  GoogleDataprocService,
  InstanceName,
  RegionName,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockToolDAO, ToolDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterErrorQuery, clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorState.Check
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{IP, TraceId}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters._

class DataprocRuntimeMonitorSpec extends AnyFlatSpec with TestComponent with LeonardoTestSuite with EitherValues {
  "creatingRuntime" should "check again if cluster doesn't exist yet" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Creating
    )
    val monitorContext =
      MonitorContext(Instant.now(), runtime.id, appContext.ask.unsafeRunSync().traceId, RuntimeStatus.Creating)

    val res = for {
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor()(successToolDao)
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.creatingRuntime(None, monitorContext, savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
    } yield {
      r._2 shouldBe (Some(Check(runtimeAndRuntimeConfig, None)))
    }

    res.unsafeRunSync()
  }

  it should "will check again status is either Creating or Unknown" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    val cluster1 = getCluster(State.CREATING)
    val cluster2 = getCluster(State.UNKNOWN)

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Starting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor()(successToolDao)
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r1 <- monitor.creatingRuntime(Some(cluster1),
                                    monitorContext,
                                    savedRuntime,
                                    CommonTestData.defaultDataprocRuntimeConfig)
      r2 <- monitor.creatingRuntime(Some(cluster2),
                                    monitorContext,
                                    savedRuntime,
                                    CommonTestData.defaultDataprocRuntimeConfig)
    } yield {
      r1._2 shouldBe (Some(Check(runtimeAndRuntimeConfig, None)))
      r2._2 shouldBe (Some(Check(runtimeAndRuntimeConfig, None)))
    }

    res.unsafeRunSync()
  }

  it should "will check again status is Running but not all instances are running" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    val cluster = getCluster(State.RUNNING, Some(ZoneName("zone-a")))

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Starting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Provisioning))(successToolDao)
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.creatingRuntime(Some(cluster),
                                   monitorContext,
                                   savedRuntime,
                                   CommonTestData.defaultDataprocRuntimeConfig)
    } yield {
      r._2 shouldBe (Some(Check(runtimeAndRuntimeConfig, None)))
    }

    res.unsafeRunSync()
  }

  it should "will error if can't find zone for master instance" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    val cluster = getCluster(State.RUNNING, None)

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Starting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Running, Some(IP("fakeIp"))))(failureToolDao)
      r <- monitor.creatingRuntime(Some(cluster),
                                   monitorContext,
                                   savedRuntime,
                                   CommonTestData.defaultDataprocRuntimeConfig)
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      r._2 shouldBe None
      error.head.errorMessage shouldBe ("Can't find master instance for this cluster")
    }

    res.unsafeRunSync()
  }

  it should "will persist error if cluster is in Error state" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    val cluster = getCluster(State.ERROR)

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Starting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Running, Some(IP("fakeIp"))))(failureToolDao)
      r <- monitor.creatingRuntime(Some(cluster),
                                   monitorContext,
                                   savedRuntime,
                                   CommonTestData.defaultDataprocRuntimeConfig)
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      r._2 shouldBe None
      error.head.errorMessage shouldBe ("Error not available")
    }

    res.unsafeRunSync()
  }

  it should "will error if cluster is in unexpected state when trying to create" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    val cluster = getCluster(State.UPDATING)

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Starting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Running, Some(IP("fakeIp"))))(failureToolDao)
      r <- monitor.creatingRuntime(Some(cluster),
                                   monitorContext,
                                   savedRuntime,
                                   CommonTestData.defaultDataprocRuntimeConfig)
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      r._2 shouldBe None
      error.head.errorMessage shouldBe ("unexpected Dataproc cluster status Updating when trying to creating an instance")
    }

    res.unsafeRunSync()
  }

  it should "will try to persist user script error if cluster is in Error state but no error shown from dataproc" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    val cluster = getCluster(State.ERROR)

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Starting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Running, Some(IP("fakeIp"))))(failureToolDao)
      r <- monitor.creatingRuntime(Some(cluster),
                                   monitorContext,
                                   savedRuntime,
                                   CommonTestData.defaultDataprocRuntimeConfig)
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      r._2 shouldBe None
      error.head.errorCode shouldBe None
      error.head.errorMessage shouldBe ("Error not available")
    }

    res.unsafeRunSync()
  }

  it should "will persist error if cluster is in Error state when Creating" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    val cluster = getCluster(State.ERROR)

    val dataproc = new BaseFakeGoogleDataprocService {
      override def getClusterError(
        region: RegionName,
        operationName: google2.OperationName
      )(implicit ev: Ask[IO, TraceId]): IO[Option[ClusterError]] =
        IO.pure(Some(ClusterError(4, "time out")))
    }
    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Starting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Running, Some(IP("fakeIp"))),
                                       dataprocService = dataproc)(failureToolDao)
      r <- monitor.creatingRuntime(Some(cluster),
                                   monitorContext,
                                   savedRuntime,
                                   CommonTestData.defaultDataprocRuntimeConfig)
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      r._2 shouldBe None
      error.head.errorCode shouldBe Some(4)
      error.head.errorMessage shouldBe ("time out")
    }

    res.unsafeRunSync()
  }

  "startingRuntime" should "will check again if not all instances are Running when trying to start" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    val cluster = getCluster(State.RUNNING, Some(ZoneName("zone-a")))

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Starting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Provisioning, Some(IP("fakeIp"))))(
        failureToolDao
      )
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.startingRuntime(Some(cluster), monitorContext, runtimeAndRuntimeConfig)
    } yield {
      r._2 shouldBe (Some(Check(runtimeAndRuntimeConfig, None)))
    }

    res.unsafeRunSync()
  }

  it should "will check again if master instance doesn't have IP when trying to start" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    val cluster = getCluster(State.RUNNING, Some(ZoneName("zone-a")))

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Starting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Provisioning, None))(
        failureToolDao
      )
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.startingRuntime(Some(cluster), monitorContext, runtimeAndRuntimeConfig)
    } yield {
      r._2 shouldBe (Some(Check(runtimeAndRuntimeConfig, None)))
    }

    res.unsafeRunSync()
  }

  it should "will persist error if cluster is in Error state when Starting" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Starting
    )

    val cluster = getCluster(State.ERROR)

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Starting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Running, Some(IP("fakeIp"))))(failureToolDao)
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.startingRuntime(Some(cluster), monitorContext, runtimeAndRuntimeConfig)
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      r._2 shouldBe None
      error.head.errorCode shouldBe None
      error.head.errorMessage shouldBe ("Cluster failed to start")
    }

    res.unsafeRunSync()
  }

  "stoppingRuntime" should "check again if there's still instance Running" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopping
    )

    val cluster = getCluster(State.RUNNING, Some(ZoneName("zone-a")))

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Stopping)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Running, Some(IP("fakeIp"))))(failureToolDao)
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.stoppingRuntime(Some(cluster), monitorContext, runtimeAndRuntimeConfig)
    } yield {
      r._2 shouldBe (Some(Check(runtimeAndRuntimeConfig, None)))
    }

    res.unsafeRunSync()
  }

  "stoppingRuntime" should "update DB when all instances are stopped" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopping
    )

    val cluster = getCluster(State.RUNNING, Some(ZoneName("zone-a")))

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Stopping)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Terminated, Some(IP("fakeIp"))))(failureToolDao)
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.stoppingRuntime(Some(cluster), monitorContext, runtimeAndRuntimeConfig)
      updatedStatus <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      r._2 shouldBe None
      updatedStatus shouldBe (Some(RuntimeStatus.Stopped))
    }

    res.unsafeRunSync()
  }

  it should "terminate monitoring stopping if cluster doesn't exist" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopping
    )

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Stopping)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Running, Some(IP("fakeIp"))))(failureToolDao)
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.stoppingRuntime(None, monitorContext, runtimeAndRuntimeConfig).attempt
    } yield {
      r shouldBe Left(
        InvalidMonitorRequest(
          s"-1/${ctx.traceId.asString} | Can't stop an instance that hasn't been initialized yet or doesn't exist"
        )
      )
    }

    res.unsafeRunSync()
  }

  "updatingRuntime" should "terminate monitoring stopping if cluster doesn't exist" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopping
    )

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Stopping)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor()(successToolDao)
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.updatingRuntime(None, monitorContext, runtimeAndRuntimeConfig).attempt
      error <- clusterErrorQuery.get(savedRuntime.id).transaction
    } yield {
      r shouldBe Left(
        InvalidMonitorRequest(
          s"-1/${ctx.traceId.asString} | Can't update an instance that hasn't been initialized yet or doesn't exist"
        )
      )
      error.head.errorMessage shouldBe (s"-1/${ctx.traceId.asString} | Can't update an instance that hasn't been initialized yet or doesn't exist")
    }

    res.unsafeRunSync()
  }

  it should "check again if cluster is still being updated" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Updating
    )

    val cluster = getCluster(State.UPDATING, Some(ZoneName("zone-a")))

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Updating)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor()(successToolDao)
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.updatingRuntime(Some(cluster), monitorContext, runtimeAndRuntimeConfig)
    } yield {
      r._2 shouldBe Some(Check(runtimeAndRuntimeConfig, None))
    }

    res.unsafeRunSync()
  }

  it should "check again if not all instances are Running" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Stopping
    )

    val cluster = getCluster(State.RUNNING, Some(ZoneName("zone-a")))

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Stopping)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Provisioning, Some(IP("fakeIp"))))(
        successToolDao
      )
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.updatingRuntime(Some(cluster), monitorContext, runtimeAndRuntimeConfig)
    } yield {
      r._2 shouldBe Some(Check(runtimeAndRuntimeConfig, None))
    }

    res.unsafeRunSync()
  }

  it should "check again if instance IP is not available yet" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Updating
    )

    val cluster = getCluster(State.RUNNING, Some(ZoneName("zone-a")))

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Updating)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Running, None))(
        successToolDao
      )
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.updatingRuntime(Some(cluster), monitorContext, runtimeAndRuntimeConfig)
    } yield {
      r._2 shouldBe Some(Check(runtimeAndRuntimeConfig, None))
    }

    res.unsafeRunSync()
  }

  it should "complete update" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Updating
    )

    val cluster = getCluster(State.RUNNING, Some(ZoneName("zone-a")))

    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Updating)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor(computeService(GceInstanceStatus.Running, Some(IP("fakeIp"))))(
        successToolDao
      )
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.updatingRuntime(Some(cluster), monitorContext, runtimeAndRuntimeConfig)
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      r._2 shouldBe None
      status.head shouldBe (RuntimeStatus.Running)
    }

    res.unsafeRunSync()
  }

  "deleteRuntime" should "check again if cluster still exists" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Deleting
    )
    val cluster = getCluster(State.RUNNING, Some(ZoneName("zone-a")))
    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Deleting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor()(
        successToolDao
      )
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.deletedRuntime(Some(cluster), monitorContext, runtimeAndRuntimeConfig)
    } yield {
      r._2 shouldBe Some(Check(runtimeAndRuntimeConfig, None))
    }

    res.unsafeRunSync()
  }

  it should "delete runtime if runtime no longer exists in google" in isolatedDbTest {
    val runtime = makeCluster(1).copy(
      serviceAccount = clusterServiceAccountFromProject(project).get,
      asyncRuntimeFields = Some(makeAsyncRuntimeFields(1)),
      status = RuntimeStatus.Deleting
    )
    val res = for {
      ctx <- appContext.ask[AppContext]
      monitorContext = MonitorContext(Instant.now(), runtime.id, ctx.traceId, RuntimeStatus.Deleting)
      savedRuntime <- IO(runtime.save())
      monitor = dataprocRuntimeMonitor()(
        successToolDao
      )
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(savedRuntime, CommonTestData.defaultDataprocRuntimeConfig)
      r <- monitor.deletedRuntime(None, monitorContext, runtimeAndRuntimeConfig)
      status <- clusterQuery.getClusterStatus(savedRuntime.id).transaction
    } yield {
      r._2 shouldBe None
      status shouldBe Some(RuntimeStatus.Deleted)
    }

    res.unsafeRunSync()
  }

  val successToolDao: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType] = _ => MockToolDAO(true)
  val failureToolDao: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType] = _ => MockToolDAO(false)

  def dataprocRuntimeMonitor(
    googleComputeService: GoogleComputeService[IO] = FakeGoogleComputeService,
    dataprocService: GoogleDataprocService[IO] = FakeGoogleDataprocService
  )(implicit ev: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType]): DataprocRuntimeMonitor[IO] =
    new DataprocRuntimeMonitor[IO](
      Config.dataprocMonitorConfig,
      googleComputeService,
      MockAuthProvider,
      FakeGoogleStorageService,
      FakeDataproInterp,
      dataprocService
    )

  def getCluster(state: State, zoneName: Option[ZoneName] = None): Cluster = {
    val instances = InstanceGroupConfig
      .newBuilder()
      .addAllInstanceNames(List("master-instance").asJava)
      .build()
    val configBuilder = ClusterConfig
      .newBuilder()
      .setMasterConfig(instances)

    val config = zoneName.fold(configBuilder.build())(z =>
      configBuilder.setGceClusterConfig(GceClusterConfig.newBuilder().setZoneUri(s"zone/${z.value}")).build()
    )

    Cluster
      .newBuilder()
      .setConfig(config)
      .setStatus(ClusterStatus.newBuilder().setState(state).build())
      .build()
  }

  def computeService(status: GceInstanceStatus, ip: Option[IP] = None): GoogleComputeService[IO] =
    new FakeGoogleComputeService {
      override def getInstance(project: GoogleProject, zone: ZoneName, instanceName: InstanceName)(
        implicit ev: Ask[IO, TraceId]
      ): IO[Option[Instance]] = {
        val instanceBuilder = Instance
          .newBuilder()
          .setStatus(status.toString)
          .setId("100")
        val instance = ip.fold(instanceBuilder.build()) { i =>
          instanceBuilder
            .addNetworkInterfaces(
              NetworkInterface
                .newBuilder()
                .addAccessConfigs(AccessConfig.newBuilder().setNatIP(i.asString).build())
                .build()
            )
            .build()
        }

        IO.pure(Some(instance))
      }
    }
}

class BaseFakeDataproInterp extends RuntimeAlgebra[IO] {
  override def createRuntime(params: CreateRuntimeParams)(
    implicit ev: Ask[IO, AppContext]
  ): IO[Option[CreateGoogleRuntimeResponse]] = ???

  override def deleteRuntime(params: DeleteRuntimeParams)(
    implicit ev: Ask[IO, AppContext]
  ): IO[Option[Operation]] = IO.pure(None)

  override def finalizeDelete(params: FinalizeDeleteParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
    IO.unit

  override def stopRuntime(
    params: StopRuntimeParams
  )(implicit ev: Ask[IO, AppContext]): IO[Option[Operation]] =
    IO.pure(None)

  override def startRuntime(params: StartRuntimeParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???

  override def updateMachineType(params: UpdateMachineTypeParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] =
    ???

  override def updateDiskSize(params: UpdateDiskSizeParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???

  override def resizeCluster(params: ResizeClusterParams)(implicit ev: Ask[IO, AppContext]): IO[Unit] = ???
}

object FakeDataproInterp extends BaseFakeDataproInterp
