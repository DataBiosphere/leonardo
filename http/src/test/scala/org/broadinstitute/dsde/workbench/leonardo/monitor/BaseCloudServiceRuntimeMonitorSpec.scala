package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant
import cats.Parallel
import cats.effect.{Async, IO}
import cats.mtl.Ask
import com.google.cloud.storage.Storage
import fs2.Stream
import org.broadinstitute.dsde.workbench.RetryConfig
import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.{GoogleDiskService, GoogleStorageService}
import org.broadinstitute.dsde.workbench.google2.mock.{BaseFakeGoogleStorage, MockGoogleDiskService}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.{Config, RuntimeBucketConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockToolDAO, ToolDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{
  clusterQuery,
  persistentDiskQuery,
  DbReference,
  RuntimeConfigQueries,
  TestComponent
}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.{IP, TraceId}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsde.workbench.leonardo.http.ctxConversion
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class BaseCloudServiceRuntimeMonitorSpec extends AnyFlatSpec with Matchers with TestComponent with LeonardoTestSuite {
  it should "terminate monitoring process if cluster status is changed in the middle of it" in isolatedDbTest {
    val runtimeMonitor = baseRuntimeMonitor(true)

    val res = for {
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      s1 = runtimeMonitor.process(runtime.id, RuntimeStatus.Creating, None)
      s2 = Stream.sleep[IO](2 seconds) ++ Stream.eval(
        clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Deleting, Instant.now()).transaction
      )
      // run s1 and s2 concurrently. s1 will run indefinitely if s2 doesn't happen. So by validating the combined Stream terminate, we verify s1 is terminated due to unexpected status change for the runtime
      _ <- Stream(s1, s2).parJoin(2).compile.drain.timeout(10 seconds)
    } yield succeed
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "transition cluster to Stopping if Starting times out" in isolatedDbTest {
    val res = for {
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Starting).save())
      runtimeMonitor = new MockRuntimeMonitor(true, Map(RuntimeStatus.Starting -> 2.seconds)) {
        override def handleCheck(monitorContext: MonitorContext,
                                 runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                 checkToolsInterruptAfter: Option[FiniteDuration]
        )(implicit
          ev: Ask[IO, AppContext]
        ): IO[(Unit, Option[MonitorState])] = checkAgain(monitorContext, runtimeAndRuntimeConfig, None, None, None)
      }
      assersions = for {
        status <- clusterQuery.getClusterStatus(runtime.id).transaction
      } yield status.get shouldBe RuntimeStatus.Stopping
      _ <- withInfiniteStream(runtimeMonitor.process(runtime.id, RuntimeStatus.Starting, None), assersions)
    } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  "handleCheckTools" should "move to Running status if all tools are ready" in isolatedDbTest {
    val runtimeMonitor = baseRuntimeMonitor(true)

    val res = for {
      start <- IO.realTimeInstant
      tid <- traceId.ask[TraceId]
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(runtime, CommonTestData.defaultDataprocRuntimeConfig)
      monitorContext = MonitorContext(start, runtime.id, tid, RuntimeStatus.Creating)
      res <- runtimeMonitor.handleCheckTools(monitorContext, runtimeAndRuntimeConfig, IP("1.2.3.4"), None, true, None)
      end <- IO.realTimeInstant
      elapsed = end.toEpochMilli - start.toEpochMilli
      status <- clusterQuery.getClusterStatus(runtime.id).transaction
    } yield {
      // handleCheckTools has an intial delay of 1 second
      elapsed should be >= 1000L
      // it should not have reached the max timeout of 10 seconds
      elapsed should be < 7000L
      status shouldBe Some(RuntimeStatus.Running)
      res shouldBe (((), None))
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "move to failed status if tools are not ready" in isolatedDbTest {
    val runtimeMonitor = baseRuntimeMonitor(false)

    val res = for {
      disk <- makePersistentDisk().save()
      start <- IO.realTimeInstant
      tid <- traceId.ask[TraceId]
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Creating)
          .saveWithRuntimeConfig(CommonTestData.defaultGceRuntimeWithPDConfig(Some(disk.id)))
      )
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(runtime, CommonTestData.defaultDataprocRuntimeConfig)
      monitorContext = MonitorContext(start, runtime.id, tid, RuntimeStatus.Creating)
      res <- runtimeMonitor.handleCheckTools(monitorContext, runtimeAndRuntimeConfig, IP("1.2.3.4"), None, true, None)
      end <- IO.realTimeInstant
      elapsed = end.toEpochMilli - start.toEpochMilli
      status <- clusterQuery.getClusterStatus(runtime.id).transaction
      runtimeConfig <- RuntimeConfigQueries
        .getRuntimeConfig(runtime.runtimeConfigId)(scala.concurrent.ExecutionContext.Implicits.global)
        .transaction
    } yield {
      // handleCheckTools should have been interrupted after 10 seconds and moved the runtime to Error status
      elapsed shouldBe 10000L +- 2000L
      status shouldBe Some(RuntimeStatus.Error)
      res shouldBe (((), None))
      runtimeConfig.asInstanceOf[RuntimeConfig.GceWithPdConfig].persistentDiskId shouldBe None
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "move to failed status if checkTools times out after a custom timeout" in isolatedDbTest {
    val runtimeMonitor = baseRuntimeMonitor(false)

    val customcheckToolsInterruptAfter = Some(5 seconds)
    val customCheckTools =
      runtimeMonitor.getCustomInterruptablePollMonitorConfig(
        InterruptablePollMonitorConfig(5, 5 seconds, 25 seconds),
        customcheckToolsInterruptAfter
      )
    val customInterval = customCheckTools.interval

    val res = for {
      disk <- makePersistentDisk().save()
      start <- IO.realTimeInstant
      tid <- traceId.ask[TraceId]
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Creating)
          .saveWithRuntimeConfig(CommonTestData.defaultGceRuntimeWithPDConfig(Some(disk.id)))
      )
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(runtime, CommonTestData.defaultDataprocRuntimeConfig)
      monitorContext = MonitorContext(start, runtime.id, tid, RuntimeStatus.Creating)
      res <- runtimeMonitor.handleCheckTools(monitorContext,
                                             runtimeAndRuntimeConfig,
                                             IP("1.2.3.4"),
                                             None,
                                             true,
                                             customcheckToolsInterruptAfter
      )
      end <- IO.realTimeInstant
      elapsed = end.toEpochMilli - start.toEpochMilli
      status <- clusterQuery.getClusterStatus(runtime.id).transaction
      runtimeConfig <- RuntimeConfigQueries
        .getRuntimeConfig(runtime.runtimeConfigId)(scala.concurrent.ExecutionContext.Implicits.global)
        .transaction
    } yield {
      customInterval shouldBe 1.seconds
      // handleCheckTools should have been interrupted after 5 seconds and moved the runtime to Error status
      elapsed shouldBe 5000L +- 1000L
      status shouldBe Some(RuntimeStatus.Error)
      res shouldBe (((), None))
      runtimeConfig.asInstanceOf[RuntimeConfig.GceWithPdConfig].persistentDiskId shouldBe None
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not fail deleteInitBucket if bucket doesn't exist" in isolatedDbTest {
    val googleStorage = new BaseFakeGoogleStorage {
      override def deleteBucket(googleProject: GoogleProject,
                                bucketName: GcsBucketName,
                                isRecursive: Boolean,
                                bucketSourceOptions: List[Storage.BucketSourceOption],
                                traceId: Option[TraceId],
                                retryConfig: RetryConfig
      ): Stream[IO, Boolean] =
        Stream.raiseError[IO](
          new com.google.cloud.storage.StorageException(404, "The specified bucket does not exist.")
        )
    }
    val runtimeMonitor = baseRuntimeMonitor(false, googleStorageService = googleStorage)

    val res = for {
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      _ <- runtimeMonitor.deleteInitBucket(runtime.cloudContext.asInstanceOf[CloudContext.Gcp].value,
                                           runtime.runtimeName
      )
    } yield succeed

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not fail setStagingBucketLifecycle if bucket doesn't exist" in isolatedDbTest {
    val googleStorage = new BaseFakeGoogleStorage {
      override def deleteBucket(googleProject: GoogleProject,
                                bucketName: GcsBucketName,
                                isRecursive: Boolean,
                                bucketSourceOptions: List[Storage.BucketSourceOption],
                                traceId: Option[TraceId],
                                retryConfig: RetryConfig
      ): Stream[IO, Boolean] =
        Stream.raiseError[IO](
          new com.google.cloud.storage.StorageException(404, "The specified bucket does not exist.")
        )
    }
    val runtimeMonitor = baseRuntimeMonitor(false, googleStorageService = googleStorage)

    val res = for {
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      _ <- runtimeMonitor.setStagingBucketLifecycle(runtime, 10 days)
    } yield succeed

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not move to failed status if tools are not ready and runtime is Deleted" in isolatedDbTest {
    val runtimeMonitor = baseRuntimeMonitor(false)

    val res = for {
      start <- IO.realTimeInstant
      tid <- traceId.ask[TraceId]
      implicit0(ec: ExecutionContext) = scala.concurrent.ExecutionContext.Implicits.global
      disk <- makePersistentDisk().save()
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Creating)
          .saveWithRuntimeConfig(CommonTestData.defaultGceRuntimeWithPDConfig(Some(disk.id)))
      )
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction

      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(runtime, runtimeConfig)
      monitorContext = MonitorContext(start, runtime.id, tid, RuntimeStatus.Creating)

      runCheckTools = Stream.eval(
        runtimeMonitor.handleCheckTools(monitorContext, runtimeAndRuntimeConfig, IP("1.2.3.4"), None, true, None)
      )
      deleteRuntime = Stream.sleep[IO](2 seconds) ++ Stream.eval(
        clusterQuery.completeDeletion(runtime.id, start).transaction
      )
      // run above tasks concurrently and wait for both to terminate
      _ <- Stream(runCheckTools, deleteRuntime).parJoin(2).compile.drain.timeout(15 seconds)

      end <- IO.realTimeInstant
      elapsed = end.toEpochMilli - start.toEpochMilli
      status <- clusterQuery.getClusterStatus(runtime.id).transaction
      diskStatus <- persistentDiskQuery.getStatus(disk.id).transaction
    } yield {
      // handleCheckTools should have timed out after 10 seconds and the runtime should remain in Deleted status
      elapsed should be >= 10000L
      status shouldBe Some(RuntimeStatus.Deleted)
      diskStatus shouldBe (Some(DiskStatus.Ready))
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "delete creating disk on failed runtime start" in isolatedDbTest {
    val runtimeMonitor = baseRuntimeMonitor(false)

    val res = for {
      start <- IO.realTimeInstant
      tid <- traceId.ask[TraceId]
      implicit0(ec: ExecutionContext) = scala.concurrent.ExecutionContext.Implicits.global
      disk <- makePersistentDisk().save()
      _ <- persistentDiskQuery.updateStatus(disk.id, DiskStatus.Failed, Instant.now()).transaction
      runtime <- IO(
        makeCluster(0)
          .copy(status = RuntimeStatus.Creating)
          .saveWithRuntimeConfig(CommonTestData.defaultGceRuntimeWithPDConfig(Some(disk.id)))
      )
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(runtime.runtimeConfigId).transaction

      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(runtime, runtimeConfig)
      monitorContext = MonitorContext(start, runtime.id, tid, RuntimeStatus.Creating)
      runCheckTools = Stream.eval(
        runtimeMonitor.handleCheckTools(monitorContext, runtimeAndRuntimeConfig, IP("1.2.3.4"), None, true, None)
      )
      deleteRuntime = Stream.sleep[IO](2 seconds) ++ Stream.eval(
        clusterQuery.completeDeletion(runtime.id, start).transaction
      )
      // run above tasks concurrently and wait for both to terminate
      _ <- Stream(runCheckTools, deleteRuntime).parJoin(2).compile.drain.timeout(15 seconds)

      end <- IO.realTimeInstant
      elapsed = end.toEpochMilli - start.toEpochMilli
      status <- clusterQuery.getClusterStatus(runtime.id).transaction
      diskStatus <- persistentDiskQuery.getStatus(disk.id).transaction
    } yield {
      // handleCheckTools should have timed out after 10 seconds and the runtime should remain in Deleted status
      elapsed should be >= 10000L
      status shouldBe Some(RuntimeStatus.Deleted)
      diskStatus shouldBe (Some(DiskStatus.Deleted))
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  class MockRuntimeMonitor(isWelderReady: Boolean,
                           timeouts: Map[RuntimeStatus, FiniteDuration],
                           googleStorageService: GoogleStorageService[IO] = FakeGoogleStorageService
  )(
    implicit override val F: Async[IO],
    implicit override val parallel: Parallel[IO]
  ) extends BaseCloudServiceRuntimeMonitor[IO] {
    implicit override def dbRef: DbReference[IO] = testDbRef

    implicit override def ec: ExecutionContext = scala.concurrent.ExecutionContext.global

    implicit override def runtimeToolToToolDao
      : RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType] = x =>
      x match {
        case RuntimeContainerServiceType.WelderService => MockToolDAO(isWelderReady)
        case _                                         => MockToolDAO(true)
      }

    implicit override def openTelemetry: OpenTelemetryMetrics[IO] = metrics

    override def runtimeAlg: RuntimeAlgebra[IO] = MockRuntimeAlgebra

    override def logger: StructuredLogger[IO] = loggerIO

    override def googleStorage: GoogleStorageService[IO] = googleStorageService

    override def googleDisk: GoogleDiskService[IO] = MockGoogleDiskService

    override def monitorConfig: MonitorConfig = MonitorConfig.GceMonitorConfig(
      2 seconds,
      PollMonitorConfig(2 seconds, 5, 1 second),
      timeouts,
      InterruptablePollMonitorConfig(60, 1 second, 10 seconds),
      RuntimeBucketConfig(3 seconds),
      Config.imageConfig
    )

    override def handleCheck(monitorContext: MonitorContext,
                             runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                             checkToolsInterruptAfter: Option[FiniteDuration]
    )(implicit
      ev: Ask[IO, AppContext]
    ): IO[(Unit, Option[MonitorState])] = ???
  }

  def baseRuntimeMonitor(
    isWelderReady: Boolean,
    timeouts: Map[RuntimeStatus, FiniteDuration] = Map.empty,
    googleStorageService: GoogleStorageService[IO] = FakeGoogleStorageService
  ): BaseCloudServiceRuntimeMonitor[IO] =
    new MockRuntimeMonitor(isWelderReady, timeouts, googleStorageService)
}
