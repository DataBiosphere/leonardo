package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.Parallel
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.mtl.Ask
import com.google.cloud.compute.v1._
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.{Config, RuntimeBucketConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockToolDAO, ToolDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{IP, TraceId}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.StructuredLogger

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._

class BaseCloudServiceRuntimeMonitorSpec extends AnyFlatSpec with Matchers with TestComponent with LeonardoTestSuite {
  it should "terminate monitoring process if cluster status is changed in the middle of it" in isolatedDbTest {
    val runtimeMonitor = baseRuntimeMonitor(true)

    val res = for {
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      s1 = runtimeMonitor.process(runtime.id, RuntimeStatus.Creating)
      s2 = Stream.sleep(2 seconds) ++ Stream.eval(
        clusterQuery.updateClusterStatus(runtime.id, RuntimeStatus.Deleting, Instant.now()).transaction
      )
      // run s1 and s2 concurrently. s1 will run indefinitely if s2 doesn't happen. So by validating the combined Stream terminate, we verify s1 is terminated due to unexpected status change for the runtime
      _ <- Stream(s1, s2).parJoin(2).compile.drain.timeout(10 seconds)
    } yield succeed
    res.unsafeRunSync()
  }

  it should "transition cluster to Stopping if Starting times out" in isolatedDbTest {
    val res = for {
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Starting).save())
      runtimeMonitor = new MockRuntimeMonitor(true, Map(RuntimeStatus.Starting -> 2.seconds)) {
        override def handleCheck(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
          implicit ev: Ask[IO, AppContext]
        ): IO[(Unit, Option[MonitorState])] = checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, None, None)
      }
      assersions = for {
        status <- clusterQuery.getClusterStatus(runtime.id).transaction
      } yield status.get shouldBe RuntimeStatus.Stopping
      _ <- withInfiniteStream(runtimeMonitor.process(runtime.id, RuntimeStatus.Starting), assersions)
    } yield ()
    res.unsafeRunSync()
  }

  "handleCheckTools" should "move to Running status if all tools are ready" in isolatedDbTest {
    val runtimeMonitor = baseRuntimeMonitor(true)

    val res = for {
      start <- nowInstant
      tid <- traceId.ask[TraceId]
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(runtime, CommonTestData.defaultDataprocRuntimeConfig)
      monitorContext = MonitorContext(start, runtime.id, tid, RuntimeStatus.Creating)
      res <- runtimeMonitor.handleCheckTools(monitorContext, runtimeAndRuntimeConfig, IP("1.2.3.4"), Set.empty)
      end <- nowInstant
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

    res.unsafeRunSync()
  }

  it should "move to failed status if tools are not ready" in isolatedDbTest {
    val runtimeMonitor = baseRuntimeMonitor(false)

    val res = for {
      start <- nowInstant
      tid <- traceId.ask[TraceId]
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(runtime, CommonTestData.defaultDataprocRuntimeConfig)
      monitorContext = MonitorContext(start, runtime.id, tid, RuntimeStatus.Creating)
      res <- runtimeMonitor.handleCheckTools(monitorContext, runtimeAndRuntimeConfig, IP("1.2.3.4"), Set.empty)
      end <- nowInstant
      elapsed = end.toEpochMilli - start.toEpochMilli
      status <- clusterQuery.getClusterStatus(runtime.id).transaction
    } yield {
      // handleCheckTools should have timed out after 10 seconds and moved the runtime to Error status
      elapsed should be >= 10000L
      status shouldBe Some(RuntimeStatus.Error)
      res shouldBe (((), None))
    }

    res.unsafeRunSync()
  }

  it should "not move to failed status if tools are not ready and runtime is Deleted" in isolatedDbTest {
    val runtimeMonitor = baseRuntimeMonitor(false)

    val res = for {
      start <- nowInstant
      tid <- traceId.ask[TraceId]
      implicit0(ec: ExecutionContext) = global
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(runtime, CommonTestData.defaultDataprocRuntimeConfig)
      monitorContext = MonitorContext(start, runtime.id, tid, RuntimeStatus.Creating)

      runCheckTools = Stream.eval(
        runtimeMonitor.handleCheckTools(monitorContext, runtimeAndRuntimeConfig, IP("1.2.3.4"), Set.empty)
      )
      deleteRuntime = Stream.sleep(2 seconds) ++ Stream.eval(
        clusterQuery.completeDeletion(runtime.id, start).transaction
      )
      // run above tasks concurrently and wait for both to terminate
      _ <- Stream(runCheckTools, deleteRuntime).parJoin(2).compile.drain.timeout(15 seconds)

      end <- nowInstant
      elapsed = end.toEpochMilli - start.toEpochMilli
      status <- clusterQuery.getClusterStatus(runtime.id).transaction
    } yield {
      // handleCheckTools should have timed out after 10 seconds and the runtime should remain in Deleted status
      elapsed should be >= 10000L
      status shouldBe Some(RuntimeStatus.Deleted)
    }

    res.unsafeRunSync()
  }

  class MockRuntimeMonitor(isWelderReady: Boolean, timeouts: Map[RuntimeStatus, FiniteDuration])
      extends BaseCloudServiceRuntimeMonitor[IO] {
    implicit override def F: ConcurrentEffect[IO] = IO.ioConcurrentEffect(cs)

    implicit override def parallel: Parallel[IO] = IO.ioParallel(cs)

    override def timer: Timer[IO] = testTimer

    implicit override def dbRef: DbReference[IO] = testDbRef

    implicit override def ec: ExecutionContext = global

    implicit override def runtimeToolToToolDao
      : RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType] = x => {
      x match {
        case RuntimeContainerServiceType.WelderService => MockToolDAO(isWelderReady)
        case _                                         => MockToolDAO(true)
      }
    }

    implicit override def openTelemetry: OpenTelemetryMetrics[IO] = metrics

    override def runtimeAlg: RuntimeAlgebra[IO] = MockRuntimeAlgebra

    override def logger: StructuredLogger[IO] = loggerIO

    override def googleStorage: GoogleStorageService[IO] = ???

    override def monitorConfig: MonitorConfig = MonitorConfig.GceMonitorConfig(
      2 seconds,
      PollMonitorConfig(5, 1 second),
      timeouts,
      InterruptablePollMonitorConfig(5, 1 second, 5 seconds),
      RuntimeBucketConfig(3 seconds),
      Config.imageConfig
    )

    override def pollCheck(googleProject: GoogleProject,
                           runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                           operation: Operation,
                           action: RuntimeStatus)(implicit ev: Ask[IO, TraceId]): IO[Unit] = ???

    override def handleCheck(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
      implicit ev: Ask[IO, AppContext]
    ): IO[(Unit, Option[MonitorState])] = IO.pure(((), Some(MonitorState.Check(runtimeAndRuntimeConfig, None))))
  }

  def baseRuntimeMonitor(isWelderReady: Boolean,
                         timeouts: Map[RuntimeStatus, FiniteDuration] = Map.empty): BaseCloudServiceRuntimeMonitor[IO] =
    new MockRuntimeMonitor(isWelderReady, timeouts)
}
