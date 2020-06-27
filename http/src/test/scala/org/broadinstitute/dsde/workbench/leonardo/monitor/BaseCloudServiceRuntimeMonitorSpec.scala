package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import cats.effect.{Async, IO, Timer}
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1._
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.{Config, RuntimeBucketConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.{MockToolDAO, ToolDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, DbReference, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import fs2.Stream

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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

  "handleCheckTools" should "if all tools are ready" in isolatedDbTest {
    val runtimeMonitor = baseRuntimeMonitor(true)

    val res = for {
      now <- nowInstant
      tid <- traceId.ask
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(runtime, CommonTestData.defaultDataprocRuntimeConfig)
      monitorContext = MonitorContext(now, runtime.id, tid, RuntimeStatus.Creating)
      r <- runtimeMonitor.handleCheckTools(monitorContext, runtimeAndRuntimeConfig, IP("1.2.3.4"), Set.empty)
    } yield {
      r shouldBe (((), None))
    }
    res.unsafeRunSync()
  }

  def baseRuntimeMonitor(isWelderReady: Boolean): BaseCloudServiceRuntimeMonitor[IO] =
    new BaseCloudServiceRuntimeMonitor[IO] {
      implicit override def F: Async[IO] = IO.ioConcurrentEffect(cs)

      implicit override def timer: Timer[IO] = testTimer

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

      override def runtimeAlg: RuntimeAlgebra[IO] = ???

      override def logger: Logger[IO] = loggerIO

      override def googleStorage: GoogleStorageService[IO] = ???

      override def monitorConfig: MonitorConfig = MonitorConfig.GceMonitorConfig(
        2 seconds,
        1 seconds,
        5,
        1 seconds,
        RuntimeBucketConfig(3 seconds),
        Map.empty,
        ZoneName("zoneName"),
        Config.imageConfig
      )

      override def pollCheck(googleProject: GoogleProject,
                             runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                             operation: Operation,
                             action: RuntimeStatus)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] = ???

      override def failedRuntime(
        monitorContext: MonitorContext,
        runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
        errorDetails: Option[RuntimeErrorDetails],
        instances: Set[DataprocInstance]
      )(implicit ev: ApplicativeAsk[IO, AppContext]): IO[(Unit, Option[MonitorState])] = ???

      override def handleCheck(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
        implicit ev: ApplicativeAsk[IO, AppContext]
      ): IO[(Unit, Option[MonitorState])] = IO.pure(((), Some(MonitorState.Check(runtimeAndRuntimeConfig))))
    }
}
