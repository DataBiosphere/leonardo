package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.{Deferred, IO}
import cats.effect.std.Queue
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.dao.{JupyterDAO, MockJupyterDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.scalatest.flatspec.AnyFlatSpec
import java.time.temporal.ChronoUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class AutopauseMonitorSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {

  it should "auto freeze the cluster when kernel is idle" in isolatedDbTest {
    val jupyterDAO = new MockJupyterDAO {
      override def isAllKernelsIdle(cloudContext: CloudContext, clusterName: RuntimeName): IO[Boolean] =
        IO.pure(true)
    }

    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      now <- IO.realTimeInstant
      runningRuntime <- IO(
        makeCluster(1)
          .copy(auditInfo = auditInfo.copy(dateAccessed = now.minus(5, ChronoUnit.MINUTES)),
                status = RuntimeStatus.Running,
                autopauseThreshold = 1
          )
          .save()
      )
      _ <- monitor(jupyterDAO, queue)(3 seconds)
      status <- clusterQuery.getClusterStatus(runningRuntime.id).transaction
      event <- queue.tryTake
    } yield {
      status.get shouldBe (RuntimeStatus.PreStopping)
      event.get shouldBe a[LeoPubsubMessage.StopRuntimeMessage]
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not auto freeze the cluster if jupyter kernel is still running" in isolatedDbTest {
    val jupyterDAO = new MockJupyterDAO {
      override def isAllKernelsIdle(cloudContext: CloudContext, clusterName: RuntimeName): IO[Boolean] =
        IO.pure(false)
    }

    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      now <- IO.realTimeInstant
      runningRuntime <- IO(
        makeCluster(1)
          .copy(auditInfo = auditInfo.copy(dateAccessed = now.minus(45, ChronoUnit.SECONDS)),
                status = RuntimeStatus.Running,
                autopauseThreshold = 1
          )
          .save()
      )
      _ <- monitor(jupyterDAO, queue)(3 seconds)
      status <- clusterQuery.getClusterStatus(runningRuntime.id).transaction
      event <- queue.tryTake
    } yield {
      status.get shouldBe (RuntimeStatus.Running)
      event shouldBe None
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "auto freeze the cluster if we fail to get jupyter kernel status" in isolatedDbTest {
    val jupyterDAO = new MockJupyterDAO {
      override def isAllKernelsIdle(cloudContext: CloudContext, clusterName: RuntimeName): IO[Boolean] =
        IO.raiseError(new Exception)
    }

    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      now <- IO.realTimeInstant
      runningRuntime <- IO(
        makeCluster(1)
          .copy(auditInfo = auditInfo.copy(dateAccessed = now.minus(5, ChronoUnit.MINUTES)),
                status = RuntimeStatus.Running,
                autopauseThreshold = 1
          )
          .save()
      )
      _ <- monitor(jupyterDAO, queue)(3 seconds)
      status <- clusterQuery.getClusterStatus(runningRuntime.id).transaction
      event <- queue.tryTake
    } yield {
      status.get shouldBe (RuntimeStatus.PreStopping)
      event.get shouldBe a[LeoPubsubMessage.StopRuntimeMessage]
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "auto freeze the cluster if the max kernel busy time is exceeded" in isolatedDbTest {
    val jupyterDAO = new MockJupyterDAO {
      override def isAllKernelsIdle(cloudContext: CloudContext, clusterName: RuntimeName): IO[Boolean] =
        IO.pure(true)
    }

    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      now <- IO.realTimeInstant
      runningRuntime <- IO(
        makeCluster(1)
          .copy(
            auditInfo = auditInfo.copy(
              dateAccessed = now.minus(25, ChronoUnit.HOURS)
            ),
            kernelFoundBusyDate = Some(now.minus(25, ChronoUnit.HOURS)),
            status = RuntimeStatus.Running
          )
          .save()
      )
      _ <- monitor(jupyterDAO, queue)(3 seconds)
      status <- clusterQuery.getClusterStatus(runningRuntime.id).transaction
      event <- queue.tryTake
    } yield {
      status.get shouldBe (RuntimeStatus.PreStopping)
      event.get shouldBe a[LeoPubsubMessage.StopRuntimeMessage]
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  private def monitor(
    jupyterDAO: JupyterDAO[IO] = MockJupyterDAO,
    publisherQueue: Queue[IO, LeoPubsubMessage]
  )(waitDuration: FiniteDuration): IO[Unit] = {
    val monitorProcess = AutopauseMonitor.process[IO](autoFreezeConfig, jupyterDAO, publisherQueue)
    val process = Stream.eval(Deferred[IO, Unit]).flatMap { signalToStop =>
      val signal = Stream.sleep[IO](waitDuration).evalMap(_ => signalToStop.complete(())).void
      val p = Stream(monitorProcess.interruptWhen(signalToStop.get.attempt.map(_.map(_ => ()))), signal)
        .parJoin(3)
      p ++ Stream.eval(signalToStop.get)
    }
    process.compile.lastOrError
  }
}
