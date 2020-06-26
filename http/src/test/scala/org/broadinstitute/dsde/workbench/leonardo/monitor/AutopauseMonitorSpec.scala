package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.temporal.ChronoUnit

import cats.effect.IO
import cats.effect.concurrent.Deferred
import fs2.Stream
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.dao.{JupyterDAO, MockJupyterDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.{dbioToIO}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec

class AutopauseMonitorSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {

  it should "auto freeze the cluster when kernel is idle" in isolatedDbTest {
    val jupyterDAO = new MockJupyterDAO {
      override def isAllKernelsIdle(googleProject: GoogleProject, clusterName: RuntimeName): IO[Boolean] =
        IO.pure(true)
    }

    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      now <- nowInstant
      runningRuntime <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Running,
                auditInfo = auditInfo.copy(dateAccessed = now.minus(5, ChronoUnit.MINUTES)),
                autopauseThreshold = 1)
          .save()
      )
      _ <- monitor(jupyterDAO, queue)(3 seconds)
      status <- clusterQuery.getClusterStatus(runningRuntime.id).transaction
      event <- queue.tryDequeue1
    } yield {
      status.get shouldBe (RuntimeStatus.PreStopping)
      event.get shouldBe a[LeoPubsubMessage.StopRuntimeMessage]
    }

    res.unsafeRunSync()
  }

  it should "not auto freeze the cluster if jupyter kernel is still running" in isolatedDbTest {
    val jupyterDAO = new MockJupyterDAO {
      override def isAllKernelsIdle(googleProject: GoogleProject, clusterName: RuntimeName): IO[Boolean] =
        IO.pure(false)
    }

    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      now <- nowInstant
      runningRuntime <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Running,
                auditInfo = auditInfo.copy(dateAccessed = now.minus(45, ChronoUnit.SECONDS)),
                autopauseThreshold = 1)
          .save()
      )
      _ <- monitor(jupyterDAO, queue)(3 seconds)
      status <- clusterQuery.getClusterStatus(runningRuntime.id).transaction
      event <- queue.tryDequeue1
    } yield {
      status.get shouldBe (RuntimeStatus.Running)
      event shouldBe (None)
    }

    res.unsafeRunSync()
  }

  it should "auto freeze the cluster if we fail to get jupyter kernel status" in isolatedDbTest {
    val jupyterDAO = new MockJupyterDAO {
      override def isAllKernelsIdle(googleProject: GoogleProject, clusterName: RuntimeName): IO[Boolean] =
        IO.raiseError(new Exception)
    }

    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      now <- nowInstant
      runningRuntime <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Running,
                auditInfo = auditInfo.copy(dateAccessed = now.minus(5, ChronoUnit.MINUTES)),
                autopauseThreshold = 1)
          .save()
      )
      _ <- monitor(jupyterDAO, queue)(3 seconds)
      status <- clusterQuery.getClusterStatus(runningRuntime.id).transaction
      event <- queue.tryDequeue1
    } yield {
      status.get shouldBe (RuntimeStatus.PreStopping)
      event.get shouldBe a[LeoPubsubMessage.StopRuntimeMessage]
    }

    res.unsafeRunSync()
  }

  it should "auto freeze the cluster if the max kernel busy time is exceeded" in isolatedDbTest {
    val jupyterDAO = new MockJupyterDAO {
      override def isAllKernelsIdle(googleProject: GoogleProject, clusterName: RuntimeName): IO[Boolean] =
        IO.pure(true)
    }

    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      now <- nowInstant
      runningRuntime <- IO(
        makeCluster(1)
          .copy(
            status = RuntimeStatus.Running,
            auditInfo = auditInfo.copy(
              dateAccessed = now.minus(25, ChronoUnit.HOURS)
            ),
            kernelFoundBusyDate = Some(now.minus(25, ChronoUnit.HOURS))
          )
          .save()
      )
      _ <- monitor(jupyterDAO, queue)(3 seconds)
      status <- clusterQuery.getClusterStatus(runningRuntime.id).transaction
      event <- queue.tryDequeue1
    } yield {
      status.get shouldBe (RuntimeStatus.PreStopping)
      event.get shouldBe a[LeoPubsubMessage.StopRuntimeMessage]
    }

    res.unsafeRunSync()
  }

  private def monitor(
    jupyterDAO: JupyterDAO[IO] = MockJupyterDAO,
    publisherQueue: InspectableQueue[IO, LeoPubsubMessage]
  )(waitDuration: FiniteDuration): IO[Unit] = {
    val monitor = AutopauseMonitor[IO](autoFreezeConfig, jupyterDAO, publisherQueue)
    val process = Stream.eval(Deferred[IO, Unit]).flatMap { signalToStop =>
      val signal = Stream.sleep(waitDuration).evalMap(_ => signalToStop.complete(()))
      val p = Stream(monitor.process.interruptWhen(signalToStop.get.attempt.map(_.map(_ => ()))), signal)
        .parJoin(3)
      p ++ Stream.eval(signalToStop.get)
    }
    process.compile.lastOrError
  }
}
