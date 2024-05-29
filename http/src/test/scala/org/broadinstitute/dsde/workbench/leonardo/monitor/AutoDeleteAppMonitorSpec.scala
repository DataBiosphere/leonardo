package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.std.Queue
import cats.effect.{Deferred, IO}
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.{AppStatus, AutodeleteThreshold, LeonardoTestSuite}
import org.broadinstitute.dsde.workbench.leonardo.db.{appQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.scalatestplus.mockito.MockitoSugar.mock

import java.time.temporal.ChronoUnit
class AutoDeleteAppMonitorSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {

  it should "auto delete the app when dateAccessed exceeds auto delete threshold" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val authProvider = mock[LeoAuthProvider[IO]]
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      now <- IO.realTimeInstant
      runningApp <- IO(
        makeApp(1, savedNodepool1.id)
          .copy(
            auditInfo = auditInfo.copy(dateAccessed = now.minus(5, ChronoUnit.MINUTES)),
            status = AppStatus.Running,
            autodeleteThreshold = Some(AutodeleteThreshold(1)),
            autodeleteEnabled = true
          )
          .save()
      )
      _ <- monitor(queue, authProvider)(3 seconds)
      status <- appQuery.getAppStatus(runningApp.id).transaction
      event <- queue.tryTake
    } yield {
      status.get shouldBe (AppStatus.Predeleting)
      event.get shouldBe a[LeoPubsubMessage.DeleteAppMessage]
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "not auto delete the app when dateAccessed within auto delete threshold" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val authProvider = mock[LeoAuthProvider[IO]]
    val res = for {
      queue <- Queue.bounded[IO, LeoPubsubMessage](10)
      now <- IO.realTimeInstant
      runningApp <- IO(
        makeApp(2, savedNodepool1.id)
          .copy(
            auditInfo = auditInfo.copy(dateAccessed = now.minus(5, ChronoUnit.MINUTES)),
            status = AppStatus.Running,
            autodeleteThreshold = Some(AutodeleteThreshold(6)),
            autodeleteEnabled = true
          )
          .save()
      )
      _ <- monitor(queue, authProvider)(3 seconds)
      status <- appQuery.getAppStatus(runningApp.id).transaction
      event <- queue.tryTake
    } yield {
      status.get shouldBe (AppStatus.Running)
      event shouldBe None
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  private def monitor(
    publisherQueue: Queue[IO, LeoPubsubMessage],
    authProvider: LeoAuthProvider[IO]
  )(waitDuration: FiniteDuration): IO[Unit] = {
    val monitorProcess = AutoDeleteAppMonitor.process[IO](autodeleteConfig, publisherQueue, authProvider)
    val process = Stream.eval(Deferred[IO, Unit]).flatMap { signalToStop =>
      val signal = Stream.sleep[IO](waitDuration).evalMap(_ => signalToStop.complete(())).void
      val p = Stream(monitorProcess.interruptWhen(signalToStop.get.attempt.map(_.map(_ => ()))), signal)
        .parJoin(3)
      p ++ Stream.eval(signalToStop.get)
    }
    process.compile.lastOrError
  }
}
