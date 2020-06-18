package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant

import cats.data.Chain
import cats.effect.IO
import cats.effect.concurrent.Deferred
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.monitor.DateAccessedUpdater._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.config.Config

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.scalatest.flatspec.AnyFlatSpec

class DateAccessedUpdaterSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {
  it should "sort UpdateDateAccessMessage properly" in {
    val msg1 = UpdateDateAccessMessage(RuntimeName("r1"), GoogleProject("p1"), Instant.ofEpochMilli(1588264615480L))
    val msg2 = UpdateDateAccessMessage(RuntimeName("r1"), GoogleProject("p1"), Instant.ofEpochMilli(1588264615490L))
    val msg3 = UpdateDateAccessMessage(RuntimeName("r2"), GoogleProject("p1"), Instant.ofEpochMilli(1588264615480L))
    val msg4 = UpdateDateAccessMessage(RuntimeName("r1"), GoogleProject("p2"), Instant.ofEpochMilli(1588264615480L))

    val messages = Chain(
      msg1,
      msg2,
      msg3,
      msg4
    )

    val expectedResult = List(
      msg2,
      msg3,
      msg4
    )

    messagesToUpdate(messages) should contain theSameElementsAs (expectedResult)
  }

  it should "update date accessed" in isolatedDbTest {
    val runtime1 = makeCluster(1).save()
    val runtime2 = makeCluster(2).save()

    val messagesToEnqueue = Stream(
      UpdateDateAccessMessage(runtime1.runtimeName, runtime1.googleProject, Instant.ofEpochMilli(1588264615480L)),
      UpdateDateAccessMessage(runtime1.runtimeName, runtime1.googleProject, Instant.ofEpochMilli(1588264615490L)),
      UpdateDateAccessMessage(runtime2.runtimeName, runtime2.googleProject, Instant.ofEpochMilli(1588264615480L))
    ).covary[IO]

    for {
      queue <- InspectableQueue.bounded[IO, UpdateDateAccessMessage](10)
      _ <- (messagesToEnqueue through queue.enqueue).compile.drain
      _ <- monitor(queue)(5 seconds)
      updatedRuntime1 <- clusterQuery.getClusterById(runtime1.id).transaction
      updatedRuntime2 <- clusterQuery.getClusterById(runtime2.id).transaction
    } yield {
      updatedRuntime1.get.auditInfo.dateAccessed.toEpochMilli shouldBe (1588264615490L)
      updatedRuntime2.get.auditInfo.dateAccessed.toEpochMilli shouldBe (1588264615480L)
    }
  }

  private def monitor(
    queue: InspectableQueue[IO, UpdateDateAccessMessage]
  )(waitDuration: FiniteDuration): IO[Unit] = {
    val monitor = new DateAccessedUpdater[IO](Config.dateAccessUpdaterConfig, queue)
    val process = Stream.eval(Deferred[IO, Unit]).flatMap { signalToStop =>
      val signal = Stream.sleep(waitDuration).evalMap(_ => signalToStop.complete(()))
      val p = Stream(monitor.process.interruptWhen(signalToStop.get.attempt.map(_.map(_ => ()))), signal)
        .parJoin(2)
      p ++ Stream.eval(signalToStop.get)
    }
    process.compile.lastOrError
  }
}
