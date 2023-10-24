package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.data.Chain
import cats.effect.{Deferred, IO}
import cats.effect.std.Queue
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.{clusterQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.DateAccessedUpdater._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.flatspec.AnyFlatSpec

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DateAccessedUpdaterSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {
  it should "sort UpdateDateAccessedMessage properly" in {
    val msg1 = UpdateDateAccessedMessage(UpdateTarget.Runtime(RuntimeName("r1")),
                                         CloudContext.Gcp(GoogleProject("p1")),
                                         Instant.ofEpochMilli(1588264615480L)
    )
    val msg2 = UpdateDateAccessedMessage(UpdateTarget.Runtime(RuntimeName("r1")),
                                         CloudContext.Gcp(GoogleProject("p1")),
                                         Instant.ofEpochMilli(1588264615490L)
    )
    val msg3 = UpdateDateAccessedMessage(UpdateTarget.Runtime(RuntimeName("r2")),
                                         CloudContext.Gcp(GoogleProject("p1")),
                                         Instant.ofEpochMilli(1588264615480L)
    )
    val msg4 = UpdateDateAccessedMessage(UpdateTarget.Runtime(RuntimeName("r1")),
                                         CloudContext.Gcp(GoogleProject("p2")),
                                         Instant.ofEpochMilli(1588264615480L)
    )

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

    messagesToUpdate(messages) should contain theSameElementsAs expectedResult
  }

  it should "update date accessed" in isolatedDbTest {
    val runtime1 = makeCluster(1).save()
    val runtime2 = makeCluster(2).save()

    val messagesToEnqueue = Stream(
      UpdateDateAccessedMessage(UpdateTarget.Runtime(runtime1.runtimeName),
                                runtime1.cloudContext,
                                Instant.ofEpochMilli(1588264615480L)
      ),
      UpdateDateAccessedMessage(UpdateTarget.Runtime(runtime1.runtimeName),
                                runtime1.cloudContext,
                                Instant.ofEpochMilli(1588264615490L)
      ),
      UpdateDateAccessedMessage(UpdateTarget.Runtime(runtime2.runtimeName),
                                runtime2.cloudContext,
                                Instant.ofEpochMilli(1588264615480L)
      )
    ).covary[IO]

    for {
      queue <- Queue.bounded[IO, UpdateDateAccessedMessage](10)
      _ <- (messagesToEnqueue through (in => in.evalMap(m => queue.offer(m)))).compile.drain
      _ <- monitor(queue)(5 seconds)
      updatedRuntime1 <- clusterQuery.getClusterById(runtime1.id).transaction
      updatedRuntime2 <- clusterQuery.getClusterById(runtime2.id).transaction
    } yield {
      updatedRuntime1.get.auditInfo.dateAccessed.toEpochMilli shouldBe 1588264615490L
      updatedRuntime2.get.auditInfo.dateAccessed.toEpochMilli shouldBe 1588264615480L
    }
  }

  private def monitor(
    queue: Queue[IO, UpdateDateAccessedMessage]
  )(waitDuration: FiniteDuration): IO[Unit] = {
    val monitor = new DateAccessedUpdater[IO](Config.dateAccessUpdaterConfig, queue)
    val process = Stream.eval(Deferred[IO, Unit]).flatMap { signalToStop =>
      val signal = Stream.sleep[IO](waitDuration).evalMap(_ => signalToStop.complete(())).void
      val p = Stream(monitor.process.interruptWhen(signalToStop.get.attempt.map(_.map(_ => ()))), signal)
        .parJoin(2)
      p ++ Stream.eval(signalToStop.get)
    }
    process.compile.lastOrError
  }
}
