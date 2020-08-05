package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.IO
import cats.Eq
import cats.implicits._
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.{LeoLenses, LeonardoTestSuite, RuntimeStatus}
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.scalatest.flatspec.AnyFlatSpec
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.scalatest.Assertions

import scala.concurrent.ExecutionContext.Implicits.global

class MonitorAtBootSpec extends AnyFlatSpec with TestComponent with LeonardoTestSuite {
  implicit val msgEq: Eq[LeoPubsubMessage] =
    Eq.instance[LeoPubsubMessage]((x, y) =>
      (x, y) match {
        case (xx: LeoPubsubMessage.StopRuntimeMessage, yy: LeoPubsubMessage.StopRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.DeleteRuntimeMessage, yy: LeoPubsubMessage.DeleteRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.CreateRuntimeMessage, yy: LeoPubsubMessage.CreateRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx: LeoPubsubMessage.StartRuntimeMessage, yy: LeoPubsubMessage.StartRuntimeMessage) =>
          xx.copy(traceId = None) == yy.copy(traceId = None)
        case (xx, yy) =>
          Assertions.fail(s"unexpected messages ${xx}, ${yy}", null)
      }
    )

  it should "recover RuntimeStatus.Stopping properly" in isolatedDbTest {
    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Stopping).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryDequeue1
    } yield {
      (msg eqv Some(LeoPubsubMessage.StopRuntimeMessage(runtime.id, None))) shouldBe (true)
    }
    res.unsafeRunSync()
  }

  it should "recover RuntimeStatus.Deleting properly" in isolatedDbTest {
    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Deleting).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryDequeue1
    } yield {
      (msg eqv Some(LeoPubsubMessage.DeleteRuntimeMessage(runtime.id, None, None))) shouldBe (true)
    }
    res.unsafeRunSync()
  }

  it should "recover RuntimeStatus.Starting properly" in isolatedDbTest {
    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Starting).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryDequeue1
    } yield {
      (msg eqv Some(LeoPubsubMessage.StartRuntimeMessage(runtime.id, None))) shouldBe (true)
    }
    res.unsafeRunSync()
  }

  it should "recover RuntimeStatus.Creating properly" in isolatedDbTest {
    val res = for {
      queue <- InspectableQueue.bounded[IO, LeoPubsubMessage](10)
      monitorAtBoot = createMonitorAtBoot(queue)
      runtime <- IO(makeCluster(0).copy(status = RuntimeStatus.Creating).save())
      _ <- monitorAtBoot.process.take(1).compile.drain
      msg <- queue.tryDequeue1
    } yield {
      val runtimeConfigInCreateRuntimeMessage = LeoLenses.runtimeConfigPrism.getOption(defaultDataprocRuntimeConfig).get
      (msg eqv Some(
        LeoPubsubMessage.CreateRuntimeMessage.fromRuntime(
          runtime,
          runtimeConfigInCreateRuntimeMessage,
          None
        )
      )) shouldBe (true)
    }
    res.unsafeRunSync()
  }

  def createMonitorAtBoot(
    queue: InspectableQueue[IO, LeoPubsubMessage] = InspectableQueue.bounded[IO, LeoPubsubMessage](10).unsafeRunSync
  ): MonitorAtBoot[IO] =
    new MonitorAtBoot[IO](queue)
}
