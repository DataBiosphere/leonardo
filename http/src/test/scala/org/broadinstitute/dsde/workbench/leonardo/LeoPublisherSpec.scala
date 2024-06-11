package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import cats.effect.{Deferred, IO}
import cats.mtl.Ask
import fs2.Pipe
import io.circe.Encoder
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  LeoPubsubMessage,
  RuntimeConfigInCreateRuntimeMessage,
  RuntimeToMonitor
}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.{FakeOpenTelemetryMetricsInterpreter, OpenTelemetryMetrics}
import org.broadinstitute.dsde.workbench.util2.messaging.CloudPublisher
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

class LeoPublisherSpec extends AnyFlatSpecLike with MockitoSugar with Matchers with BeforeAndAfterEach {
  implicit val logger: StructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit val ec: ExecutionContext = cats.effect.unsafe.IORuntime.global.compute
  implicit val dbRef: DbReference[IO] = createDbRefMock
  implicit val metrics: OpenTelemetryMetrics[IO] = FakeOpenTelemetryMetricsInterpreter

  "LeoPublisher" should "publish message with leonardo=true attribute" in {
    val cloudPublisher = new TestCloudPublisher()
    val publisherQueue = Queue.unbounded[IO, LeoPubsubMessage].unsafeRunSync()

    val leoPublisher = new LeoPublisher[IO](publisherQueue, cloudPublisher)
    val message: LeoPubsubMessage.CreateRuntimeMessage = createRuntimeMessage(None)

    val result = for {
      _ <- leoPublisher.process.compile.drain.start
      _ <- publisherQueue.offer(message)
      _ <- cloudPublisher.stopSignal.get.attempt
    } yield ()

    result.unsafeRunSync()

    cloudPublisher.messages.length shouldEqual 1
    val (_, capturedMap) = cloudPublisher.messages.head
    capturedMap should contain("leonardo" -> "true")
  }

  it should "publish message with traceId attribute" in {
    val cloudPublisher = new TestCloudPublisher()
    val publisherQueue = Queue.unbounded[IO, LeoPubsubMessage].unsafeRunSync()

    val leoPublisher = new LeoPublisher[IO](publisherQueue, cloudPublisher)
    val traceId = TraceId("1")
    val message: LeoPubsubMessage.CreateRuntimeMessage = createRuntimeMessage(Some(traceId))

    val result = for {
      _ <- leoPublisher.process.compile.drain.start
      _ <- publisherQueue.offer(message)
      _ <- cloudPublisher.stopSignal.get.attempt
    } yield ()

    result.unsafeRunSync()

    cloudPublisher.messages.length shouldEqual 1
    val (_, capturedMap) = cloudPublisher.messages.head
    capturedMap should contain("traceId" -> "1")
  }

  it should "publish message with traceId and leonardo=true attributes" in {
    val cloudPublisher = new TestCloudPublisher()
    val publisherQueue = Queue.unbounded[IO, LeoPubsubMessage].unsafeRunSync()

    val leoPublisher = new LeoPublisher[IO](publisherQueue, cloudPublisher)
    val traceId = TraceId("1")
    val message: LeoPubsubMessage.CreateRuntimeMessage = createRuntimeMessage(Some(traceId))

    val result = for {
      _ <- leoPublisher.process.compile.drain.start
      _ <- publisherQueue.offer(message)
      _ <- cloudPublisher.stopSignal.get.attempt
    } yield ()

    result.unsafeRunSync()

    cloudPublisher.messages.length shouldEqual 1
    val (_, capturedMap) = cloudPublisher.messages.head
    capturedMap should contain("traceId" -> "1")
    capturedMap should contain("leonardo" -> "true")
  }

  it should "publish multiple messages" in {
    val cloudPublisher = new TestCloudPublisher(2)
    val publisherQueue = Queue.unbounded[IO, LeoPubsubMessage].unsafeRunSync()

    val leoPublisher = new LeoPublisher[IO](publisherQueue, cloudPublisher)
    val traceId = TraceId("1")
    val message1: LeoPubsubMessage.CreateRuntimeMessage = createRuntimeMessage(Some(traceId))
    val message2: LeoPubsubMessage.CreateRuntimeMessage = createRuntimeMessage(Some(traceId))

    val result = for {
      _ <- leoPublisher.process.compile.drain.start
      _ <- publisherQueue.offer(message1)
      _ <- publisherQueue.offer(message2)
      _ <- cloudPublisher.stopSignal.get.attempt
    } yield ()

    result.unsafeRunSync()

    cloudPublisher.messages.length shouldEqual 2
  }

  private def createRuntimeMessage(traceId: Option[TraceId]) = {
    val message = LeoPubsubMessage.CreateRuntimeMessage(
      1,
      RuntimeProjectAndName(CloudContext.Gcp(GoogleProject("googleProject")), RuntimeName("runtimeName1")),
      WorkbenchEmail("email1"),
      None,
      AuditInfo(WorkbenchEmail("email1"), Instant.now, None, Instant.now),
      None,
      None,
      None,
      None,
      Set.empty,
      Set.empty,
      false,
      Map.empty,
      RuntimeConfigInCreateRuntimeMessage.GceConfig(MachineTypeName("n1-standard-4"),
                                                    DiskSize(50),
                                                    bootDiskSize = DiskSize(50),
                                                    zone = ZoneName("us-central1-a"),
                                                    None
      ),
      traceId,
      Some(1)
    )
    message
  }
  private def createDbRefMock: DbReference[IO] = {
    val dbRef = mock[DbReference[IO]]
    when(dbRef.inTransaction[Seq[RuntimeToMonitor]](any(), any())).thenReturn(IO(Seq.empty))
    dbRef
  }
}

class TestCloudPublisher(stopAtNumber: Int = 1) extends CloudPublisher[IO] {
  // Using a ListBuffer to store messages and attributes for assertion
  val messages: ListBuffer[(Any, Map[String, String])] = ListBuffer.empty
  val stopSignal = Deferred.unsafe[IO, Unit]
  // Stubs for other methods
  override def publish[MessageType: Encoder]: Pipe[IO, MessageType, Unit] = ???
  override def publishString: Pipe[IO, String, Unit] = ???

  // Capture messages and attributes passed to publishOne
  override def publishOne[MessageType: Encoder](message: MessageType,
                                                messageAttributes: Map[String, String] = Map.empty
  )(implicit
    ev: Ask[IO, TraceId]
  ): IO[Unit] = {
    messages.addOne((message, messageAttributes))
    // here we are signaling the end of processing, when the expected number of messages are received
    if (messages.length == stopAtNumber)
      stopSignal.complete().attempt.void
    else
      IO.unit
  }
}
