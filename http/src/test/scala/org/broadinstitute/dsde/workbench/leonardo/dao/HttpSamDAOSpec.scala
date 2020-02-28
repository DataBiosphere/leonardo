package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.{Blocker, IO}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.dsde.workbench.util.health.Subsystems.{GoogleGroups, GoogleIam, GooglePubSub, OpenDJ}
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.{HttpApp, Response, Status, Uri}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import io.circe.parser._
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProviderConfig
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import cats.implicits._

import scala.concurrent.duration._
import java.io.File
import java.util.UUID

import scala.concurrent.ExecutionContext.global
import cats.mtl.ApplicativeAsk
import org.http4s.client.middleware.{Retry, RetryPolicy}

import scala.util.control.NoStackTrace

class HttpSamDAOSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val timer = IO.timer(global)
  implicit val cs = IO.contextShift(global)
  val blocker = Blocker.liftExecutionContext(global)

  val config = HttpSamDaoConfig(Uri.unsafeFromString("localhost"),
                                false,
                                1 seconds,
                                10,
                                ServiceAccountProviderConfig(new File("test"), WorkbenchEmail("test")))
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]
  implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID())) //we don't care much about traceId in unit tests, hence providing a constant UUID here

  "HttpSamDAO" should "get Sam ok status" in {
    val okResponse =
      """
        |{
        |  "ok": true,
        |  "systems": {
        |    "GoogleGroups": {
        |      "ok": true
        |    },
        |    "GooglePubSub": {
        |      "ok": true
        |    },
        |    "GoogleIam": {
        |      "ok": true
        |    },
        |    "OpenDJ": {
        |      "ok": true
        |    }
        |  }
        |}
        |""".stripMargin

    val okSam = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(okResponse)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val samDao = new HttpSamDAO(okSam, config, blocker)
    val expectedResponse = StatusCheckResponse(
      true,
      Map(
        OpenDJ -> SubsystemStatus(true, None),
        GoogleIam -> SubsystemStatus(true, None),
        GoogleGroups -> SubsystemStatus(true, None),
        GooglePubSub -> SubsystemStatus(true, None)
      )
    )
    samDao.getStatus.unsafeRunSync() shouldBe expectedResponse
  }

  it should "get Sam ok status with no systems" in {
    val okResponse =
      """
        |{
        |  "ok": true,
        |  "systems": {
        |  }
        |}
        |""".stripMargin
    val okSam = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(okResponse)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val samDao = new HttpSamDAO(okSam, config, blocker)
    val expectedResponse = StatusCheckResponse(true, Map.empty)

    samDao.getStatus.unsafeRunSync() shouldBe expectedResponse
  }

  it should "get Sam unhealthy status with no systems" in {
    val response =
      """
        |{
        |  "ok": false,
        |  "systems": {
        |    "GoogleIam": {
        |      "ok": true
        |    },
        |    "OpenDJ": {
        |      "ok": false,
        |      "messages": ["OpenDJ is down. Panic!"]
        |    }
        |  }
        |}
        |""".stripMargin
    val okSam = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )

    val samDao = new HttpSamDAO(okSam, config, blocker)
    val expectedResponse =
      StatusCheckResponse(false,
                          Map(GoogleIam -> SubsystemStatus(true, None),
                              OpenDJ -> SubsystemStatus(false, Some(List("OpenDJ is down. Panic!")))))

    samDao.getStatus.unsafeRunSync() shouldBe expectedResponse
  }

  it should "throws exception once client times out" in {
    var hitCount = -1
    val retryPolicy = RetryPolicy[IO](RetryPolicy.exponentialBackoff(5 milliseconds, 4))
    val errorSam = Client.fromHttpApp[IO](
      HttpApp { _ =>
        IO(hitCount = hitCount + 1) >> IO.raiseError[Response[IO]](FakeException(s"retried ${hitCount + 1} times"))
      }
    )
    val clientWithRetry = Retry(retryPolicy)(errorSam)
    val samDao = new HttpSamDAO(clientWithRetry, config, blocker)

    val res = for {
      result <- samDao.getStatus.attempt
    } yield {
      result shouldBe Left(FakeException("retried 5 times"))
    }

    res.unsafeRunSync()
  }
}

final case class FakeException(msg: String) extends NoStackTrace {
  override def getMessage: String = msg
}
