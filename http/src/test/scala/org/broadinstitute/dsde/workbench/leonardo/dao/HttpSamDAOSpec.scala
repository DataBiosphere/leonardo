package org.broadinstitute.dsde.workbench.leonardo
package dao

import java.nio.file.Paths
import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.circe.parser._
import org.broadinstitute.dsde.workbench.leonardo.model.ServiceAccountProviderConfig
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.util.health.Subsystems.{GoogleGroups, GoogleIam, GooglePubSub, OpenDJ}
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus}
import org.http4s._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.client.middleware.{Retry, RetryPolicy}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class HttpSamDAOSpec extends AnyFlatSpec with LeonardoTestSuite with BeforeAndAfterAll {
  val config = HttpSamDaoConfig(Uri.unsafeFromString("localhost"),
                                false,
                                1 seconds,
                                10,
                                ServiceAccountProviderConfig(Paths.get("test"), WorkbenchEmail("test")))
  implicit def unsafeLogger = Slf4jLogger.getLogger[IO]

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

    val res = Dispatcher[IO].use { d =>
      val samDao = new HttpSamDAO(okSam, config, d)
      val expectedResponse = StatusCheckResponse(
        true,
        Map(
          OpenDJ -> SubsystemStatus(true, None),
          GoogleIam -> SubsystemStatus(true, None),
          GoogleGroups -> SubsystemStatus(true, None),
          GooglePubSub -> SubsystemStatus(true, None)
        )
      )
      samDao.getStatus.map(s => s shouldBe expectedResponse)
    }

    res.unsafeRunSync
  }

  it should "get Sam ok status with no systems" in {
    val res = Dispatcher[IO].use { d =>
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

      val samDao = new HttpSamDAO(okSam, config, d)
      val expectedResponse = StatusCheckResponse(true, Map.empty)

      samDao.getStatus.map(s => s shouldBe expectedResponse)
    }

    res.unsafeRunSync

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

    val res = Dispatcher[IO].use { d =>
      val samDao = new HttpSamDAO(okSam, config, d)
      val expectedResponse =
        StatusCheckResponse(false,
                            Map(GoogleIam -> SubsystemStatus(true, None),
                                OpenDJ -> SubsystemStatus(false, Some(List("OpenDJ is down. Panic!")))))

      samDao.getStatus.map(s => s shouldBe expectedResponse)
    }

    res.unsafeRunSync

  }

  it should "throws exception once client times out" in {
    var hitCount = -1
    val retryPolicy = RetryPolicy[IO](RetryPolicy.exponentialBackoff(5 milliseconds, 4))
    val errorSam = Client.fromHttpApp[IO](
      HttpApp { _ =>
        IO { hitCount = hitCount + 1 } >> IO.raiseError[Response[IO]](FakeException(s"retried ${hitCount + 1} times"))
      }
    )
    val clientWithRetry = Retry(retryPolicy)(errorSam)

    val res = Dispatcher[IO].use { d =>
      val samDao = new HttpSamDAO(clientWithRetry, config, d)

      for {
        result <- samDao.getStatus.attempt
      } yield {
        result shouldBe Left(FakeException("retried 5 times"))
      }
    }

    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }
}

final case class FakeException(msg: String) extends NoStackTrace {
  override def getMessage: String = msg
}
