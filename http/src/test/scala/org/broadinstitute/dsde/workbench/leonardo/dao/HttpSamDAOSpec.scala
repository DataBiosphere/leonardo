package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import com.github.benmanes.caffeine.cache.Caffeine
import io.circe.Json
import io.circe.parser._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.WorkspaceResourceSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config.httpSamDaoConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO.listResourceResponseDecoder
import org.broadinstitute.dsde.workbench.leonardo.http.ctxConversion
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
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalacache.caffeine.CaffeineCache

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class HttpSamDAOSpec extends AnyFlatSpec with LeonardoTestSuite with BeforeAndAfterAll {
  val config = HttpSamDaoConfig(Uri.unsafeFromString("localhost"),
                                false,
                                1 seconds,
                                10,
                                ServiceAccountProviderConfig(Paths.get("test"), WorkbenchEmail("test"))
  )
  implicit def unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]
  val underlyingPetTokenCache = Caffeine
    .newBuilder()
    .maximumSize(httpSamDaoConfig.petCacheMaxSize)
    .build[UserEmailAndProject, scalacache.Entry[Option[Json]]]()
  val petTokenCache = CaffeineCache[IO, UserEmailAndProject, Option[Json]](underlyingPetTokenCache)

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
      val samDao = new HttpSamDAO(okSam, config, petTokenCache)
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

    val samDao = new HttpSamDAO(okSam, config, petTokenCache)
    val expectedResponse = StatusCheckResponse(true, Map.empty)

    samDao.getStatus.map(s => s shouldBe expectedResponse)
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

    val samDao = new HttpSamDAO(okSam, config, petTokenCache)
    val expectedResponse =
      StatusCheckResponse(false,
                          Map(GoogleIam -> SubsystemStatus(true, None),
                              OpenDJ -> SubsystemStatus(false, Some(List("OpenDJ is down. Panic!")))
                          )
      )

    val res = samDao.getStatus.map(s => s shouldBe expectedResponse)

    res.unsafeRunSync

  }

  it should "identify when a user is NOT an admin" in {
    val response =
      """
        |{
        |  "causes": [],
        |  "message": "You must be an admin.",
        |  "source": "sam",
        |  "stackTrace": [],
        |  "statusCode": 403
        |}
        |""".stripMargin

    val noAdminSam = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Forbidden).withEntity(r))))
    )
    val samDao = new HttpSamDAO(noAdminSam, config, petTokenCache)
    val res = samDao.isAdminUser(CommonTestData.userInfo).map(s => s shouldBe false)

    res.unsafeRunSync
  }

  it should "identify when a user is an admin" in {
    val response =
      """
        |{
        |  "enabled": {
        |    "tosAccepted": true,
        |    "google": true,
        |    "ldap": true,
        |    "allUsersGroup": true,
        |    "adminEnabled": true
        |  },
        |  "userInfo": {
        |    "userEmail": "someone@broadinstitute.org",
        |    "userSubjectId": "abc123"
        |  }
        |}
        |""".stripMargin

    val yesAdminSam = Client.fromHttpApp[IO](
      HttpApp(_ => IO.fromEither(parse(response)).flatMap(r => IO(Response(status = Status.Ok).withEntity(r))))
    )
    val samDao = new HttpSamDAO(yesAdminSam, config, petTokenCache)
    val res = samDao.isAdminUser(CommonTestData.userInfo).map(s => s shouldBe true)

    res.unsafeRunSync
  }

  it should "throw an error when we can't tell whether the user is an admin" in {
    val errorSam = Client.fromHttpApp[IO](
      HttpApp(_ => IO(Response(status = Status.NotFound)))
    )
    val samDao = new HttpSamDAO(errorSam, config, petTokenCache)
    val res = samDao.isAdminUser(CommonTestData.userInfo)

    assertThrows[AuthProviderException](res.unsafeRunSync)
  }

  it should "decode ListResourceResponse properly" in {
    val response =
      """
        |{
        |    "authDomainGroups":
        |    [],
        |    "direct":
        |    {
        |        "actions":
        |        [],
        |        "roles":
        |        [
        |            "project-owner",
        |            "owner"
        |        ]
        |    },
        |    "inherited":
        |    {
        |        "actions":
        |        [],
        |        "roles":
        |        []
        |    },
        |    "missingAuthDomainGroups":
        |    [],
        |    "public":
        |    {
        |        "actions":
        |        [],
        |        "roles":
        |        []
        |    },
        |    "resourceId": "cea587e9-9a8e-45b6-b985-9e3803754020"
        |}
        |""".stripMargin

    import org.broadinstitute.dsde.workbench.leonardo.JsonCodec.workspaceSamResourceIdDecoder
    val decodedResp = decode[ListResourceResponse[WorkspaceResourceSamResourceId]](response)
    val expected = ListResourceResponse(
      WorkspaceResourceSamResourceId(WorkspaceId(UUID.fromString("cea587e9-9a8e-45b6-b985-9e3803754020"))),
      Set(
        SamPolicyName.Owner,
        SamPolicyName.Other("project-owner")
      )
    )

    decodedResp shouldBe Right(expected)
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
      val samDao = new HttpSamDAO(clientWithRetry, config, petTokenCache)

      for {
        result <- samDao.getStatus.attempt
      } yield result shouldBe Left(FakeException("retried 5 times"))
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}

final case class FakeException(msg: String) extends NoStackTrace {
  override def getMessage: String = msg
}
