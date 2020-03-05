package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import _root_.java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import cats.effect.{IO, Timer}
import cats.mtl.ApplicativeAsk
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutes.validateRuntimeNameDirective
import org.broadinstitute.dsde.workbench.leonardo.http.service.{RuntimeConfigRequest, RuntimeService}
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}
import RuntimeRoutes._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import LeoRoutesJsonCodec.dataprocConfigDecoder
import JsonCodec._
import scala.concurrent.duration._

class RuntimeRoutes(runtimeService: RuntimeService[IO], userInfoDirectives: UserInfoDirectives)(
  implicit timer: Timer[IO]
) {
  val routes: server.Route = userInfoDirectives.requireUserInfo { userInfo =>
    implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))
    // https://doc.akka.io//docs/akka-http/current/routing-dsl/path-matchers.html?language=scala#examples
    pathPrefix("google" / "v1" / "runtime") {
      pathPrefix(googleProjectSegment / Segment) { (googleProject, runtimeNameString) =>
        validateRuntimeNameDirective(runtimeNameString) { runtimeName =>
          pathEndOrSingleSlash {
            post {
              entity(as[CreateRuntime2Request]) { req =>
                complete(
                  createRuntimeHandler(
                    userInfo,
                    googleProject,
                    runtimeName,
                    req
                  )
                )
              }
            }
          }
        }
      }
    }
  }

  private[api] def createRuntimeHandler(userInfo: UserInfo,
                                        googleProject: GoogleProject,
                                        runtimeName: RuntimeName, //TODO: rename this to RuntimeName once Rob's PR is in
                                        req: CreateRuntime2Request): IO[ToResponseMarshallable] =
    for {
      traceId <- IO(UUID.randomUUID())
      implicit0(context: ApplicativeAsk[IO, RuntimeServiceContext]) <- timer.clock
        .realTime(TimeUnit.MILLISECONDS)
        .map(
          n =>
            ApplicativeAsk
              .const[IO, RuntimeServiceContext](RuntimeServiceContext(TraceId(traceId), Instant.ofEpochMilli(n)))
        )
      _ <- runtimeService.createRuntime(
        userInfo,
        googleProject,
        runtimeName,
        req
      )
    } yield StatusCodes.Accepted
}

object RuntimeRoutes {
  implicit val gceConfigDecoder: Decoder[RuntimeConfigRequest.GceConfig] = Decoder.forProduct2(
    "machineType",
    "diskSize"
  )((mt, ds) => RuntimeConfigRequest.GceConfig(mt, ds))

  implicit val runtimeConfigDecoder: Decoder[RuntimeConfigRequest] = Decoder.instance { x =>
    //For newer version of requests, we use `cloudService` field to distinguish whether user is
    for {
      cloudService <- x.downField("cloudService").as[CloudService]
      r <- cloudService match {
        case CloudService.Dataproc =>
          x.as[RuntimeConfigRequest.DataprocConfig]
        case CloudService.GCE =>
          x.as[RuntimeConfigRequest.GceConfig]
      }
    } yield r
  }

  implicit val createRuntimeRequestDecoder: Decoder[CreateRuntime2Request] = Decoder.instance { c =>
    for {
      l <- c.downField("labels").as[Option[LabelMap]]
      je <- c.downField("jupyterExtensionUri").as[Option[GcsPath]]
      jus <- c.downField("jupyterUserScriptUri").as[Option[UserScriptPath]]
      jsus <- c.downField("jupyterStartUserScriptUri").as[Option[UserScriptPath]]
      rc <- c.downField("runtimeConfig").as[Option[RuntimeConfigRequest]]
      p <- c.downField("properties").as[Option[LabelMap]]
      uje <- c.downField("userJupyterExtensionConfig").as[Option[UserJupyterExtensionConfig]]
      a <- c.downField("autopause").as[Option[Boolean]]
      apt <- c.downField("autopauseThreshold").as[Option[Int]]
      dc <- c.downField("defaultClientId").as[Option[String]]
      tdi <- c.downField("toolDockerImage").as[Option[ContainerImage]]
      wdi <- c.downField("welderDockerImage").as[Option[ContainerImage]]
      s <- c.downField("scopes").as[Option[Set[String]]]
      c <- c.downField("customClusterEnvironmentVariables").as[Option[LabelMap]]
    } yield CreateRuntime2Request(
      l.getOrElse(Map.empty),
      je,
      jus,
      jsus,
      rc,
      p.getOrElse(Map.empty),
      uje,
      a,
      apt.map(_.minute),
      dc,
      tdi,
      wdi,
      s.getOrElse(Set.empty),
      c.getOrElse(Map.empty)
    )
  }
}

final case class RuntimeServiceContext(traceId: TraceId, now: Instant)

final case class CreateRuntime2Request(labels: LabelMap,
                                       jupyterExtensionUri: Option[GcsPath],
                                       jupyterUserScriptUri: Option[UserScriptPath],
                                       jupyterStartUserScriptUri: Option[UserScriptPath],
                                       runtimeConfig: Option[RuntimeConfigRequest],
                                       properties: Map[String, String],
                                       userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                       autopause: Option[Boolean],
                                       autopauseThreshold: Option[FiniteDuration],
                                       defaultClientId: Option[String],
                                       toolDockerImage: Option[ContainerImage],
                                       welderDockerImage: Option[ContainerImage],
                                       scopes: Set[String],
                                       customClusterEnvironmentVariables: Map[String, String])
