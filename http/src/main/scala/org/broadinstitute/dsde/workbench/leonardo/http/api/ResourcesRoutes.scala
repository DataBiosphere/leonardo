package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import cats.effect.IO
import cats.mtl.Ask
import io.opencensus.scala.akka.http.TracingDirective.traceRequestForService
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.leonardo.http.service.ResourcesService
import org.broadinstitute.dsde.workbench.leonardo.model.BadRequestException
import org.typelevel.log4cats.StructuredLogger

/**
 * Routes intended to be used to manages all types of resources handled by leonardo,
 * which are runtimes and apps
 */

class ResourcesRoutes(resourcesService: ResourcesService[IO], userInfoDirectives: UserInfoDirectives)(implicit
  metrics: OpenTelemetryMetrics[IO],
  logger: StructuredLogger[IO]
) {

  val routes: server.Route = traceRequestForService(serviceData) { span =>
    extractAppContext(Some(span)) { implicit ctx =>
      userInfoDirectives.requireUserInfo { userInfo =>
        CookieSupport.setTokenCookie(userInfo) {
          pathPrefix("google" / "v1" / "resources") {
            pathPrefix(googleProjectSegment) { googleProject =>
              path("deleteAll") {
                delete {
                  parameterMap { params =>
                    complete(
                      deleteAllResourcesForGoogleProjectHandler(
                        userInfo,
                        googleProject,
                        params
                      )
                    )
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def deleteAllResourcesForGoogleProjectHandler(userInfo: UserInfo,
                                                googleProject: GoogleProject,
                                                params: Map[String, String]
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      deleteInCloud = params.get("deleteInCloud").exists(_ == "true")
      deleteDisk = params.get("deleteDisk").exists(_ == "true")
      // We do not support both deleteInCloud AND deleteDisk flags to be set to false
      _ = if (deleteInCloud == false && deleteDisk == false)
        logger.info(
          s"Non supported combination of deleteInCloud and deleteDisk flags: ${(deleteInCloud, deleteDisk)}"
        ) >> IO.raiseError(
          BadRequestException(s"Invalid `deleteInCLoud` and `deleteDisk` ${(deleteInCloud, deleteDisk)}",
                              Some(ctx.traceId)
          )
        )
      apiCall = resourcesService.deleteAllResources(userInfo, googleProject, deleteInCloud, deleteDisk)
      tags = Map("deleteInCloud" -> deleteInCloud.toString, "deleteDisk" -> deleteDisk.toString)
      _ <- metrics.incrementCounter("deleteAllResources", 1, tags)
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "deleteAllResources")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable
}
