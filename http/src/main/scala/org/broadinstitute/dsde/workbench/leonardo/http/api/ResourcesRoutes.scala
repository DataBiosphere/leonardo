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

/**
 * Routes intended to be used to manages all types of resources handled by leonardo,
 * which are runtimes and apps
 */

class ResourcesRoutes(resourcesService: ResourcesService[IO], userInfoDirectives: UserInfoDirectives)(implicit
  metrics: OpenTelemetryMetrics[IO]
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
          } ~
            pathPrefix(googleProjectSegment) { googleProject =>
              path("cleanupAll") {
                delete {
                  parameterMap { params =>
                    complete(
                      cleanupAllResourcesForGoogleProjectHandler(
                        userInfo,
                        googleProject
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

  def deleteAllResourcesForGoogleProjectHandler(userInfo: UserInfo,
                                                googleProject: GoogleProject,
                                                params: Map[String, String]
  )(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      deleteDisk = params.get("deleteDisk").exists(_ == "true")
      cloudContext = CloudContext.Gcp(googleProject)
      apiCall = resourcesService.deleteAllResourcesInCloud(userInfo, cloudContext, deleteDisk)
      tags = Map("deleteDisk" -> deleteDisk.toString)
      _ <- metrics.incrementCounter("deleteAllResources", 1, tags)
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "deleteAllResources")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable

  def cleanupAllResourcesForGoogleProjectHandler(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    ev: Ask[IO, AppContext]
  ): IO[ToResponseMarshallable] =
    for {
      ctx <- ev.ask[AppContext]
      cloudContext = CloudContext.Gcp(googleProject)
      apiCall = resourcesService.deleteAllResourcesRecords(userInfo, cloudContext)
      _ <- metrics.incrementCounter("cleanupAllResources", 1)
      _ <- ctx.span.fold(apiCall)(span =>
        spanResource[IO](span, "cleanupAllResources")
          .use(_ => apiCall)
      )
    } yield StatusCodes.Accepted: ToResponseMarshallable
}
