package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.Materializer
import cats.effect.{ContextShift, IO, Timer}
import cats.mtl.Ask
import com.typesafe.scalalogging.LazyLogging
import LeoRoutesJsonCodec._
import LeoRoutesSprayJsonCodec._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import org.broadinstitute.dsde.workbench.leonardo.http.api.RouteValidation._
import org.broadinstitute.dsde.workbench.leonardo.api.CookieSupport
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateRuntimeRequest, LeonardoService}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace
case class AuthenticationError(email: Option[WorkbenchEmail] = None)
    extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is not authenticated",
                         StatusCodes.Unauthorized)
    with NoStackTrace

// TODO: This can probably renamed to legacyRuntimeRoutes
// Future runtime related APIs should be added to `RuntimeRoutes`
class LeoRoutesh(
  val leonardoService: LeonardoService,
  userInfoDirectives: UserInfoDirectives
)(implicit val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext,
  val cs: ContextShift[IO],
  timer: Timer[IO])
    extends LazyLogging {

  import io.opencensus.scala.akka.http.TracingDirective._

  val route: Route =
    userInfoDirectives.requireUserInfo { userInfo =>
      implicit val traceId = Ask.const[IO, TraceId](TraceId(UUID.randomUUID()))

      CookieSupport.setTokenCookie(userInfo, CookieSupport.tokenCookieName) {
        pathPrefix("cluster") {
          pathPrefix("v2" / Segment / Segment) { (googleProject, clusterNameString) =>
            validateNameDirective(clusterNameString, RuntimeName.apply) { clusterName =>
              pathEndOrSingleSlash {
                put {
                  entity(as[CreateRuntimeRequest]) { cluster =>
                    complete {
                      leonardoService
                        .createCluster(userInfo, GoogleProject(googleProject), clusterName, cluster)
                        .map(cluster => StatusCodes.Accepted -> cluster)
                    }
                  }
                }
              }
            }
          } ~
            pathPrefix(Segment / Segment) { (googleProject, clusterNameString) =>
              validateNameDirective(clusterNameString, RuntimeName.apply) { clusterName =>
                pathEndOrSingleSlash {
                  put {
                    entity(as[CreateRuntimeRequest]) { cluster =>
                      complete {
                        leonardoService
                          .createCluster(userInfo, GoogleProject(googleProject), clusterName, cluster)
                          .map(cluster => StatusCodes.OK -> cluster)
                      }
                    }
                  } ~
                    get {
                      complete {
                        leonardoService
                          .getClusterAPI(userInfo, GoogleProject(googleProject), clusterName)
                          .map(clusterDetails => StatusCodes.OK -> clusterDetails)
                      }
                    } ~
                    delete {
                      complete {
                        leonardoService
                          .deleteCluster(userInfo, GoogleProject(googleProject), clusterName)
                          .as(StatusCodes.Accepted)
                      }
                    }
                } ~
                  path("stop") {
                    traceRequestForService(serviceData) { span => // Use `LABEL:service.name:leonardo` to find the span on stackdriver console
                      post {
                        complete {
                          for {
                            implicit0(ctx: Ask[IO, AppContext]) <- AppContext.lift[IO](Some(span))
                            res <- leonardoService
                              .stopCluster(userInfo, GoogleProject(googleProject), clusterName)
                              .as(StatusCodes.Accepted)
                            _ <- IO(span.end())
                          } yield res
                        }
                      }
                    }
                  } ~
                  path("start") {
                    traceRequestForService(serviceData) { span =>
                      post {
                        complete {
                          for {
                            implicit0(ctx: Ask[IO, AppContext]) <- AppContext.lift[IO](Some(span))
                            res <- leonardoService
                              .startCluster(userInfo, GoogleProject(googleProject), clusterName)
                              .as(StatusCodes.Accepted)
                            _ <- IO(span.end())
                          } yield res
                        }
                      }
                    }
                  }
              }
            }
        } ~
          pathPrefix("clusters") {
            parameterMap { params =>
              path(Segment) { googleProject =>
                get {
                  complete {
                    leonardoService
                      .listClusters(userInfo, params, Some(GoogleProject(googleProject)))
                      .map(clusters => StatusCodes.OK -> clusters)
                  }
                }
              } ~
                pathEndOrSingleSlash {
                  get {
                    complete {
                      leonardoService
                        .listClusters(userInfo, params)
                        .map(clusters => StatusCodes.OK -> clusters)
                    }
                  }
                }
            }
          }
      }
    }
}
