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
import cats.effect.{ContextShift, IO}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.typesafe.scalalogging.LazyLogging
import LeoRoutesJsonCodec._
import LeoRoutesSprayJsonCodec._
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import org.broadinstitute.dsde.workbench.leonardo.http.api.LeoRoutes._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{CreateRuntimeRequest, LeonardoService}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, RequestValidationError}
import org.broadinstitute.dsde.workbench.leonardo.util.CookieHelper
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext
case class AuthenticationError(email: Option[WorkbenchEmail] = None)
    extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is not authenticated",
                         StatusCodes.Unauthorized)

// TODO: This can probably renamed to legacyRuntimeRoutes
// Future runtime related APIs should be added to `RuntimeRoutes`
class LeoRoutes(
  val leonardoService: LeonardoService,
  userInfoDirectives: UserInfoDirectives
)(implicit val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext,
  val cs: ContextShift[IO])
    extends LazyLogging
    with CookieHelper {

  val route: Route =
    userInfoDirectives.requireUserInfo { userInfo =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))

      setTokenCookie(userInfo, tokenCookieName) {
        pathPrefix("cluster") {
          pathPrefix("v2" / Segment / Segment) { (googleProject, clusterNameString) =>
            validateRuntimeNameDirective(clusterNameString) { clusterName =>
              pathEndOrSingleSlash {
                put {
                  entity(as[CreateRuntimeRequest]) { cluster =>
                    complete {
                      leonardoService
                        .createCluster(userInfo, GoogleProject(googleProject), clusterName, cluster)
                        .map { cluster =>
                          StatusCodes.Accepted -> cluster
                        }
                    }
                  }
                }
              }
            }
          } ~
            pathPrefix(Segment / Segment) { (googleProject, clusterNameString) =>
              validateRuntimeNameDirective(clusterNameString) { clusterName =>
                pathEndOrSingleSlash {
                  patch {
                    entity(as[CreateRuntimeRequest]) { cluster =>
                      complete {
                        leonardoService
                          .updateCluster(userInfo, GoogleProject(googleProject), clusterName, cluster)
                          .map { cluster =>
                            StatusCodes.Accepted -> cluster
                          }
                      }
                    }
                  } ~
                    put {
                      entity(as[CreateRuntimeRequest]) { cluster =>
                        complete {
                          leonardoService
                            .createCluster(userInfo, GoogleProject(googleProject), clusterName, cluster)
                            .map { cluster =>
                              StatusCodes.OK -> cluster
                            }
                        }
                      }
                    } ~
                    get {
                      complete {
                        leonardoService
                          .getClusterAPI(userInfo, GoogleProject(googleProject), clusterName)
                          .map { clusterDetails =>
                            StatusCodes.OK -> clusterDetails
                          }
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
                    post {
                      complete {
                        leonardoService
                          .stopCluster(userInfo, GoogleProject(googleProject), clusterName)
                          .as(StatusCodes.Accepted)
                      }
                    }
                  } ~
                  path("start") {
                    post {
                      complete {
                        leonardoService
                          .startCluster(userInfo, GoogleProject(googleProject), clusterName)
                          .as(StatusCodes.Accepted)
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
                      .map { clusters =>
                        StatusCodes.OK -> clusters
                      }
                  }
                }
              } ~
                pathEndOrSingleSlash {
                  get {
                    complete {
                      leonardoService
                        .listClusters(userInfo, params)
                        .map { clusters =>
                          StatusCodes.OK -> clusters
                        }
                    }
                  }
                }
            }
          }
      }
    }
}

object LeoRoutes {
  private val clusterNameReg = "([a-z|0-9|-])*".r
  private def validateRuntimeName(clusterNameString: String): Either[Throwable, RuntimeName] =
    clusterNameString match {
      case clusterNameReg(_) => Right(RuntimeName(clusterNameString))
      case _ =>
        Left(
          RequestValidationError(
            s"invalid cluster name ${clusterNameString}. Only lowercase alphanumeric characters, numbers and dashes are allowed in cluster name"
          )
        )
    }

  def validateRuntimeNameDirective(clusterNameString: String): Directive1[RuntimeName] =
    Directive { inner =>
      validateRuntimeName(clusterNameString) match {
        case Left(e)  => failWith(e)
        case Right(c) => inner(Tuple1(c))
      }
    }
}
