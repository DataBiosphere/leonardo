package org.broadinstitute.dsde.workbench.leonardo
package api

import java.util.UUID

import akka.actor.ActorSystem
import akka.event.Logging.LogLevel
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry, LoggingMagnet}
import akka.http.scaladsl.server._
import akka.stream.Materializer
import akka.stream.scaladsl._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.SwaggerConfig
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoServiceJsonCodec.listClusterResponseWriter
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, LeoException, RequestValidationError}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.service.{
  LeonardoService,
  ProxyService,
  StatusService
}
import org.broadinstitute.dsde.workbench.leonardo.util.CookieHelper
import org.broadinstitute.dsde.workbench.model.ErrorReportJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{
  ErrorReport,
  TraceId,
  WorkbenchEmail,
  WorkbenchException,
  WorkbenchExceptionWithErrorReport
}
import LeoRoutes._
import cats.effect.IO
import cats.mtl.ApplicativeAsk

import scala.concurrent.{ExecutionContext, Future}

case class AuthenticationError(email: Option[WorkbenchEmail] = None)
    extends LeoException(s"${email.map(e => s"'${e.value}'").getOrElse("Your account")} is not authenticated",
                         StatusCodes.Unauthorized)

abstract class LeoRoutes(
  val leonardoService: LeonardoService,
  val proxyService: ProxyService,
  val statusService: StatusService,
  val swaggerConfig: SwaggerConfig
)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext)
    extends LazyLogging
    with CookieHelper
    with ProxyRoutes
    with SwaggerRoutes
    with StatusRoutes
    with UserInfoDirectives {

  def unauthedRoutes: Route =
    path("ping") {
      pathEndOrSingleSlash {
        get {
          complete {
            StatusCodes.OK
          }
        }
      }
    }

  def leoRoutes: Route =
    requireUserInfo { userInfo =>
      implicit val traceId = ApplicativeAsk.const[IO, TraceId](TraceId(UUID.randomUUID()))

      setTokenCookie(userInfo, tokenCookieName) {
        pathPrefix("cluster") {
          pathPrefix("v2" / Segment / Segment) { (googleProject, clusterNameString) =>
            validateClusterNameDirective(clusterNameString) { clusterName =>
              pathEndOrSingleSlash {
                put {
                  entity(as[ClusterRequest]) { cluster =>
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
              validateClusterNameDirective(clusterNameString) { clusterName =>
                pathEndOrSingleSlash {
                  patch {
                    entity(as[ClusterRequest]) { cluster =>
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
                      entity(as[ClusterRequest]) { cluster =>
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
                          .getActiveClusterDetails(userInfo, GoogleProject(googleProject), clusterName)
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

  def route: Route = (logRequestResult & handleExceptions(myExceptionHandler)) {
    swaggerRoutes ~ unauthedRoutes ~ proxyRoutes ~ statusRoutes ~
      pathPrefix("api") { leoRoutes }
  }

  private val myExceptionHandler = {
    ExceptionHandler {
      case requestValidationError: RequestValidationError =>
        complete(StatusCodes.BadRequest, requestValidationError.getMessage)
      case leoException: LeoException =>
        complete(leoException.statusCode, leoException.toErrorReport)
      case withErrorReport: WorkbenchExceptionWithErrorReport =>
        complete(
          withErrorReport.errorReport.statusCode
            .getOrElse(StatusCodes.InternalServerError) -> withErrorReport.errorReport
        )
      case workbenchException: WorkbenchException =>
        val report = ErrorReport(Option(workbenchException.getMessage).getOrElse(""),
                                 Some(StatusCodes.InternalServerError),
                                 Seq(),
                                 Seq(),
                                 Some(workbenchException.getClass))
        complete(StatusCodes.InternalServerError -> report)
      case e: Throwable =>
        //NOTE: this needs SprayJsonSupport._, ErrorReportJsonSupport._, and errorReportSource all imported to work
        complete(StatusCodes.InternalServerError -> ErrorReport(e))
    }
  }

  // basis for logRequestResult lifted from http://stackoverflow.com/questions/32475471/how-does-one-log-akka-http-client-requests
  private def logRequestResult: Directive0 = {
    def entityAsString(entity: HttpEntity): Future[String] =
      entity.dataBytes
        .map(_.decodeString(entity.contentType.charsetOption.getOrElse(HttpCharsets.`UTF-8`).value))
        .runWith(Sink.head)

    def myLoggingFunction(logger: LoggingAdapter)(req: HttpRequest)(res: Any): Unit = {
      val entry = res match {
        case Complete(resp) =>
          val logLevel: LogLevel = resp.status.intValue / 100 match {
            case 5 => Logging.ErrorLevel
            case _ => Logging.DebugLevel
          }
          entityAsString(resp.entity)
            .map(data => LogEntry(s"${req.method} ${req.uri}: ${resp.status} entity: $data", logLevel))
        case other =>
          Future.successful(LogEntry(s"$other", Logging.ErrorLevel)) // I don't really know when this case happens
      }
      entry.map(_.logTo(logger))
    }

    DebuggingDirectives.logRequestResult(LoggingMagnet(myLoggingFunction))
  }

}

object LeoRoutes {
  private val clusterNameReg = "([a-z|0-9|-])*".r
  private def validateClusterName(clusterNameString: String): Either[Throwable, ClusterName] =
    clusterNameString match {
      case clusterNameReg(_) => Right(ClusterName(clusterNameString))
      case _ =>
        Left(
          new RequestValidationError(
            s"invalid cluster name ${clusterNameString}. Only lowercase alphanumeric characters, numbers and dashes are allowed in cluster name"
          )
        )
    }

  def validateClusterNameDirective(clusterNameString: String): Directive1[ClusterName] =
    Directive { inner =>
      validateClusterName(clusterNameString) match {
        case Left(e)  => failWith(e)
        case Right(c) => inner(Tuple1(c))
      }
    }
}
