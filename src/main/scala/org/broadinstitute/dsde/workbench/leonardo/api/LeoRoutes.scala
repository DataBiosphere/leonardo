package org.broadinstitute.dsde.workbench.leonardo.api

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
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.SwaggerConfig
import org.broadinstitute.dsde.workbench.leonardo.errorReportSource
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterRequest, LeoException}
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.service.{LeonardoService, ProxyService, StatusService}
import org.broadinstitute.dsde.workbench.model.ErrorReportJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{ErrorReport, WorkbenchExceptionWithErrorReport}

import scala.concurrent.{ExecutionContext, Future}

abstract class LeoRoutes(val leonardoService: LeonardoService, val proxyService: ProxyService, val statusService: StatusService, val swaggerConfig: SwaggerConfig)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends LazyLogging with ProxyRoutes with SwaggerRoutes with StatusRoutes with UserInfoDirectives {

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
      path("isWhitelisted") {
        get {
          complete {
            leonardoService.isWhitelisted(userInfo).map { _ =>
              StatusCodes.OK
            }
          }
        }
      } ~
      path("cluster" / Segment / Segment) { (googleProject, clusterName) =>
        put {
          entity(as[ClusterRequest]) { cluster =>
            complete {
              leonardoService.createCluster(userInfo, GoogleProject(googleProject), ClusterName(clusterName), cluster).map { cluster =>
                StatusCodes.OK -> cluster
              }
            }
          }
        } ~
          get {
            complete {
              leonardoService.getActiveClusterDetails(userInfo, GoogleProject(googleProject), ClusterName(clusterName)).map { clusterDetails =>
                StatusCodes.OK -> clusterDetails
              }
            }
          } ~
          delete {
            complete {
              leonardoService.deleteCluster(userInfo, GoogleProject(googleProject), ClusterName(clusterName)).map { _ =>
                StatusCodes.Accepted
              }
            }
          }
      } ~
        path("clusters") {
          parameterMap { params =>
            complete {
              leonardoService.listClusters(userInfo, params).map { clusters =>
                StatusCodes.OK -> clusters
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
      case leoException: LeoException =>
        complete(leoException.statusCode, leoException.toErrorReport)
      case withErrorReport: WorkbenchExceptionWithErrorReport =>
        complete(withErrorReport.errorReport.statusCode.getOrElse(StatusCodes.InternalServerError), withErrorReport.errorReport)
      case e: Throwable =>
        //NOTE: this needs SprayJsonSupport._, ErrorReportJsonSupport._, and errorReportSource all imported to work
        complete(StatusCodes.InternalServerError, ErrorReport(e))
    }
  }

  // basis for logRequestResult lifted from http://stackoverflow.com/questions/32475471/how-does-one-log-akka-http-client-requests
  private def logRequestResult: Directive0 = {
    def entityAsString(entity: HttpEntity): Future[String] = {
      entity.dataBytes
        .map(_.decodeString(entity.contentType.charsetOption.getOrElse(HttpCharsets.`UTF-8`).value))
        .runWith(Sink.head)
    }

    def myLoggingFunction(logger: LoggingAdapter)(req: HttpRequest)(res: Any): Unit = {
      val entry = res match {
        case Complete(resp) =>
          val logLevel: LogLevel = resp.status.intValue / 100 match {
            case 5 => Logging.ErrorLevel
            case _ => Logging.DebugLevel
          }
          entityAsString(resp.entity).map(data => LogEntry(s"${req.method} ${req.uri}: ${resp.status} entity: $data", logLevel))
        case other =>
          Future.successful(LogEntry(s"$other", Logging.ErrorLevel)) // I don't really know when this case happens
      }
      entry.map(_.logTo(logger))
    }

    DebuggingDirectives.logRequestResult(LoggingMagnet(myLoggingFunction))
  }

}
