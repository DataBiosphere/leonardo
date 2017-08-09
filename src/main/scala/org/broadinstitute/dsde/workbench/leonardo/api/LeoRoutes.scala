package org.broadinstitute.dsde.workbench.leonardo.api

import akka.event.Logging.LogLevel
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry, LoggingMagnet}
import akka.http.scaladsl.server.{Directive0, ExceptionHandler}
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import net.ceedubs.ficus.Ficus._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.model.{ErrorReport, WorkbenchExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.ErrorReportJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterRequest
import org.broadinstitute.dsde.workbench.leonardo.model.LeonardoJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.errorReportSource
import org.broadinstitute.dsde.workbench.leonardo.config.SwaggerConfig
import org.broadinstitute.dsde.workbench.leonardo.service.LeonardoService
import spray.json.{JsBoolean, JsObject, JsString}

import scala.concurrent.{ExecutionContext, Future}

class LeoRoutes(val leonardoService: LeonardoService, val swaggerConfig: SwaggerConfig)(implicit val materializer: Materializer, val executionContext: ExecutionContext) extends LazyLogging  with SwaggerRoutes  {

  def leoRoutes: server.Route =
    path("ping") {
      pathEndOrSingleSlash {
        get {
          complete {
            StatusCodes.OK
          }
        }
      }
    } ~
    path("cluster" / Segment / Segment) { (googleProject, clusterName) =>
      put {
        entity(as[ClusterRequest]) { cluster =>
          complete {
            leonardoService.createCluster(googleProject, clusterName, cluster).map { clusterResponse =>
              StatusCodes.OK -> clusterResponse
            }
          }
        }
      }
    }



  def route: server.Route = (logRequestResult & handleExceptions(myExceptionHandler)) {
    swaggerRoutes ~
      pathPrefix("api") { leoRoutes }
  }

  private val myExceptionHandler = {
    ExceptionHandler {
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
