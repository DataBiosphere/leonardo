package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.actor.ActorSystem
import akka.event.Logging.LogLevel
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpCharsets, HttpEntity, HttpRequest, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry, LoggingMagnet}
import akka.http.scaladsl.server.{Directive0, ExceptionHandler, Route}
import akka.stream.scaladsl.Sink
import cats.effect.{ContextShift, IO, Timer}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.config.SwaggerConfig
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  LeonardoService,
  ProxyService,
  RuntimeService,
  StatusService
}
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoException, RequestValidationError}
import org.broadinstitute.dsde.workbench.model.ErrorReportJsonSupport._
import org.broadinstitute.dsde.workbench.model.ErrorReport

import scala.concurrent.{ExecutionContext, Future}

class HttpRoutes(
  swaggerConfig: SwaggerConfig,
  statusService: StatusService,
  proxyService: ProxyService,
  leonardoService: LeonardoService,
  runtimeService: RuntimeService[IO],
  diskService: DiskService[IO],
  userInfoDirectives: UserInfoDirectives,
  contentSecurityPolicy: String
)(implicit timer: Timer[IO], ec: ExecutionContext, ac: ActorSystem, cs: ContextShift[IO])
    extends LazyLogging {
  private val swaggerRoutes = new SwaggerRoutes(swaggerConfig)
  private val statusRoutes = new StatusRoutes(statusService)
  private val corsSupport = new CorsSupport(contentSecurityPolicy)
  private val proxyRoutes = new ProxyRoutes(proxyService, corsSupport)
  private val leonardoRoutes = new LeoRoutes(leonardoService, userInfoDirectives)
  private val runtimeRoutes = new RuntimeRoutes(runtimeService, userInfoDirectives)
  private val diskRoutes = new DiskRoutes(diskService, userInfoDirectives)

  private val myExceptionHandler = {
    ExceptionHandler {
      case requestValidationError: RequestValidationError =>
        complete(StatusCodes.BadRequest, requestValidationError.getMessage)
      case leoException: LeoException =>
        logger.error(s"request failed due to: ${leoException.getMessage}", leoException)
        complete(leoException.statusCode, leoException.toErrorReport)
      case e: Throwable =>
        logger.error(s"Unexpected error occurred processing route: ${e.getMessage}", e)
        complete(
          StatusCodes.InternalServerError -> ErrorReport(e.getMessage,
                                                         Some(StatusCodes.InternalServerError),
                                                         Seq(),
                                                         Seq(),
                                                         Some(e.getClass))
        )
    }
  }

  // basis for logRequestResult lifted from http://stackoverflow.com/questions/32475471/how-does-one-log-akka-http-client-requests
  private val logRequestResult: Directive0 = {
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

  val route: Route = {
    logRequestResult {
      handleExceptions(myExceptionHandler) {
        swaggerRoutes.routes ~ proxyRoutes.route ~ statusRoutes.route ~
          pathPrefix("api") {
            leonardoRoutes.route ~ runtimeRoutes.routes ~ diskRoutes.routes
          }
      }
    }
  }
}
