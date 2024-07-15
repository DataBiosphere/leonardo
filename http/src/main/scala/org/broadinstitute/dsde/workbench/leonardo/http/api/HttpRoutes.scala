package org.broadinstitute.dsde.workbench.leonardo.http
package api

import akka.actor.ActorSystem
import akka.event.Logging.LogLevel
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.Complete
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry, LoggingMagnet}
import akka.stream.scaladsl.Sink
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import io.circe.Encoder
import org.broadinstitute.dsde.workbench.google2.JsonCodec.traceIdEncoder
import org.broadinstitute.dsde.workbench.leonardo.config.{ContentSecurityPolicyConfig, RefererConfig}
import org.broadinstitute.dsde.workbench.leonardo.http.api.HttpRoutes.errorReportEncoder
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.leonardo.util.ServicesRegistry
import org.broadinstitute.dsde.workbench.model.ErrorReport
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.{ExecutionContext, Future}

class HttpRoutes(
  oidcConfig: OpenIDConnectConfiguration,
  statusService: StatusService,
  gcpOnlyServicesRegistry: ServicesRegistry,
  diskV2Service: DiskV2Service[IO],
  kubernetesService: AppService[IO],
  azureService: RuntimeV2Service[IO],
  adminService: AdminService[IO],
  userInfoDirectives: UserInfoDirectives,
  contentSecurityPolicy: ContentSecurityPolicyConfig,
  refererConfig: RefererConfig,
  enableAzureOnlyRoutes: Boolean = false
)(implicit ec: ExecutionContext, ac: ActorSystem, metrics: OpenTelemetryMetrics[IO], logger: StructuredLogger[IO]) {

  private val statusRoutes = new StatusRoutes(statusService)
  private val corsSupport = new CorsSupport(contentSecurityPolicy, refererConfig)
  private val kubernetesRoutes = new AppRoutes(kubernetesService, userInfoDirectives)
  private val appV2Routes = new AppV2Routes(kubernetesService, userInfoDirectives)
  private val runtimeV2Routes = new RuntimeV2Routes(refererConfig, azureService, userInfoDirectives)
  private val diskV2Routes = new DiskV2Routes(diskV2Service, userInfoDirectives)
  private val adminRoutes = new AdminRoutes(adminService, userInfoDirectives)
  private val diskRoutes = createDiskRoutesUsingServicesRegistry
  private val runtimeRoutes = createRuntimeRoutesUsingServicesRegistry
  private val resourcesRoutes = createResourcesRoutesUsingServicesRegistry
  private val proxyRoutes = createProxyRoutesUsingServicesRegistry

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

  implicit val myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case leoException: LeoException =>
        logger
          .error(leoException.getLoggingContext, leoException)(
            s"request failed due to: ${leoException.getLoggingMessage}"
          )
          .unsafeToFuture()
        complete(leoException.statusCode, leoException.toErrorReport)
      case e: Throwable =>
        logger.error(e)(s"Unexpected error occurred processing route: ${e.getMessage}").unsafeToFuture()
        complete(
          StatusCodes.InternalServerError -> ErrorReport(e.getMessage,
                                                         Some(StatusCodes.InternalServerError),
                                                         Seq(),
                                                         Seq(),
                                                         Some(e.getClass),
                                                         None
          )
        )
    }

  implicit val myRejectionHandler: RejectionHandler =
    RejectionHandler
      .newBuilder()
      .handle { case ValidationRejection(msg, cause) =>
        complete(HttpResponse(StatusCodes.BadRequest, entity = s"${cause.map(_.getMessage).getOrElse(msg)}"))
      }
      .handleNotFound(
        complete(
          (StatusCodes.NotFound, "API not found. Make sure you're calling the correct endpoint with correct method")
        )
      )
      .result()

  val route: Route =
    logRequestResult {
      enableAzureOnlyRoutes match {
        case false =>
          Route.seal(
            oidcConfig
              .swaggerRoutes(
                "swagger/api-docs.yaml"
              ) ~ oidcConfig.oauth2Routes ~ proxyRoutes.get.route ~ statusRoutes.route ~
              pathPrefix("api") {
                runtimeRoutes.get.routes ~ runtimeV2Routes.routes ~
                  diskRoutes.get.routes ~ kubernetesRoutes.routes ~ appV2Routes.routes ~ diskV2Routes.routes ~ adminRoutes.routes ~
                  resourcesRoutes.get.routes
              }
          )
        case true =>
          Route.seal(
            oidcConfig
              .swaggerRoutes("swagger/api-docs.yaml") ~ oidcConfig.oauth2Routes ~ statusRoutes.route ~
              pathPrefix("api") {
                runtimeRoutes.get.routes ~ runtimeV2Routes.routes ~
                  diskRoutes.get.routes ~ diskV2Routes.routes ~ appV2Routes.routes ~ adminRoutes.routes
              }
          )
      }
    }
  private def createResourcesRoutesUsingServicesRegistry =
    gcpOnlyServicesRegistry
      .lookup[ResourcesService[IO]]
      .map(resourcesService => new ResourcesRoutes(resourcesService, userInfoDirectives))

  private def createDiskRoutesUsingServicesRegistry =
    gcpOnlyServicesRegistry
      .lookup[DiskService[IO]]
      .map(diskService => new DiskRoutes(diskService, userInfoDirectives))

  private def createRuntimeRoutesUsingServicesRegistry =
    gcpOnlyServicesRegistry
      .lookup[RuntimeService[IO]]
      .map(runtimeService => new RuntimeRoutes(refererConfig, runtimeService, userInfoDirectives))

  private def createProxyRoutesUsingServicesRegistry =
    gcpOnlyServicesRegistry
      .lookup[ProxyService]
      .map(proxyService => new ProxyRoutes(proxyService, corsSupport, refererConfig))

}

object HttpRoutes {
  implicit val statusCodeEncoder: Encoder[StatusCode] = Encoder.encodeInt.contramap(_.intValue())
  implicit val classEncoder: Encoder[Class[_]] = Encoder.encodeString.contramap(_.toString)
  implicit val errorReportEncoder: Encoder[ErrorReport] = Encoder.forProduct5(
    "source",
    "message",
    "statusCode",
    "exceptionClass",
    "traceId"
  )(x => (x.source, x.message, x.statusCode, x.exceptionClass, x.traceId))
}
