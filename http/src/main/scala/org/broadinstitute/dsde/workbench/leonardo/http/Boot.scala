package org.broadinstitute.dsde.workbench.leonardo
package http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import cats.effect._
import cats.mtl.Ask
import cats.syntax.all._
import fs2.Stream
import io.circe.syntax._
import org.broadinstitute.dsde.workbench.leonardo.config.Config._
import org.broadinstitute.dsde.workbench.leonardo.config.LeoExecutionModeConfig
import org.broadinstitute.dsde.workbench.leonardo.http.api.{BuildTimeVersion, HttpRoutes, LivenessRoutes, StandardUserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Boot extends IOApp {
  val workbenchMetricsBaseName = "google"

  private def startup(): IO[Unit] = {
    // We need an ActorSystem to host our application in

    implicit val system = ActorSystem(applicationConfig.applicationName)
    import system.dispatcher
    implicit val logger =
      StructuredLogger.withContext[IO](Slf4jLogger.getLogger[IO])(
        Map(
          "serviceContext" -> org.broadinstitute.dsde.workbench.leonardo.http.serviceData.asJson.toString,
          "version" -> BuildTimeVersion.version.getOrElse("unknown")
        )
      )

    val livenessRoutes = new LivenessRoutes

    logger
      .info("Liveness server has been created, starting...")
      .unsafeToFuture()(cats.effect.unsafe.IORuntime.global) >> Http()
      .newServerAt("0.0.0.0", 9000)
      .bindFlow(livenessRoutes.route)
      .onError { case t: Throwable =>
        logger
          .error(t)("FATAL - failure starting liveness http server")
          .unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
      }

    logger.info("Liveness server has been started").unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    AppDependenciesBuilder().createAppDependencies().use({ leoDependencies =>

      val frontEndDependencies = leoDependencies.servicesDependencies
      val backEndDependencies = leoDependencies.leoAppProcesses

      implicit val openTelemetryMetrics = frontEndDependencies.baselineDependencies.openTelemetryMetrics

      val resourcesService = new ResourcesServiceInterp[IO](
        appDependencies.authProvider,
        runtimeService,
        leoKubernetesService,
        diskService
      )

      val httpRoutes = new HttpRoutes(
        frontEndDependencies.baselineDependencies.openIDConnectConfiguration,
        frontEndDependencies.statusService,
        frontEndDependencies.cloudSpecificDependenciesRegistry,
        frontEndDependencies.diskV2Service,
        frontEndDependencies.kubernetesService,
        frontEndDependencies.azureService,
        frontEndDependencies.adminService,
        resourcesService,
        StandardUserInfoDirectives,
        contentSecurityPolicy,
        refererConfig
      )

      val httpServer = for {
        start <- IO.realTimeInstant
        implicit0(ctx: Ask[IO, AppContext]) = Ask.const[IO, AppContext](
          AppContext(TraceId(s"Boot_${start}"), start)
        )
        // This only needs to happen once in each environment
        _ <- frontEndDependencies.baselineDependencies.samDAO.registerLeo.handleErrorWith { case e =>
          logger.warn(e)("fail to register Leonardo SA")
        }
        _ <-
          if (leoExecutionModeConfig == LeoExecutionModeConfig.BackLeoOnly) {
            //assuming this is only required when running on GCP, the dataprocInterp should be in the
            // in the dependencies registry.
            frontEndDependencies.cloudSpecificDependenciesRegistry.lookup[DataprocInterpreter[IO]].get.setupDataprocImageGoogleGroup
          } else IO.unit

        _ <- IO.fromFuture {
          IO {
            Http()
              .newServerAt("0.0.0.0", 8080)
              .bindFlow(httpRoutes.route)
              .onError { case t: Throwable =>
                logger
                  .error(t)("FATAL - failure starting http server")
                  .unsafeToFuture()(cats.effect.unsafe.IORuntime.global)
              }
          }
        }
      } yield ()

      val allStreams = backEndDependencies.processesList ++ Stream.eval[IO, Unit](httpServer) // start http server

      val app = Stream.emits(allStreams).covary[IO].parJoin(allStreams.length)

      app
        .handleErrorWith(error => Stream.eval(logger.error(error)("Failed to start leonardo")))
        .compile
        .drain
    })
  }


  override def run(args: List[String]): IO[ExitCode] = startup().as(ExitCode.Success)
}
