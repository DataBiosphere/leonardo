package org.broadinstitute.dsde.workbench.leonardo.http
import akka.actor.ActorSystem
import cats.effect.{IO, Resource}
import org.broadinstitute.dsde.workbench.leonardo.config.Config.{appServiceConfig, gkeCustomAppConfig}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.http.service.LeoAppServiceInterp
import org.broadinstitute.dsde.workbench.leonardo.util.ServicesRegistry
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

class AzureDependencyBuilder extends CloudDependenciesBuilder {

  /**
   * Registers the OT tracing for the cloud hosting provider.
   *
   * @return
   */
  // TODO: we must implement this for Azure.
  override def registryOpenTelemetryTracing: Resource[IO, Unit] =  Resource.pure[IO, Unit](())

  /**
   * Creates an empty list back-end processes for azure..
   *
   * @param baselineDependencies              Leo baseline dependencies.
   * @param cloudSpecificDependenciesRegistry Dependency registry containing cloud specific dependencies.
   * @param logger                            Logger.
   * @param ec                                Execution context.
   * @param dbReference                       DB Reference.
   * @param openTelemetry                     OT metrics
   * @return
   */
  override def createCloudSpecificProcessesList(baselineDependencies: BaselineDependencies[IO],
                                                cloudSpecificDependenciesRegistry: ServicesRegistry
  )(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    dbReference: DbReference[IO],
    openTelemetry: OpenTelemetryMetrics[IO]
  ): List[fs2.Stream[IO, Unit]] = List.empty

  /**
   * Create a dependency registry for Azure, as there is no Azure specific references required only when hosting Leo on Azure.
   *
   * @param baselineDependencies Leo baseline dependencies.
   * @param logger               Logger.
   * @param ec                   Execution context.
   * @param dbReference          DB Reference.
   * @param openTelemetry        OT metrics
   * @return
   */
  override def createDependenciesRegistry(baselineDependencies: BaselineDependencies[IO])(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    as: ActorSystem,
    dbReference: DbReference[IO],
    openTelemetry: OpenTelemetryMetrics[IO]
  ): Resource[IO, ServicesRegistry] = {
    // The AppService is used by App V1, App V2 and the Resources routes.
    // Only App V2 routes are required for Azure functionality,
    // so we need an instance of the AppService without gcp deps (Compute and Resources)
    val leoKubernetesService: LeoAppServiceInterp[IO] =
      new LeoAppServiceInterp(
        appServiceConfig,
        baselineDependencies.authProvider,
        baselineDependencies.serviceAccountProvider,
        baselineDependencies.publisherQueue,
        None,
        None,
        gkeCustomAppConfig,
        baselineDependencies.wsmDAO,
        baselineDependencies.wsmClientProvider
      )

    var servicesRegistry = ServicesRegistry()

    servicesRegistry.register[LeoAppServiceInterp[IO]](leoKubernetesService)

    Resource.make(IO(servicesRegistry))(_ => IO.unit)
  }
}
