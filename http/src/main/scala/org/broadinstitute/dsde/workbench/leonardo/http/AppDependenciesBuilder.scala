package org.broadinstitute.dsde.workbench.leonardo.http

import akka.actor.ActorSystem
import cats.Parallel
import cats.effect.std.Semaphore
import cats.effect.{Async, IO, Resource}
import org.broadinstitute.dsde.workbench.leonardo.config.Config.{appServiceConfig, applicationConfig, contentSecurityPolicy, dbConcurrency, gkeCustomAppConfig, liquibaseConfig, prometheusConfig, refererConfig}
import org.broadinstitute.dsde.workbench.leonardo.config.{ContentSecurityPolicyConfig, RefererConfig}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.http.api.{StandardUserInfoDirectives, UserInfoDirectives}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.util.ServicesRegistry
import org.broadinstitute.dsde.workbench.oauth2.OpenIDConnectConfiguration
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

trait AppDependenciesBuilder {
    def createAppDependencies(implicit
                              logger: StructuredLogger[IO],
                              ec: ExecutionContext,
                              as: ActorSystem): Resource[IO,LeoAppDependencies]
}

abstract class AppDependenciesBuilderImpl extends AppDependenciesBuilder {
  override def createAppDependencies(implicit
                                  logger: StructuredLogger[IO],
                                  ec: ExecutionContext,
                                  as: ActorSystem): Resource[IO,LeoAppDependencies] = {
    for {
      concurrentDbAccessPermits <- Resource.eval(Semaphore[IO](dbConcurrency))

      implicit0(dbRef: DbReference[IO]) <- DbReference.init(liquibaseConfig, concurrentDbAccessPermits)
      implicit0(openTelemetry: OpenTelemetryMetrics[IO]) <- OpenTelemetryMetrics
        .resource[IO](applicationConfig.applicationName, prometheusConfig.endpointPort)

      baseDependencies <- BaselineDependenciesBuilder().createBaselineDependencies[IO]()

      dependenciesRegistry <- createDependenciesRegistry(baseDependencies)

      httpRoutesDependencies <- createHttpRoutesDependencies(baseDependencies, dependenciesRegistry)

      backEndDependencies <- createBackEndDependencies(baseDependencies)
    } yield {
      LeoAppDependencies(httpRoutesDependencies, backEndDependencies)
    }
  }

    private def createHttpRoutesDependencies(appDependencies: BaselineDependencies[IO],
                                             dependenciesRegistry: ServicesRegistry
    )(implicit
      logger: StructuredLogger[IO],
      ec: ExecutionContext,
      as: ActorSystem,
      dbReference: DbReference[IO],
      openTelemetry: OpenTelemetryMetrics[IO],
    ): Resource[IO,HttpRoutesDependencies] = {
      val statusService = new StatusService(appDependencies.samDAO, dbReference)
      val diskV2Service = new DiskV2ServiceInterp[IO](
        ConfigReader.appConfig.persistentDisk,
        appDependencies.authProvider,
        appDependencies.wsmDAO,
        appDependencies.samDAO,
        appDependencies.publisherQueue,
        appDependencies.wsmClientProvider
      )
      val leoKubernetesService =
        new LeoAppServiceInterp(
          appServiceConfig,
          appDependencies.authProvider,
          appDependencies.serviceAccountProvider,
          appDependencies.publisherQueue,
          dependenciesRegistry,
          gkeCustomAppConfig,
          appDependencies.wsmDAO,
          appDependencies.wsmClientProvider
        )

      val azureService = new RuntimeV2ServiceInterp[IO](
        appDependencies.runtimeServicesConfig,
        appDependencies.authProvider,
        appDependencies.wsmDAO,
        appDependencies.publisherQueue,
        appDependencies.dateAccessedUpdaterQueue,
        appDependencies.wsmClientProvider
      )
      val adminService = new AdminServiceInterp[IO](appDependencies.authProvider, appDependencies.publisherQueue)

      Resource.make(IO(HttpRoutesDependencies(
        appDependencies.openIDConnectConfiguration,
        statusService,
        dependenciesRegistry,
        diskV2Service,
        leoKubernetesService,
        azureService,
        adminService,
        StandardUserInfoDirectives,
        contentSecurityPolicy,
        refererConfig
      )))(_ => IO.unit)
//      HttpRoutesDependencies(
//        appDependencies.openIDConnectConfiguration,
//        statusService,
//        gcpServicesRegistry,
//        diskV2Service,
//        leoKubernetesService,
//        azureService,
//        adminService,
//        StandardUserInfoDirectives,
//        contentSecurityPolicy,
//        refererConfig
//      )
    }

  def createBackEndDependencies[F[_]:Parallel](baselineDependencies: BaselineDependencies[F])(implicit F:Async[F]): Resource[F, BackEndDependencies]

  def createDependenciesRegistry[F[_]:Parallel](baselineDependencies: BaselineDependencies[F])(implicit F:Async[F]): Resource[F, ServicesRegistry]
}

final case class LeoAppDependencies(
                                     httpRoutesDependencies: HttpRoutesDependencies,
                                     backEndDependencies: BackEndDependencies,
                                   )
final case class HttpRoutesDependencies(
                                         oidcConfig: OpenIDConnectConfiguration,
                                         statusService: StatusService,
                                         gcpOnlyServicesRegistry: ServicesRegistry,
                                         diskV2Service: DiskV2Service[IO],
                                         kubernetesService: AppService[IO],
                                         azureService: RuntimeV2Service[IO],
                                         adminService: AdminService[IO],
                                         userInfoDirectives: UserInfoDirectives,
                                         contentSecurityPolicy: ContentSecurityPolicyConfig,
                                         refererConfig: RefererConfig
                                       )
final case class BackEndDependencies()