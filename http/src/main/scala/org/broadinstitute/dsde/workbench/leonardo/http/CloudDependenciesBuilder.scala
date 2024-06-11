package org.broadinstitute.dsde.workbench.leonardo.http

import akka.actor.ActorSystem
import cats.effect.{IO, Resource}
import fs2.Stream
import org.broadinstitute.dsde.workbench.leonardo.config.{ContentSecurityPolicyConfig, RefererConfig}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.http.api.UserInfoDirectives
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.util.ServicesRegistry
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

/***
 * A trait that wraps the creation of dependencies that depend on the hosting environment.
 */
trait CloudDependenciesBuilder {

  /**
   * Registers the OT tracing for the cloud hosting provider.
   * @return
   */
  def registryOpenTelemetryTracing: Resource[IO, Unit]

  /**
   * Creates a list back-end processes for the cloud hosting provider.
   * @param baselineDependencies Leo baseline dependencies.
   * @param cloudSpecificDependenciesRegistry Dependency registry containing cloud specific dependencies.
   * @param logger Logger.
   * @param ec Execution context.
   * @param dbReference DB Reference.
   * @param openTelemetry OT metrics
   * @return
   */
  def createCloudSpecificProcessesList(baselineDependencies: BaselineDependencies[IO],
                                       cloudSpecificDependenciesRegistry: ServicesRegistry
  )(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    dbReference: DbReference[IO],
    openTelemetry: OpenTelemetryMetrics[IO]
  ): List[Stream[IO, Unit]]

  /**
   * Create a dependency registry of dependencies that are specific to the cloud hosting provider.
   * @param baselineDependencies Leo baseline dependencies.
   * @param logger Logger.
   * @param ec Execution context.
   * @param dbReference DB Reference.
   * @param openTelemetry OT metrics
   * @return
   */
  def createDependenciesRegistry(baselineDependencies: BaselineDependencies[IO])(implicit
    logger: StructuredLogger[IO],
    ec: ExecutionContext,
    as: ActorSystem,
    dbReference: DbReference[IO],
    openTelemetry: OpenTelemetryMetrics[IO]
  ): Resource[IO, ServicesRegistry]
}

final case class LeoAppDependencies(
  servicesDependencies: ServicesDependencies,
  leoAppProcesses: LeoAppProcesses
)

/**
 * Contains all dependencies for the creation of the HTTP routes (services).
 */
final case class ServicesDependencies(
  statusService: StatusService,
  cloudSpecificDependenciesRegistry: ServicesRegistry,
  diskV2Service: DiskV2Service[IO],
  kubernetesService: AppService[IO],
  azureService: RuntimeV2Service[IO],
  adminService: AdminService[IO],
  userInfoDirectives: UserInfoDirectives,
  contentSecurityPolicy: ContentSecurityPolicyConfig,
  refererConfig: RefererConfig,
  baselineDependencies: BaselineDependencies[IO]
)

/**
 * Contains a list of back-end processes.
 */
final case class LeoAppProcesses(processesList: List[Stream[IO, Unit]])
