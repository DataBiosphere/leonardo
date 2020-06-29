package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.Parallel
import cats.effect.{Async, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.dataproc.v1.Cluster
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.{
  DataprocClusterName,
  DataprocRole,
  GoogleComputeService,
  GoogleDataprocInterpreter,
  GoogleDataprocService,
  GoogleStorageService,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{getInstanceIP, parseGoogleTimestamp}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorConfig.DataprocMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeMonitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class DataprocRuntimeMonitor[F[_]: Parallel](
  config: DataprocMonitorConfig,
  googleComputeService: GoogleComputeService[F],
  authProvider: LeoAuthProvider[F],
  googleStorageService: GoogleStorageService[F],
  override val runtimeAlg: RuntimeAlgebra[F],
  googleDataprocService: GoogleDataprocService[F]
)(implicit override val dbRef: DbReference[F],
  override val runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[F, RuntimeContainerServiceType],
  override val F: Async[F],
  override val timer: Timer[F],
  override val logger: Logger[F],
  override val ec: ExecutionContext,
  override val openTelemetry: OpenTelemetryMetrics[F])
    extends BaseCloudServiceRuntimeMonitor[F] {
  override val googleStorage: GoogleStorageService[F] = googleStorageService
  override val monitorConfig: MonitorConfig = config

  // Function used for transitions that we can get an Operation
  def pollCheck(googleProject: GoogleProject,
                runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                operation: com.google.cloud.compute.v1.Operation,
                action: RuntimeStatus)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    F.pure(new NotImplementedError("pollCheck is not supported for monitoring dataproc clusters"))

  /**
   * Queries Google for the cluster status and takes appropriate action depending on the result.
   * @return ClusterMonitorMessage
   */
  override def handleCheck(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[CheckResult] =
    for {
      cluster <- googleDataprocService.getCluster(
        runtimeAndRuntimeConfig.runtime.googleProject,
        config.regionName,
        DataprocClusterName(runtimeAndRuntimeConfig.runtime.runtimeName.asString)
      )
      result <- runtimeAndRuntimeConfig.runtime.status match {
        case RuntimeStatus.Creating =>
          creatingRuntime(cluster, monitorContext, runtimeAndRuntimeConfig)
        case RuntimeStatus.Deleting =>
          deletedRuntime(cluster, monitorContext, runtimeAndRuntimeConfig)
        case RuntimeStatus.Starting =>
          startingRuntime(cluster, monitorContext, runtimeAndRuntimeConfig)
        case RuntimeStatus.Updating =>
          updatingRuntime(cluster, monitorContext, runtimeAndRuntimeConfig)
        case RuntimeStatus.Stopping =>
          stoppingRuntime(cluster, monitorContext, runtimeAndRuntimeConfig)
        case status =>
          logger.error(s"${status} is not a transition status for GCE; hence no need to monitor").as(((), None))
      }
    } yield result

  private[monitor] def creatingRuntime(
    cluster: Option[Cluster],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[CheckResult] = cluster match {
    case None =>
      checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some(s"Can't retrieve cluster yet"))
    case Some(c) =>
      for {
        instances <- getDataprocInstances(c, runtimeAndRuntimeConfig.runtime.googleProject)
        runtimeStatus = DataprocClusterStatus
          .withNameInsensitiveOption(c.getStatus.getState.name())
          .getOrElse(DataprocClusterStatus.Unknown) //TODO: this needs to be verified
        r <- runtimeStatus match {
          case DataprocClusterStatus.Creating | DataprocClusterStatus.Unknown =>
            checkAgain(monitorContext, runtimeAndRuntimeConfig, instances, Some(s"Cluster is still in creating"))
          case DataprocClusterStatus.Running if (instances.exists(_.status != GceInstanceStatus.Running)) =>
            checkAgain(monitorContext,
                       runtimeAndRuntimeConfig,
                       instances,
                       Some(s"Not all instances for this cluster is Running yet"))
          case DataprocClusterStatus.Running =>
            val masterInstance = instances.find(_.dataprocRole == DataprocRole.Master)

            masterInstance match {
              case Some(i) =>
                i.ip match {
                  case Some(ip) =>
                    // It takes a bit for jupyter to startup, hence wait 5 seconds before we check jupyter
                    Timer[F]
                      .sleep(8 seconds) >> handleCheckTools(monitorContext, runtimeAndRuntimeConfig, ip, instances)
                  case None =>
                    checkAgain(monitorContext,
                               runtimeAndRuntimeConfig,
                               instances,
                               Some("Could not retrieve instance IP"))
                }
              case None =>
                failedRuntime(
                  monitorContext,
                  runtimeAndRuntimeConfig,
                  Some(RuntimeErrorDetails(s"Can't find master instance for this cluster")),
                  instances
                )
            }
          case DataprocClusterStatus.Error =>
            val operationName = runtimeAndRuntimeConfig.runtime.asyncRuntimeFields.map(_.operationName)
            for {
              error <- operationName.flatTraverse(o =>
                googleDataprocService.getClusterError(google2.OperationName(o.value))
              )
              res <- failedRuntime(
                monitorContext,
                runtimeAndRuntimeConfig,
                error
                  .map(e => RuntimeErrorDetails(e.message, Some(e.code), Some(e.message))),
                instances
              )
            } yield res
          case ss =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              Some(
                RuntimeErrorDetails(s"unexpected Dataproc cluster status ${ss} when trying to creating an instance")
              ),
              instances
            )
        }
      } yield r
  }

  private[monitor] def startingRuntime(
    cluster: Option[Cluster],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[CheckResult] = cluster match {
    case None =>
      logger
        .error(
          s"${monitorContext} | Fail to retrieve cluster when trying to start ${runtimeAndRuntimeConfig.runtime.projectNameString}"
        )
        .as(((), None)) //TODO: shall we delete runtime in this case?
    case Some(c) =>
      for {
        instances <- getDataprocInstances(c, runtimeAndRuntimeConfig.runtime.googleProject)
        clusterStatus = DataprocClusterStatus
          .withNameInsensitiveOption(c.getStatus.getState.name())
          .getOrElse(DataprocClusterStatus.Unknown) //TODO: this needs to be verified
        r <- clusterStatus match {
          case DataprocClusterStatus.Running if (instances.exists(_.status != GceInstanceStatus.Running)) =>
            checkAgain(monitorContext,
                       runtimeAndRuntimeConfig,
                       instances,
                       Some(s"Not all instances for this cluster is Running yet"))
          case DataprocClusterStatus.Running =>
            instances.find(_.dataprocRole == DataprocRole.Master).flatMap(_.ip) match {
              case Some(ip) =>
                // It takes a bit for jupyter to startup, hence wait 5 seconds before we check jupyter
                Timer[F]
                  .sleep(8 seconds) >> handleCheckTools(monitorContext, runtimeAndRuntimeConfig, ip, instances)
              case None =>
                checkAgain(monitorContext, runtimeAndRuntimeConfig, instances, Some("Could not retrieve instance IP"))
            }
          case DataprocClusterStatus.Error =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              Some(RuntimeErrorDetails(s"Cluster failed to start")),
              instances
            )
          case ss =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              Some(RuntimeErrorDetails(s"unexpected Cluster ${ss} when trying to start it")),
              instances
            )
        }
      } yield r
  }

  private[monitor] def stoppingRuntime(
    cluster: Option[Cluster],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[CheckResult] = cluster match {
    case None =>
      for {
        _ <- logger
          .error(s"${monitorContext} | Can't stop an instance that hasn't been initialized yet or doesn't exist")
        _ <- failedRuntime(
          monitorContext,
          runtimeAndRuntimeConfig,
          Some(RuntimeErrorDetails("Can't stop an instance that hasn't been initialized yet or doesn't exist")),
          Set.empty
        )
      } yield ((), None) //TODO: Ideally we should have sentry report this case
    case Some(c) =>
      for {
        instances <- getDataprocInstances(c, runtimeAndRuntimeConfig.runtime.googleProject)
        res <- if (instances
                     .forall(i => i.status == GceInstanceStatus.Stopped || i.status == GceInstanceStatus.Terminated)) {
          stopRuntime(runtimeAndRuntimeConfig, instances, monitorContext)
        } else {
          checkAgain(
            monitorContext,
            runtimeAndRuntimeConfig,
            instances,
            Some(s"not all instances has been terminated yet. ${instances}")
          )
        }
      } yield res
  }

  private[monitor] def updatingRuntime(
    cluster: Option[Cluster],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[CheckResult] = cluster match {
    case None =>
      for {
        _ <- logger
          .error(s"${monitorContext} | Can't update an instance that hasn't been initialized yet or doesn't exist")
        _ <- failedRuntime(
          monitorContext,
          runtimeAndRuntimeConfig,
          Some(RuntimeErrorDetails("Can't update an instance that hasn't been initialized yet or doesn't exist")),
          Set.empty
        )
      } yield ((), None) //TODO: Ideally we should have sentry report this case
    case Some(c) =>
      for {
        instances <- getDataprocInstances(c, runtimeAndRuntimeConfig.runtime.googleProject)
        clusterStatus = DataprocClusterStatus
          .withNameInsensitiveOption(c.getStatus.getState.name())
          .getOrElse(DataprocClusterStatus.Unknown) //TODO: this needs to be verified
        r <- clusterStatus match {
          case DataprocClusterStatus.Updating =>
            checkAgain(monitorContext,
                       runtimeAndRuntimeConfig,
                       instances,
                       Some(s"Dataproc cluster still being updated"))
          case DataprocClusterStatus.Running if (instances.exists(_.status != GceInstanceStatus.Running)) =>
            checkAgain(monitorContext,
                       runtimeAndRuntimeConfig,
                       instances,
                       Some(s"Not all instances for this cluster is Running yet"))
          case DataprocClusterStatus.Running => //TODO: is this right? we can only start runtime if it's a Running dataproc cluster
            instances.find(_.dataprocRole == DataprocRole.Master).flatMap(_.ip) match {
              case Some(ip) =>
                // It takes a bit for jupyter to startup, hence wait a few seconds before we check jupyter
                Timer[F]
                  .sleep(3 seconds) >> handleCheckTools(monitorContext, runtimeAndRuntimeConfig, ip, instances)
              case None =>
                checkAgain(monitorContext, runtimeAndRuntimeConfig, instances, Some("Could not retrieve instance IP"))
            }
          case DataprocClusterStatus.Error =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              Some(RuntimeErrorDetails(s"Cluster failed to Update")),
              instances
            )
          case ss =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              Some(RuntimeErrorDetails(s"unexpected Cluster ${ss} when trying to start an instance")),
              instances
            )
        }
      } yield r
  }

  override def failedRuntime(monitorContext: MonitorContext,
                             runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                             errorDetails: Option[RuntimeErrorDetails],
                             instances: Set[DataprocInstance])(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[CheckResult] =
    for {
      ctx <- ev.ask
      _ <- List(
        // Delete the cluster in Google
        runtimeAlg
          .deleteRuntime(
            DeleteRuntimeParams(
              runtimeAndRuntimeConfig.runtime.googleProject,
              runtimeAndRuntimeConfig.runtime.runtimeName,
              runtimeAndRuntimeConfig.runtime.asyncRuntimeFields.isDefined,
              None
            )
          )
          .void, //TODO is this right when deleting or stopping fails?
        persistClusterErrors(
          errorDetails,
          runtimeAndRuntimeConfig.runtime.id,
          None //TODO: pass in startup script path in the future
        ),
        dbRef.inTransaction {
          clusterQuery.mergeInstances(runtimeAndRuntimeConfig.runtime.copy(dataprocInstances = instances))
        }.void
      ).parSequence_

      // Record metrics in NewRelic
      _ <- RuntimeMonitor.recordStatusTransitionMetrics(
        monitorContext.start,
        RuntimeMonitor.getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Error,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )
      // update the cluster status to Error
      _ <- dbRef.inTransaction {
        clusterQuery.updateClusterStatus(runtimeAndRuntimeConfig.runtime.id, RuntimeStatus.Error, ctx.now)
      }
      tags = Map(
        "cloudService" -> runtimeAndRuntimeConfig.runtimeConfig.cloudService.asString,
        "errorCode" -> errorDetails.flatMap(_.shortMessage).getOrElse("unknown")
      )
      _ <- openTelemetry.incrementCounter(s"runtimeCreationFailure", 1, tags)
    } yield ((), None): CheckResult

  private[monitor] def deletedRuntime(cluster: Option[Cluster],
                                      monitorContext: MonitorContext,
                                      runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[CheckResult] =
    cluster match {
      case Some(c) =>
        for {
          instances <- getDataprocInstances(c, runtimeAndRuntimeConfig.runtime.googleProject)
          r <- checkAgain(monitorContext, runtimeAndRuntimeConfig, instances, Some("Instance hasn't been deleted yet"))
        } yield r
      case None =>
        for {
          ctx <- ev.ask
          duration = (ctx.now.toEpochMilli - monitorContext.start.toEpochMilli).millis
          _ <- logger.info(
            s"${monitorContext} | Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} has been deleted after ${duration.toSeconds} seconds."
          )

          // delete the init bucket so we don't continue to accrue costs after cluster is deleted
          _ <- deleteInitBucket(runtimeAndRuntimeConfig.runtime.googleProject,
                                runtimeAndRuntimeConfig.runtime.runtimeName)

          // set the staging bucket to be deleted in ten days so that logs are still accessible until then
          _ <- setStagingBucketLifecycle(runtimeAndRuntimeConfig.runtime,
                                         config.runtimeBucketConfig.stagingBucketExpiration)

          _ <- dbRef.inTransaction {
            clusterQuery.mergeInstances(runtimeAndRuntimeConfig.runtime)
          }.void //TODO: confirm this is reasonable

          _ <- dbRef.inTransaction {
            clusterQuery.completeDeletion(runtimeAndRuntimeConfig.runtime.id, ctx.now)
          }

          _ <- authProvider
            .notifyResourceDeleted(
              runtimeAndRuntimeConfig.runtime.samResource,
              runtimeAndRuntimeConfig.runtime.auditInfo.creator,
              runtimeAndRuntimeConfig.runtime.auditInfo.creator,
              runtimeAndRuntimeConfig.runtime.googleProject
            )

          // Record metrics in NewRelic
          _ <- RuntimeMonitor.recordStatusTransitionMetrics(
            monitorContext.start,
            RuntimeMonitor.getRuntimeUI(runtimeAndRuntimeConfig.runtime),
            runtimeAndRuntimeConfig.runtime.status,
            RuntimeStatus.Deleted,
            runtimeAndRuntimeConfig.runtimeConfig.cloudService
          )

        } yield ((), None)
    }

  private def getDataprocInstances(
    cluster: Cluster,
    googleProject: GoogleProject
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[Set[DataprocInstance]] =
    for {
      ctx <- ev.ask
      instances = GoogleDataprocInterpreter.getAllInstanceNames(cluster)
      zone = getZone(cluster)
      dataprocInstances <- zone.fold(F.pure(Set.empty[DataprocInstance])) { z =>
        instances.toList
          .flatTraverse {
            case (role, instances) =>
              instances.toList.traverseFilter { i =>
                googleComputeService.getInstance(googleProject, z, i).map { instanceOpt => //TODO: is this necessary? do we actually need to know all instance's IP?
                  instanceOpt.map { instance =>
                    DataprocInstance(
                      DataprocInstanceKey(googleProject, z, i),
                      BigInt(instance.getId),
                      GceInstanceStatus.withNameInsensitive(instance.getStatus),
                      getInstanceIP(instance),
                      role,
                      parseGoogleTimestamp(instance.getCreationTimestamp).getOrElse(ctx.now)
                    )
                  }
                }
              }
          }
          .map(_.toSet)
      }
    } yield dataprocInstances

  private def getZone(cluster: Cluster): Option[ZoneName] = {
    def parseZone(zoneUri: String): Option[ZoneName] =
      zoneUri.lastIndexOf('/') match {
        case -1 =>
          if (zoneUri.nonEmpty)
            Some(ZoneName(zoneUri))
          else
            None
        case n => Some(ZoneName(zoneUri.substring(n + 1)))
      }

    for {
      config <- Option(cluster.getConfig)
      gceConfig <- Option(config.getGceClusterConfig)
      zone <- Option(gceConfig.getZoneUri).flatMap(parseZone)
    } yield zone
  }

  private def persistClusterErrors(
    errorDetails: Option[RuntimeErrorDetails],
    runtimeId: Long,
    startUpScriptOutputFile: Option[GcsPath]
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit] = {
    val result = for {
      ctx <- ev.ask
      errorToPersist <- errorDetails match {
        case Some(error) =>
          F.pure(error)
        case None =>
          startUpScriptOutputFile match {
            case Some(startUpScriptOutput) =>
              for {
                isSuccessOpt <- checkUserScriptsOutputFile(startUpScriptOutput)
                msg <- if (isSuccessOpt.exists(identity)) {
                  F.pure(RuntimeErrorDetails("Error not available", shortMessage = Some("Unknown")))
                } else {
                  F.pure(
                    RuntimeErrorDetails(s"User startup script failed. See output in ${startUpScriptOutput.toUri}",
                                        shortMessage = Some("user_startup_script"))
                  )
                }
              } yield msg
            case None =>
              F.pure(RuntimeErrorDetails("Error not available", shortMessage = Some("Unknown")))
          }
      }
      _ <- saveClusterError(runtimeId, errorToPersist.longMessage, errorToPersist.code.getOrElse(-1), ctx.now)
    } yield ()

    result.onError {
      case e =>
        logger.error(e)(s"Failed to persist cluster errors for cluster ${runtimeId}: ${e.getMessage}")
    }
  }
}

object DataprocRuntimeMonitor {}
