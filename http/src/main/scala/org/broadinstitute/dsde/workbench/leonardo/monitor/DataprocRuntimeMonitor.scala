package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.Parallel
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.cloud.compute.v1.Instance
import com.google.cloud.dataproc.v1.Cluster
import org.broadinstitute.dsde.workbench.google2
import org.broadinstitute.dsde.workbench.google2.{
  DataprocClusterName,
  DataprocRole,
  DataprocRoleZonePreemptibility,
  GoogleComputeService,
  GoogleDataprocInterpreter,
  GoogleDataprocService,
  GoogleDiskService,
  GoogleStorageService,
  ZoneName
}
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.{getInstanceIP, parseGoogleTimestamp}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, LeoException}
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorConfig.DataprocMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeMonitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import org.broadinstitute.dsde.workbench.leonardo.http.ctxConversion

class DataprocRuntimeMonitor[F[_]: Parallel](
  config: DataprocMonitorConfig,
  googleComputeService: GoogleComputeService[F],
  authProvider: LeoAuthProvider[F],
  googleStorageService: GoogleStorageService[F],
  googleDiskService: GoogleDiskService[F],
  override val runtimeAlg: RuntimeAlgebra[F],
  googleDataprocService: GoogleDataprocService[F]
)(implicit
  override val dbRef: DbReference[F],
  override val runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[F, RuntimeContainerServiceType],
  override val F: Async[F],
  override val parallel: Parallel[F],
  override val logger: StructuredLogger[F],
  override val ec: ExecutionContext,
  override val openTelemetry: OpenTelemetryMetrics[F]
) extends BaseCloudServiceRuntimeMonitor[F] {
  override val googleStorage: GoogleStorageService[F] = googleStorageService
  override val monitorConfig: MonitorConfig = config
  override def googleDisk: GoogleDiskService[F] = googleDiskService

  /**
   * Queries Google for the cluster status and takes appropriate action depending on the result.
   * @return ClusterMonitorMessage
   */
  override def handleCheck(monitorContext: MonitorContext,
                           runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                           checkToolsInterruptAfter: Option[FiniteDuration]
  )(implicit
    ev: Ask[F, AppContext]
  ): F[CheckResult] =
    for {
      ctx <- ev.ask
      dataprocConfig <- runtimeAndRuntimeConfig.runtimeConfig match {
        case x: RuntimeConfig.DataprocConfig => F.pure(x)
        case _ =>
          F.raiseError[RuntimeConfig.DataprocConfig](
            new LeoException("DataprocRuntimeMonitor should not get a GCE request", traceId = Some(ctx.traceId))
          )
      }
      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(runtimeAndRuntimeConfig.runtime.cloudContext),
        new RuntimeException("this should never happen. Dataproc runtime's cloud context should be a google project")
      )
      cluster <- googleDataprocService
        .getCluster(
          googleProject,
          dataprocConfig.region,
          DataprocClusterName(runtimeAndRuntimeConfig.runtime.runtimeName.asString)
        )
        .recoverWith { case _: com.google.api.gax.rpc.PermissionDeniedException =>
          logger
            .info(ctx.loggingCtx)(s"Leo SA can't access the project. ${googleProject} might've been deleted.")
            .as(None)
        }
      result <- runtimeAndRuntimeConfig.runtime.status match {
        case RuntimeStatus.Creating =>
          creatingRuntime(cluster,
                          monitorContext,
                          runtimeAndRuntimeConfig.runtime,
                          dataprocConfig,
                          checkToolsInterruptAfter
          )
        case RuntimeStatus.Deleting =>
          deletedRuntime(cluster, monitorContext, runtimeAndRuntimeConfig)
        case RuntimeStatus.Starting =>
          startingRuntime(cluster, monitorContext, runtimeAndRuntimeConfig)
        case RuntimeStatus.Updating =>
          updatingRuntime(cluster, monitorContext, runtimeAndRuntimeConfig)
        case RuntimeStatus.Stopping =>
          stoppingRuntime(cluster, monitorContext, runtimeAndRuntimeConfig)
        case status =>
          logger
            .error(monitorContext.loggingContext)(
              s"${status} is not a transition status for GCE; hence no need to monitor"
            )
            .as(((), None))
      }
    } yield result

  private[monitor] def creatingRuntime(
    cluster: Option[Cluster],
    monitorContext: MonitorContext,
    runtime: Runtime,
    dataprocConfig: RuntimeConfig.DataprocConfig,
    checkToolsInterruptAfter: Option[FiniteDuration]
  )(implicit ev: Ask[F, AppContext]): F[CheckResult] = {
    val runtimeAndRuntimeConfig = RuntimeAndRuntimeConfig(runtime, dataprocConfig)
    cluster match {
      case None =>
        checkAgain(monitorContext, runtimeAndRuntimeConfig, None, Some(s"Can't retrieve cluster yet"))
      case Some(c) =>
        for {
          ctx <- ev.ask

          googleProject <- F.fromOption(
            LeoLenses.cloudContextToGoogleProject.get(runtimeAndRuntimeConfig.runtime.cloudContext),
            new RuntimeException(
              "this should never happen. Dataproc runtime's cloud context should be a google project"
            )
          )
          fetchInstances = getDataprocInstances(c, googleProject).map(x => x.map(_._1))

          runtimeStatus = DataprocClusterStatus
            .withNameInsensitiveOption(c.getStatus.getState.name())
            .getOrElse(DataprocClusterStatus.Unknown) // TODO: this needs to be verified
          r <- runtimeStatus match {
            case DataprocClusterStatus.Creating | DataprocClusterStatus.Unknown =>
              checkAgain(monitorContext,
                         runtimeAndRuntimeConfig,
                         Some(fetchInstances),
                         Some(s"Cluster is still in creating")
              )
            case DataprocClusterStatus.Running =>
              for {
                dataprocAndComputeInstances <- getDataprocInstances(c, googleProject)
                instances = dataprocAndComputeInstances.map(_._1)
                r <-
                  if (instances.exists(_.status != GceInstanceStatus.Running))
                    checkAgain(monitorContext,
                               runtimeAndRuntimeConfig,
                               Some(fetchInstances),
                               Some(s"Not all instances for this cluster is Running yet")
                    )
                  else {
                    // Note we don't need to check startup script results here because Dataproc
                    // won't transition the cluster to Running if a startup script failed.

                    val masterInstance = instances.find(_.dataprocRole == DataprocRole.Master)

                    masterInstance match {
                      case Some(dataprocInstance) =>
                        dataprocInstance.ip match {
                          case Some(ip) =>
                            // It takes a bit for jupyter to startup, hence wait 5 seconds before we check jupyter
                            F.sleep(8 seconds) >> handleCheckTools(monitorContext,
                                                                   runtimeAndRuntimeConfig,
                                                                   ip,
                                                                   masterInstance,
                                                                   true,
                                                                   checkToolsInterruptAfter
                            )
                          case None =>
                            checkAgain(monitorContext,
                                       runtimeAndRuntimeConfig,
                                       Some(fetchInstances),
                                       Some("Could not retrieve instance IP")
                            )
                        }
                      case None =>
                        failedRuntime(
                          monitorContext,
                          runtimeAndRuntimeConfig,
                          RuntimeErrorDetails(s"Can't find master instance for this cluster",
                                              shortMessage = Some("dataproc_no_master_instance")
                          ),
                          instances.find(_.dataprocRole == DataprocRole.Master)
                        )
                    }
                  }
              } yield r

            case DataprocClusterStatus.Error =>
              for {
                dataprocAndComputeInstances <- getDataprocInstances(c, googleProject)
                instances = dataprocAndComputeInstances.map(_._1)

                userScriptOutputFile = runtime.asyncRuntimeFields
                  .map(_.stagingBucket)
                  .map(b => RuntimeTemplateValues.userScriptOutputUriPath(b))
                userStartupScriptOutputFile = dataprocAndComputeInstances
                  .find(_._1.dataprocRole == DataprocRole.Master)
                  .map(_._2)
                  .flatMap(getUserScript)

                validationResult <- validateBothScripts(
                  userScriptOutputFile,
                  userStartupScriptOutputFile,
                  runtime.userScriptUri,
                  runtime.startUserScriptUri
                )
                // If an error occurred in a user script, persist that error instead of the Dataproc error
                r <- validationResult match {
                  case UserScriptsValidationResult.Error(msg) =>
                    logger
                      .info(ctx.loggingCtx)(
                        s"${runtime.projectNameString} user script failed ${msg}"
                      ) >> failedRuntime(
                      monitorContext,
                      runtimeAndRuntimeConfig,
                      RuntimeErrorDetails(msg, shortMessage = Some("user_startup_script")),
                      instances.find(_.dataprocRole == DataprocRole.Master)
                    )
                  case _ =>
                    val operationName = runtime.asyncRuntimeFields.map(_.operationName)
                    for {
                      error <- operationName.flatTraverse(o =>
                        googleDataprocService.getClusterError(dataprocConfig.region, google2.OperationName(o.value))
                      )
                      r <- failedRuntime(
                        monitorContext,
                        runtimeAndRuntimeConfig,
                        error
                          .map(e => RuntimeErrorDetails(e.message, Some(e.code), Some("creation_error")))
                          .getOrElse(
                            RuntimeErrorDetails("Error not available", shortMessage = Some("creation_error"))
                          ),
                        instances.find(_.dataprocRole == DataprocRole.Master)
                      )
                    } yield r
                }
              } yield r
            case ss =>
              fetchInstances.flatMap(instances =>
                failedRuntime(
                  monitorContext,
                  runtimeAndRuntimeConfig,
                  RuntimeErrorDetails(s"unexpected Dataproc cluster status ${ss} when trying to creating an instance"),
                  instances.find(_.dataprocRole == DataprocRole.Master)
                )
              )
          }
        } yield r
    }
  }

  private[monitor] def startingRuntime(
    cluster: Option[Cluster],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: Ask[F, AppContext]): F[CheckResult] = cluster match {
    case None =>
      logger
        .error(monitorContext.loggingContext)(
          s"Fail to retrieve cluster when trying to start ${runtimeAndRuntimeConfig.runtime.projectNameString}"
        )
        .as(((), None)) // TODO: shall we delete runtime in this case?
    case Some(c) =>
      for {
        googleProject <- F.fromOption(
          LeoLenses.cloudContextToGoogleProject.get(runtimeAndRuntimeConfig.runtime.cloudContext),
          new RuntimeException("this should never happen. GCE runtime's cloud context should be a google project")
        )
        fetchInstances = getDataprocInstances(c, googleProject).map(x => x.map(_._1))
        dataprocAndComputeInstances <- getDataprocInstances(
          c,
          googleProject
        )
        instances = dataprocAndComputeInstances.map(_._1)
        clusterStatus = DataprocClusterStatus
          .withNameInsensitiveOption(c.getStatus.getState.name())
          .getOrElse(DataprocClusterStatus.Unknown) // TODO: this needs to be verified
        r <- clusterStatus match {
          case DataprocClusterStatus.Stopped | DataprocClusterStatus.Starting =>
            checkAgain(monitorContext,
                       runtimeAndRuntimeConfig,
                       Some(fetchInstances),
                       Some(s"Dataproc cluster is still STOPPED")
            )
          case DataprocClusterStatus.Running if instances.exists(_.status != GceInstanceStatus.Running) =>
            checkAgain(monitorContext,
                       runtimeAndRuntimeConfig,
                       Some(fetchInstances),
                       Some(s"Not all instances for this cluster is Running yet")
            )
          case DataprocClusterStatus.Running =>
            val main = dataprocAndComputeInstances
              .find(_._1.dataprocRole == DataprocRole.Master)
            // Check output if start user script, if defined
            val userStartupScriptOutputFile = main.map(_._2).flatMap(getUserScript)
            for {
              validationResult <- validateUserStartupScript(userStartupScriptOutputFile,
                                                            runtimeAndRuntimeConfig.runtime.startUserScriptUri
              )
              r <- validationResult match {
                case UserScriptsValidationResult.CheckAgain(msg) =>
                  checkAgain(monitorContext, runtimeAndRuntimeConfig, None, Some(msg))
                case UserScriptsValidationResult.Error(msg) =>
                  failedRuntime(monitorContext,
                                runtimeAndRuntimeConfig,
                                RuntimeErrorDetails(
                                  msg,
                                  shortMessage = Some("user_startup_script")
                                ),
                                None
                  )
                case UserScriptsValidationResult.Success =>
                  main.flatMap(_._1.ip) match {
                    case Some(ip) =>
                      // It takes a bit for jupyter to startup, hence wait 5 seconds before we check jupyter
                      F.sleep(8 seconds) >> handleCheckTools(monitorContext,
                                                             runtimeAndRuntimeConfig,
                                                             ip,
                                                             main.map(_._1),
                                                             false,
                                                             None
                      )
                    case None =>
                      checkAgain(monitorContext,
                                 runtimeAndRuntimeConfig,
                                 Some(fetchInstances),
                                 Some("Could not retrieve instance IP")
                      )
                  }
              }
            } yield r
          case DataprocClusterStatus.Error =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(s"Cluster failed to start", shortMessage = Some("cluster_fail_to_start")),
              instances.find(_.dataprocRole == DataprocRole.Master)
            )
          case ss =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(s"unexpected Cluster ${ss} when trying to start it",
                                  shortMessage = Some("unexpected_status")
              ),
              instances.find(_.dataprocRole == DataprocRole.Master)
            )
        }
      } yield r
  }

  private[monitor] def stoppingRuntime(
    cluster: Option[Cluster],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: Ask[F, AppContext]): F[CheckResult] = cluster match {
    case None =>
      val e = InvalidMonitorRequest(
        s"${monitorContext} | Can't stop an instance that hasn't been initialized yet or doesn't exist"
      )
      failedRuntime(
        monitorContext,
        runtimeAndRuntimeConfig,
        RuntimeErrorDetails(e.getMessage, shortMessage = Some("invalid_stopping")),
        None
      ) >> F.raiseError[CheckResult](e)
    case Some(c) =>
      for {
        googleProject <- F.fromOption(
          LeoLenses.cloudContextToGoogleProject.get(runtimeAndRuntimeConfig.runtime.cloudContext),
          new RuntimeException("this should never happen. Dataproc runtime's cloud context should be a google project")
        )
        fetchInstances = getDataprocInstances(c, googleProject).map(x => x.map(_._1))
        res <-
          if (c.getStatus.getState == com.google.cloud.dataproc.v1.ClusterStatus.State.STOPPED) {
            stopRuntime(runtimeAndRuntimeConfig, monitorContext)
          } else {
            for {
              instances <- fetchInstances
              r <-
                if (
                  instances
                    .forall(i => i.status == GceInstanceStatus.Stopped || i.status == GceInstanceStatus.Terminated)
                )
                  stopRuntime(runtimeAndRuntimeConfig, monitorContext)
                else
                  checkAgain(
                    monitorContext,
                    runtimeAndRuntimeConfig,
                    Some(fetchInstances),
                    Some(s"not all instances has been terminated yet.")
                  )
            } yield r
          }
      } yield res
  }

  private[monitor] def updatingRuntime(
    cluster: Option[Cluster],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: Ask[F, AppContext]): F[CheckResult] = cluster match {
    case None =>
      val e = InvalidMonitorRequest(
        s"${monitorContext} | Can't update an instance that hasn't been initialized yet or doesn't exist"
      )
      failedRuntime(
        monitorContext,
        runtimeAndRuntimeConfig,
        RuntimeErrorDetails(e.getMessage, shortMessage = Some("invalid_update")),
        None
      ) >> F.raiseError[CheckResult](e)
    case Some(c) =>
      for {
        project <- F.fromOption(
          LeoLenses.cloudContextToGoogleProject.get(runtimeAndRuntimeConfig.runtime.cloudContext),
          new RuntimeException("this should never happen. Dataproc runtime's cloud context should be a google project")
        )
        fetchInstances = getDataprocInstances(c, project).map(x => x.map(_._1))
        instances <- fetchInstances
        clusterStatus = DataprocClusterStatus
          .withNameInsensitiveOption(c.getStatus.getState.name())
          .getOrElse(DataprocClusterStatus.Unknown) // TODO: this needs to be verified
        r <- clusterStatus match {
          case DataprocClusterStatus.Updating =>
            checkAgain(monitorContext,
                       runtimeAndRuntimeConfig,
                       Some(fetchInstances),
                       Some(s"Dataproc cluster still being updated")
            )
          case DataprocClusterStatus.Running if instances.exists(_.status != GceInstanceStatus.Running) =>
            checkAgain(monitorContext,
                       runtimeAndRuntimeConfig,
                       Some(fetchInstances),
                       Some(s"Not all instances for this cluster is Running yet")
            )
          case DataprocClusterStatus.Running => // TODO: is this right? we can only start runtime if it's a Running dataproc cluster
            val main = instances.find(_.dataprocRole == DataprocRole.Master)
            main.flatMap(_.ip) match {
              case Some(ip) =>
                // It takes a bit for jupyter to startup, hence wait a few seconds before we check jupyter
                F.sleep(3 seconds) >> handleCheckTools(monitorContext, runtimeAndRuntimeConfig, ip, main, false, None)
              case None =>
                checkAgain(monitorContext,
                           runtimeAndRuntimeConfig,
                           Some(fetchInstances),
                           Some("Could not retrieve instance IP")
                )
            }
          case DataprocClusterStatus.Error =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(s"Cluster failed to Update", shortMessage = Some("fail_to_update")),
              instances.find(_.dataprocRole == DataprocRole.Master)
            )
          case ss =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(s"unexpected Cluster ${ss} when trying to start an instance",
                                  shortMessage = Some("unexpected_status")
              ),
              instances.find(_.dataprocRole == DataprocRole.Master)
            )
        }
      } yield r
  }

  private[monitor] def deletedRuntime(cluster: Option[Cluster],
                                      monitorContext: MonitorContext,
                                      runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit
    ev: Ask[F, AppContext]
  ): F[CheckResult] =
    cluster match {
      case Some(c) =>
        for {
          googleProject <- F.fromOption(
            LeoLenses.cloudContextToGoogleProject.get(runtimeAndRuntimeConfig.runtime.cloudContext),
            new RuntimeException(
              "this should never happen. Dataproc runtime's cloud context should be a google project"
            )
          )
          fetchInstances = getDataprocInstances(c, googleProject).map(x => x.map(_._1))
          r <- checkAgain(monitorContext,
                          runtimeAndRuntimeConfig,
                          Some(fetchInstances),
                          Some("Instance hasn't been deleted yet")
          )
        } yield r
      case None =>
        for {
          ctx <- ev.ask
          duration = (ctx.now.toEpochMilli - monitorContext.start.toEpochMilli).millis

          googleProject <- F.fromOption(
            LeoLenses.cloudContextToGoogleProject.get(runtimeAndRuntimeConfig.runtime.cloudContext),
            new RuntimeException(
              "this should never happen. Dataproc runtime's cloud context should be a google project"
            )
          )
          // delete the init bucket so we don't continue to accrue costs after cluster is deleted
          _ <- deleteInitBucket(googleProject, runtimeAndRuntimeConfig.runtime.runtimeName)

          // set the staging bucket to be deleted in ten days so that logs are still accessible until then
          _ <- setStagingBucketLifecycle(runtimeAndRuntimeConfig.runtime,
                                         config.runtimeBucketConfig.stagingBucketExpiration
          )

          _ <- dbRef.inTransaction {
            clusterQuery.completeDeletion(runtimeAndRuntimeConfig.runtime.id, ctx.now)
          }

          _ <- logger.info(monitorContext.loggingContext)(
            s"Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} has been deleted after ${duration.toSeconds} seconds."
          )

          _ <- authProvider
            .notifyResourceDeleted(
              runtimeAndRuntimeConfig.runtime.samResource,
              runtimeAndRuntimeConfig.runtime.auditInfo.creator,
              googleProject
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
  )(implicit ev: Ask[F, AppContext]): F[Set[(DataprocInstance, Instance)]] =
    for {
      ctx <- ev.ask
      instances = GoogleDataprocInterpreter.getAllInstanceNames(cluster)
      zone = getZone(cluster)
      dataprocInstances <- zone.fold(F.pure(Set.empty[(DataprocInstance, Instance)])) { z =>
        instances.toList
          .flatTraverse { case (DataprocRoleZonePreemptibility(role, _, _), instances) =>
            instances.toList.traverseFilter { i =>
              googleComputeService.getInstance(googleProject, z, i).map {
                instanceOpt => // TODO: is this necessary? do we actually need to know all instance's IP?
                  instanceOpt.map { instance =>
                    (DataprocInstance(
                       DataprocInstanceKey(googleProject, z, i),
                       BigInt(instance.getId),
                       GceInstanceStatus.withNameInsensitive(instance.getStatus()),
                       getInstanceIP(instance),
                       role,
                       parseGoogleTimestamp(instance.getCreationTimestamp).getOrElse(ctx.now)
                     ),
                     instance
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
}
