package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.Parallel
import cats.effect.std.Queue
import cats.effect.{Async, Temporal}
import cats.mtl.Ask
import cats.syntax.all._
import com.google.cloud.compute.v1.Instance
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, GoogleDiskService, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo.GceInstanceStatus._
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.getInstanceIP
import org.broadinstitute.dsde.workbench.leonardo.dao.sam.SamService
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{ctxConversion, dbioToIO}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.DeleteRuntimeMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorConfig.GceMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeMonitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util2.InstanceName
import org.typelevel.log4cats.StructuredLogger

import java.sql.SQLDataException
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class GceRuntimeMonitor[F[_]: Parallel](
  config: GceMonitorConfig,
  googleComputeService: GoogleComputeService[F],
  authProvider: LeoAuthProvider[F],
  googleStorageService: GoogleStorageService[F],
  googleDiskService: GoogleDiskService[F],
  publisherQueue: Queue[F, LeoPubsubMessage],
  override val runtimeAlg: RuntimeAlgebra[F],
  samService: SamService[F]
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

  def handlePollCheckCompletion(monitorContext: MonitorContext,
                                runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  ): F[Unit] =
    for {
      timeAfterPoll <- nowInstant
      implicit0(ctx: Ask[F, AppContext]) = Ask.const[F, AppContext](
        AppContext(monitorContext.traceId, timeAfterPoll)
      )
      _ <- monitorContext.action match {
        case RuntimeStatus.Deleting =>
          deletedRuntime(monitorContext, runtimeAndRuntimeConfig)
        case RuntimeStatus.Stopping =>
          stopRuntime(runtimeAndRuntimeConfig, monitorContext)
        case x =>
          F.raiseError(new Exception(s"Monitoring ${x} with pollOperation is not supported"))
      }
    } yield ()

  def handlePollCheckTimeout(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig) =
    for {
      timeAfterPoll <- nowInstant
      implicit0(ctx: Ask[F, AppContext]) = Ask.const[F, AppContext](
        AppContext(monitorContext.traceId, timeAfterPoll)
      )
      _ <- failedRuntime(
        monitorContext,
        runtimeAndRuntimeConfig,
        RuntimeErrorDetails(
          s"${monitorContext.action} ${runtimeAndRuntimeConfig.runtime.projectNameString} fail to complete in a timely manner",
          shortMessage = Some("timeout")
        ),
        None
      )
    } yield ()

  def handlePollCheckWhenInterrupted(monitorContext: MonitorContext,
                                     runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  ): F[Unit] =
    for {
      newStatus <- clusterQuery.getClusterStatus(runtimeAndRuntimeConfig.runtime.id).transaction
      timeWhenInterrupted <- nowInstant
      _ <- (monitorContext.action, newStatus) match {
        case (RuntimeStatus.Stopping, Some(RuntimeStatus.Deleting)) =>
          clusterQuery
            .updateClusterStatus(runtimeAndRuntimeConfig.runtime.id, RuntimeStatus.PreDeleting, timeWhenInterrupted)
            .transaction >> publisherQueue
            .offer(
              DeleteRuntimeMessage(runtimeAndRuntimeConfig.runtime.id, None, Some(monitorContext.traceId))
              // Known limitation, we set deleteDisk is falsehere; but in reality, it could be `true`
            )
        case _ =>
          F.unit
      }
      tags = Map("original_status" -> monitorContext.action.toString, "interrupted_by" -> newStatus.toString)
      _ <- openTelemetry.incrementCounter("earlyTerminationOfMonitoring", 1, tags)
      _ <- logger.warn(monitorContext.loggingContext)(
        s"status transitioned from ${monitorContext.action} -> ${newStatus}. This could be caused by a new status transition call!"
      )
    } yield ()

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
      zoneParam <- F.fromOption(
        LeoLenses.gceZone.getOption(runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException(
          "GceRuntimeMonitor shouldn't get a dataproc runtime creation request. Something is very wrong"
        )
      )
      project <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(runtimeAndRuntimeConfig.runtime.cloudContext),
        new RuntimeException("this should never happen. GCE runtime's cloud context should be a google project")
      )
      instance <- googleComputeService.getInstance(
        project,
        zoneParam,
        InstanceName(runtimeAndRuntimeConfig.runtime.runtimeName.asString)
      )
      result <- runtimeAndRuntimeConfig.runtime.status match {
        case RuntimeStatus.Creating =>
          creatingRuntime(instance, monitorContext, runtimeAndRuntimeConfig, checkToolsInterruptAfter)
        // This is needed because during boot time, we no longer have reference to the previous delete operation
        // But this path should only happen during boot and there's on-going delete
        case RuntimeStatus.Deleting =>
          instance match {
            case None => deletedRuntime(monitorContext, runtimeAndRuntimeConfig)
            case _ =>
              checkAgain(monitorContext, runtimeAndRuntimeConfig, None, Some("Instance hasn't been deleted yet"))
          }
        case RuntimeStatus.Starting =>
          startingRuntime(instance, monitorContext, runtimeAndRuntimeConfig)
        // This is needed because during boot time, we no longer have reference to the previous delete operation
        // But this path should only happen during boot and there's on-going delete
        case RuntimeStatus.Stopping =>
          stoppingRuntime(instance, monitorContext, runtimeAndRuntimeConfig)
        case status =>
          logger
            .error(monitorContext.loggingContext)(
              s"${status} is not a transition status for GCE; hence no need to monitor"
            )
            .as(((), None))
      }
    } yield result

  private[monitor] def creatingRuntime(
    instance: Option[Instance],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
    checkToolsInterruptAfter: Option[FiniteDuration]
  )(implicit ev: Ask[F, AppContext]): F[CheckResult] = instance match {
    case None =>
      nowInstant
        .flatMap { now =>
          if (now.toEpochMilli - monitorContext.start.toEpochMilli > 60000)
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(
                "Creation may have failed due to temporary resource unavailability in Google Cloud Platform (`ZONE_RESOURCE_POOL_EXHAUSTED` error). Please try again later or refer to http://broad.io/different-zone for creating a cloud environment in a different zone.",
                shortMessage = Some("fail_to_create")
              ),
              None
            )
          else
            checkAgain(monitorContext, runtimeAndRuntimeConfig, None, Some(s"Can't retrieve instance yet"))
        }
    case Some(i) =>
      for {
        context <- ev.ask
        gceStatus <- F.fromOption(GceInstanceStatus
                                    .withNameInsensitiveOption(i.getStatus),
                                  new SQLDataException(s"Unknown GCE instance status ${i.getStatus}")
        )
        r <- gceStatus match {
          case GceInstanceStatus.Provisioning | GceInstanceStatus.Staging =>
            checkAgain(monitorContext, runtimeAndRuntimeConfig, None, Some(s"Instance is still in ${gceStatus}"))
          case GceInstanceStatus.Running =>
            val userScriptOutputFile = runtimeAndRuntimeConfig.runtime.asyncRuntimeFields
              .map(_.stagingBucket)
              .map(b => RuntimeTemplateValues.userScriptOutputUriPath(b))

            val userStartupScriptOutputFile = getUserScript(i)

            for {
              validationResult <- validateBothScripts(
                userScriptOutputFile,
                userStartupScriptOutputFile,
                runtimeAndRuntimeConfig.runtime.userScriptUri,
                runtimeAndRuntimeConfig.runtime.startUserScriptUri
              )
              r <- validationResult match {
                case UserScriptsValidationResult.CheckAgain(msg) =>
                  checkAgain(monitorContext, runtimeAndRuntimeConfig, None, Some(msg))
                case UserScriptsValidationResult.Error(msg) =>
                  logger
                    .info(context.loggingCtx)(
                      s"${runtimeAndRuntimeConfig.runtime.projectNameString} user script failed ${msg}"
                    ) >> failedRuntime(
                    monitorContext,
                    runtimeAndRuntimeConfig,
                    RuntimeErrorDetails(msg, shortMessage = Some("user_startup_script")),
                    None
                  )
                case UserScriptsValidationResult.Success =>
                  getInstanceIP(i) match {
                    case Some(ip) =>
                      // It takes a bit for jupyter to startup, hence wait 5 seconds before we check jupyter
                      Temporal[F]
                        .sleep(8 seconds) >> handleCheckTools(monitorContext,
                                                              runtimeAndRuntimeConfig,
                                                              ip,
                                                              None,
                                                              true,
                                                              checkToolsInterruptAfter
                      )
                    case None =>
                      checkAgain(monitorContext, runtimeAndRuntimeConfig, None, Some("Could not retrieve instance IP"))
                  }
              }
            } yield r
          case ss =>
            logger.info(context.loggingCtx)(
              s"Going to delete runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} due to unexpected status ${ss}"
            ) >> failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(s"unexpected GCE instance status ${ss} when trying to creating an instance",
                                  shortMessage = Some("unexpected_status")
              ),
              None
            )
        }
      } yield r
  }

  private def startingRuntime(
    instance: Option[Instance],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: Ask[F, AppContext]): F[CheckResult] = instance match {
    case None =>
      logger
        .error(monitorContext.loggingContext)(
          s"Fail retrieve instance status when trying to start runtime ${runtimeAndRuntimeConfig.runtime.projectNameString}"
        )
        .as(((), None))
    case Some(i) =>
      for {
        gceStatus <- F.fromEither(
          GceInstanceStatus
            .withNameInsensitiveOption(i.getStatus)
            .toRight(new Exception(s"Unknown GCE instance status ${i.getStatus}"))
        )
        startableStatuses = Set(
          GceInstanceStatus.Suspending,
          GceInstanceStatus.Provisioning,
          GceInstanceStatus.Staging,
          GceInstanceStatus.Suspended,
          GceInstanceStatus.Stopping
        )
        r <- gceStatus match {
          // This happens when a VM fails to start. Potential causes are: start up script failure,
          case GceInstanceStatus.Terminated =>
            nowInstant
              .flatMap { now =>
                if (now.toEpochMilli - monitorContext.start.toEpochMilli > 5000)
                  clusterQuery
                    .updateClusterStatusAndHostIp(runtimeAndRuntimeConfig.runtime.id, RuntimeStatus.Stopped, None, now)
                    .transaction
                    .as(((), None): CheckResult)
                else
                  checkAgain(monitorContext,
                             runtimeAndRuntimeConfig,
                             None,
                             Some(s"Instance is still in ${GceInstanceStatus.Terminated}")
                  )
              }

          case s if startableStatuses.contains(s) =>
            checkAgain(monitorContext, runtimeAndRuntimeConfig, None, Some(s"Instance is still in ${gceStatus}"))
          case GceInstanceStatus.Running =>
            val userStartupScript = getUserScript(i)

            for {
              validationResult <- validateUserStartupScript(userStartupScript,
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
                                None,
                                false
                  )
                case UserScriptsValidationResult.Success =>
                  getInstanceIP(i) match {
                    case Some(ip) =>
                      // It takes a bit for jupyter to startup, hence wait 5 seconds before we check jupyter
                      Temporal[F]
                        .sleep(8 seconds) >> handleCheckTools(monitorContext,
                                                              runtimeAndRuntimeConfig,
                                                              ip,
                                                              None,
                                                              false,
                                                              None
                      )
                    case None =>
                      checkAgain(monitorContext, runtimeAndRuntimeConfig, None, Some("Could not retrieve instance IP"))
                  }
              }
            } yield r
          case ss =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(s"unexpected GCE instance status ${ss} when trying to start an instance",
                                  shortMessage = Some("unexpected_status")
              ),
              None,
              false
            )
        }
      } yield r
  }

  private def stoppingRuntime(
    instance: Option[Instance],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: Ask[F, AppContext]): F[CheckResult] = {
    val gceStatus = instance.flatMap(i => GceInstanceStatus.withNameInsensitiveOption(i.getStatus))
    gceStatus match {
      case None =>
        logger
          .error(monitorContext.loggingContext)(
            s"Can't stop an instance that hasn't been initialized yet or doesn't exist"
          )
          .as(((), None)) // TODO: Ideally we should have sentry report this case
      case Some(s) =>
        s match {
          case Stopped | Terminated =>
            stopRuntime(runtimeAndRuntimeConfig, monitorContext)
          case _ =>
            checkAgain(
              monitorContext,
              runtimeAndRuntimeConfig,
              None,
              Some(s"hasn't been terminated yet"),
              None
            )
        }
    }
  }

  private def deletedRuntime(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(implicit
    ev: Ask[F, AppContext]
  ): F[CheckResult] =
    for {
      ctx <- ev.ask
      duration = (ctx.now.toEpochMilli - monitorContext.start.toEpochMilli).millis

      googleProject <- F.fromOption(
        LeoLenses.cloudContextToGoogleProject.get(runtimeAndRuntimeConfig.runtime.cloudContext),
        new RuntimeException("this should never happen. GCE runtime's cloud context should be a google project")
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

      // Delete the notebook-cluster Sam resource
      petToken <- samService.getPetServiceAccountToken(runtimeAndRuntimeConfig.runtime.auditInfo.creator, googleProject)
      _ <- samService.deleteResource(petToken, runtimeAndRuntimeConfig.runtime.samResource)

      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(
        monitorContext.start,
        getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Deleted,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )

    } yield ((), None)
}

sealed abstract class UserScriptsValidationResult extends Product with Serializable
object UserScriptsValidationResult {
  final case class CheckAgain(msg: String) extends UserScriptsValidationResult
  final case class Error(msg: String) extends UserScriptsValidationResult
  case object Success extends UserScriptsValidationResult
}
