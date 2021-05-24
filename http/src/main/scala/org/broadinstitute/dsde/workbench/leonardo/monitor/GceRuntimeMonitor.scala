package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.sql.SQLDataException

import cats.Parallel
import cats.effect.{Async, Timer}
import cats.mtl.Ask
import cats.syntax.all._
import com.google.cloud.compute.v1.Instance
import fs2.Stream
import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.google2.{
  ComputePollOperation,
  GoogleComputeService,
  GoogleStorageService,
  InstanceName
}
import org.broadinstitute.dsde.workbench.leonardo.GceInstanceStatus._
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.getInstanceIP
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.DeleteRuntimeMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorConfig.GceMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeMonitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class GceRuntimeMonitor[F[_]: Parallel](
  config: GceMonitorConfig,
  googleComputeService: GoogleComputeService[F],
  computePollOperation: ComputePollOperation[F],
  authProvider: LeoAuthProvider[F],
  googleStorageService: GoogleStorageService[F],
  publisherQueue: fs2.concurrent.Queue[F, LeoPubsubMessage],
  override val runtimeAlg: RuntimeAlgebra[F]
)(implicit override val dbRef: DbReference[F],
  override val runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[F, RuntimeContainerServiceType],
  override val F: Async[F],
  override val parallel: Parallel[F],
  override val timer: Timer[F],
  override val logger: StructuredLogger[F],
  override val ec: ExecutionContext,
  override val openTelemetry: OpenTelemetryMetrics[F])
    extends BaseCloudServiceRuntimeMonitor[F] {

  override val googleStorage: GoogleStorageService[F] = googleStorageService
  override val monitorConfig: MonitorConfig = config

  val pollCheckSupportedStatuses = Set(RuntimeStatus.Deleting, RuntimeStatus.Stopping)
  // Function used for transitions that we can get an Operation
  override def pollCheck(googleProject: GoogleProject,
                         runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                         operation: com.google.cloud.compute.v1.Operation,
                         action: RuntimeStatus)(implicit ev: Ask[F, TraceId]): F[Unit] =
    for {
      // building up a stream that will terminate when gce runtime is ready
      traceId <- ev.ask
      startMonitoring <- nowInstant
      monitorContext = MonitorContext(startMonitoring, runtimeAndRuntimeConfig.runtime.id, traceId, action)
      _ <- Timer[F].sleep(config.initialDelay)
      haltWhenTrue = (Stream
        .eval(clusterQuery.getClusterStatus(runtimeAndRuntimeConfig.runtime.id).transaction.map { newStatus =>
          if (action == RuntimeStatus.Deleting) // Deleting is not interruptible
            false
          else {
            newStatus != Some(action) && newStatus == Some(
              RuntimeStatus.Deleting
            ) // Interrupt Stopping if new Deleting request comes in
          }
        }) ++ Stream.sleep_(5 seconds)).repeat
      _ <- if (pollCheckSupportedStatuses.contains(action))
        F.unit
      else F.raiseError(new Exception(s"Monitoring ${action} with pollOperation is not supported"))
      _ <- computePollOperation
        .pollOperation(googleProject,
                       operation,
                       config.pollingInterval,
                       config.pollCheckMaxAttempts,
                       Some(haltWhenTrue))(
          handlePollCheckCompletion(monitorContext, runtimeAndRuntimeConfig),
          handlePollCheckTimeout(monitorContext, runtimeAndRuntimeConfig),
          handlePollCheckWhenInterrupted(monitorContext, runtimeAndRuntimeConfig)
        )
    } yield ()

  def handlePollCheckCompletion(monitorContext: MonitorContext,
                                runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig): F[Unit] =
    for {
      timeAfterPoll <- nowInstant
      implicit0(ctx: Ask[F, AppContext]) = Ask.const[F, AppContext](
        AppContext(monitorContext.traceId, timeAfterPoll)
      )
      _ <- monitorContext.action match {
        case RuntimeStatus.Deleting =>
          deletedRuntime(monitorContext, runtimeAndRuntimeConfig)
        case RuntimeStatus.Stopping =>
          stopRuntime(runtimeAndRuntimeConfig, Set.empty, monitorContext)
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
          s"${monitorContext.action} ${runtimeAndRuntimeConfig.runtime.projectNameString} fail to complete in a timely manner"
        ),
        Set.empty
      )
    } yield ()

  def handlePollCheckWhenInterrupted(monitorContext: MonitorContext,
                                     runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig): F[Unit] =
    for {
      newStatus <- clusterQuery.getClusterStatus(runtimeAndRuntimeConfig.runtime.id).transaction
      timeWhenInterrupted <- nowInstant
      _ <- (monitorContext.action, newStatus) match {
        case (RuntimeStatus.Stopping, Some(RuntimeStatus.Deleting)) =>
          clusterQuery
            .updateClusterStatus(runtimeAndRuntimeConfig.runtime.id, RuntimeStatus.PreDeleting, timeWhenInterrupted)
            .transaction >> publisherQueue
            .enqueue1(
              DeleteRuntimeMessage(runtimeAndRuntimeConfig.runtime.id, None, Some(monitorContext.traceId))
              //Known limitation, we set deleteDisk is falsehere; but in reality, it could be `true`
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
  override def handleCheck(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
    implicit ev: Ask[F, AppContext]
  ): F[CheckResult] =
    for {
      zoneParam <- F.fromOption(
        LeoLenses.gceZone.getOption(runtimeAndRuntimeConfig.runtimeConfig),
        new RuntimeException(
          "GceRuntimeMonitor shouldn't get a dataproc runtime creation request. Something is very wrong"
        )
      )

      instance <- googleComputeService.getInstance(
        runtimeAndRuntimeConfig.runtime.googleProject,
        zoneParam,
        InstanceName(runtimeAndRuntimeConfig.runtime.runtimeName.asString)
      )
      result <- runtimeAndRuntimeConfig.runtime.status match {
        case RuntimeStatus.Creating =>
          creatingRuntime(instance, monitorContext, runtimeAndRuntimeConfig)
        // This is needed because during boot time, we no longer have reference to the previous delete operation
        // But this path should only happen during boot and there's on-going delete
        case RuntimeStatus.Deleting =>
          instance match {
            case None => deletedRuntime(monitorContext, runtimeAndRuntimeConfig)
            case _ =>
              checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some("Instance hasn't been deleted yet"))
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
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: Ask[F, AppContext]): F[CheckResult] = instance match {
    case None =>
      checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some(s"Can't retrieve instance yet"))
    case Some(i) =>
      for {
        context <- ev.ask
        gceStatus <- F.fromOption(GceInstanceStatus
                                    .withNameInsensitiveOption(i.getStatus),
                                  new SQLDataException(s"Unknown GCE instance status ${i.getStatus}"))
        r <- gceStatus match {
          case GceInstanceStatus.Provisioning | GceInstanceStatus.Staging =>
            checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some(s"Instance is still in ${gceStatus}"))
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
                  checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some(msg))
                case UserScriptsValidationResult.Error(msg) =>
                  logger
                    .info(context.loggingCtx)(
                      s"${runtimeAndRuntimeConfig.runtime.projectNameString} user script failed ${msg}"
                    ) >> failedRuntime(
                    monitorContext,
                    runtimeAndRuntimeConfig,
                    RuntimeErrorDetails(msg, shortMessage = Some("user_startup_script")),
                    Set.empty
                  )
                case UserScriptsValidationResult.Success =>
                  getInstanceIP(i) match {
                    case Some(ip) =>
                      // It takes a bit for jupyter to startup, hence wait 5 seconds before we check jupyter
                      Timer[F]
                        .sleep(8 seconds) >> handleCheckTools(monitorContext, runtimeAndRuntimeConfig, ip, Set.empty)
                    case None =>
                      checkAgain(monitorContext,
                                 runtimeAndRuntimeConfig,
                                 Set.empty,
                                 Some("Could not retrieve instance IP"))
                  }
              }
            } yield r
          case ss =>
            logger.info(context.loggingCtx)(
              s"Going to delete runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} due to unexpected status ${ss}"
            ) >> failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(s"unexpected GCE instance status ${ss} when trying to creating an instance"),
              Set.empty
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
                             Set.empty,
                             Some(s"Instance is still in ${GceInstanceStatus.Terminated}"))
              }

          case s if (startableStatuses.contains(s)) =>
            checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some(s"Instance is still in ${gceStatus}"))
          case GceInstanceStatus.Running =>
            val userStartupScript = getUserScript(i)

            for {
              validationResult <- validateUserStartupScript(userStartupScript,
                                                            runtimeAndRuntimeConfig.runtime.startUserScriptUri)
              r <- validationResult match {
                case UserScriptsValidationResult.CheckAgain(msg) =>
                  checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some(msg))
                case UserScriptsValidationResult.Error(msg) =>
                  failedRuntime(monitorContext,
                                runtimeAndRuntimeConfig,
                                RuntimeErrorDetails(
                                  msg,
                                  shortMessage = Some("user_startup_script")
                                ),
                                Set.empty)
                case UserScriptsValidationResult.Success =>
                  getInstanceIP(i) match {
                    case Some(ip) =>
                      // It takes a bit for jupyter to startup, hence wait 5 seconds before we check jupyter
                      Timer[F]
                        .sleep(8 seconds) >> handleCheckTools(monitorContext, runtimeAndRuntimeConfig, ip, Set.empty)
                    case None =>
                      checkAgain(monitorContext,
                                 runtimeAndRuntimeConfig,
                                 Set.empty,
                                 Some("Could not retrieve instance IP"))
                  }
              }
            } yield r
          case ss =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(s"unexpected GCE instance status ${ss} when trying to start an instance"),
              Set.empty
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
          .as(((), None)) //TODO: Ideally we should have sentry report this case
      case Some(s) =>
        s match {
          case Stopped | Terminated =>
            stopRuntime(runtimeAndRuntimeConfig, Set.empty, monitorContext)
          case _ =>
            checkAgain(
              monitorContext,
              runtimeAndRuntimeConfig,
              Set.empty,
              Some(s"hasn't been terminated yet"),
              None
            )
        }
    }
  }

  private def deletedRuntime(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
    implicit ev: Ask[F, AppContext]
  ): F[CheckResult] =
    for {
      ctx <- ev.ask
      duration = (ctx.now.toEpochMilli - monitorContext.start.toEpochMilli).millis
      _ <- logger.info(monitorContext.loggingContext)(
        s"Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} has been deleted after ${duration.toSeconds} seconds."
      )

      // delete the init bucket so we don't continue to accrue costs after cluster is deleted
      _ <- deleteInitBucket(runtimeAndRuntimeConfig.runtime.googleProject, runtimeAndRuntimeConfig.runtime.runtimeName)

      // set the staging bucket to be deleted in ten days so that logs are still accessible until then
      _ <- setStagingBucketLifecycle(runtimeAndRuntimeConfig.runtime,
                                     config.runtimeBucketConfig.stagingBucketExpiration)

      _ <- dbRef.inTransaction {
        clusterQuery.completeDeletion(runtimeAndRuntimeConfig.runtime.id, ctx.now)
      }

      _ <- authProvider
        .notifyResourceDeleted(
          runtimeAndRuntimeConfig.runtime.samResource,
          runtimeAndRuntimeConfig.runtime.auditInfo.creator,
          runtimeAndRuntimeConfig.runtime.googleProject
        )

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
