package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.Parallel
import cats.effect.{Async, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Instance
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.DoneCheckableInstances.computeDoneCheckable
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, GoogleStorageService, InstanceName}
import org.broadinstitute.dsde.workbench.leonardo.GceInstanceStatus._
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.getInstanceIP
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorConfig.GceMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeMonitor._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class GceRuntimeMonitor[F[_]: Parallel](
  config: GceMonitorConfig,
  googleComputeService: GoogleComputeService[F],
  authProvider: LeoAuthProvider[F],
  googleStorageService: GoogleStorageService[F],
  override val runtimeAlg: RuntimeAlgebra[F]
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
  override def pollCheck(googleProject: GoogleProject,
                         runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                         operation: com.google.cloud.compute.v1.Operation,
                         action: RuntimeStatus)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      // building up a stream that will terminate when gce runtime is ready
      traceId <- ev.ask
      startMonitoring <- nowInstant
      monitorContext = MonitorContext(startMonitoring, runtimeAndRuntimeConfig.runtime.id, traceId, action)
      _ <- Timer[F].sleep(config.initialDelay)
      poll = googleComputeService
        .pollOperation(googleProject, operation, config.pollingInterval, config.pollCheckMaxAttempts)
        .compile
        .lastOrError
      op <- poll
      timeAfterPoll <- nowInstant
      implicit0(ctx: ApplicativeAsk[F, AppContext]) = ApplicativeAsk.const[F, AppContext](
        AppContext(traceId, timeAfterPoll)
      )
      _ <- action match {
        case RuntimeStatus.Deleting =>
          for {
            _ <- if (op.isDone) deletedRuntime(monitorContext, runtimeAndRuntimeConfig)
            else
              failedRuntime(
                monitorContext,
                runtimeAndRuntimeConfig,
                Some(
                  RuntimeErrorDetails(
                    s"${action} ${runtimeAndRuntimeConfig.runtime.projectNameString} fail to complete in a timely manner"
                  )
                ),
                Set.empty
              )
          } yield ()
        case RuntimeStatus.Stopping =>
          for {
            _ <- if (op.isDone) stopRuntime(runtimeAndRuntimeConfig, Set.empty, monitorContext)
            else
              failedRuntime(
                monitorContext,
                runtimeAndRuntimeConfig,
                Some(
                  RuntimeErrorDetails(
                    s"${action} ${runtimeAndRuntimeConfig.runtime.projectNameString} fail to complete in a timely manner",
                    labels = Map("transition" -> "starting")
                  )
                ),
                Set.empty
              )
          } yield ()
        case x =>
          F.raiseError(new Exception(s"Monitoring ${x} with pollOperation is not supported"))
      }
    } yield ()

  /**
   * Queries Google for the cluster status and takes appropriate action depending on the result.
   * @return ClusterMonitorMessage
   */
  override def handleCheck(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[CheckResult] =
    for {
      instance <- googleComputeService.getInstance(
        runtimeAndRuntimeConfig.runtime.googleProject,
        config.gceZoneName,
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
          logger.error(s"${status} is not a transition status for GCE; hence no need to monitor").as(((), None))
      }
    } yield result

  private[monitor] def creatingRuntime(
    instance: Option[Instance],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[CheckResult] = instance match {
    case None =>
      checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some(s"Can't retrieve instance yet"))
    case Some(i) =>
      for {
        gceStatus <- F.fromEither(
          GceInstanceStatus
            .withNameInsensitiveOption(i.getStatus)
            .toRight(new Exception(s"Unknown GCE instance status ${i.getStatus}"))
        ) //TODO: Ideally we should have sentry report this case
        r <- gceStatus match {
          case GceInstanceStatus.Provisioning | GceInstanceStatus.Staging =>
            checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some(s"Instance is still in ${gceStatus}"))
          case GceInstanceStatus.Running =>
            val userScriptOutputFile = runtimeAndRuntimeConfig.runtime.asyncRuntimeFields
              .map(_.stagingBucket)
              .map(b => RuntimeTemplateValues.jupyterUserScriptOutputUriPath(b))

            val userStartupScriptOutputFile = getUserScript(i)

            for {
              validationResult <- validateBothScripts(
                userScriptOutputFile,
                userStartupScriptOutputFile,
                runtimeAndRuntimeConfig.runtime.jupyterUserScriptUri,
                runtimeAndRuntimeConfig.runtime.jupyterStartUserScriptUri
              )
              r <- validationResult match {
                case UserScriptsValidationResult.CheckAgain(msg) =>
                  checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some(msg))
                case UserScriptsValidationResult.Error(msg) =>
                  failedRuntime(monitorContext, runtimeAndRuntimeConfig, Some(RuntimeErrorDetails(msg)), Set.empty)
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
              Some(RuntimeErrorDetails(s"unexpected GCE instance status ${ss} when trying to creating an instance")),
              Set.empty
            )
        }
      } yield r
  }

  private def startingRuntime(
    instance: Option[Instance],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[CheckResult] = instance match {
    case None =>
      logger
        .error(
          s"${monitorContext} | Fail retrieve instance status when trying to start runtime ${runtimeAndRuntimeConfig.runtime.projectNameString}"
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
          GceInstanceStatus.Terminated,
          GceInstanceStatus.Suspended,
          GceInstanceStatus.Stopping
        )
        r <- gceStatus match {
          case s if (startableStatuses.contains(s)) =>
            checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some(s"Instance is still in ${gceStatus}"))
          case GceInstanceStatus.Running =>
            val userStartupScript = getUserScript(i)

            for {
              validationResult <- validateUserStartupScript(userStartupScript,
                                                            runtimeAndRuntimeConfig.runtime.jupyterStartUserScriptUri)
              r <- validationResult match {
                case UserScriptsValidationResult.CheckAgain(msg) =>
                  checkAgain(monitorContext, runtimeAndRuntimeConfig, Set.empty, Some(msg))
                case UserScriptsValidationResult.Error(msg) =>
                  failedRuntime(monitorContext,
                                runtimeAndRuntimeConfig,
                                Some(
                                  RuntimeErrorDetails(
                                    msg
                                  )
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
              Some(RuntimeErrorDetails(s"unexpected GCE instance status ${ss} when trying to start an instance")),
              Set.empty
            )
        }
      } yield r
  }

  private def stoppingRuntime(
    instance: Option[Instance],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[CheckResult] = {
    val gceStatus = instance.flatMap(i => GceInstanceStatus.withNameInsensitiveOption(i.getStatus))
    gceStatus match {
      case None =>
        logger
          .error(s"${monitorContext} | Can't stop an instance that hasn't been initialized yet or doesn't exist")
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
        //save cluster error in the DB
        saveClusterError(runtimeAndRuntimeConfig.runtime.id,
                         errorDetails.map(_.longMessage).getOrElse("No error available"),
                         errorDetails.flatMap(_.code).getOrElse(-1),
                         ctx.now)
      ).parSequence_

      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(
        monitorContext.start,
        getRuntimeUI(runtimeAndRuntimeConfig.runtime),
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
        "errorCode" -> errorDetails.map(_.shortMessage.getOrElse("leonardo")).getOrElse("leonardo")
      )
      _ <- openTelemetry.incrementCounter(s"runtimeCreationFailure", 1, tags)
    } yield ((), None): CheckResult

  private def deletedRuntime(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[CheckResult] =
    for {
      ctx <- ev.ask
      duration = (ctx.now.toEpochMilli - monitorContext.start.toEpochMilli).millis
      _ <- logger.info(
        s"${monitorContext} | Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} has been deleted after ${duration.toSeconds} seconds."
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

  private[monitor] def validateBothScripts(
    userScriptOutputFile: Option[GcsPath],
    userStartupScriptOutputFile: Option[GcsPath],
    jupyterUserScriptUriInDB: Option[UserScriptPath],
    jupyterStartUserScriptUriInDB: Option[UserScriptPath]
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[UserScriptsValidationResult] =
    for {
      userScriptRes <- validateUserScript(userScriptOutputFile, jupyterUserScriptUriInDB)
      res <- userScriptRes match {
        case UserScriptsValidationResult.Success =>
          validateUserStartupScript(userStartupScriptOutputFile, jupyterStartUserScriptUriInDB)
        case x: UserScriptsValidationResult.Error =>
          F.pure(x)
        case x: UserScriptsValidationResult.CheckAgain =>
          F.pure(x)
      }
    } yield res

  private[monitor] def validateUserScript(
    userScriptOutputPathFromDB: Option[GcsPath],
    jupyterUserScriptUriInDB: Option[UserScriptPath]
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[UserScriptsValidationResult] =
    (userScriptOutputPathFromDB, jupyterUserScriptUriInDB) match {
      case (Some(output), Some(_)) =>
        checkUserScriptsOutputFile(output).map { o =>
          o match {
            case Some(true)  => UserScriptsValidationResult.Success
            case Some(false) => UserScriptsValidationResult.Error(s"user script ${output.toUri} failed")
            case None        => UserScriptsValidationResult.CheckAgain(s"user script ${output.toUri} hasn't finished yet")
          }
        }
      case (None, Some(_)) =>
        for {
          ctx <- ev.ask
          r <- F.raiseError[UserScriptsValidationResult](
            new Exception(s"${ctx} | staging bucket field hasn't been updated properly before monitoring started")
          ) // worth error reporting
        } yield r
      case (_, None) =>
        F.pure(UserScriptsValidationResult.Success)
    }

  private[monitor] def validateUserStartupScript(
    userStartupScriptOutputFile: Option[GcsPath],
    jupyterStartUserScriptUriInDB: Option[UserScriptPath]
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[UserScriptsValidationResult] =
    (userStartupScriptOutputFile, jupyterStartUserScriptUriInDB) match {
      case (Some(output), Some(_)) =>
        checkUserScriptsOutputFile(output).map { o =>
          o match {
            case Some(true)  => UserScriptsValidationResult.Success
            case Some(false) => UserScriptsValidationResult.Error(s"user startup script ${output.toUri} failed")
            case None =>
              UserScriptsValidationResult.CheckAgain(s"user startup script ${output.toUri} hasn't finished yet")
          }
        }
      case (None, Some(_)) =>
        for {
          ctx <- ev.ask
          r <- F.pure(UserScriptsValidationResult.CheckAgain(s"${ctx} | Instance is not ready yet"))
        } yield r
      case (_, None) =>
        F.pure(UserScriptsValidationResult.Success)
    }
}

sealed abstract class UserScriptsValidationResult extends Product with Serializable
object UserScriptsValidationResult {
  final case class CheckAgain(msg: String) extends UserScriptsValidationResult
  final case class Error(msg: String) extends UserScriptsValidationResult
  case object Success extends UserScriptsValidationResult
}
