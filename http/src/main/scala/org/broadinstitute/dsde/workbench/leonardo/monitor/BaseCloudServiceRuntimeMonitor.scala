package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.Parallel
import cats.effect.concurrent.Ref
import cats.effect.{Async, Sync, Timer}
import cats.mtl.Ask
import cats.syntax.all._
import com.google.cloud.storage.BucketInfo
import fs2.Stream
import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.google2.{streamFUntilDone, GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorState._
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeMonitor.{
  getRuntimeUI,
  recordStatusTransitionMetrics,
  CheckResult
}
import monocle.macros.syntax.lens._
import org.broadinstitute.dsde.workbench.leonardo.util.{DeleteRuntimeParams, RuntimeAlgebra, StopRuntimeParams}
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{IP, TraceId}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

abstract class BaseCloudServiceRuntimeMonitor[F[_]] {
  implicit def F: Async[F]
  implicit def parallel: Parallel[F]
  implicit def timer: Timer[F]
  implicit def dbRef: DbReference[F]
  implicit def ec: ExecutionContext
  implicit def runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[F, RuntimeContainerServiceType]
  implicit def openTelemetry: OpenTelemetryMetrics[F]

  implicit val toolsDoneCheckable: DoneCheckable[List[(RuntimeImageType, Boolean)]] = x => x.forall(_._2)

  def runtimeAlg: RuntimeAlgebra[F]
  def logger: StructuredLogger[F]
  def googleStorage: GoogleStorageService[F]

  def monitorConfig: MonitorConfig

  def process(runtimeId: Long, runtimeStatus: RuntimeStatus)(implicit ev: Ask[F, TraceId]): Stream[F, Unit] =
    for {
      // building up a stream that will terminate when gce runtime is ready
      traceId <- Stream.eval(ev.ask)
      startMonitoring <- Stream.eval(nowInstant[F])
      monitorContextRef <- Stream.eval(
        Ref.of[F, MonitorContext](MonitorContext(startMonitoring, runtimeId, traceId, runtimeStatus))
      )
      _ <- Stream.sleep(monitorConfig.initialDelay) ++ Stream.unfoldLoopEval[F, MonitorState, Unit](Initial) { s =>
        for {
          _ <- s.newTransition.traverse(newStatus => monitorContextRef.modify(x => (x.copy(action = newStatus), ())))
          monitorContext <- monitorContextRef.get
          _ <- Timer[F].sleep(monitorConfig.pollingInterval)
          res <- handler(
            monitorContext,
            s
          )
        } yield res
      }
    } yield ()

  def pollCheck(googleProject: GoogleProject,
                runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                operation: com.google.cloud.compute.v1.Operation,
                action: RuntimeStatus)(implicit ev: Ask[F, TraceId]): F[Unit]

  private[monitor] def handler(monitorContext: MonitorContext, monitorState: MonitorState): F[CheckResult] =
    for {
      now <- nowInstant
      ctx = AppContext(monitorContext.traceId, now)
      implicit0(ct: Ask[F, AppContext]) = Ask.const[F, AppContext](
        ctx
      )
      currentStatus <- clusterQuery.getClusterStatus(monitorContext.runtimeId).transaction
      res <- currentStatus match {
        case None =>
          logger
            .error(ctx.loggingCtx)(s"disappeared from database in the middle of status transition!")
            .as(((), None))
        case Some(status) if (status != monitorContext.action) =>
          val tags = Map("original_status" -> monitorContext.action.toString, "interrupted_by" -> status.toString)
          openTelemetry.incrementCounter("earlyTerminationOfMonitoring", 1, tags) >> logger
            .warn(ctx.loggingCtx)(
              s"status transitioned from ${monitorContext.action} -> ${status}. This could be caused by a new status transition call!"
            )
            .as(((), None))
        case Some(_) =>
          monitorState match {
            case MonitorState.Initial => handleInitial(monitorContext)
            case MonitorState.Check(runtimeAndRuntimeConfig, _) =>
              handleCheck(monitorContext, runtimeAndRuntimeConfig)
          }
      }
    } yield res

  def failedRuntime(monitorContext: MonitorContext,
                    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                    errorDetails: RuntimeErrorDetails,
                    instances: Set[DataprocInstance])(
    implicit ev: Ask[F, AppContext]
  ): F[CheckResult] =
    for {
      ctx <- ev.ask
      _ <- List(
        // Delete the cluster in Google
        runtimeAlg
          .deleteRuntime(
            DeleteRuntimeParams(runtimeAndRuntimeConfig)
          )
          .void, //TODO is this right when deleting or stopping fails?
        //save cluster error in the DB
        saveRuntimeError(
          runtimeAndRuntimeConfig.runtime.id,
          errorDetails
        ),
        if (instances.nonEmpty)
          clusterQuery
            .mergeInstances(runtimeAndRuntimeConfig.runtime.copy(dataprocInstances = instances))
            .transaction
            .void
        else F.unit
      ).parSequence_

      // Record metrics in NewRelic
      _ <- recordStatusTransitionMetrics(
        monitorContext.start,
        getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Error,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )

      // Update the cluster status to Error only if the runtime is non-Deleted.
      // If the user has explicitly deleted their runtime by this point then
      // we don't want to move it back to Error status.
      _ <- for {
        curStatusOpt <- clusterQuery.getClusterStatus(runtimeAndRuntimeConfig.runtime.id).transaction
        curStatus <- F.fromOption(
          curStatusOpt,
          new Exception(s"Cluster with id ${runtimeAndRuntimeConfig.runtime.id} not found in the database")
        )
        _ <- curStatus match {
          case RuntimeStatus.Deleted =>
            logger.info(ctx.loggingCtx)(
              s"failedRuntime: not moving runtime with id ${runtimeAndRuntimeConfig.runtime.id} because it is in Deleted status."
            )
          case _ =>
            logger.info(ctx.loggingCtx)(
              s"failedRuntime: moving runtime with id  ${runtimeAndRuntimeConfig.runtime.id} to Error status."
            ) >> clusterQuery
              .updateClusterStatus(runtimeAndRuntimeConfig.runtime.id, RuntimeStatus.Error, ctx.now)
              .transaction
              .void
        }
      } yield ()

      tags = Map(
        "cloudService" -> runtimeAndRuntimeConfig.runtimeConfig.cloudService.asString,
        "errorCode" -> errorDetails.shortMessage.getOrElse("leonardo")
      )
      _ <- openTelemetry.incrementCounter(s"runtimeCreationFailure", 1, tags)
    } yield ((), None): CheckResult

  def readyRuntime(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                   publicIp: IP,
                   monitorContext: MonitorContext,
                   dataprocInstances: Set[DataprocInstance]): F[CheckResult] =
    for {
      now <- nowInstant
      _ <- persistInstances(runtimeAndRuntimeConfig, dataprocInstances)
      _ <- clusterQuery.setToRunning(runtimeAndRuntimeConfig.runtime.id, publicIp, now).transaction
      _ <- runtimeAndRuntimeConfig.runtimeConfig match {
        case x: RuntimeConfig.GceWithPdConfig =>
          for {
            diskId <- F.fromEither(
              x.persistentDiskId.toRight(
                new RuntimeException("DiskId should exist when we try to create a runtime with persistent disk")
              )
            )
            _ <- persistentDiskQuery
              .updateStatusAndIsFormatted(diskId, DiskStatus.Ready, FormattedBy.GCE, now)
              .transaction
          } yield ()
        case _ =>
          F.unit
      }
      _ <- RuntimeMonitor.recordStatusTransitionMetrics(
        monitorContext.start,
        RuntimeMonitor.getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Running,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )
      _ <- if (runtimeAndRuntimeConfig.runtime.status == RuntimeStatus.Creating)
        RuntimeMonitor.recordClusterCreationMetrics(
          runtimeAndRuntimeConfig.runtime.auditInfo.createdDate,
          runtimeAndRuntimeConfig.runtime.runtimeImages,
          monitorConfig.imageConfig,
          runtimeAndRuntimeConfig.runtimeConfig.cloudService
        )
      else F.unit
      timeElapsed = (now.toEpochMilli - monitorContext.start.toEpochMilli).milliseconds
      _ <- logger.info(monitorContext.loggingContext)(
        s"Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} is ready for use after ${timeElapsed.toSeconds} seconds!"
      )
    } yield ((), None)

  def checkAgain(
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
    dataprocInstances: Set[DataprocInstance], //only applies to dataproc
    message: Option[String],
    ip: Option[IP] = None
  ): F[CheckResult] =
    for {
      now <- nowInstant[F]
      ctx = AppContext(monitorContext.traceId, now)
      implicit0(traceId: Ask[F, AppContext]) = Ask.const[F, AppContext](
        ctx
      )
      timeElapsed = (now.toEpochMilli - monitorContext.start.toEpochMilli).millis

      res <- monitorConfig.monitorStatusTimeouts.get(runtimeAndRuntimeConfig.runtime.status) match {
        case Some(timeLimit) if timeElapsed > timeLimit =>
          for {
            _ <- logger.info(ctx.loggingCtx)(
              s"Detected that ${runtimeAndRuntimeConfig.runtime.projectNameString} has been stuck in status ${runtimeAndRuntimeConfig.runtime.status} too long."
            )
            r <- // Take care not to Error out a cluster if it timed out in Starting status
            if (runtimeAndRuntimeConfig.runtime.status == RuntimeStatus.Starting) {
              for {
                _ <- runtimeAlg.stopRuntime(
                  StopRuntimeParams(runtimeAndRuntimeConfig, now)
                )
                // Stopping the runtime
                _ <- clusterQuery
                  .updateClusterStatusAndHostIp(runtimeAndRuntimeConfig.runtime.id, RuntimeStatus.Stopping, ip, now)
                  .transaction
                runtimeAndRuntimeConfigAfterSetIp = ip.fold(runtimeAndRuntimeConfig)(i =>
                  LeoLenses.ipRuntimeAndRuntimeConfig.set(i)(runtimeAndRuntimeConfig)
                )
                rrc = runtimeAndRuntimeConfigAfterSetIp.lens(_.runtime.status).set(RuntimeStatus.Stopping)
              } yield ((), Some(MonitorState.Check(rrc, Some(RuntimeStatus.Stopping)))): CheckResult
            } else
              failedRuntime(
                monitorContext,
                runtimeAndRuntimeConfig,
                RuntimeErrorDetails(
                  s"Failed to transition ${runtimeAndRuntimeConfig.runtime.projectNameString} from status ${runtimeAndRuntimeConfig.runtime.status} within the time limit: ${timeLimit.toSeconds} seconds"
                ),
                dataprocInstances
              )
          } yield r
        case _ =>
          logger
            .info(monitorContext.loggingContext)(
              s"Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} is still in ${runtimeAndRuntimeConfig.runtime.status}, not in ${runtimeAndRuntimeConfig.runtime.status.terminalStatus
                .getOrElse("final")} state yet and has taken ${timeElapsed.toSeconds} seconds so far. Checking again in ${monitorConfig.pollingInterval}. ${message
                .getOrElse("")}"
            )
            .as(((), Some(Check(runtimeAndRuntimeConfig, None))))
      }
    } yield res

  def handleCheck(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
    implicit ev: Ask[F, AppContext]
  ): F[CheckResult]

  protected def handleInitial(
    monitorContext: MonitorContext
  )(implicit ct: Ask[F, AppContext]): F[CheckResult] =
    for {
      runtimeAndRuntimeConfig <- getDbRuntimeAndRuntimeConfig(monitorContext.runtimeId)
      next <- runtimeAndRuntimeConfig.runtime.status match {
        case status if status.isMonitored =>
          logger.info(monitorContext.loggingContext)(
            s"Start monitor runtime ${runtimeAndRuntimeConfig.runtime.projectNameString}'s ${status} process."
          ) >> handleCheck(monitorContext, runtimeAndRuntimeConfig)
        case _ =>
          F.pure(((), None): CheckResult)
      }
    } yield next

  protected def setStagingBucketLifecycle(runtime: Runtime, stagingBucketExpiration: FiniteDuration)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] =
    // Get the staging bucket path for this cluster, then set the age for it to be deleted the specified number of days after the deletion of the cluster.
    clusterQuery.getStagingBucket(runtime.googleProject, runtime.runtimeName).transaction.flatMap {
      case None =>
        logger.warn(s"Could not lookup staging bucket for cluster ${runtime.projectNameString}: cluster not in db")
      case Some(bucketPath) =>
        val res = for {
          ctx <- ev.ask
          ageToDelete = (ctx.now.toEpochMilli - runtime.auditInfo.createdDate.toEpochMilli).millis.toDays.toInt + stagingBucketExpiration.toDays.toInt
          condition = BucketInfo.LifecycleRule.LifecycleCondition.newBuilder().setAge(ageToDelete).build()
          action = BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction()
          rule = new BucketInfo.LifecycleRule(action, condition)
          _ <- googleStorage
            .setBucketLifecycle(bucketPath.bucketName, List(rule), Some(ctx.traceId))
            .compile
            .drain
          _ <- logger.debug(
            s"Set staging bucket $bucketPath for cluster ${runtime.projectNameString} to be deleted in fake days."
          )
        } yield ()

        res.handleErrorWith {
          case e =>
            logger.error(e)(s"Error occurred setting staging bucket lifecycle for cluster ${runtime.projectNameString}")
        }
    }

  protected def stopRuntime(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                            dataprocInstances: Set[DataprocInstance],
                            monitorContext: MonitorContext)(
    implicit ev: Ask[F, AppContext]
  ): F[CheckResult] =
    for {
      ctx <- ev.ask
      _ <- persistInstances(runtimeAndRuntimeConfig, dataprocInstances)
      stoppingDuration = (ctx.now.toEpochMilli - monitorContext.start.toEpochMilli).millis
      // this sets the cluster status to stopped and clears the cluster IP
      _ <- clusterQuery
        .updateClusterStatus(runtimeAndRuntimeConfig.runtime.id, RuntimeStatus.Stopped, ctx.now)
        .transaction
      // reset the time at which the kernel was last found to be busy
      _ <- clusterQuery.clearKernelFoundBusyDate(runtimeAndRuntimeConfig.runtime.id, ctx.now).transaction
      _ <- RuntimeMonitor.recordStatusTransitionMetrics(
        monitorContext.start,
        RuntimeMonitor.getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Stopped,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )
      _ <- logger.info(ctx.loggingCtx)(
        s"Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} has been stopped after ${stoppingDuration.toSeconds} seconds."
      )
    } yield ((), None)

  private[monitor] def validateBothScripts(
    userScriptOutputFile: Option[GcsPath],
    userStartupScriptOutputFile: Option[GcsPath],
    userScriptUriInDB: Option[UserScriptPath],
    startUserScriptUriInDB: Option[UserScriptPath]
  )(implicit ev: Ask[F, AppContext]): F[UserScriptsValidationResult] =
    for {
      userScriptRes <- validateUserScript(userScriptOutputFile, userScriptUriInDB)
      res <- userScriptRes match {
        case UserScriptsValidationResult.Success =>
          validateUserStartupScript(userStartupScriptOutputFile, startUserScriptUriInDB)
        case x: UserScriptsValidationResult.Error =>
          F.pure(x)
        case x: UserScriptsValidationResult.CheckAgain =>
          F.pure(x)
      }
    } yield res

  private[monitor] def validateUserScript(
    userScriptOutputPathFromDB: Option[GcsPath],
    userScriptUriInDB: Option[UserScriptPath]
  )(implicit ev: Ask[F, AppContext]): F[UserScriptsValidationResult] =
    (userScriptOutputPathFromDB, userScriptUriInDB) match {
      case (Some(output), Some(_)) =>
        checkUserScriptsOutputFile(output).map { o =>
          o match {
            case Some(true) => UserScriptsValidationResult.Success
            case Some(false) =>
              UserScriptsValidationResult.Error(s"User script failed. See output in ${output.toUri}")
            case None =>
              UserScriptsValidationResult.CheckAgain(
                s"User script hasn't finished yet. See output in ${output.toUri}"
              )
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
    startUserScriptUriInDB: Option[UserScriptPath]
  )(implicit ev: Ask[F, AppContext]): F[UserScriptsValidationResult] =
    (userStartupScriptOutputFile, startUserScriptUriInDB) match {
      case (Some(output), Some(_)) =>
        checkUserScriptsOutputFile(output).map { o =>
          o match {
            case Some(true) => UserScriptsValidationResult.Success
            case Some(false) =>
              UserScriptsValidationResult.Error(s"User startup script failed. See output in ${output.toUri}")
            case None =>
              UserScriptsValidationResult.CheckAgain(
                s"User startup script hasn't finished yet. See output in ${output.toUri}"
              )
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

  private[monitor] def checkUserScriptsOutputFile(
    gcsPath: GcsPath
  )(implicit ev: Ask[F, TraceId]): F[Option[Boolean]] =
    for {
      traceId <- ev.ask
      blobOpt <- googleStorage
        .getBlob(gcsPath.bucketName, GcsBlobName(gcsPath.objectName.value), traceId = Some(traceId))
        .compile
        .last
      output = blobOpt.flatMap(blob => Option(blob).flatMap(b => Option(b.getMetadata.get("passed"))))
    } yield output.map(_ != "false")

  protected def getDbRuntimeAndRuntimeConfig(
    runtimeId: Long
  ): F[RuntimeAndRuntimeConfig] =
    for {
      clusterOpt <- clusterQuery.getClusterById(runtimeId).transaction
      cluster <- Sync[F].fromEither(
        clusterOpt.toRight(new Exception(s"Cluster with id ${runtimeId} not found in the database"))
      )
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
    } yield RuntimeAndRuntimeConfig(cluster, runtimeConfig)

  private[monitor] def handleCheckTools(monitorContext: MonitorContext,
                                        runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                        ip: IP,
                                        dataprocInstances: Set[DataprocInstance]) // only applies to dataproc
  (
    implicit ev: Ask[F, AppContext]
  ): F[CheckResult] = {
    // Update the Host IP in the database so DNS cache can be properly populated with the first cache miss
    // Otherwise, when a cluster is resumed and transitions from Starting to Running, we get stuck
    // in that state - at least with the way HttpJupyterDAO.isProxyAvailable works
    val res = for {
      ctx <- ev.ask
      imageTypes <- dbRef
        .inTransaction { //If toolsToCheck is not defined, then we haven't check any tools yet. Hence retrieve all tools from database
          for {
            _ <- clusterQuery.updateClusterHostIp(runtimeAndRuntimeConfig.runtime.id, Some(ip), ctx.now)
            images <- clusterImageQuery.getAllForCluster(runtimeAndRuntimeConfig.runtime.id)
          } yield images.toList
        }
      checkTools = imageTypes.traverseFilter { imageType =>
        RuntimeContainerServiceType.imageTypeToRuntimeContainerServiceType
          .get(imageType)
          .traverse(
            _.isProxyAvailable(runtimeAndRuntimeConfig.runtime.googleProject,
                               runtimeAndRuntimeConfig.runtime.runtimeName).map(b => (imageType, b))
          )
      }
      // wait for 10 minutes for tools to start up before time out.
      availableTools <- streamFUntilDone(checkTools,
                                         monitorConfig.checkToolsMaxAttempts,
                                         monitorConfig.checkToolsInterval).compile.lastOrError
      r <- availableTools match {
        case a if a.forall(_._2) =>
          readyRuntime(runtimeAndRuntimeConfig, ip, monitorContext, dataprocInstances)
        case a =>
          val toolsStillNotAvailable = a.collect { case x if x._2 == false => x._1 }
          failedRuntime(
            monitorContext,
            runtimeAndRuntimeConfig,
            RuntimeErrorDetails(s"${toolsStillNotAvailable} didn't start up properly", None, Some("tool_start_up")),
            dataprocInstances
          )
      }
    } yield r

    openTelemetry.time(
      "tools_start_up_time",
      List(5 seconds, 20 seconds, 30 seconds, 1 minutes, 2 minutes),
      Map("cloud_service" -> runtimeAndRuntimeConfig.runtimeConfig.cloudService.asString)
    )(res)
  }

  protected def saveRuntimeError(runtimeId: Long, errorDetails: RuntimeErrorDetails)(
    implicit ev: Ask[F, AppContext]
  ): F[Unit] = {
    val result = for {
      ctx <- ev.ask
      _ <- clusterErrorQuery
        .save(runtimeId, RuntimeError(errorDetails.longMessage, errorDetails.code, ctx.now, Some(ctx.traceId)))
        .transaction
        .void
        .adaptError {
          case e =>
            new Exception(
              s"Error persisting runtime error with message '${errorDetails.longMessage}' to database: ${e}",
              e
            )
        }
    } yield ()

    result.onError {
      case e =>
        logger.error(e)(s"Failed to persist runtime errors for runtime ${runtimeId}: ${e.getMessage}")
    }
  }

  protected def deleteInitBucket(googleProject: GoogleProject,
                                 runtimeName: RuntimeName)(implicit ev: Ask[F, AppContext]): F[Unit] =
    for {
      ctx <- ev.ask
      bucketPathOpt <- clusterQuery.getInitBucket(googleProject, runtimeName).transaction
      _ <- bucketPathOpt match {
        case None =>
          logger.warn(ctx.loggingCtx)(
            s"Could not lookup init bucket for runtime ${googleProject.value}/${runtimeName.asString}: cluster not in db"
          )
        case Some(bucketPath) =>
          for {
            ctx <- ev.ask
            r <- googleStorage
              .deleteBucket(googleProject, bucketPath.bucketName, isRecursive = true)
              .compile
              .lastOrError
              .attempt
            _ <- r match {
              case Left(e) =>
                logger.error(ctx.loggingCtx, e)(
                  s"Fail to delete init bucket $bucketPath for runtime ${googleProject.value}/${runtimeName.asString}"
                )
              case Right(_) =>
                logger.debug(ctx.loggingCtx)(
                  s"Successfully deleted init bucket $bucketPath for runtime ${googleProject.value}/${runtimeName.asString} or bucket doesn't exist"
                )
            }
          } yield ()
      }
    } yield ()

  private def persistInstances(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                               dataprocInstances: Set[DataprocInstance]): F[Unit] =
    runtimeAndRuntimeConfig.runtimeConfig.cloudService match {
      case CloudService.GCE => F.unit
      case CloudService.Dataproc =>
        dbRef.inTransaction {
          clusterQuery.mergeInstances(runtimeAndRuntimeConfig.runtime.copy(dataprocInstances = dataprocInstances))
        }.void
    }
}

final case class InvalidMonitorRequest(msg: String) extends RuntimeException {
  override def getMessage: String = msg
}
