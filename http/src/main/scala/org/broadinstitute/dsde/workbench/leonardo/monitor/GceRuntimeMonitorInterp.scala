package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import cats.Parallel
import cats.effect.{Async, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.compute.v1.Instance
import com.google.cloud.storage.BucketInfo
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import io.grpc.Status.Code
import monocle.macros.syntax.lens._
import org.broadinstitute.dsde.workbench.DoneCheckableInstances.computeDoneCheckable
import org.broadinstitute.dsde.workbench.DoneCheckableSyntax._
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleComputeService, GoogleStorageService, InstanceName}
import org.broadinstitute.dsde.workbench.leonardo.GceInstanceStatus._
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.google.getInstanceIP
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.{ctxConversion, dbioToIO, nowInstant, userScriptStartupOutputUriMetadataKey}
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.GceRuntimeMonitor._
import org.broadinstitute.dsde.workbench.leonardo.monitor.GceMonitorState._
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import slick.dbio.DBIOAction
import GceRuntimeMonitorInterp._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class GceRuntimeMonitorInterp[F[_]: Timer: Parallel](
  config: GceMonitorConfig,
  googleComputeService: GoogleComputeService[F],
  authProvider: LeoAuthProvider[F],
  googleStorageService: GoogleStorageService[F],
  gceInterpreter: RuntimeAlgebra[F]
)(implicit dbRef: DbReference[F],
  runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[F, RuntimeContainerServiceType],
  F: Async[F],
  logger: Logger[F],
  ec: ExecutionContext,
  openTelemetry: OpenTelemetryMetrics[F])
    extends GceRuntimeMonitor[F] {
  def process(runtimeId: Long)(implicit ev: ApplicativeAsk[F, TraceId]): Stream[F, Unit] =
    for {
      // building up a stream that will terminate when gce runtime is ready
      traceId <- Stream.eval(ev.ask)
      startMonitoring <- Stream.eval(nowInstant[F])
      monitorContext = MonitorContext(startMonitoring, runtimeId, traceId)
      _ <- Stream.sleep(config.initialDelay) ++ Stream.unfoldLoopEval[F, GceMonitorState, Unit](Initial)(
        s => Timer[F].sleep(config.pollingInterval) >> handler(monitorContext, s)
      )
    } yield ()

  // Function used for transitions that we can get an Operation
  def pollCheck(googleProject: GoogleProject,
                runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                operation: com.google.cloud.compute.v1.Operation,
                action: RuntimeStatus)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      // building up a stream that will terminate when gce runtime is ready
      traceId <- ev.ask
      startMonitoring <- nowInstant
      monitorContext = MonitorContext(startMonitoring, runtimeAndRuntimeConfig.runtime.id, traceId)
      _ <- Timer[F].sleep(config.initialDelay)
      poll = googleComputeService.pollOperation(googleProject, operation, config.pollingInterval, config.pollCheckMaxAttempts).compile.lastOrError
      _ <- action match {
        case RuntimeStatus.Deleting =>
          for {
            op <- poll
            timeAfterPoll <- nowInstant
            implicit0(ctx: ApplicativeAsk[F, AppContext]) = ApplicativeAsk.const[F, AppContext](AppContext(traceId, timeAfterPoll))
            _ <- if (op.isDone) deletedRuntime(monitorContext, runtimeAndRuntimeConfig) else failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(
                Code.DEADLINE_EXCEEDED.value,
                Some(s"${action} ${runtimeAndRuntimeConfig.runtime.projectNameString} fail to complete in a timely manner")
              )
            )
          } yield ()
        case RuntimeStatus.Stopping =>
          for {
            op <- poll
            timeAfterPoll <- nowInstant
            implicit0(ctx: ApplicativeAsk[F, AppContext]) = ApplicativeAsk.const[F, AppContext](AppContext(traceId, timeAfterPoll))
            _ <- if (op.isDone) stopRuntime(runtimeAndRuntimeConfig, monitorContext) else failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(
                Code.DEADLINE_EXCEEDED.value,
                Some(s"${action} ${runtimeAndRuntimeConfig.runtime.projectNameString} fail to complete in a timely manner")
              )
            )
          } yield ()
        case x                      =>
          F.raiseError(new Exception(s"Monitoring ${x} with pollOperation is not supported"))
      }
    } yield ()

  private def handler(monitorContext: MonitorContext, gcsMonitorState: GceMonitorState): F[CheckResult] =
    for {
      now <- nowInstant
      implicit0(ct: ApplicativeAsk[F, AppContext]) = ApplicativeAsk.const[F, AppContext](
        AppContext(monitorContext.traceId, now)
      )
      r <- gcsMonitorState match {
        case GceMonitorState.Initial => handleInitial(monitorContext)
        case GceMonitorState.CheckTools(ip, runtimeAndRuntimeConfig, images) =>
          handleCheckTools(monitorContext, runtimeAndRuntimeConfig, ip, images)
        case GceMonitorState.Check(runtimeAndRuntimeConfig) => handleCheck(monitorContext, runtimeAndRuntimeConfig)
      }
    } yield r

  private[monitor] def handleInitial(
    monitorContext: MonitorContext
  )(implicit ct: ApplicativeAsk[F, AppContext]): F[CheckResult] =
    for {
      runtimeAndRuntimeConfig <- getDbRuntimeAndRuntimeConfig(monitorContext.runtimeId)
      next <- runtimeAndRuntimeConfig.runtime.status match {
        case status if status.isMonitored =>
          logger.info(
            s"${monitorContext} | Start monitor runtime ${runtimeAndRuntimeConfig.runtime.projectNameString}'s ${status} process."
          ) >> handleCheck(monitorContext, runtimeAndRuntimeConfig)
        case _ =>
          F.pure(((), None): CheckResult)
      }
    } yield next

  /**
   * Queries Google for the cluster status and takes appropriate action depending on the result.
   * @return ClusterMonitorMessage
   */
  private def handleCheck(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
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
            case _    => checkAgain(monitorContext, runtimeAndRuntimeConfig, Some("Instance hasn't been deleted yet"))
          }
        case RuntimeStatus.Starting =>
          startingRuntime(instance, monitorContext, runtimeAndRuntimeConfig)
        // This is needed because during boot time, we no longer have reference to the previous delete operation
        // But this path should only happen during boot and there's on-going delete
        case RuntimeStatus.Stopping =>
          val gceStatus = instance.flatMap(i => GceInstanceStatus.withNameInsensitiveOption(i.getStatus))
          gceStatus match {
            case None =>
              logger
                .error(s"${monitorContext} | Can't stop an instance that hasn't been initialized yet or doesn't exist")
                .as(((), None)) //TODO: Ideally we should have sentry report this case
            case Some(s) =>
              s match {
                case Stopped | Terminated =>
                  stopRuntime(runtimeAndRuntimeConfig, monitorContext)
                case _ =>
                  checkAgain(
                    monitorContext,
                    runtimeAndRuntimeConfig,
                    Some(s"hasn't been terminated yet")
                  )
              }
          }
        case status => logger.error(s"${status} is not a transition status for GCE; hence no need to monitor").as(((), None))
      }
    } yield result

  private def creatingRuntime(
    instance: Option[Instance],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[CheckResult] = instance match {
    case None =>
      checkAgain(monitorContext, runtimeAndRuntimeConfig, Some(s"Can't retrieve instance yet"))
    case Some(i) =>
      for {
        gceStatus <- F.fromEither(GceInstanceStatus.withNameInsensitiveOption(i.getStatus).toRight(new Exception(s"Unknown GCE instance status ${i.getStatus}"))) //TODO: Ideally we should have sentry report this case
        r <- gceStatus match {
          case GceInstanceStatus.Provisioning | GceInstanceStatus.Staging =>
            checkAgain(monitorContext, runtimeAndRuntimeConfig, Some(s"Instance is still in ${gceStatus}"))
          case GceInstanceStatus.Running =>
            val userScript = runtimeAndRuntimeConfig.runtime.asyncRuntimeFields
              .map(_.stagingBucket)
              .map(b => RuntimeTemplateValues.jupyterUserScriptOutputUriPath(b))

            val userStartupScript = getUserScript(i)

            for {
              userScriptSuccess <- checkUserScripts(userScript)
              startUpScriptSuccess <- checkUserScripts(userStartupScript)
              r <- (userScriptSuccess, startUpScriptSuccess) match {
                case (true, true) =>
                  getInstanceIP(i) match {
                    case Some(ip) =>
                      // It takes a bit for jupyter to startup, hence wait 5 seconds before we check jupyter
                      Timer[F]
                        .sleep(8 seconds) >> handleCheckTools(monitorContext, runtimeAndRuntimeConfig, ip, List.empty)
                    case None =>
                      checkAgain(monitorContext, runtimeAndRuntimeConfig, Some("Could not retrieve instance IP"))
                  }
                case (false, true) =>
                  failedRuntime(monitorContext,
                    runtimeAndRuntimeConfig,
                    RuntimeErrorDetails(-1, Some(s"user script ${userScript.map(_.toUri).getOrElse("")} failed")))
                case (true, false) =>
                  failedRuntime(monitorContext,
                    runtimeAndRuntimeConfig,
                    RuntimeErrorDetails(-1, Some(s"user startUp script ${userStartupScript.map(_.toUri).getOrElse("")} failed")))
                case (false, false) =>
                  failedRuntime(monitorContext,
                    runtimeAndRuntimeConfig,
                    RuntimeErrorDetails(
                      -1,
                      Some(s"both user script ${userScript.map(_.toUri).getOrElse("")} and startUp ${userStartupScript.map(_.toUri).getOrElse("")} script failed")
                    ))
              }
            } yield r
          case ss =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(-1, Some(s"unexpected GCE instance status ${ss} when trying to creating an instance"))
            )
        }
      } yield r
  }

  private def startingRuntime(
    instance: Option[Instance],
    monitorContext: MonitorContext,
    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
  )(implicit ev: ApplicativeAsk[F, AppContext]): F[CheckResult] = instance match {
    case None =>
      logger
        .error(
          s"${monitorContext} | Fail retrieve instance status when trying to start runtime ${runtimeAndRuntimeConfig.runtime.projectNameString}"
        )
        .as(((), None))
    case Some(i) =>
      for {
        gceStatus <- F.fromEither(GceInstanceStatus.withNameInsensitiveOption(i.getStatus).toRight(new Exception(s"Unknown GCE instance status ${i.getStatus}")))
        startableStatuses = Set(
          GceInstanceStatus.Suspending,
          GceInstanceStatus.Provisioning,
          GceInstanceStatus.Staging,
          GceInstanceStatus.Terminated,
          GceInstanceStatus.Suspended,
          GceInstanceStatus.Stopping,
        )
        r <- gceStatus match {
          case s if(startableStatuses.contains(s)) =>
            checkAgain(monitorContext, runtimeAndRuntimeConfig, Some(s"Instance is still in ${gceStatus}"))
          case GceInstanceStatus.Running =>
            val userStartupScript = getUserScript(i)

            for {
              startUpScriptSuccess <- checkUserScripts(userStartupScript)
              r <- if (startUpScriptSuccess)
                getInstanceIP(i) match {
                  case Some(ip) =>
                    // It takes a bit for jupyter to startup, hence wait 5 seconds before we check jupyter
                    Timer[F].sleep(8 seconds) >> handleCheckTools(monitorContext, runtimeAndRuntimeConfig, ip, List.empty)
                  case None =>
                    checkAgain(monitorContext, runtimeAndRuntimeConfig, Some("Could not retrieve instance IP"))
                } else
                failedRuntime(monitorContext,
                  runtimeAndRuntimeConfig,
                  RuntimeErrorDetails(
                    -1,
                    Some(s"user startUp script ${userStartupScript.map(_.toUri).getOrElse("")} failed")
                  ))
            } yield r
          case ss =>
            failedRuntime(
              monitorContext,
              runtimeAndRuntimeConfig,
              RuntimeErrorDetails(-1, Some(s"unexpected GCE instance status ${ss} when trying to start an instance"))
            )
        }
      } yield r
  }

  private def getDbRuntimeAndRuntimeConfig(runtimeId: Long): F[RuntimeAndRuntimeConfig] =
    for {
      clusterOpt <- clusterQuery.getClusterById(runtimeId).transaction
      cluster <- F.fromEither(
        clusterOpt.toRight(new Exception(s"Cluster with id ${runtimeId} not found in the database"))
      )
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
    } yield RuntimeAndRuntimeConfig(cluster, runtimeConfig)

  private[monitor] def readyRuntime(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                    publicIp: IP,
                                    monitorContext: MonitorContext): F[CheckResult] =
    for {
      now <- nowInstant
      // update DB after auth futures finish
      _ <- clusterQuery.setToRunning(runtimeAndRuntimeConfig.runtime.id, publicIp, now).transaction
      // Record metrics in NewRelic
      _ <- ClusterMonitorActor.recordStatusTransitionMetrics(
        monitorContext.start,
        ClusterMonitorActor.getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Running,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )
      _ <- if (runtimeAndRuntimeConfig.runtime.status == RuntimeStatus.Creating)
        ClusterMonitorActor.recordClusterCreationMetrics(
          runtimeAndRuntimeConfig.runtime.auditInfo.createdDate,
          runtimeAndRuntimeConfig.runtime.runtimeImages,
          config.imageConfig,
          runtimeAndRuntimeConfig.runtimeConfig.cloudService
        )
      else F.unit
      timeElapsed = (now.toEpochMilli - monitorContext.start.toEpochMilli).milliseconds
      // Finally pipe a shutdown message to this actor
      _ <- logger.info(
        s"${monitorContext} | Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} is ready for use after ${timeElapsed.toSeconds} seconds!"
      )
    } yield ((), None)

  private def handleCheckTools(monitorContext: MonitorContext,
                               runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                               ip: IP,
                               toolsToCheck: List[RuntimeImageType])(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[CheckResult] =
    // Update the Host IP in the database so DNS cache can be properly populated with the first cache miss
    // Otherwise, when a cluster is resumed and transitions from Starting to Running, we get stuck
    // in that state - at least with the way HttpJupyterDAO.isProxyAvailable works
    for {
      ctx <- ev.ask
      imageTypes <- if (toolsToCheck.nonEmpty) //If toolsToCheck is defined, we've checked some tools already, and only checking tools that haven't been available yet
        F.pure(toolsToCheck)
      else
        dbRef
          .inTransaction { //If toolsToCheck is defined, then we haven't check any tools yet. Hence retrieve all tools from database
            for {
              _ <- clusterQuery.updateClusterHostIp(runtimeAndRuntimeConfig.runtime.id, Some(ip), ctx.now)
              images <- clusterImageQuery.getAllForCluster(runtimeAndRuntimeConfig.runtime.id)
            } yield images.toList.map(_.imageType)
          }
      availableTools <- imageTypes.traverseFilter { imageType =>
        RuntimeContainerServiceType.imageTypeToRuntimeContainerServiceType
          .get(imageType)
          .traverse(
            _.isProxyAvailable(runtimeAndRuntimeConfig.runtime.googleProject,
                               runtimeAndRuntimeConfig.runtime.runtimeName).map(b => (imageType, b))
          )
      }
      res <- availableTools match {
        case a if a.forall(_._2) =>
          readyRuntime(runtimeAndRuntimeConfig, ip, monitorContext)
        case a =>
          val toolsStillNotAvailable = a.collect { case x if x._2 == false => x._1 }
          checkAgain(
            monitorContext,
            runtimeAndRuntimeConfig,
            Some(s"Services not available: ${toolsStillNotAvailable}"),
            Some(ip),
            toolsStillNotAvailable
          )
      }
    } yield res

  private def checkAgain(monitorContext: MonitorContext,
                         runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                         message: Option[String],
                         ip: Option[IP] = None,
                         toolsToCheck: List[RuntimeImageType] = List.empty
  ): F[CheckResult] = {
    for {
      now <- nowInstant
      implicit0(traceId: ApplicativeAsk[F, AppContext]) = ApplicativeAsk.const[F, AppContext](AppContext(monitorContext.traceId, now))
      timeElapsed = (now.toEpochMilli - monitorContext.start.toEpochMilli).millis
      res <- config.monitorStatusTimeouts.get(runtimeAndRuntimeConfig.runtime.status) match {
        case Some(timeLimit) if timeElapsed > timeLimit =>
          for {
            _ <- logger.info(
              s"Detected that ${runtimeAndRuntimeConfig.runtime.projectNameString} has been stuck in status ${runtimeAndRuntimeConfig.runtime.status} too long."
            )
            r <- // Take care not to Error out a cluster if it timed out in Starting status
            if (runtimeAndRuntimeConfig.runtime.status == RuntimeStatus.Starting) {
              for {
                _ <- gceInterpreter.stopRuntime(StopRuntimeParams(runtimeAndRuntimeConfig, now))
                // Stopping the runtime
                _ <- clusterQuery
                  .updateClusterStatusAndHostIp(runtimeAndRuntimeConfig.runtime.id,
                                                RuntimeStatus.Stopping,
                                                ip,
                    now)
                  .transaction
                runtimeAndRuntimeConfigAfterSetIp = ip.fold(runtimeAndRuntimeConfig)(
                  i => LeoLenses.ipRuntimeAndRuntimeConfig.set(i)(runtimeAndRuntimeConfig)
                )
                rrc = runtimeAndRuntimeConfigAfterSetIp.lens(_.runtime.status).set(RuntimeStatus.Stopping)
              } yield ((), Some(Check(rrc))): CheckResult
            } else
              failedRuntime(
                monitorContext,
                runtimeAndRuntimeConfig,
                RuntimeErrorDetails(
                  Code.DEADLINE_EXCEEDED.value,
                  Some(
                    s"Failed to transition ${runtimeAndRuntimeConfig.runtime.projectNameString} from status ${runtimeAndRuntimeConfig.runtime.status} within the time limit: ${timeLimit.toSeconds} seconds"
                  )
                )
              )
          } yield r
        case _ =>
          ip match {
            case Some(x) =>
              logger
                .info(
                  s"Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString}'s tools(${toolsToCheck}) is not ready yet and has taken ${timeElapsed.toSeconds} seconds so far. Checking again in ${config.pollingInterval}. ${message
                    .getOrElse("")}"
                )
                .as(((), Some(CheckTools(x, runtimeAndRuntimeConfig, toolsToCheck))))
            case None =>
              logger
                .info(
                  s"Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} is not in final state yet and has taken ${timeElapsed.toSeconds} seconds so far. Checking again in ${config.pollingInterval}. ${message
                    .getOrElse("")}"
                )
                .as(((), Some(Check(runtimeAndRuntimeConfig))))
          }
      }
    } yield res
  }

  private def failedRuntime(monitorContext: MonitorContext,
                            runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                            errorDetails: RuntimeErrorDetails)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[CheckResult] =
    for {
      ctx <- ev.ask
      _ <- List(
        // Delete the cluster in Google
        gceInterpreter.deleteRuntime(DeleteRuntimeParams(runtimeAndRuntimeConfig.runtime)), //TODO is this right when deleting or stopping fails?
        //save cluster error in the DB
        saveClusterError(runtimeAndRuntimeConfig.runtime, errorDetails.message.getOrElse(""), errorDetails.code, ctx.now)
      ).parSequence_

      // Record metrics in NewRelic
      _ <- ClusterMonitorActor.recordStatusTransitionMetrics(
        monitorContext.start,
        ClusterMonitorActor.getRuntimeUI(runtimeAndRuntimeConfig.runtime),
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
        "errorCode" -> io.grpc.Status.fromCodeValue(errorDetails.code).toString
      )
      _ <- openTelemetry.incrementCounter(s"runtimeCreationFailure", 1, tags)
    } yield ((), None): CheckResult

  //TODO: can this just be getting the runtime from id(long)?
  private def saveClusterError(runtime: Runtime, errorMessage: String, errorCode: Int, now: Instant): F[Unit] =
    dbRef
      .inTransaction {
        val clusterId = clusterQuery.getIdByUniqueKey(runtime)
        clusterId flatMap {
          case Some(a) =>
            clusterErrorQuery.save(a, RuntimeError(errorMessage, errorCode, now))
          case None => {
            logger.warn(
              s"Could not find Id for Cluster ${runtime.projectNameString}  with google cluster ID ${runtime.asyncRuntimeFields
                .map(_.googleId)}."
            )
            DBIOAction.successful(0)
          }
        }
      }
      .void
      .adaptError {
        case e => new Exception(s"Error persisting cluster error with message '${errorMessage}' to database: ${e}", e)
      }

  private def deletedRuntime(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[CheckResult] =
    for {
      ctx <- ev.ask
      duration = (ctx.now.toEpochMilli - monitorContext.start.toEpochMilli).millis
      _ <- logger.info(
        s"Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} has been deleted after ${duration.toSeconds} seconds."
      )

      // delete the init bucket so we don't continue to accrue costs after cluster is deleted
      _ <- deleteInitBucket(runtimeAndRuntimeConfig.runtime.googleProject, runtimeAndRuntimeConfig.runtime.runtimeName)

      // set the staging bucket to be deleted in ten days so that logs are still accessible until then
      _ <- setStagingBucketLifecycle(runtimeAndRuntimeConfig.runtime, monitorContext.traceId)

      _ <- dbRef.inTransaction {
        clusterQuery.completeDeletion(runtimeAndRuntimeConfig.runtime.id, ctx.now)
      }

      _ <- authProvider
        .notifyClusterDeleted(
          runtimeAndRuntimeConfig.runtime.internalId,
          runtimeAndRuntimeConfig.runtime.auditInfo.creator,
          runtimeAndRuntimeConfig.runtime.auditInfo.creator,
          runtimeAndRuntimeConfig.runtime.googleProject,
          runtimeAndRuntimeConfig.runtime.runtimeName
        )

      // Record metrics in NewRelic
      _ <- ClusterMonitorActor.recordStatusTransitionMetrics(
        monitorContext.start,
        ClusterMonitorActor.getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Deleted,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )

    } yield ((), None)

  private def stopRuntime(runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                          monitorContext: MonitorContext)(implicit ev: ApplicativeAsk[F, AppContext]): F[CheckResult] =
    for {
      ctx <- ev.ask
      stoppingDuration = (ctx.now.toEpochMilli - monitorContext.start.toEpochMilli).millis
      // this sets the cluster status to stopped and clears the cluster IP
      _ <- clusterQuery
        .updateClusterStatus(runtimeAndRuntimeConfig.runtime.id, RuntimeStatus.Stopped, ctx.now)
        .transaction
      // reset the time at which the kernel was last found to be busy
      _ <- clusterQuery.clearKernelFoundBusyDate(runtimeAndRuntimeConfig.runtime.id, ctx.now).transaction
      _ <- ClusterMonitorActor.recordStatusTransitionMetrics(
        monitorContext.start,
        ClusterMonitorActor.getRuntimeUI(runtimeAndRuntimeConfig.runtime),
        runtimeAndRuntimeConfig.runtime.status,
        RuntimeStatus.Stopped,
        runtimeAndRuntimeConfig.runtimeConfig.cloudService
      )
      _ <- logger.info(
        s"Gce runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} has been stopped after ${stoppingDuration.toSeconds} seconds."
      )
    } yield ((), None)

  private def deleteInitBucket(googleProject: GoogleProject,
                               runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit] =
    clusterQuery.getInitBucket(googleProject, runtimeName).transaction.flatMap {
      case None =>
        logger.warn(
          s"Could not lookup init bucket for runtime ${googleProject.value}/${runtimeName.asString}: cluster not in db"
        )
      case Some(bucketPath) =>
        for {
          ctx <- ev.ask
          r <- googleStorageService
            .deleteBucket(googleProject, bucketPath.bucketName, isRecursive = true)
            .compile
            .lastOrError
            .attempt
          _ <- r match {
            case Left(e) =>
              logger.error(e)(
                s"${ctx} | Fail to delete init bucket $bucketPath for runtime ${googleProject.value}/${runtimeName.asString}"
              )
            case Right(_) =>
              logger.debug(
                s"${ctx} |Successfully deleted init bucket $bucketPath for runtime ${googleProject.value}/${runtimeName.asString} or bucket doesn't exist"
              )
          }
        } yield ()
    }

  private def setStagingBucketLifecycle(runtime: Runtime,
                                        traceId: TraceId)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit] =
    // Get the staging bucket path for this cluster, then set the age for it to be deleted the specified number of days after the deletion of the cluster.
    clusterQuery.getStagingBucket(runtime.googleProject, runtime.runtimeName).transaction.flatMap {
      case None =>
        logger.warn(s"Could not lookup staging bucket for cluster ${runtime.projectNameString}: cluster not in db")
      case Some(bucketPath) =>
        val res = for {
          ctx <- ev.ask
          ageToDelete = (ctx.now.toEpochMilli - runtime.auditInfo.createdDate.toEpochMilli).millis.toDays.toInt + config.runtimeBucketConfig.stagingBucketExpiration.toDays.toInt
          condition = BucketInfo.LifecycleRule.LifecycleCondition.newBuilder().setAge(ageToDelete).build()
          action = BucketInfo.LifecycleRule.LifecycleAction.newDeleteAction()
          rule = new BucketInfo.LifecycleRule(action, condition)
          _ <- googleStorageService.setBucketLifecycle(bucketPath.bucketName, List(rule), Some(traceId)).compile.drain
          _ <- logger.debug(
            s"Set staging bucket $bucketPath for cluster ${runtime.projectNameString} to be deleted in fake days."
          )
        } yield ()

        res.handleErrorWith {
          case e =>
            logger.error(e)(s"Error occurred setting staging bucket lifecycle for cluster ${runtime.projectNameString}")
        }
    }

  private[monitor] def checkUserScripts(gcsPath: Option[GcsPath])(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] =
    for {
      traceId <- ev.ask
      r <- gcsPath
        .flatTraverse { path =>
          for {
            startScriptPassedOutput <- googleStorageService
              .getBlob(path.bucketName, GcsBlobName(path.objectName.value), traceId = Some(traceId))
              .compile
              .last
              .map { x =>
                x.flatMap(blob => Option(blob).flatMap(b => Option(b.getMetadata.get("passed"))))
              }
          } yield startScriptPassedOutput
        }
        .map(output => !output.exists(_ == "false"))
    } yield r
}

object GceRuntimeMonitorInterp {
  def getUserScript(instance: Instance): Option[GcsPath] = for {
    metadata <- Option(instance.getMetadata)
    item <- metadata.getItemsList.asScala.toList.filter(item => item.getKey == userScriptStartupOutputUriMetadataKey).headOption
    s <- org.broadinstitute.dsde.workbench.model.google.parseGcsPath(item.getValue).toOption
  } yield s
}
