package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant

import cats.effect.{Async, Sync, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.cloud.storage.BucketInfo
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.dao.ToolDAO
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.monitor.RuntimeMonitor.CheckResult
import org.broadinstitute.dsde.workbench.leonardo.util.{RuntimeAlgebra, StopRuntimeParams}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.{GcsPath, GoogleProject}
import monocle.macros.syntax.lens._
import org.broadinstitute.dsde.workbench.DoneCheckable
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorState._
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.google2.streamFUntilDone

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

abstract class BaseCloudServiceRuntimeMonitor[F[_]] {
  implicit def F: Async[F]
  implicit def timer: Timer[F]
  implicit def dbRef: DbReference[F]
  implicit def ec: ExecutionContext
  implicit def runtimeToolToToolDao: RuntimeContainerServiceType => ToolDAO[F, RuntimeContainerServiceType]
  implicit def openTelemetry: OpenTelemetryMetrics[F]

  implicit val toolsDoneCheckable: DoneCheckable[List[(RuntimeImageType, Boolean)]] = x => x.forall(_._2)

  def runtimeAlg: RuntimeAlgebra[F]
  def logger: Logger[F]
  def googleStorage: GoogleStorageService[F]

  def monitorConfig: MonitorConfig

  def process(runtimeId: Long, runtimeStatus: RuntimeStatus)(implicit ev: ApplicativeAsk[F, TraceId]): Stream[F, Unit] =
    for {
      // building up a stream that will terminate when gce runtime is ready
      traceId <- Stream.eval(ev.ask)
      startMonitoring <- Stream.eval(nowInstant[F])
      monitorContext = MonitorContext(startMonitoring, runtimeId, traceId, runtimeStatus)
      _ <- Stream.sleep(monitorConfig.initialDelay) ++ Stream.unfoldLoopEval[F, MonitorState, Unit](Initial)(s =>
        Timer[F].sleep(monitorConfig.pollingInterval) >> handler(monitorContext, s)
      )
    } yield ()

  def pollCheck(googleProject: GoogleProject,
                runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                operation: com.google.cloud.compute.v1.Operation,
                action: RuntimeStatus)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit]

  private[monitor] def handler(monitorContext: MonitorContext, monitorState: MonitorState): F[CheckResult] =
    for {
      now <- nowInstant
      implicit0(ct: ApplicativeAsk[F, AppContext]) = ApplicativeAsk.const[F, AppContext](
        AppContext(monitorContext.traceId, now)
      )
      currentStatus <- clusterQuery.getClusterStatus(monitorContext.runtimeId).transaction
      res <- currentStatus match {
        case None =>
          logger
            .error(s"${monitorContext} disappeared from database in the middle of status transition!")
            .as(((), None))
        case Some(status) if (status != monitorContext.action) =>
          val tags = Map("original_status" -> monitorContext.action.toString, "interrupted_by" -> status.toString)
          openTelemetry.incrementCounter("earlyTerminationOfMonitoring", 1, tags) >> logger
            .warn(
              s"${monitorContext} | status transitioned from ${monitorContext.action} -> ${status}. This could be caused by a new status transition call!"
            )
            .as(((), None))
        case Some(_) =>
          monitorState match {
            case MonitorState.Initial                        => handleInitial(monitorContext)
            case MonitorState.Check(runtimeAndRuntimeConfig) => handleCheck(monitorContext, runtimeAndRuntimeConfig)
          }
      }
    } yield res

  def failedRuntime(monitorContext: MonitorContext,
                    runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                    errorDetails: Option[RuntimeErrorDetails],
                    instances: Set[DataprocInstance])(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[CheckResult]

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
      _ <- logger.info(
        s"${monitorContext} | Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} is ready for use after ${timeElapsed.toSeconds} seconds!"
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
      implicit0(traceId: ApplicativeAsk[F, AppContext]) = ApplicativeAsk.const[F, AppContext](
        AppContext(monitorContext.traceId, now)
      )
      timeElapsed = (now.toEpochMilli - monitorContext.start.toEpochMilli).millis
      res <- monitorConfig.monitorStatusTimeouts.get(runtimeAndRuntimeConfig.runtime.status) match {
        case Some(timeLimit) if timeElapsed > timeLimit =>
          for {
            _ <- logger.info(
              s"Detected that ${runtimeAndRuntimeConfig.runtime.projectNameString} has been stuck in status ${runtimeAndRuntimeConfig.runtime.status} too long."
            )
            r <- // Take care not to Error out a cluster if it timed out in Starting status
            if (runtimeAndRuntimeConfig.runtime.status == RuntimeStatus.Starting) {
              for {
                _ <- runtimeAlg.stopRuntime(StopRuntimeParams(runtimeAndRuntimeConfig, now))
                // Stopping the runtime
                _ <- clusterQuery
                  .updateClusterStatusAndHostIp(runtimeAndRuntimeConfig.runtime.id, RuntimeStatus.Stopping, ip, now)
                  .transaction
                runtimeAndRuntimeConfigAfterSetIp = ip.fold(runtimeAndRuntimeConfig)(i =>
                  LeoLenses.ipRuntimeAndRuntimeConfig.set(i)(runtimeAndRuntimeConfig)
                )
                rrc = runtimeAndRuntimeConfigAfterSetIp.lens(_.runtime.status).set(RuntimeStatus.Stopping)
              } yield ((), Some(MonitorState.Check(rrc))): CheckResult
            } else
              failedRuntime(
                monitorContext,
                runtimeAndRuntimeConfig,
                Some(
                  RuntimeErrorDetails(
                    s"Failed to transition ${runtimeAndRuntimeConfig.runtime.projectNameString} from status ${runtimeAndRuntimeConfig.runtime.status} within the time limit: ${timeLimit.toSeconds} seconds"
                  )
                ),
                dataprocInstances
              )
          } yield r
        case _ =>
          logger
            .info(
              s"${monitorContext} | Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} is not in final state yet and has taken ${timeElapsed.toSeconds} seconds so far. Checking again in ${monitorConfig.pollingInterval}. ${message
                .getOrElse("")}"
            )
            .as(((), Some(Check(runtimeAndRuntimeConfig))))
      }
    } yield res

  def handleCheck(monitorContext: MonitorContext, runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig)(
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[CheckResult]

  protected def handleInitial(
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

  protected def setStagingBucketLifecycle(runtime: Runtime, stagingBucketExpiration: FiniteDuration)(
    implicit ev: ApplicativeAsk[F, AppContext]
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
    implicit ev: ApplicativeAsk[F, AppContext]
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
      _ <- logger.info(
        s"Runtime ${runtimeAndRuntimeConfig.runtime.projectNameString} has been stopped after ${stoppingDuration.toSeconds} seconds."
      )
    } yield ((), None)

  private[monitor] def checkUserScriptsOutputFile(
    gcsPath: GcsPath
  )(implicit ev: ApplicativeAsk[F, TraceId]): F[Option[Boolean]] =
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
    implicit ev: ApplicativeAsk[F, AppContext]
  ): F[CheckResult] =
    // Update the Host IP in the database so DNS cache can be properly populated with the first cache miss
    // Otherwise, when a cluster is resumed and transitions from Starting to Running, we get stuck
    // in that state - at least with the way HttpJupyterDAO.isProxyAvailable works
    for {
      ctx <- ev.ask
      imageTypes <- dbRef
        .inTransaction { //If toolsToCheck is not defined, then we haven't check any tools yet. Hence retrieve all tools from database
          for {
            _ <- clusterQuery.updateClusterHostIp(runtimeAndRuntimeConfig.runtime.id, Some(ip), ctx.now)
            images <- clusterImageQuery.getAllForCluster(runtimeAndRuntimeConfig.runtime.id)
          } yield images.toList.map(_.imageType)
        }
      checkTools = imageTypes.traverseFilter { imageType =>
        RuntimeContainerServiceType.imageTypeToRuntimeContainerServiceType
          .get(imageType)
          .traverse(
            _.isProxyAvailable(runtimeAndRuntimeConfig.runtime.googleProject,
                               runtimeAndRuntimeConfig.runtime.runtimeName).map(b => (imageType, b))
          )
      }
      availableTools <- streamFUntilDone(checkTools, 10, 5 seconds).compile.lastOrError
      res <- availableTools match {
        case a if a.forall(_._2) =>
          readyRuntime(runtimeAndRuntimeConfig, ip, monitorContext, dataprocInstances)
        case a =>
          val toolsStillNotAvailable = a.collect { case x if x._2 == false => x._1 }
          failedRuntime(
            monitorContext,
            runtimeAndRuntimeConfig,
            Some(
              RuntimeErrorDetails(s"${toolsStillNotAvailable} didn't start up properly", None, Some("tool_start_up"))
            ),
            dataprocInstances
          )
      }
    } yield res

  protected def saveClusterError(runtimeId: Long, errorMessage: String, errorCode: Int, now: Instant): F[Unit] =
    dbRef
      .inTransaction {
        clusterErrorQuery.save(runtimeId, RuntimeError(errorMessage, errorCode, now))
      }
      .void
      .adaptError {
        case e => new Exception(s"Error persisting cluster error with message '${errorMessage}' to database: ${e}", e)
      }

  protected def deleteInitBucket(googleProject: GoogleProject,
                                 runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[F, AppContext]): F[Unit] =
    clusterQuery.getInitBucket(googleProject, runtimeName).transaction.flatMap {
      case None =>
        logger.warn(
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
