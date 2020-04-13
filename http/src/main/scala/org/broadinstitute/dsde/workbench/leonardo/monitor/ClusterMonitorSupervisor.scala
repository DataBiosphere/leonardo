package org.broadinstitute.dsde.workbench.leonardo
package monitor

import java.time.Instant
import java.util.UUID

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, Timers}
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, GoogleStorageService}
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.dao.{RStudioDAO, ToolDAO, WelderDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, RuntimeConfigQueries, clusterQuery}
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoAuthProvider
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.ClusterSupervisorMessage._
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterMonitorSupervisor.{ClusterSupervisorMessage, _}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.CreateRuntimeMessage
import org.broadinstitute.dsde.workbench.leonardo.util.{RuntimeInstances, StopRuntimeParams}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchException}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext

object ClusterMonitorSupervisor {
  def props(
    monitorConfig: MonitorConfig,
    dataprocConfig: DataprocConfig,
    gceConfig: GceConfig,
    imageConfig: ImageConfig,
    clusterBucketConfig: RuntimeBucketConfig,
    gdDAO: GoogleDataprocDAO,
    googleComputeService: GoogleComputeService[IO],
    google2StorageDAO: GoogleStorageService[IO],
    authProvider: LeoAuthProvider[IO],
    rstudioProxyDAO: RStudioDAO[IO],
    welderDAO: WelderDAO[IO],
    publisherQueue: fs2.concurrent.InspectableQueue[IO, LeoPubsubMessage]
  )(implicit openTelemetryMetrics: OpenTelemetryMetrics[IO],
    timer: Timer[IO],
    dbRef: DbReference[IO],
    ec: ExecutionContext,
    clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType],
    cs: ContextShift[IO],
    runtimeInstances: RuntimeInstances[IO]): Props =
    Props(
      new ClusterMonitorSupervisor(monitorConfig,
                                   dataprocConfig,
                                   gceConfig,
                                   imageConfig,
                                   clusterBucketConfig,
                                   gdDAO,
                                   googleComputeService,
                                   google2StorageDAO,
                                   authProvider,
                                   rstudioProxyDAO,
                                   welderDAO,
                                   publisherQueue)
    )

  sealed trait ClusterSupervisorMessage extends Product with Serializable

  object ClusterSupervisorMessage {

    // sent after a cluster is created by the user
    case class ClusterCreated(cluster: Runtime, stopAfterCreate: Boolean = false) extends ClusterSupervisorMessage

    // sent after a cluster is deleted by the user
    case class ClusterDeleted(cluster: Runtime, recreate: Boolean = false) extends ClusterSupervisorMessage

    // sent after a cluster is stopped by the user
    case class ClusterStopped(cluster: Runtime) extends ClusterSupervisorMessage

    // sent after a cluster is started by the user
    case class ClusterStarted(cluster: Runtime) extends ClusterSupervisorMessage

    // sent after a cluster is updated by the user
    case class ClusterUpdated(cluster: Runtime) extends ClusterSupervisorMessage

    // sent after cluster creation fails, and the cluster should be recreated
    case class RecreateCluster(cluster: Runtime) extends ClusterSupervisorMessage

    // sent after cluster creation succeeds, and the cluster should be stopped
    case class StopClusterAfterCreation(cluster: Runtime) extends ClusterSupervisorMessage

    //Sent when the cluster should be removed from the monitored cluster list
    case class RemoveFromList(cluster: Runtime) extends ClusterSupervisorMessage
  }

  case object CheckClusterTimerKey
  private case object CheckForClusters extends ClusterSupervisorMessage
}

class ClusterMonitorSupervisor(
  monitorConfig: MonitorConfig,
  dataprocConfig: DataprocConfig,
  gceConfig: GceConfig,
  imageConfig: ImageConfig,
  clusterBucketConfig: RuntimeBucketConfig,
  gdDAO: GoogleDataprocDAO,
  googleComputeService: GoogleComputeService[IO],
  google2StorageDAO: GoogleStorageService[IO],
  authProvider: LeoAuthProvider[IO],
  rstudioProxyDAO: RStudioDAO[IO],
  welderProxyDAO: WelderDAO[IO],
  publisherQueue: fs2.concurrent.InspectableQueue[IO, LeoPubsubMessage]
)(implicit openTelemetryMetrics: OpenTelemetryMetrics[IO],
  ec: ExecutionContext,
  dbRef: DbReference[IO],
  clusterToolToToolDao: RuntimeContainerServiceType => ToolDAO[IO, RuntimeContainerServiceType],
  cs: ContextShift[IO],
  timer: Timer[IO],
  runtimeInstances: RuntimeInstances[IO])
    extends Actor
    with Timers
    with LazyLogging {

  var monitoredClusterIds: Set[Long] = Set.empty

  override def preStart(): Unit = {
    super.preStart()

    timers.startTimerWithFixedDelay(CheckClusterTimerKey, CheckForClusters, monitorConfig.pollPeriod)
  }

  override def receive: Receive = {

    case ClusterCreated(cluster, stopAfterCreate) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} for initialization.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster, if (stopAfterCreate) Some(StopClusterAfterCreation(cluster)) else None)

    case ClusterDeleted(cluster, recreate) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} for deletion.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster, if (recreate) Some(RecreateCluster(cluster)) else None)

    case ClusterUpdated(cluster) =>
      logger.info(s"Monitor cluster ${cluster.projectNameString} for updating.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster, None)

    case RecreateCluster(cluster) =>
      val traceId = TraceId(UUID.randomUUID())
      implicit val traceIdIO = ApplicativeAsk.const[IO, TraceId](traceId)

      val res = if (monitorConfig.recreateCluster) {
        logger.info(s"[$traceId] Recreating cluster ${cluster.projectNameString}...")
        removeFromMonitoredClusters(cluster)
        for {
          now <- IO(Instant.now)
          _ <- (clusterQuery.clearAsyncClusterCreationFields(cluster, now) >>
            clusterQuery.updateClusterStatus(cluster.id, RuntimeStatus.Creating, now)).transaction
          runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
          _ <- publisherQueue.enqueue1(CreateRuntimeMessage.fromRuntime(cluster, runtimeConfig, Some(traceId)))
        } yield ()
      } else {
        IO(
          logger.warn(
            s"[$traceId] Received RecreateCluster message for cluster ${cluster.projectNameString} but cluster recreation is disabled."
          )
        )
      }

      res.unsafeRunSync()

    case StopClusterAfterCreation(cluster) =>
      val traceId = UUID.randomUUID()
      implicit val traceIdIO = ApplicativeAsk.const[IO, TraceId](TraceId(traceId))

      logger.info(s"Stopping cluster ${cluster.projectNameString} after creation...")
      (for {
        now <- IO(Instant.now())
        runtimeOpt <- clusterQuery.getClusterById(cluster.id).transaction
        _ <- runtimeOpt match {
          case Some(resolvedCluster) if resolvedCluster.status.isStoppable => stopCluster(resolvedCluster, now)
          case Some(resolvedCluster) =>
            IO(
              logger.warn(
                s"Unable to stop cluster ${resolvedCluster.projectNameString} in status ${resolvedCluster.status.toString} after creation."
              )
            )
          case None =>
            IO.raiseError(new WorkbenchException(s"Cluster ${cluster.projectNameString} not found in the database"))
        }
      } yield ())
        .handleErrorWith(e =>
          IO(logger.error(s"Error occurred stopping cluster ${cluster.projectNameString} after creation", e)) >> IO
            .raiseError(e)
        )
        .unsafeToFuture()

    case ClusterStopped(cluster) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} after stopping.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster)

    case ClusterStarted(cluster) =>
      logger.info(s"Monitoring cluster ${cluster.projectNameString} after starting.")
      addToMonitoredClusters(cluster)
      startClusterMonitorActor(cluster)

    case CheckForClusters =>
      createClusterMonitors.unsafeRunSync()

    case RemoveFromList(cluster) =>
      removeFromMonitoredClusters(cluster)
  }

  def createChildActor(cluster: Runtime): ActorRef =
    context.actorOf(
      ClusterMonitorActor.props(
        cluster.id,
        monitorConfig,
        dataprocConfig,
        gceConfig,
        imageConfig,
        clusterBucketConfig,
        gdDAO,
        googleComputeService,
        google2StorageDAO,
        dbRef,
        authProvider,
        publisherQueue
      )
    )

  def startClusterMonitorActor(cluster: Runtime, watchMessageOpt: Option[ClusterSupervisorMessage] = None): Unit = {
    val child = createChildActor(cluster)
    watchMessageOpt.foreach {
      case RecreateCluster(_) if !monitorConfig.recreateCluster =>
      // don't recreate clusters if not configured to do so
      case watchMsg =>
        context.watchWith(child, watchMsg)
    }
  }

  private def stopCluster(cluster: Runtime, now: Instant)(implicit ev: ApplicativeAsk[IO, TraceId]): IO[Unit] =
    for {
      // Flush the welder cache to disk
      _ <- if (cluster.welderEnabled) {
        welderProxyDAO
          .flushCache(cluster.googleProject, cluster.runtimeName)
          .handleError(e => logger.error(s"Failed to flush welder cache for ${cluster.projectNameString}", e))
      } else IO.unit
      runtimeConfig <- RuntimeConfigQueries.getRuntimeConfig(cluster.runtimeConfigId).transaction
      // Stop the cluster in Google
      _ <- runtimeConfig.cloudService.interpreter
        .stopRuntime(StopRuntimeParams(RuntimeAndRuntimeConfig(cluster, runtimeConfig), now))
      // Update the cluster status to Stopping
      _ <- dbRef.inTransaction(clusterQuery.setToStopping(cluster.id, now))
    } yield ()

  private def createClusterMonitors(): IO[Unit] =
    dbRef
      .inTransaction(clusterQuery.listMonitoredDataproc)
      .attempt
      .flatMap {
        case Right(clusters) =>
          val clustersNotAlreadyBeingMonitored = clusters.filterNot(c => monitoredClusterIds.contains(c.id))
          clustersNotAlreadyBeingMonitored.toList.traverse_ {
                case c if c.status == RuntimeStatus.Deleting => IO(self ! ClusterDeleted(c))

                case c if c.status == RuntimeStatus.Stopping => IO(self ! ClusterStopped(c))

                case c if c.status == RuntimeStatus.Starting => IO(self ! ClusterStarted(c))

                case c if c.status == RuntimeStatus.Updating => IO(self ! ClusterUpdated(c))

                case c if c.status == RuntimeStatus.Creating && c.asyncRuntimeFields.isDefined =>
                  IO(self ! ClusterCreated(c, c.stopAfterCreation))

                case c => IO(logger.warn(s"Unhandled status(${c.status}) in ClusterMonitorSupervisor"))
              }
        case Left(e) =>
          IO(logger.error("Error starting cluster monitor", e))
      }

  private def addToMonitoredClusters(cluster: Runtime) =
    monitoredClusterIds += cluster.id
  private def removeFromMonitoredClusters(cluster: Runtime) =
    monitoredClusterIds -= cluster.id

  override val supervisorStrategy = {
    // TODO add threshold monitoring stuff from Rawls
    // for now always restart the child actor in case of failure
    OneForOneStrategy(maxNrOfRetries = monitorConfig.maxRetries) {
      case _ => Restart
    }
  }
}
