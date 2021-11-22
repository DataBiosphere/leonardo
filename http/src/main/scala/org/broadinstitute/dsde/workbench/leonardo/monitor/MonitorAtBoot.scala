package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.effect.std.Queue
import cats.syntax.all._
import cats.mtl.Ask
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.broadinstitute.dsde.workbench.errorReporting.ErrorReporting
import org.broadinstitute.dsde.workbench.errorReporting.ReportWorthySyntax._
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{CreateAppMessage, DeleteAppMessage}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext

class MonitorAtBoot[F[_]](publisherQueue: Queue[F, LeoPubsubMessage],
                          computeService: GoogleComputeService[F],
                          errorReporting: ErrorReporting[F])(
  implicit F: Async[F],
  dbRef: DbReference[F],
  logger: Logger[F],
  ec: ExecutionContext,
  metrics: OpenTelemetryMetrics[F]
) {
  implicit private val traceId = Ask.const[F, TraceId](TraceId("BootMonitoring"))

  val process: Stream[F, Unit] = processRuntimes ++ processApps

  private def processRuntimes: Stream[F, Unit] =
    monitoredRuntimes
      .parEvalMapUnordered(10)(r => handleRuntime(r) >> handleRuntimePatchInProgress(r))
      .handleErrorWith(e =>
        Stream
          .eval(logger.error(e)("MonitorAtBoot: Error retrieving runtimes that need to be monitored during startup"))
      )

  private def processApps: Stream[F, Unit] =
    monitoredApps
      .parEvalMapUnordered(10) { case (a, n, c) => handleApp(a, n, c) }
      .handleErrorWith(e =>
        Stream.eval(logger.error(e)("MonitorAtBoot: Error retrieving apps that need to be monitored during startup"))
      )

  private def monitoredRuntimes: Stream[F, RuntimeToMonitor] =
    Stream.evals(clusterQuery.listMonitored.transaction.map(_.toList))

  private def monitoredApps: Stream[F, (App, Nodepool, KubernetesCluster)] =
    for {
      c <- Stream.evals(KubernetesServiceDbQueries.listMonitoredApps.transaction)
      n <- Stream.emits(c.nodepools)
      a <- Stream.emits(n.apps)
    } yield (a, n, c)

  private def handleRuntime(runtimeToMonitor: RuntimeToMonitor)(implicit ev: Ask[F, TraceId]): F[Unit] = {
    val res = for {
      traceId <- ev.ask[TraceId]
      msg <- runtimeStatusToMessage(runtimeToMonitor, traceId)
      _ <- publisherQueue.offer(msg)
    } yield ()
    res.handleErrorWith { e =>
      logger.error(e)(s"MonitorAtBoot: Error monitoring runtime ${runtimeToMonitor.id}") >>
        (if (e.isReportWorthy) errorReporting.reportError(e) else F.unit)
    }
  }

  private def handleRuntimePatchInProgress(
    runtimeToMonitor: RuntimeToMonitor
  )(implicit ev: Ask[F, TraceId]): F[Unit] =
    for {
      traceId <- ev.ask
      patchInProgress <- patchQuery.isInprogress(runtimeToMonitor.id).transaction
      _ <- if (patchInProgress) {
        for {
          statusOpt <- clusterQuery.getClusterStatus(runtimeToMonitor.id).transaction
          s <- F.fromEither(
            statusOpt
              .toRight(
                MonitorAtBootException(s"${runtimeToMonitor.id} not found after transition. This is very weird!",
                                       traceId)
              )
          )
          _ <- if (s != RuntimeStatus.Running) {
            // There's slight chance where pubsub message is never published during a redeploy.
            // In this case, user will see that the runtime doesn't get patched after clicking patch button.
            // In the ideal case, patch is completed, and runtime has come back to Running.
            metrics.incrementCounter("PatchInProgressFailed")
          } else {
            // If patch is in progress and we didn't finish patching, we don't really have a good way to recover;
            // There is a chance that leonardo will be able to recover if the UpdateRuntimeEvent has already been sent to pubsub,
            // we'll evaluate if this edge case is worth addressing based on PatchInProgressAtStartUp metrics
            F.unit
          }
          _ <- patchQuery.updatePatchAsComplete(runtimeToMonitor.id).transaction
          _ <- metrics.incrementCounter("PatchInProgressAtStartUp")
        } yield ()
      } else F.unit
    } yield ()

  private def handleApp(app: App, nodepool: Nodepool, cluster: KubernetesCluster)(
    implicit ev: Ask[F, TraceId]
  ): F[Unit] = {
    val res = for {
      traceId <- ev.ask
      msg <- appStatusToMessage(app, nodepool, cluster, traceId)
      _ <- publisherQueue.offer(msg)
    } yield ()
    res.handleErrorWith { e =>
      logger.error(e)(s"MonitorAtBoot: Error monitoring app ${app.id}") >>
        (if (e.isReportWorthy) errorReporting.reportError(e) else F.unit)
    }
  }

  private def appStatusToMessage(app: App,
                                 nodepool: Nodepool,
                                 cluster: KubernetesCluster,
                                 traceId: TraceId): F[LeoPubsubMessage] =
    app.status match {
      case AppStatus.Provisioning =>
        for {
          action <- (cluster.status, nodepool.status) match {
            case (KubernetesClusterStatus.Provisioning, _) =>
              for {
                dnpOpt <- nodepoolQuery.getDefaultNodepoolForCluster(cluster.id).transaction
                dnp <- F.fromOption(dnpOpt,
                                    MonitorAtBootException(
                                      s"Default nodepool not found for cluster ${cluster.id} in Provisioning status",
                                      traceId
                                    ))
              } yield Some(ClusterNodepoolAction.CreateClusterAndNodepool(cluster.id, dnp.id, nodepool.id)): Option[
                ClusterNodepoolAction
              ]
            case (KubernetesClusterStatus.Running, NodepoolStatus.Provisioning) =>
              F.pure[Option[ClusterNodepoolAction]](Some(ClusterNodepoolAction.CreateNodepool(nodepool.id)))
            case (KubernetesClusterStatus.Running, NodepoolStatus.Running) =>
              F.pure[Option[ClusterNodepoolAction]](None)
            case (cs, ns) =>
              F.raiseError[Option[ClusterNodepoolAction]](
                MonitorAtBootException(
                  s"Unexpected cluster status [${cs.toString} or nodepool status [${ns.toString}] for app ${app.id} in Provisioning status",
                  traceId
                )
              )
          }
          machineType <- computeService
            .getMachineType(
              cluster.googleProject,
              ZoneName("us-central1-a"),
              machineTypeName
            ) //TODO: if use non `us-central1-a` zone for galaxy, this needs to be udpated
            .flatMap(opt =>
              F.fromOption(opt,
                           new LeoException(s"can't find machine config for ${ctx.requestUri}",
                                            traceId = Some(ctx.traceId)))
            )
          machineType <- if (machineType.getMemoryMb < 5000)
            F.raiseError(BadRequestException("Galaxy needs more memorary configuration", Some(ctx.traceId)))
          else if (machineType.getGuestCpus < 3)
            F.raiseError(BadRequestException("Galaxy needs more CPU configuration", Some(ctx.traceId)))
          else F.pure(AppMachineType(machineType.getMemoryMb, machineType.getGuestCpus))

          diskIdOpt = app.appResources.disk.flatMap(d => if (d.status == DiskStatus.Creating) Some(d.id) else None)
          msg = CreateAppMessage(
            cluster.googleProject,
            action,
            app.id,
            app.appName,
            diskIdOpt,
            app.customEnvironmentVariables,
            app.appType,
            app.appResources.namespace.name,
            Some(traceId)
          )
        } yield msg

      case AppStatus.Deleting =>
        F.pure(
          DeleteAppMessage(
            app.id,
            app.appName,
            cluster.googleProject,
            None, // Assume we do not want to delete the disk, since we don't currently persist that information
            Some(traceId)
          )
        )

      case x => F.raiseError(MonitorAtBootException(s"Unexpected status for app ${app.id}: ${x}", traceId))
    }

  private def runtimeStatusToMessage(runtime: RuntimeToMonitor, traceId: TraceId): F[LeoPubsubMessage] =
    runtime.status match {
      case RuntimeStatus.Stopping =>
        F.pure(LeoPubsubMessage.StopRuntimeMessage(runtime.id, Some(traceId)))
      case RuntimeStatus.Deleting =>
        F.pure(
          LeoPubsubMessage.DeleteRuntimeMessage(
            runtime.id,
            None,
            Some(traceId)
          )
        ) //If user specified `deleteDisk` being true in the original request, then we can't really recover; User will have to explicitly delete disk in UI again
      case RuntimeStatus.Starting =>
        F.pure(
          LeoPubsubMessage.StartRuntimeMessage(
            runtime.id,
            Some(traceId)
          )
        )
      case RuntimeStatus.Creating =>
        for {
          fullRuntime <- clusterQuery.getClusterById(runtime.id).transaction
          rt <- F.fromOption(fullRuntime, new Exception(s"can't find ${runtime.id} in DB"))
          rtConfig <- RuntimeConfigQueries.getRuntimeConfig(rt.runtimeConfigId).transaction
          r = rtConfig match {
            case x: RuntimeConfig.GceConfig =>
              for {
                bootDiskSize <- x.bootDiskSize.toRight(
                  s"disk Size field not found for ${rt.id}. This should never happen"
                )
              } yield RuntimeConfigInCreateRuntimeMessage.GceConfig(
                x.machineType,
                x.diskSize,
                bootDiskSize,
                x.zone,
                x.gpuConfig
              ): RuntimeConfigInCreateRuntimeMessage
            case x: RuntimeConfig.GceWithPdConfig =>
              for {
                diskId <- x.persistentDiskId.toRight(
                  s"disk id field not found for ${rt.id}. This should never happen"
                )
              } yield RuntimeConfigInCreateRuntimeMessage.GceWithPdConfig(
                x.machineType,
                diskId,
                x.bootDiskSize,
                x.zone,
                x.gpuConfig
              ): RuntimeConfigInCreateRuntimeMessage
            case _: RuntimeConfig.DataprocConfig =>
              Right(
                LeoLenses.runtimeConfigPrism.getOption(rtConfig).get: RuntimeConfigInCreateRuntimeMessage
              )
          }
          rtConfigInMessage <- F.fromEither(r.leftMap(s => MonitorAtBootException(s, traceId)))
        } yield {
          LeoPubsubMessage.CreateRuntimeMessage.fromRuntime(
            rt,
            rtConfigInMessage,
            Some(traceId)
          )
        }
      case x => F.raiseError(MonitorAtBootException(s"Unexpected status for runtime ${runtime.id}: ${x}", traceId))
    }
}

final case class RuntimeToMonitor(
  id: Long,
  cloudService: CloudService,
  status: RuntimeStatus,
  patchInProgress: Boolean
)

final case class MonitorAtBootException(msg: String, traceId: TraceId)
    extends Exception(s"MonitorAtBoot: $msg | trace id: ${traceId.asString}")
