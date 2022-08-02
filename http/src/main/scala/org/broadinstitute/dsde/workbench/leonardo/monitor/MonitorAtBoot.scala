package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.{GoogleComputeService, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.dao.{SamDAO, WsmDao}
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{CreateAppMessage, DeleteAppMessage}
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

class MonitorAtBoot[F[_]](publisherQueue: Queue[F, LeoPubsubMessage],
                          computeService: GoogleComputeService[F],
                          samDAO: SamDAO[F],
                          wsmDao: WsmDao[F]
)(implicit
  F: Async[F],
  dbRef: DbReference[F],
  logger: Logger[F],
  ec: ExecutionContext,
  metrics: OpenTelemetryMetrics[F]
) {
  val process: Stream[F, Unit] = processRuntimes ++ processApps

  private def processRuntimes: Stream[F, Unit] =
    monitoredRuntimes
      .parEvalMapUnordered(10) { r =>
        for {
          now <- F.realTimeInstant
          implicit0(traceId: Ask[F, TraceId]) = Ask.const[F, TraceId](TraceId(s"BootMonitoring${now}"))
          _ <- handleRuntime(r)
          _ <- handleRuntimePatchInProgress(r)
        } yield ()
      }
      .handleErrorWith(e =>
        Stream
          .eval(logger.error(e)("MonitorAtBoot: Error retrieving runtimes that need to be monitored during startup"))
      )

  private def processApps: Stream[F, Unit] =
    monitoredApps
      .parEvalMapUnordered(10) { case (a, n, c) =>
        for {
          now <- F.realTimeInstant
          implicit0(traceId: Ask[F, TraceId]) = Ask.const[F, TraceId](TraceId(s"BootMonitoring${now}"))
          _ <- handleApp(a, n, c)
        } yield ()
      }
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
      msg <- runtimeToMonitor.cloudContext match {
        case CloudContext.Gcp(_) =>
          runtimeStatusToMessageGCP(runtimeToMonitor, traceId)
        case CloudContext.Azure(_) =>
          runtimeStatusToMessageAzure(runtimeToMonitor, traceId)
      }
      _ <- publisherQueue.offer(msg)
    } yield ()
    res.handleErrorWith(e => logger.error(e)(s"MonitorAtBoot: Error monitoring runtime ${runtimeToMonitor.id}"))
  }

  private def handleRuntimePatchInProgress(
    runtimeToMonitor: RuntimeToMonitor
  )(implicit ev: Ask[F, TraceId]): F[Unit] =
    for {
      traceId <- ev.ask
      patchInProgress <- patchQuery.isInprogress(runtimeToMonitor.id).transaction
      _ <-
        if (patchInProgress) {
          for {
            statusOpt <- clusterQuery.getClusterStatus(runtimeToMonitor.id).transaction
            s <- F.fromEither(
              statusOpt
                .toRight(
                  MonitorAtBootException(s"${runtimeToMonitor.id} not found after transition. This is very weird!",
                                         traceId
                  )
                )
            )
            _ <-
              if (s != RuntimeStatus.Running) {
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

  private def handleApp(app: App, nodepool: Nodepool, cluster: KubernetesCluster)(implicit
    ev: Ask[F, TraceId]
  ): F[Unit] = {
    val res = for {
      msg <- appStatusToMessage(app, nodepool, cluster)
      _ <- publisherQueue.offer(msg)
    } yield ()
    res.handleErrorWith(e => logger.error(e)(s"MonitorAtBoot: Error monitoring app ${app.id}"))
  }

  private def appStatusToMessage(app: App, nodepool: Nodepool, cluster: KubernetesCluster)(implicit
    ev: Ask[F, TraceId]
  ): F[LeoPubsubMessage] =
    ev.ask.flatMap(traceId =>
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
                                      )
                  )
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
            googleProject <- F.fromOption(
              LeoLenses.cloudContextToGoogleProject.get(cluster.cloudContext),
              new RuntimeException(
                "trying to provision app on Azure during MonitorAtBoot. This is not supported yet"
              ) // TODO: support azure
            )
            machineType <- computeService
              .getMachineType(
                googleProject,
                ZoneName("us-central1-a"),
                nodepool.machineType
              ) // TODO: if use non `us-central1-a` zone for galaxy, this needs to be udpated
              .flatMap(opt =>
                F.fromOption(
                  opt,
                  new LeoException(s"can't find machine config for ${app.appName.value}", traceId = Some(traceId))
                )
              )

            diskIdOpt = app.appResources.disk.flatMap(d => if (d.status == DiskStatus.Creating) Some(d.id) else None)
            msg = CreateAppMessage(
              googleProject,
              action,
              app.id,
              app.appName,
              diskIdOpt,
              app.customEnvironmentVariables,
              app.appType,
              app.appResources.namespace.name,
              Some(AppMachineType(machineType.getMemoryMb / 1024, machineType.getGuestCpus)),
              Some(traceId)
            )
          } yield msg

        case AppStatus.Deleting =>
          for {
            googleProject <- F.fromOption(
              LeoLenses.cloudContextToGoogleProject.get(cluster.cloudContext),
              new RuntimeException(
                "trying to provision app on Azure during MonitorAtBoot. This is not supported yet"
              ) // TODO: support azure
            )
          } yield DeleteAppMessage(
            app.id,
            app.appName,
            googleProject,
            None, // Assume we do not want to delete the disk, since we don't currently persist that information
            Some(traceId)
          )

        case x => F.raiseError(MonitorAtBootException(s"Unexpected status for app ${app.id}: ${x}", traceId))
      }
    )

  private def runtimeStatusToMessageGCP(runtime: RuntimeToMonitor, traceId: TraceId): F[LeoPubsubMessage] =
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
        ) // If user specified `deleteDisk` being true in the original request, then we can't really recover; User will have to explicitly delete disk in UI again
      case RuntimeStatus.Starting =>
        F.pure(
          LeoPubsubMessage.StartRuntimeMessage(
            runtime.id,
            Some(traceId)
          )
        )
      case RuntimeStatus.Creating =>
        val message: Either[String, RuntimeConfigInCreateRuntimeMessage] = runtime.runtimeConfig match {
          case x: RuntimeConfig.GceConfig =>
            for {
              bootDiskSize <- x.bootDiskSize.toRight(
                s"Disk Size field not found for ${runtime.id}. This should never happen"
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
                s"disk id field not found for ${runtime.id}. This should never happen"
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
              LeoLenses.runtimeConfigPrism.getOption(runtime.runtimeConfig).get: RuntimeConfigInCreateRuntimeMessage
            )
          case _: RuntimeConfig.AzureConfig =>
            "Azure runtime should be handled separately. This should not happen"
              .asLeft[RuntimeConfigInCreateRuntimeMessage]
        }

        for {
          rtConfigInMessage <- F.fromEither(message.leftMap(s => MonitorAtBootException(s, traceId)))
          extra <- clusterQuery.getExtraInfo(runtime.id).transaction
        } yield LeoPubsubMessage.CreateRuntimeMessage(
          runtime.id,
          RuntimeProjectAndName(runtime.cloudContext, runtime.runtimeName),
          runtime.serviceAccount,
          runtime.asyncRuntimeFields,
          runtime.auditInfo,
          runtime.userScriptUri,
          runtime.startUserScriptUri,
          extra.userJupyterExtensionConfig,
          runtime.defaultClientId,
          extra.runtimeImages,
          extra.scopes,
          runtime.welderEnabled,
          runtime.customEnvironmentVariables,
          rtConfigInMessage,
          Some(traceId)
        )
      case x => F.raiseError(MonitorAtBootException(s"Unexpected status for runtime ${runtime.id}: ${x}", traceId))
    }

  private def runtimeStatusToMessageAzure(runtime: RuntimeToMonitor, traceId: TraceId): F[LeoPubsubMessage] =
    runtime.status match {
      case RuntimeStatus.Stopping =>
        F.raiseError(MonitorAtBootException("Stopping Azure runtime is not supported yet", traceId))
      case RuntimeStatus.Deleting =>
        for {
          wid <- F.fromOption(runtime.workspaceId,
                              MonitorAtBootException(s"no workspaceId found for ${runtime.id.toString}", traceId)
          )
          controlledResourceOpt <- controlledResourceQuery
            .getWsmRecordForRuntime(runtime.id, WsmResourceType.AzureVm)
            .transaction
        } yield LeoPubsubMessage.DeleteAzureRuntimeMessage(
          runtimeId = runtime.id,
          None,
          workspaceId = wid,
          wsmResourceId = controlledResourceOpt.map(_.resourceId),
          traceId = Some(traceId)
        )
      case RuntimeStatus.Starting =>
        F.raiseError(MonitorAtBootException("Startting Azure runtime is not supported yet", traceId))
      case RuntimeStatus.Creating =>
        for {
          now <- F.realTimeInstant
          wid <- F.fromOption(runtime.workspaceId,
                              MonitorAtBootException(s"no workspaceId found for ${runtime.id.toString}", traceId)
          )
          leoAuth <- samDAO.getLeoAuthToken
          azureRuntimeConfig <- runtime.runtimeConfig match {
            case x: RuntimeConfig.AzureConfig => F.pure(x)
            case _ =>
              F.raiseError(MonitorAtBootException("Azure runtime shouldn't have non Azure runtime config", traceId))
          }
          implicit0(appContext: Ask[F, AppContext]) <- F.pure(Ask.const(AppContext(traceId, now)))
          relayNamespaceOpt <- wsmDao.getRelayNamespace(wid, azureRuntimeConfig.region, leoAuth)
          rns <- F.fromOption(relayNamespaceOpt,
                              MonitorAtBootException(s"no relay namespace found for ${wid}", traceId)
          )
        } yield LeoPubsubMessage.CreateAzureRuntimeMessage(
          runtime.id,
          wid,
          rns,
          Some(traceId)
        )
      case x => F.raiseError(MonitorAtBootException(s"Unexpected status for runtime ${runtime.id}: ${x}", traceId))
    }
}

final case class RuntimeToMonitor(
  id: Long,
  workspaceId: Option[WorkspaceId],
  cloudContext: CloudContext,
  runtimeName: RuntimeName,
  status: RuntimeStatus,
  patchInProgress: Boolean,
  runtimeConfig: RuntimeConfig,
  serviceAccount: WorkbenchEmail,
  asyncRuntimeFields: Option[AsyncRuntimeFields],
  auditInfo: AuditInfo,
  userScriptUri: Option[UserScriptPath],
  startUserScriptUri: Option[UserScriptPath],
  defaultClientId: Option[String],
  welderEnabled: Boolean,
  customEnvironmentVariables: Map[String, String]
)

final case class ExtraInfoForCreateRuntime(runtimeImages: Set[RuntimeImage],
                                           userJupyterExtensionConfig: Option[UserJupyterExtensionConfig],
                                           scopes: Set[String]
)

final case class MonitorAtBootException(msg: String, traceId: TraceId)
    extends Exception(s"MonitorAtBoot: $msg | trace id: ${traceId.asString}")
