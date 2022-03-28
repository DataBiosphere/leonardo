package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.mtl.Ask
import fs2.Stream
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

class CloudServiceRuntimeMonitor[F[_]: Async](
  gceRuntimeMonitorInterp: GceRuntimeMonitor[F],
  dataprocRuntimeMonitorInterp: DataprocRuntimeMonitor[F]
) extends RuntimeMonitor[F, CloudService] {
  def process(
    a: CloudService
  )(runtimeId: Long, action: RuntimeStatus)(implicit ev: Ask[F, TraceId]): Stream[F, Unit] = a match {
    case CloudService.GCE      => gceRuntimeMonitorInterp.process(runtimeId, action)
    case CloudService.Dataproc => dataprocRuntimeMonitorInterp.process(runtimeId, action)
    case CloudService.AzureVm =>
      Stream.eval(
        Async[F].raiseError(
          AzureUnimplementedException("Azure vms should not be handled with CloudServiceRuntimeMonitor")
        )
      )
  }

  // Function used for transitions that we can get an Operation
  def pollCheck(a: CloudService)(googleProject: GoogleProject,
                                 runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig,
                                 operation: com.google.cloud.compute.v1.Operation,
                                 action: RuntimeStatus)(implicit ev: Ask[F, TraceId]): F[Unit] = a match {
    case CloudService.GCE =>
      gceRuntimeMonitorInterp.pollCheck(googleProject, runtimeAndRuntimeConfig, operation, action)
    case CloudService.Dataproc =>
      dataprocRuntimeMonitorInterp.pollCheck(googleProject, runtimeAndRuntimeConfig, operation, action)
    case CloudService.AzureVm =>
      Async[F].raiseError(
        AzureUnimplementedException("Azure vms should not be handled with CloudServiceRuntimeMonitor")
      )
  }

  def handlePollCheckCompletion(a: CloudService)(monitorContext: MonitorContext,
                                                 runtimeAndRuntimeConfig: RuntimeAndRuntimeConfig): F[Unit] = a match {
    case CloudService.GCE =>
      gceRuntimeMonitorInterp.handlePollCheckCompletion(monitorContext, runtimeAndRuntimeConfig)
    case CloudService.Dataproc =>
      Async[F].raiseError(new Exception("handlePollCheckCompletion not supported for Dataproc"))
    case CloudService.AzureVm =>
      Async[F].raiseError(
        AzureUnimplementedException("Azure vms should not be handled with CloudServiceRuntimeMonitor")
      )
  }
}
