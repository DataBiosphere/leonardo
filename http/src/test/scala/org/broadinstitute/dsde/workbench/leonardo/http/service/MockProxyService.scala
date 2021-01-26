package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Host
import cats.effect.{Blocker, ContextShift, IO, Timer}
import fs2.concurrent.InspectableQueue
import io.chrisdavenport.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.leonardo.HostStatus.HostReady
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.dao.{JupyterDAO, MockJupyterDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.{KubernetesDnsCache, RuntimeDnsCache}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.UpdateDateAccessMessage
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.ExecutionContext

class MockProxyService(
  proxyConfig: ProxyConfig,
  jupyterDAO: JupyterDAO[IO] = MockJupyterDAO,
  authProvider: LeoAuthProvider[IO],
  runtimeDnsCache: RuntimeDnsCache[IO],
  kubernetesDnsCache: KubernetesDnsCache[IO],
  googleOauth2Service: GoogleOAuth2Service[IO],
  queue: Option[InspectableQueue[IO, UpdateDateAccessMessage]] = None
)(implicit system: ActorSystem,
  executionContext: ExecutionContext,
  timer: Timer[IO],
  cs: ContextShift[IO],
  dbRef: DbReference[IO],
  metrics: OpenTelemetryMetrics[IO],
  logger: StructuredLogger[IO])
    extends ProxyService(TestUtils.sslContext(system),
                         proxyConfig,
                         jupyterDAO,
                         runtimeDnsCache,
                         kubernetesDnsCache,
                         authProvider,
                         queue.getOrElse(InspectableQueue.bounded[IO, UpdateDateAccessMessage](100).unsafeRunSync),
                         googleOauth2Service,
                         Blocker.liftExecutionContext(ExecutionContext.global)) {

  override def getRuntimeTargetHost(googleProject: GoogleProject, clusterName: RuntimeName): IO[HostStatus] =
    IO.pure(HostReady(Host("localhost")))

  override def getAppTargetHost(googleProject: GoogleProject, appName: AppName): IO[HostStatus] =
    IO.pure(HostReady(Host("localhost")))

}
