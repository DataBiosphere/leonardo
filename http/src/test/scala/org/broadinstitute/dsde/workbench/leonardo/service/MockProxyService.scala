package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Host
import cats.effect.{Blocker, ContextShift, IO, Timer}
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.HostReady
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.{KubernetesDnsCache, RuntimeDnsCache}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.UpdateDateAccessMessage
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

class MockProxyService(
  proxyConfig: ProxyConfig,
  gdDAO: GoogleDataprocDAO,
  authProvider: LeoAuthProvider[IO],
  runtimeDnsCache: RuntimeDnsCache[IO],
  kubernetesDnsCache: KubernetesDnsCache[IO],
  queue: Option[InspectableQueue[IO, UpdateDateAccessMessage]] = None
)(implicit system: ActorSystem,
  executionContext: ExecutionContext,
  timer: Timer[IO],
  cs: ContextShift[IO],
  dbRef: DbReference[IO])
    extends ProxyService(proxyConfig: ProxyConfig,
                         gdDAO: GoogleDataprocDAO,
                         runtimeDnsCache,
                         kubernetesDnsCache,
                         authProvider,
                         queue.getOrElse(InspectableQueue.bounded[IO, UpdateDateAccessMessage](100).unsafeRunSync),
                         Blocker.liftExecutionContext(ExecutionContext.global)) {

  override def getRuntimeTargetHost(googleProject: GoogleProject, clusterName: RuntimeName): IO[HostStatus] =
    IO.pure(HostReady(Host("localhost")))

  override def getAppTargetHost(googleProject: GoogleProject, appName: AppName): IO[HostStatus] =
    IO.pure(HostReady(Host("localhost")))

}
