package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Host
import cats.effect.{Blocker, ContextShift, IO, Timer}
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleDataprocDAO
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.{HostReady, HostStatus}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

/**
 * Created by rtitle on 8/25/17.
 */
class MockProxyService(
  proxyConfig: ProxyConfig,
  gdDAO: GoogleDataprocDAO,
  authProvider: LeoAuthProvider[IO],
  clusterDnsCache: ClusterDnsCache[IO]
)(implicit system: ActorSystem,
  executionContext: ExecutionContext,
  timer: Timer[IO],
  cs: ContextShift[IO],
  dbRef: DbReference[IO])
    extends ProxyService(proxyConfig: ProxyConfig,
                         gdDAO: GoogleDataprocDAO,
                         clusterDnsCache,
                         authProvider,
                         system.deadLetters,
                         Blocker.liftExecutionContext(ExecutionContext.global)) {

  override def getTargetHost(googleProject: GoogleProject, clusterName: ClusterName): IO[HostStatus] =
    IO.pure(HostReady(Host("localhost")))

}
