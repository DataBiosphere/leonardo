package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Host
import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.HostReady
import org.broadinstitute.dsde.workbench.leonardo.dao.google.GoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.dns.{KubernetesDnsCache, RuntimeDnsCache}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.monitor.UpdateDateAccessedMessage
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger
import scalacache.Cache

import java.time.Instant
import scala.concurrent.ExecutionContext

class MockProxyService(
  proxyConfig: ProxyConfig,
  jupyterDAO: JupyterDAO[IO] = MockJupyterDAO,
  authProvider: LeoAuthProvider[IO],
  runtimeDnsCache: RuntimeDnsCache[IO],
  kubernetesDnsCache: KubernetesDnsCache[IO],
  googleTokenCache: Cache[IO, String, (UserInfo, Instant)],
  samResourceCache: Cache[IO, SamResourceCacheKey, (Option[String], Option[AppAccessScope])],
  googleOauth2Service: GoogleOAuth2Service[IO],
  samDAO: Option[SamDAO[IO]] = None,
  queue: Option[Queue[IO, UpdateDateAccessedMessage]] = None
)(implicit
  system: ActorSystem,
  executionContext: ExecutionContext,
  dbRef: DbReference[IO],
  logger: StructuredLogger[IO],
  metrics: OpenTelemetryMetrics[IO]
) extends ProxyService(
      TestUtils.sslContext(system),
      proxyConfig,
      jupyterDAO,
      runtimeDnsCache,
      kubernetesDnsCache,
      authProvider,
      queue.getOrElse(Queue.bounded[IO, UpdateDateAccessedMessage](100).unsafeRunSync),
      googleOauth2Service,
      LocalProxyResolver,
      samDAO.getOrElse(new MockSamDAO()),
      googleTokenCache,
      samResourceCache
    ) {

  override def getRuntimeTargetHost(cloudContext: CloudContext, clusterName: RuntimeName): IO[HostStatus] =
    IO.pure(HostReady(Host("localhost"), "path", CloudProvider.Gcp))

  override def getAppTargetHost(cloudContext: CloudContext, appName: AppName): IO[HostStatus] =
    IO.pure(HostReady(Host("localhost"), "path", CloudProvider.Gcp))

}
