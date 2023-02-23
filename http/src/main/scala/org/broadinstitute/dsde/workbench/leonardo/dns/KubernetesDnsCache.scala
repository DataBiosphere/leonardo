package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.{HostNotFound, HostNotReady, HostReady}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.leonardo.http.{kubernetesProxyHost, GetAppResult}
import org.broadinstitute.dsde.workbench.leonardo.{AppName, CloudContext, CloudProvider}
import org.broadinstitute.dsde.workbench.model.IP
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.Logger
import scalacache.Cache

import scala.concurrent.ExecutionContext

final case class KubernetesDnsCacheKey(cloudContext: CloudContext, appName: AppName)

/**
 * This class provides an in-memory cache of (GoogleProject, AppName) -> HostStatus.
 * This is used by ProxyService to look up the hostname to connect to for a given app proxy request.
 * It also populates HostToIpMapping reference used by JupyterNameService to match a "fake" hostname to a
 * real IP address.
 */
final class KubernetesDnsCache[F[_]: Logger: OpenTelemetryMetrics](
  proxyConfig: ProxyConfig,
  dbRef: DbReference[F],
  hostToIpMapping: Ref[F, Map[Host, IP]],
  hostStatusCache: Cache[F, KubernetesDnsCacheKey, HostStatus]
)(implicit F: Async[F], ec: ExecutionContext) {
  def getHostStatus(key: KubernetesDnsCacheKey): F[HostStatus] =
    hostStatusCache.cachingF(key)(None)(getHostStatusHelper(key))

  private def getHostStatusHelper(key: KubernetesDnsCacheKey): F[HostStatus] =
    for {
      appResultOpt <- dbRef.inTransaction {
        KubernetesServiceDbQueries.getActiveFullAppByName(key.cloudContext,
                                                          key.appName
        ) // TODO: support proxying Azure apps
      }
      hostStatus <- appResultOpt match {
        case None            => F.pure[HostStatus](HostNotFound)
        case Some(appResult) => hostStatusByAppResult(appResult)
      }
    } yield hostStatus

  private def hostStatusByAppResult(appResult: GetAppResult): F[HostStatus] =
    appResult.cluster.asyncFields.map(_.loadBalancerIp) match {
      case None => F.pure[HostStatus](HostNotReady)
      case Some(ip) =>
        val h = kubernetesProxyHost(appResult.cluster, proxyConfig.proxyDomain)
        hostToIpMapping
          .getAndUpdate(_ + (h -> ip))
          .as[HostStatus](HostReady(h, "", CloudProvider.Gcp)) // TODO: update this once we start support AKS
    }
}
