package org.broadinstitute.dsde.workbench.leonardo.dns

import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.{HostNotFound, HostNotReady, HostReady}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.leonardo.http.{kubernetesProxyHost, GetAppResult}
import org.broadinstitute.dsde.workbench.leonardo.AppType.Galaxy
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
  hostToIpMapping: Ref[F, Map[String, IP]],
  hostStatusCache: Cache[F, KubernetesDnsCacheKey, HostStatus]
)(implicit F: Async[F], ec: ExecutionContext) {
  def getHostStatus(key: KubernetesDnsCacheKey): F[HostStatus] =
    hostStatusCache.cachingF(key)(None)(getHostStatusHelper(key))

  private def getHostStatusHelper(key: KubernetesDnsCacheKey): F[HostStatus] =
    for {
      appResultOpt <- dbRef.inTransaction {
        KubernetesServiceDbQueries.getActiveFullAppByName(key.cloudContext, key.appName)
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
        // Galaxy VM apps serve HTTP on port 80. The proxy should connect via plain HTTP,
        // and we map the fake hostname to the VM's external IP (stored in loadBalancerIp).
        // External IP is used because Leo's pod is in a different VPC from the user's workspace project.
        val isGalaxyVm = appResult.app.appType == Galaxy
        hostToIpMapping
          .getAndUpdate(_ + (h.address -> ip))
          .as[HostStatus](
            HostReady(h, "", CloudProvider.Gcp, useHttp = isGalaxyVm)
          ) // TODO: update this once we start support AKS
    }
}
