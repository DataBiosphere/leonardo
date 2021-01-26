package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{
  NamespaceName,
  SecretKey,
  SecretName,
  ServiceAccountName,
  ServiceName
}
import org.broadinstitute.dsde.workbench.google2.{Location, RegionName}
import org.broadinstitute.dsp.{ChartName, ChartVersion, Release}

import java.net.URL
import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration

case class SecurityFilesConfig(proxyServerCrt: Path,
                               proxyServerKey: Path,
                               proxyRootCaPem: Path,
                               proxyRootCaKey: Path,
                               rstudioLicenseFile: Path)

final case class ValueConfig(value: String) extends AnyVal
final case class SecretConfig(name: SecretName, secretFiles: List[SecretFile])
final case class SecretFile(name: SecretKey, path: Path)
final case class KubernetesIngressConfig(namespace: NamespaceName,
                                         release: Release,
                                         chartName: ChartName,
                                         chartVersion: ChartVersion,
                                         loadBalancerService: ServiceName,
                                         values: List[ValueConfig],
                                         secrets: List[SecretConfig]) {

  def chart: Chart = Chart(chartName, chartVersion)
}

final case class GalaxyAppConfig(releaseNameSuffix: String,
                                 chartName: ChartName,
                                 chartVersion: ChartVersion,
                                 namespaceNameSuffix: String,
                                 services: List[ServiceConfig],
                                 serviceAccountName: ServiceAccountName,
                                 uninstallKeepHistory: Boolean,
                                 orchUrl: String,
                                 drsUrl: String) {

  def chart: Chart = Chart(chartName, chartVersion)
}

final case class PollMonitorConfig(maxAttempts: Int, interval: FiniteDuration) {
  def totalDuration: FiniteDuration = interval * maxAttempts
}

case class AppMonitorConfig(createNodepool: PollMonitorConfig,
                            createCluster: PollMonitorConfig,
                            deleteNodepool: PollMonitorConfig,
                            deleteCluster: PollMonitorConfig,
                            createIngress: PollMonitorConfig,
                            createApp: PollMonitorConfig,
                            deleteApp: PollMonitorConfig,
                            scaleNodepool: PollMonitorConfig,
                            setNodepoolAutoscaling: PollMonitorConfig,
                            startApp: PollMonitorConfig)

case class KubernetesClusterConfig(
  location: Location,
  region: RegionName,
  authorizedNetworks: List[CidrIP],
  version: KubernetesClusterVersion,
  nodepoolLockCacheExpiryTime: FiniteDuration,
  nodepoolLockCacheMaxSize: Int
)

case class ProxyConfig(proxyDomain: String,
                       proxyUrlBase: LeonardoBaseUrl,
                       proxyPort: Int,
                       dnsPollPeriod: FiniteDuration,
                       tokenCacheExpiryTime: FiniteDuration,
                       tokenCacheMaxSize: Int,
                       internalIdCacheExpiryTime: FiniteDuration,
                       internalIdCacheMaxSize: Int) {
  def getProxyServerHostName: String = {
    val url = new URL(proxyUrlBase.asString)
    // The port is specified in fiabs, but generally unset otherwise
    val portStr = if (url.getPort == -1) "" else s":${url.getPort.toString}"
    s"${url.getProtocol}://${url.getHost}${portStr}"
  }
}

final case class GalaxyDiskConfig(nfsPersistenceName: String,
                                  postgresPersistenceName: String,
                                  // TODO: remove post-alpha once persistence is in place
                                  postgresDiskNameSuffix: String,
                                  postgresDiskSizeGB: DiskSize,
                                  postgresDiskBlockSize: BlockSize)

case class CacheConfig(cacheExpiryTime: FiniteDuration, cacheMaxSize: Int)
