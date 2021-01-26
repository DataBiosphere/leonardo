package org.broadinstitute.dsde.workbench.leonardo
package subscriber

import pureconfig.ConfigSource
import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.workbench.google2.{PublisherConfig, SubscriberConfig}
import org.broadinstitute.dsde.workbench.leonardo.algebra.{HttpSamDaoConfig, VPCConfig}
import org.broadinstitute.dsde.workbench.leonardo.db.LiquibaseConfig
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import _root_.pureconfig.generic.auto._

import java.nio.file.Path
import org.broadinstitute.dsde.workbench.leonardo.ConfigImplicits._

object Config {
  // This variable is only for initialize slick's DatabaseConfig
  val configForSlick = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load()).resolve()

  val appConfig = ConfigSource
    .fromConfig(configForSlick)
    .load[AppConfig]
    .leftMap(failures => new RuntimeException(failures.toList.map(_.description).mkString("\n")))
}

final case class DbConfig(liquibase: LiquibaseConfig, concurrency: Int)
final case class Application(serviceAccountFile: Path, appName: String, googleProject: GoogleProject) // data model represents Leonardo's infrastructure related info.
final case class GkeConfig(cluster: KubernetesClusterConfig,
                           galaxyApp: GalaxyAppConfig,
                           ingress: KubernetesIngressConfig,
                           galaxyDisk: GalaxyDiskConfig)
final case class DnsCacheConfig(kubernetes: CacheConfig)
final case class MonitorConfig(kubernetes: AppMonitorConfig)
final case class AppConfig(
  application: Application,
  mysql: DbConfig,
  authProviderConfig: HttpSamDaoConfig,
  cryptominingPublisherConfig: PublisherConfig,
  asyncTaskProcessor: AsyncTaskProcessor.Config,
  nonLeonardoMessageSubscriber: SubscriberConfig,
  gke: GkeConfig,
  vpc: VPCConfig,
  proxy: ProxyConfig,
  dnsCache: DnsCacheConfig,
  securityFiles: SecurityFilesConfig,
  monitor: MonitorConfig
)
