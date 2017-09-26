package org.broadinstitute.dsde.workbench.leonardo

import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.workbench.util.toScalaDuration


package object config {
  implicit val swaggerReader: ValueReader[SwaggerConfig] = ValueReader.relative { config =>
    SwaggerConfig(
      config.getString("googleClientId"),
      config.getString("realm")
    )
  }

  implicit val dataprocConfigReader: ValueReader[DataprocConfig] = ValueReader.relative { config =>
    DataprocConfig(config.getString("applicationName"),
      config.getString("serviceAccount"),
      config.getString("dataprocDefaultRegion"),
      config.getString("leoGoogleBucket"),
      config.getString("dataprocDockerImage"),
      config.getString("jupyterProxyDockerImage"),
      config.getString("clusterFirewallRuleName"),
      config.getString("clusterFirewallVPCNetwork"),
      config.getString("clusterNetworkTag"),
      config.getString("configFolderPath"),
      config.getString("initActionsFileName"),
      config.getString("clusterDockerComposeName"),
      config.getString("leonardoServicePemName"),
      config.getString("jupyterServerCrtName"),
      config.getString("jupyterServerKeyName"),
      config.getString("jupyterRootCaPemName"),
      config.getString("jupyterRootCaKeyName"),
      config.getString("proxySiteConf"),
      config.getString("clusterUrlBase"),
      config.getString("jupyterServerName"),
      config.getString("proxyServerName"),
      config.getString("jupyterInstallExtensionScript"),
      config.getString("userServiceAccountCredentials"))
  }

  implicit val liquibaseReader: ValueReader[LiquibaseConfig] = ValueReader.relative { config =>
    LiquibaseConfig(config.as[String]("changelog"), config.as[Boolean]("initWithLiquibase"))
  }

  implicit val proxyConfigReader: ValueReader[ProxyConfig] = ValueReader.relative { config =>
    ProxyConfig(config.getInt("jupyterPort"),
      config.getString("jupyterProtocol"),
      config.getString("jupyterDomain"),
      toScalaDuration(config.getDuration("dnsPollPeriod")))
  }

  implicit val monitorConfigReader: ValueReader[MonitorConfig] = ValueReader.relative { config =>
    MonitorConfig(toScalaDuration(config.getDuration("pollPeriod")), config.getInt("maxRetries"), config.getBoolean("recreateCluster"))
  }
}
