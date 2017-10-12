package org.broadinstitute.dsde.workbench.leonardo

import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.workbench.leonardo.model.{GoogleProject, GoogleServiceAccount}
import org.broadinstitute.dsde.workbench.util.toScalaDuration

import java.io.File


package object config {
  implicit val swaggerReader: ValueReader[SwaggerConfig] = ValueReader.relative { config =>
    SwaggerConfig(
      config.getString("googleClientId"),
      config.getString("realm")
    )
  }

  implicit val dataprocConfigReader: ValueReader[DataprocConfig] = ValueReader.relative { config =>
    DataprocConfig(
      config.getString("applicationName"),
      GoogleServiceAccount(config.getString("serviceAccount")),
      config.getString("dataprocDefaultRegion"),
      GoogleProject(config.getString("leoGoogleProject")),
      config.getString("dataprocDockerImage"),
      config.getString("clusterUrlBase"),
      config.getString("jupyterServerName"))
  }

  implicit val clusterResourcesConfigReader: ValueReader[ClusterResourcesConfig] = ValueReader.relative { config =>

    ClusterResourcesConfig(
      config.getString("configFolderPath"),
      config.getString("initActionsScript"),
      config.getString("clusterDockerCompose"),
      config.getString("leonardoServicePem"),
      config.getString("jupyterServerCrt"),
      config.getString("jupyterServerKey"),
      config.getString("jupyterRootCaPem"),
      config.getString("jupyterRootCaKey"),
      config.getString("proxySiteConf"),
      config.getString("jupyterInstallExtensionScript"),
      config.getString("userServiceAccountCredentials"))
  }

  implicit val liquibaseReader: ValueReader[LiquibaseConfig] = ValueReader.relative { config =>
    LiquibaseConfig(config.as[String]("changelog"), config.as[Boolean]("initWithLiquibase"))
  }

  implicit val proxyConfigReader: ValueReader[ProxyConfig] = ValueReader.relative { config =>
    ProxyConfig(
      config.getString("jupyterProxyDockerImage"),
      config.getString("proxyServerName"),
      config.getString("firewallRuleName"),
      config.getString("firewallVPCNetwork"),
      config.getString("networkTag"),
      config.getInt("jupyterPort"),
      config.getString("jupyterProtocol"),
      config.getString("jupyterDomain"),
      toScalaDuration(config.getDuration("dnsPollPeriod")))
  }

  implicit val monitorConfigReader: ValueReader[MonitorConfig] = ValueReader.relative { config =>
    MonitorConfig(toScalaDuration(config.getDuration("pollPeriod")), config.getInt("maxRetries"), config.getBoolean("recreateCluster"))
  }
}
