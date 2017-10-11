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
      config.getString("jupyterProxyDockerImage"),
      config.getString("clusterUrlBase"),
      config.getString("jupyterServerName"),
      config.getString("proxyServerName"))
  }

  implicit val clusterResourcesConfigReader: ValueReader[ClusterResourcesConfig] = ValueReader.relative { config =>
    val filePrefix = config.getString("configFolderPath")

    ClusterResourcesConfig(
      new File(filePrefix, config.getString("initActionsFileName")),
      new File(filePrefix, config.getString("clusterDockerComposeName")),
      new File(filePrefix, config.getString("leonardoServicePemName")),
      new File(filePrefix, config.getString("jupyterServerCrtName")),
      new File(filePrefix, config.getString("jupyterServerKeyName")),
      new File(filePrefix, config.getString("jupyterRootCaPemName")),
      new File(filePrefix, config.getString("jupyterRootCaKeyName")),
      new File(filePrefix, config.getString("proxySiteConf")),
      new File(filePrefix, config.getString("jupyterInstallExtensionScript")),
      new File(filePrefix, config.getString("userServiceAccountCredentials")))
  }

  implicit val liquibaseReader: ValueReader[LiquibaseConfig] = ValueReader.relative { config =>
    LiquibaseConfig(config.as[String]("changelog"), config.as[Boolean]("initWithLiquibase"))
  }

  implicit val proxyConfigReader: ValueReader[ProxyConfig] = ValueReader.relative { config =>
    ProxyConfig(config.getString("firewallRuleName"),
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
