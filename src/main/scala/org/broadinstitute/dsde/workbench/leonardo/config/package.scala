package org.broadinstitute.dsde.workbench.leonardo

import java.io.File

import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.util.toScalaDuration

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
      config.getString("dataprocDefaultRegion"),
      GoogleProject(config.getString("leoGoogleProject")),
      config.getString("dataprocDockerImage"),
      config.getString("clusterUrlBase"),
      config.getString("defaultExecutionTimeout"),
      config.getString("jupyterServerName"),
      config.getString("firewallRuleName"),
      config.getString("networkTag"),
      config.getAs[String]("vpcNetwork"),
      config.getAs[String]("vpcSubnet")
    )
  }

  implicit val clusterFilesConfigReader: ValueReader[ClusterFilesConfig] = ValueReader.relative { config =>
    val baseDir = config.getString("configFolderPath")
    ClusterFilesConfig(
      new File(baseDir, config.getString("jupyterServerCrt")),
      new File(baseDir, config.getString("jupyterServerKey")),
      new File(baseDir, config.getString("jupyterRootCaPem")),
      new File(baseDir, config.getString("jupyterRootCaKey"))
    )
  }

  implicit val clusterResourcesConfigReader: ValueReader[ClusterResourcesConfig] = ValueReader.relative { config =>
    ClusterResourcesConfig(
      ClusterResource(config.getString("initActionsScript")),
      ClusterResource(config.getString("clusterDockerCompose")),
      ClusterResource(config.getString("proxySiteConf")),
      ClusterResource(config.getString("jupyterCustomJs")),
      ClusterResource(config.getString("jupyterGoogleSignInJs")),
      ClusterResource(config.getString("jupyterNotebookConfigUri"))
    )
  }

  implicit val clusterDefaultConfigReader: ValueReader[ClusterDefaultsConfig] = ValueReader.relative { config =>
    ClusterDefaultsConfig(
      config.getInt("numberOfWorkers"),
      config.getString("masterMachineType"),
      config.getInt("masterDiskSize"),
      config.getString("workerMachineType"),
      config.getInt("workerDiskSize"),
      config.getInt("numberOfWorkerLocalSSDs"),
      config.getInt("numberOfPreemptibleWorkers")
    )
  }

  implicit val liquibaseReader: ValueReader[LiquibaseConfig] = ValueReader.relative { config =>
    LiquibaseConfig(config.as[String]("changelog"), config.as[Boolean]("initWithLiquibase"))
  }

  implicit val proxyConfigReader: ValueReader[ProxyConfig] = ValueReader.relative { config =>
    ProxyConfig(
      config.getString("jupyterProxyDockerImage"),
      config.getString("proxyServerName"),
      config.getInt("jupyterPort"),
      config.getString("jupyterProtocol"),
      config.getString("jupyterDomain"),
      toScalaDuration(config.getDuration("dnsPollPeriod")),
      toScalaDuration(config.getDuration("cacheExpiryTime")),
      config.getInt("cacheMaxSize")
    )
  }

  implicit val monitorConfigReader: ValueReader[MonitorConfig] = ValueReader.relative { config =>
    MonitorConfig(toScalaDuration(config.getDuration("pollPeriod")), config.getInt("maxRetries"), config.getBoolean("recreateCluster"))
  }

  implicit val samConfigReader: ValueReader[SamConfig] = ValueReader.relative { config =>
    SamConfig(config.getString("server"))
  }

  implicit val autoFreezeConfigReader: ValueReader[AutoFreezeConfig] = ValueReader.relative { config =>
    AutoFreezeConfig(
      config.getBoolean("enableAutoFreeze"),
      toScalaDuration(config.getDuration("dateAccessedMonitorScheduler")),
      toScalaDuration(config.getDuration("autoFreezeAfter")),
      toScalaDuration(config.getDuration("autoFreezeCheckScheduler"))
    )
  }

  implicit val zombieClusterConfig: ValueReader[ZombieClusterConfig] = ValueReader.relative { config =>
    ZombieClusterConfig(
      config.getBoolean("enableZombieClusterMonitor"),
      toScalaDuration(config.getDuration("pollPeriod"))
    )
  }
}
