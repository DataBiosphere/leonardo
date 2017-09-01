package org.broadinstitute.dsde.workbench.leonardo

import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.workbench.model._
import org.broadinstitute.dsde.workbench.util.toScalaDuration
import net.ceedubs.ficus.Ficus._

package object config {
  implicit val swaggerReader: ValueReader[SwaggerConfig] = ValueReader.relative { config =>
    SwaggerConfig(
      config.getString("googleClientId"),
      config.getString("realm")
    )
  }

  implicit val dataprocConfigReader: ValueReader[DataprocConfig] = ValueReader.relative { config =>
    DataprocConfig(config.getString("serviceAccount"),
      config.getString("dataprocDefaultZone"),
      config.getString("dataprocDockerImage"),
      config.getString("jupyterProxyDockerImage"),
      config.getString("jupyterConfigFolderPath"),
      config.getString("initActionsFileName"),
      config.getString("clusterDockerCompose"),
      config.getString("configFolderPath"),
      config.getString("leonardoServicePemName"),
      config.getString("jupyterServerCrtName"),
      config.getString("jupyterServerKeyName"),
      config.getString("jupyterRootCaPemName"),
      config.getString("clusterUrlBase"),
      config.getString("jupyterServerName"),
      config.getString("proxyServerName"))
  }

  implicit val liquibaseReader: ValueReader[LiquibaseConfig] = ValueReader.relative { config =>
    LiquibaseConfig(config.as[String]("changelog"), config.as[Boolean]("initWithLiquibase"))
  }

  implicit val proxyConfigReader: ValueReader[ProxyConfig] = ValueReader.relative { config =>
    ProxyConfig(config.getInt("jupyterPort"), config.getString("jupyterDomain"), toScalaDuration(config.getDuration("dnsPollPeriod")))
  }
}
