package org.broadinstitute.dsde.workbench.leonardo

import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.workbench.model._
import net.ceedubs.ficus.Ficus._

package object config {
  implicit val swaggerReader: ValueReader[SwaggerConfig] = ValueReader.relative { config =>
    SwaggerConfig()
  }

  implicit val dataprocConfigReader: ValueReader[DataprocConfig] = ValueReader.relative { config =>
    DataprocConfig(config.getString("serviceAccount"),
      config.getString("dataprocInitScriptURI"),
      config.getString("dataprocDefaultZone"),
      config.getString("dataprocDockerImage"),
      config.getString("pathToLeonardoPem"),
      config.getString("clusterUrlBase"))
  }

  implicit val liquibaseReader: ValueReader[LiquibaseConfig] = ValueReader.relative { config =>
    LiquibaseConfig(config.as[String]("changelog"), config.as[Boolean]("initWithLiquibase"))
  }
}
