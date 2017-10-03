package org.broadinstitute.dsde.firecloud.config

import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.workbench.config.WorkbenchConfig

object FireCloudConfig extends WorkbenchConfig {
  private val fireCloud = config.getConfig("fireCloud")

  object FireCloud {
    val baseUrl: String = fireCloud.getString("baseUrl")
    val fireCloudId: String = fireCloud.getString("fireCloudId")
    val orchApiUrl: String = fireCloud.getString("orchApiUrl")
    val rawlsApiUrl: String = fireCloud.getString("rawlsApiUrl")
    val samApiUrl: String = fireCloud.getString("samApiUrl")
    val thurloeApiUrl: String = fireCloud.getString("thurloeApiUrl")
    val leonardoApiUrl: String = fireCloud.getString("leonardoApiUrl")
  }
}
