package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class ProxyConfig(
  jupyterPort: Int,
  jupyterDomain: String,
  dnsPollPeriod: FiniteDuration
)
