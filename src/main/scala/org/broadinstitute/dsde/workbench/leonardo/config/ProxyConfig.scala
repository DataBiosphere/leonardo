package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class ProxyConfig(
  jupyterPort: Int,
  jupyterProtocol: String,
  jupyterDomain: String,
  dnsPollPeriod: FiniteDuration
)
