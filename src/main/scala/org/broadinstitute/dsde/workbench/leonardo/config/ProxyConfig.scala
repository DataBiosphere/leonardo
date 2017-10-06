package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class ProxyConfig(
  firewallRuleName: String,
  firewallVPCNetwork: String,
  networkTag: String,
  jupyterPort: Int,
  jupyterProtocol: String,
  jupyterDomain: String,
  dnsPollPeriod: FiniteDuration
)
