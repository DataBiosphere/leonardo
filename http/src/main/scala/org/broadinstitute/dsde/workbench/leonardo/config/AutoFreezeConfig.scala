package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class AutoFreezeConfig(
  autoFreezeRuntimeAfter: FiniteDuration,
  autoFreezeAppAfter: FiniteDuration,
  maxJupyterKernelBusyLimit: FiniteDuration,
  maxGalaxyJobBusyLimit: FiniteDuration,
  autoFreezeCheckInterval: FiniteDuration
)
