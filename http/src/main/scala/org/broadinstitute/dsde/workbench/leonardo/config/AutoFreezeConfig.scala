package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class AutoFreezeConfig(
  autoFreezeAfter: FiniteDuration,
  autoFreezeCheckInterval: FiniteDuration,
  maxKernelBusyLimit: FiniteDuration
)
