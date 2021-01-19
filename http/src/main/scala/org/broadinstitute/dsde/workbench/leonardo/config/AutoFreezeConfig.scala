package org.broadinstitute.dsde.workbench.leonardo
package config
import scala.concurrent.duration.FiniteDuration

case class AutoFreezeConfig(
  enableAutoFreeze: Boolean,
  autoFreezeAfter: FiniteDuration,
  autoFreezeCheckInterval: FiniteDuration,
  maxKernelBusyLimit: FiniteDuration
)
