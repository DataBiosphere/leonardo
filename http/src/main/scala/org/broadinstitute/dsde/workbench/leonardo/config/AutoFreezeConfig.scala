package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class AutoFreezeConfig(
                             enableAutoFreeze: Boolean, //TODO is this necessary?
                             dateAccessedMonitorScheduler: FiniteDuration,
                             autoFreezeAfter: FiniteDuration,
                             autoFreezeCheckInterval: FiniteDuration,
                             maxKernelBusyLimit: FiniteDuration
)
