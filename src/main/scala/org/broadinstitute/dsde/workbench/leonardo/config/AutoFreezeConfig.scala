package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class AutoFreezeConfig (
                              enableAutoFreeze: Boolean,
                              dateAccessedMonitorScheduler: FiniteDuration,
                              autoFreezeAfter: Int,
                              autoFreezeCheckScheduler: FiniteDuration
                            )
