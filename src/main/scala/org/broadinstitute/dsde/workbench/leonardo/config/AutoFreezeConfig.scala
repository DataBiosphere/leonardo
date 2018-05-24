package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class AutoFreezeConfig (
                              dateAccessedMonitorScheduler: FiniteDuration,
                              autoFreezeAfter: FiniteDuration,
                              autoFreezeCheckScheduler: FiniteDuration
                            )
