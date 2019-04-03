package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class AutoDeleteConfig (
                              enableAutoDelete: Boolean,
                              dateAccessedMonitorScheduler: FiniteDuration,
                              autoDeleteAfter: FiniteDuration,
                              autoDeleteCheckScheduler: FiniteDuration
                            )
