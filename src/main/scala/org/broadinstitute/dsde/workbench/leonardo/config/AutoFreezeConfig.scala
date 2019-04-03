package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class AutoFreezeConfig (
                              enableAutoFreeze: Boolean,
                              autoFreezeAfter: FiniteDuration,
                              autoFreezeCheckScheduler: FiniteDuration
                            )
