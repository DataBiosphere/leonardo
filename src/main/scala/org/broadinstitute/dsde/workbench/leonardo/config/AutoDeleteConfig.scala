package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class AutoDeleteConfig (
                              enableAutoDelete: Boolean,
                              autoDeleteAfter: FiniteDuration,
                              autoDeleteCheckScheduler: FiniteDuration
                            )
