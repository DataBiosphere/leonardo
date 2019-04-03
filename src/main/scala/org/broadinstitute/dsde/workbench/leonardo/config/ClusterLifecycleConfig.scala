package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

case class ClusterLifecycleConfig (
                              dateAccessedMonitorScheduler: FiniteDuration,
                              autoFreezeConfig: AutoFreezeConfig,
                              autoDeleteConfig: AutoDeleteConfig
                            )
