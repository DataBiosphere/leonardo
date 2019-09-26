package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus

import scala.concurrent.duration.FiniteDuration

/**
  * Created by rtitle on 9/6/17.
  */
case class MonitorConfig(pollPeriod: FiniteDuration,
                         maxRetries: Int = -1,
                         recreateCluster: Boolean = true,
                         monitorStatusTimeouts: Map[ClusterStatus, FiniteDuration])
