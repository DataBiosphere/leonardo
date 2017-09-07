package org.broadinstitute.dsde.workbench.leonardo.config

import scala.concurrent.duration.FiniteDuration

/**
  * Created by rtitle on 9/6/17.
  */
case class MonitorConfig(pollPeriod: FiniteDuration,
                         maxRetries: Int = -1,
                         canRecreateCluster: Boolean = true)
