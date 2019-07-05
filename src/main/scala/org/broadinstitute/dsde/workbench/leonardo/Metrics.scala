package org.broadinstitute.dsde.workbench.leonardo

import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics

object Metrics {
  val newRelic = NewRelicMetrics.fromNewRelic("leonardo")
}
