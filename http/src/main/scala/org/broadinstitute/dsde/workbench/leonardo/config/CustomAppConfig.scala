package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.leonardo.Chart
import org.broadinstitute.dsp.{ChartName, ChartVersion}

final case class CustomAppConfig(chartName: ChartName, chartVersion: ChartVersion, releaseNameSuffix: String) {
  def chart: Chart = Chart(chartName, chartVersion)
}
