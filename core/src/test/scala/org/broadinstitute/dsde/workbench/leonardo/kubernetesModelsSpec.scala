package org.broadinstitute.dsde.workbench.leonardo

import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsp.ChartName

class kubernetesModelsSpec extends LeonardoTestSuite with Matchers with AnyFlatSpecLike {
  it should "convert chartName to AllowedChartName correctly" in {
    AllowedChartName.fromChartName(ChartName("/leonardo/cromwell-0.2.291")) shouldBe None
    AllowedChartName.fromChartName(ChartName("/leonardo/aou-sas-chart-0.1.0")) shouldBe Some(AllowedChartName.Sas)
    AllowedChartName.fromChartName(ChartName("/leonardo/aou-rstudio-chart-0.2.0")) shouldBe Some(
      AllowedChartName.RStudio
    )
  }
}
