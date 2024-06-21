package org.broadinstitute.dsde.workbench.leonardo

import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.dsp.ChartName

class kubernetesModelsSpec extends LeonardoTestSuite with Matchers with AnyFlatSpecLike {
  it should "convert chartName to AllowedChartName correctly" in {
    AllowedChartName.fromChartName(ChartName("/leonardo/cromwell")) shouldBe None
    AllowedChartName.fromChartName(ChartName("/leonardo/sas")) shouldBe Some(AllowedChartName.Sas)
    AllowedChartName.fromChartName(ChartName("/leonardo/officesuite")) shouldBe Some(AllowedChartName.OFFICESUITE)
    AllowedChartName.fromChartName(ChartName("/leonardo/rstudio")) shouldBe Some(
      AllowedChartName.RStudio
    )
  }
}
