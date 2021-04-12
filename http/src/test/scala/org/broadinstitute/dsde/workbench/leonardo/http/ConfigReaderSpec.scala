package org.broadinstitute.dsde.workbench.leonardo.http

import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.{BlockSize, DiskSize, DiskType}
import org.broadinstitute.dsde.workbench.leonardo.util.TerraAppSetupChartConfig
import org.broadinstitute.dsp.{ChartName, ChartVersion}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigReaderSpec extends AnyFlatSpec with Matchers {
  it should "read config file correctly" in {
    val config = ConfigReader.appConfig
    val expectedConfig = AppConfig(
      TerraAppSetupChartConfig(ChartName("/leonardo/terra-app-setup"), ChartVersion("0.0.2")),
      PersistentDiskConfig(
        DiskSize(30),
        DiskType.Standard,
        BlockSize(4096),
        ZoneName("us-central1-a"),
        DiskSize(250)
      )
    )

    config shouldBe Right(expectedConfig)
  }
}
