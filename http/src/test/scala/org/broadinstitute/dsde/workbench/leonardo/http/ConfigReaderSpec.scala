package org.broadinstitute.dsde.workbench.leonardo
package http

import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoMetricsMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.util._
import org.broadinstitute.dsp._
import org.http4s.Uri
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ConfigReaderSpec extends AnyFlatSpec with Matchers {
  it should "read config file correctly" in {
    val config = ConfigReader.appConfig
    val expectedConfig = AppConfig(
      TerraAppSetupChartConfig(ChartName("/leonardo/terra-app-setup"), ChartVersion("0.1.0")),
      PersistentDiskConfig(
        DiskSize(30),
        DiskType.Standard,
        BlockSize(4096),
        ZoneName("us-central1-a"),
        DiskSize(250),
        Vector("bogus")
      ),
      OidcAuthConfig(
        Uri.unsafeFromString("https://fake"),
        org.broadinstitute.dsde.workbench.oauth2.ClientId("fakeClientId")
      ),
      DrsConfig(
        "https://drshub.dsde-dev.broadinstitute.org/api/v4/drs/resolve"
      ),
      LeoMetricsMonitorConfig(true, 5 minutes)
    )

    config shouldBe expectedConfig
  }
}
