package org.broadinstitute.dsde.workbench.leonardo
package http

import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsde.workbench.leonardo.config.{HttpWsmDaoConfig, PersistentDiskConfig}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{AzureRuntimeConfig, AzureRuntimeDefaults}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PollMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.util.{AzureMonitorConfig, TerraAppSetupChartConfig}
import org.broadinstitute.dsp.{ChartName, ChartVersion}
import org.http4s.Uri
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

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
      ),
      AzureConfig(
        AzureMonitorConfig(PollMonitorConfig(1 seconds, 120, 1 seconds), PollMonitorConfig(1 seconds, 120, 1 seconds)),
        AzureRuntimeDefaults("Azure Ip",
                             "ip",
                             "Azure Network",
                             "network",
                             "subnet",
                             CidrIP("192.168.0.0/16"),
                             CidrIP("192.168.0.0/24"),
                             "Azure Disk",
                             "Azure Vm"),
        HttpWsmDaoConfig(Uri.unsafeFromString("https://localhost:8000")),
        AzureAppRegistrationConfig(ClientId(""), ClientSecret(""), ManagedAppTenantId("")),
        AzureRuntimeConfig(
          AzureImageUri(
            "/subscriptions/3efc5bdf-be0e-44e7-b1d7-c08931e3c16c/resourceGroups/mrg-qi-1-preview-20210517084351/providers/Microsoft.Compute/galleries/msdsvm/images/customized_ms_dsvm/versions/0.1.0"
          ),
          Set.empty
        )
      )
    )

    config shouldBe expectedConfig
  }
}
