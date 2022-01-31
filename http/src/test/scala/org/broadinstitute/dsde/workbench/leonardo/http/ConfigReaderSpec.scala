package org.broadinstitute.dsde.workbench.leonardo.http

import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsde.workbench.leonardo.config.{HttpWsmDaoConfig, PersistentDiskConfig}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PollMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.{
  AzureAppRegistrationConfig,
  BlockSize,
  CidrIP,
  ClientId,
  ClientSecret,
  DiskSize,
  DiskType,
  ManagedAppTenantId
}
import org.broadinstitute.dsde.workbench.leonardo.util.{
  AzureInterpretorConfig,
  AzureMonitorConfig,
  TerraAppSetupChartConfig
}
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
        AzureMonitorConfig(PollMonitorConfig(120, 1 seconds)),
        AzureInterpretorConfig("ip",
                               "Azure Ip",
                               "Azure Network",
                               "network",
                               "subnet",
                               CidrIP("192.168.0.0/16"),
                               CidrIP("192.168.0.0/24"),
                               "Azure Disk",
                               "Azure Vm"),
        HttpWsmDaoConfig(Uri.unsafeFromString("https://localhost:8000")),
        AzureAppRegistrationConfig(ClientId(""), ClientSecret(""), ManagedAppTenantId(""))
      )
    )

    config shouldBe expectedConfig
  }
}
