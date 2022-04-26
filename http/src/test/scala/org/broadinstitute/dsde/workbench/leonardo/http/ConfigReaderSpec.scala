package org.broadinstitute.dsde.workbench.leonardo
package http

import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsde.workbench.leonardo.config.{HttpWsmDaoConfig, PersistentDiskConfig}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  AcrCredential,
  AzureRuntimeDefaults,
  CustomScriptExtensionConfig
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.PollMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.util.{AzurePubsubHandlerConfig, TerraAppSetupChartConfig}
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
        AzurePubsubHandlerConfig(
          PollMonitorConfig(1 seconds, 120, 1 seconds),
          PollMonitorConfig(1 seconds, 120, 1 seconds),
          AzureRuntimeDefaults(
            "Azure Ip",
            "ip",
            "Azure Network",
            "network",
            "subnet",
            CidrIP("192.168.0.0/16"),
            CidrIP("192.168.0.0/24"),
            "Azure Disk",
            "Azure Vm",
            AzureImage(
              "microsoft-dsvm",
              "ubuntu-1804",
              "1804-gen2"
            ),
            CustomScriptExtensionConfig(
              "vm-custom-script-extension",
              "Microsoft.Azure.Extensions",
              "CustomScript",
              "2.1",
              true,
              List(
                "https://raw.githubusercontent.com/DataBiosphere/leonardo/cf829d8589c0035905916f24f9f01056c280425d/http/src/main/resources/init-resources/azure_vm_init_script.sh"
              )
            ),
            "terradevacrpublic.azurecr.io/terra-azure-relay-listeners:53d7992",
            AcrCredential(username = "username", password = "password")
          )
        ),
        HttpWsmDaoConfig(Uri.unsafeFromString("https://localhost:8000")),
        AzureAppRegistrationConfig(ClientId(""), ClientSecret(""), ManagedAppTenantId(""))
      ),
      OidcAuthConfig(
        Uri.unsafeFromString("https://fake"),
        org.broadinstitute.dsde.workbench.oauth2.ClientId("fakeClientId"),
        Some(org.broadinstitute.dsde.workbench.oauth2.ClientSecret("fakeClientSecret")),
        org.broadinstitute.dsde.workbench.oauth2.ClientId("legacyClientSecret")
      )
    )

    config shouldBe expectedConfig
  }
}
