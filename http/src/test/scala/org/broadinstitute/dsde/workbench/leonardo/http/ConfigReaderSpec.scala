package org.broadinstitute.dsde.workbench.leonardo
package http

import org.broadinstitute.dsde.workbench.azure.{AzureAppRegistrationConfig, ClientId, ClientSecret, ManagedAppTenantId}
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsde.workbench.leonardo.config._
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  AzureRuntimeDefaults,
  CustomScriptExtensionConfig,
  VMCredential
}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{LeoMetricsMonitorConfig, PollMonitorConfig}
import org.broadinstitute.dsde.workbench.leonardo.util.{AzurePubsubHandlerConfig, TerraAppSetupChartConfig}
import org.broadinstitute.dsp._
import org.http4s.Uri
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URL
import scala.concurrent.duration._

class ConfigReaderSpec extends AnyFlatSpec with Matchers {
  it should "read config file correctly" in {
    val config = ConfigReader.appConfig
    val expectedConfig = AppConfig(
      TerraAppSetupChartConfig(ChartName("/leonardo/terra-app-setup"), ChartVersion("0.0.19")),
      PersistentDiskConfig(
        DiskSize(30),
        DiskType.Standard,
        BlockSize(4096),
        ZoneName("us-central1-a"),
        DiskSize(250),
        Vector("bogus")
      ),
      AzureConfig(
        AzurePubsubHandlerConfig(
          Uri.unsafeFromString("https://sam.test.org:443"),
          Uri.unsafeFromString("https://localhost:8000"),
          "terradevacrpublic.azurecr.io/welder-server",
          "6648f5c",
          PollMonitorConfig(1 seconds, 10, 1 seconds),
          PollMonitorConfig(1 seconds, 20, 1 seconds),
          PollMonitorConfig(1 seconds, 10, 1 seconds),
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
              "ubuntu-2004",
              "2004-gen2",
              "23.01.06"
            ),
            CustomScriptExtensionConfig(
              "vm-custom-script-extension",
              "Microsoft.Azure.Extensions",
              "CustomScript",
              "2.1",
              true,
              List(
                "https://raw.githubusercontent.com/DataBiosphere/leonardo/270bd6aad916344fadc06d1a51629c432da663a8/http/src/main/resources/init-resources/azure_vm_init_script.sh"
              )
            ),
            "terradevacrpublic.azurecr.io/terra-azure-relay-listeners:4647ac3",
            VMCredential(username = "username", password = "password")
          )
        ),
        HttpWsmDaoConfig(Uri.unsafeFromString("https://localhost:8000")),
        AzureAppRegistrationConfig(ClientId(""), ClientSecret(""), ManagedAppTenantId("")),
        CoaAppConfig(
          ChartName("/leonardo/cromwell-on-azure"),
          ChartVersion("0.2.276"),
          ReleaseNameSuffix("coa-rls"),
          NamespaceNameSuffix("coa-ns"),
          KsaName("coa-ksa"),
          List(
            ServiceConfig(ServiceName("cbas"), KubernetesServiceKindName("ClusterIP")),
            ServiceConfig(ServiceName("cbas-ui"), KubernetesServiceKindName("ClusterIP"), Some(ServicePath("/"))),
            ServiceConfig(ServiceName("cromwell"), KubernetesServiceKindName("ClusterIP"))
          ),
          instrumentationEnabled = false,
          enabled = true,
          dockstoreBaseUrl = new URL("https://fake-url-on-purpose-staging.dockstore.org/")
        ),
        WdsAppConfig(
          ChartName("/leonardo/wds"),
          ChartVersion("0.27.0"),
          ReleaseNameSuffix("wds-rls"),
          NamespaceNameSuffix("wds-ns"),
          KsaName("wds-ksa"),
          List(
            ServiceConfig(ServiceName("wds"), KubernetesServiceKindName("ClusterIP"), Some(ServicePath("/")))
          ),
          instrumentationEnabled = false,
          enabled = true,
          databaseEnabled = false
        ),
        HailBatchAppConfig(
          ChartName("/leonardo/hail-batch-terra-azure"),
          ChartVersion("0.1.9"),
          ReleaseNameSuffix("hail-rls"),
          NamespaceNameSuffix("hail-ns"),
          KsaName("hail-ksa"),
          List(
            ServiceConfig(ServiceName("batch"), KubernetesServiceKindName("ClusterIP"))
          ),
          false
        ),
        AadPodIdentityConfig(
          Namespace("aad-pod-identity"),
          Release("aad-pod-identity"),
          ChartName("aad-pod-identity/aad-pod-identity"),
          ChartVersion("4.1.14"),
          Values("operationMode=managed")
        ),
        List("WDS"),
        TdrConfig("https://jade.datarepo-dev.broadinstitute.org")
      ),
      OidcAuthConfig(
        Uri.unsafeFromString("https://fake"),
        org.broadinstitute.dsde.workbench.oauth2.ClientId("fakeClientId"),
        Some(org.broadinstitute.dsde.workbench.oauth2.ClientSecret("fakeClientSecret")),
        org.broadinstitute.dsde.workbench.oauth2.ClientId("legacyClientSecret")
      ),
      DrsConfig(
        "https://drshub.dsde-dev.broadinstitute.org/api/v4/drs/resolve"
      ),
      LeoMetricsMonitorConfig(true, 5 minutes, true)
    )

    config shouldBe expectedConfig
  }
}
