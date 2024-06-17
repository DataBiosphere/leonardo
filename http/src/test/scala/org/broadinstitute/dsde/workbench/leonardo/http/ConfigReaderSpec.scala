package org.broadinstitute.dsde.workbench.leonardo
package http

import org.broadinstitute.dsde.workbench.azure.{
  AzureAppRegistrationConfig,
  AzureServiceBusPublisherConfig,
  AzureServiceBusSubscriberConfig,
  ClientId,
  ClientSecret,
  ManagedAppTenantId
}
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
      TerraAppSetupChartConfig(ChartName("/leonardo/terra-app-setup"), ChartVersion("0.1.0")),
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
              "23.04.24"
            ),
            CustomScriptExtensionConfig(
              "vm-custom-script-extension",
              "Microsoft.Azure.Extensions",
              "CustomScript",
              "2.1",
              true,
              List(
                "https://raw.githubusercontent.com/DataBiosphere/leonardo/ed9c01641b22fd19a57ebd7ccaafc0dd4ae1e683/http/src/main/resources/init-resources/azure_vm_init_script.sh"
              )
            ),
            "terradevacrpublic.azurecr.io/terra-azure-relay-listeners:76d982c",
            VMCredential(username = "username", password = "password")
          ),
          PollMonitorConfig(1 seconds, 10, 1 seconds),
          PollMonitorConfig(1 seconds, 10, 1 seconds)
        ),
        HttpWsmDaoConfig(Uri.unsafeFromString("https://localhost:8000")),
        AzureAppRegistrationConfig(ClientId(""), ClientSecret(""), ManagedAppTenantId("")),
        CoaAppConfig(
          ChartName("cromwell-helm/cromwell-on-azure"),
          ChartVersion("0.2.506"),
          ReleaseNameSuffix("coa-rls"),
          NamespaceNameSuffix("coa-ns"),
          KsaName("coa-ksa"),
          List(
            ServiceConfig(ServiceName("cbas"), KubernetesServiceKindName("ClusterIP")),
            ServiceConfig(ServiceName("cromwell"), KubernetesServiceKindName("ClusterIP"))
          ),
          instrumentationEnabled = false,
          enabled = true,
          dockstoreBaseUrl = new URL("https://staging.dockstore.org/"),
          databaseEnabled = false,
          chartVersionsToExcludeFromUpdates = List(
            ChartVersion("0.2.341"),
            ChartVersion("0.2.338"),
            ChartVersion("0.2.334"),
            ChartVersion("0.2.332"),
            ChartVersion("0.2.328"),
            ChartVersion("0.2.291"),
            ChartVersion("0.2.277"),
            ChartVersion("0.2.276"),
            ChartVersion("0.2.268"),
            ChartVersion("0.2.265"),
            ChartVersion("0.2.263"),
            ChartVersion("0.2.251"),
            ChartVersion("0.2.242"),
            ChartVersion("0.2.239"),
            ChartVersion("0.2.237"),
            ChartVersion("0.2.232"),
            ChartVersion("0.2.231"),
            ChartVersion("0.2.229"),
            ChartVersion("0.2.225"),
            ChartVersion("0.2.223"),
            ChartVersion("0.2.220"),
            ChartVersion("0.2.219"),
            ChartVersion("0.2.218"),
            ChartVersion("0.2.217"),
            ChartVersion("0.2.216"),
            ChartVersion("0.2.215"),
            ChartVersion("0.2.213"),
            ChartVersion("0.2.212"),
            ChartVersion("0.2.211"),
            ChartVersion("0.2.210"),
            ChartVersion("0.2.209"),
            ChartVersion("0.2.204"),
            ChartVersion("0.2.201"),
            ChartVersion("0.2.199"),
            ChartVersion("0.2.197"),
            ChartVersion("0.2.195"),
            ChartVersion("0.2.192"),
            ChartVersion("0.2.191"),
            ChartVersion("0.2.187"),
            ChartVersion("0.2.184"),
            ChartVersion("0.2.179"),
            ChartVersion("0.2.160"),
            ChartVersion("0.2.159"),
            ChartVersion("0.2.148"),
            ChartVersion("0.2.39")
          )
        ),
        CromwellRunnerAppConfig(
          ChartName("terra-helm/cromwell-runner-app"),
          ChartVersion("0.150.0"),
          ReleaseNameSuffix("cra-rls"),
          NamespaceNameSuffix("cra-ns"),
          KsaName("cra-ksa"),
          List(
            ServiceConfig(ServiceName("cromwell-runner"),
                          KubernetesServiceKindName("ClusterIP"),
                          Some(ServicePath("/cromwell"))
            )
          ),
          instrumentationEnabled = false,
          enabled = true,
          chartVersionsToExcludeFromUpdates = List.empty,
          ecmBaseUri = new URL("https://externalcreds.dsde-dev.broadinstitute.org")
        ),
        WorkflowsAppConfig(
          ChartName("terra-helm/workflows-app"),
          ChartVersion("0.226.0"),
          ReleaseNameSuffix("wfa-rls"),
          NamespaceNameSuffix("wfa-ns"),
          KsaName("wfa-ksa"),
          List(
            ServiceConfig(ServiceName("cbas"), KubernetesServiceKindName("ClusterIP")),
            ServiceConfig(ServiceName("cromwell-reader"),
                          KubernetesServiceKindName("ClusterIP"),
                          Some(ServicePath("/cromwell"))
            )
          ),
          instrumentationEnabled = false,
          enabled = true,
          dockstoreBaseUrl = new URL("https://staging.dockstore.org/"),
          chartVersionsToExcludeFromUpdates = List.empty,
          ecmBaseUri = new URL("https://externalcreds.dsde-dev.broadinstitute.org"),
          bardBaseUri = new URL("https://terra-bard-dev.appspot.com"),
          bardEnabled = false
        ),
        WdsAppConfig(
          ChartName("terra-helm/wds"),
          ChartVersion("0.85.0"),
          ReleaseNameSuffix("wds-rls"),
          NamespaceNameSuffix("wds-ns"),
          KsaName("wds-ksa"),
          List(
            ServiceConfig(ServiceName("wds"), KubernetesServiceKindName("ClusterIP"), Some(ServicePath("/")))
          ),
          instrumentationEnabled = false,
          enabled = true,
          databaseEnabled = false,
          environment = "dev",
          environmentBase = "live",
          chartVersionsToExcludeFromUpdates = List(
            ChartVersion("0.3.0"),
            ChartVersion("0.7.0"),
            ChartVersion("0.13.0"),
            ChartVersion("0.16.0"),
            ChartVersion("0.17.0"),
            ChartVersion("0.19.0"),
            ChartVersion("0.20.0"),
            ChartVersion("0.21.0"),
            ChartVersion("0.22.0"),
            ChartVersion("0.24.0"),
            ChartVersion("0.26.0"),
            ChartVersion("0.27.0"),
            ChartVersion("0.28.0"),
            ChartVersion("0.31.0"),
            ChartVersion("0.38.0"),
            ChartVersion("0.39.0"),
            ChartVersion("0.41.0"),
            ChartVersion("0.42.0"),
            ChartVersion("0.43.0")
          )
        ),
        HailBatchAppConfig(
          ChartName("/leonardo/hail-batch-terra-azure"),
          ChartVersion("0.2.0"),
          ReleaseNameSuffix("hail-rls"),
          NamespaceNameSuffix("hail-ns"),
          KsaName("hail-ksa"),
          List(
            ServiceConfig(ServiceName("batch"), KubernetesServiceKindName("ClusterIP"))
          ),
          false,
          chartVersionsToExcludeFromUpdates = List()
        ),
        List(AppType.Wds, AppType.WorkflowsApp),
        TdrConfig("https://jade.datarepo-dev.broadinstitute.org"),
        ListenerChartConfig(ChartName("terra-helm/listener"), ChartVersion("0.3.0")),
        AzureHostingModeConfig(
          false,
          "AZURE",
          AzureManagedIdentityAuthConfig(".default", 30),
          AzureServiceBusPublisherConfig("replace_me", Some("replace_me"), Some("replace_me")),
          AzureServiceBusSubscriberConfig("replace_me", "replace_me", Some("replace_me"), Some("replace_me"), 1, 1)
        )
      ),
      OidcAuthConfig(
        Uri.unsafeFromString("https://fake"),
        org.broadinstitute.dsde.workbench.oauth2.ClientId("fakeClientId")
      ),
      DrsConfig(
        "https://drshub.dsde-dev.broadinstitute.org/api/v4/drs/resolve"
      ),
      LeoMetricsMonitorConfig(true, 5 minutes, true)
    )

    config shouldBe expectedConfig
  }
}
