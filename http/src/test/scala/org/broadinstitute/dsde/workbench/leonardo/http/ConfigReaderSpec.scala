package org.broadinstitute.dsde.workbench.leonardo
package http

import com.azure.core.management.AzureEnvironment
import org.broadinstitute.dsde.workbench.azure._
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
      AzureConfig(
        AzureAppRegistrationConfig(ClientId(""), ClientSecret(""), ManagedAppTenantId("")),
        List(),
        TdrConfig("https://jade.datarepo-dev.broadinstitute.org"),
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

  it should "convert AzureHostingMode strings to AzureEnvironments correctly" in {
    val govEnv = AzureEnvironmentConverter.fromString(AzureEnvironmentConverter.AzureGov)
    val expectedGovEnv = AzureEnvironment.AZURE_US_GOVERNMENT
    govEnv shouldBe expectedGovEnv

    val govRelay = AzureEnvironmentConverter.relaySuffixFromString(AzureEnvironmentConverter.AzureGov)
    val expectedGovRelay = AzureEnvironmentConverter.relaySuffixFromEnvironment(AzureEnvironment.AZURE_US_GOVERNMENT)
    govRelay shouldBe expectedGovRelay

    val govPostgres = AzureEnvironmentConverter.postgresSuffixFromString(AzureEnvironmentConverter.AzureGov)
    val expGovPostgres = AzureEnvironmentConverter.postgresSuffixFromEnvironment(AzureEnvironment.AZURE_US_GOVERNMENT)
    govPostgres shouldBe expGovPostgres

    val govBatch = AzureEnvironmentConverter.batchAccountSuffixFromString(AzureEnvironmentConverter.AzureGov)
    val expGovBatch = AzureEnvironmentConverter.batchAccountSuffixFromEnvironment(AzureEnvironment.AZURE_US_GOVERNMENT)
    govBatch shouldBe expGovBatch

    val defaultRelay = AzureEnvironmentConverter.relaySuffixFromString("")
    val expectedDefaultRelay = AzureEnvironmentConverter.relaySuffixFromEnvironment(AzureEnvironment.AZURE)
    defaultRelay shouldBe expectedDefaultRelay

    val defaultPostgres = AzureEnvironmentConverter.postgresSuffixFromString("")
    val expectedDefaultPostgres = AzureEnvironmentConverter.postgresSuffixFromEnvironment(AzureEnvironment.AZURE)
    defaultPostgres shouldBe expectedDefaultPostgres

    val defaultBatch = AzureEnvironmentConverter.batchAccountSuffixFromString(AzureEnvironmentConverter.Azure)
    val expectedDefaultBatch = AzureEnvironmentConverter.batchAccountSuffixFromEnvironment(AzureEnvironment.AZURE)
    defaultBatch shouldBe expectedDefaultBatch
  }
}
