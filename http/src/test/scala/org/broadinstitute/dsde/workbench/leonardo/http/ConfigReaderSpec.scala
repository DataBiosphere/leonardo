package org.broadinstitute.dsde.workbench.leonardo
package http

import org.broadinstitute.dsde.workbench.google2.{FirewallRuleName, NetworkName, RegionName, SubnetworkName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.config.PersistentDiskConfig
import org.broadinstitute.dsde.workbench.leonardo.util.TerraAppSetupChartConfig
import org.broadinstitute.dsp.{ChartName, ChartVersion}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ConfigReaderSpec extends AnyFlatSpec with Matchers {
  val allSupportedRegions = List(
    RegionName("us-central1"),
    RegionName("northamerica-northeast1"),
    RegionName("southamerica-east1"),
    RegionName("us-east1"),
    RegionName("us-east4"),
    RegionName("us-west1"),
    RegionName("us-west2"),
    RegionName("us-west3"),
    RegionName("us-west4"),
    RegionName("europe-central2"),
    RegionName("europe-north1"),
    RegionName("europe-west1"),
    RegionName("europe-west2"),
    RegionName("europe-west3"),
    RegionName("europe-west4"),
    RegionName("europe-west6"),
    RegionName("asia-east1"),
    RegionName("asia-east2"),
    RegionName("asia-northeast1"),
    RegionName("asia-northeast2"),
    RegionName("asia-northeast3"),
    RegionName("asia-south1"),
    RegionName("asia-southeast1"),
    RegionName("asia-southeast2"),
    RegionName("australia-southeast1"),
    RegionName("northamerica-northeast2")
  )
  it should "read config file correctly" in {
    val config = ConfigReader.appConfig
    val expectedVPCConfig = VPCConfig(
      NetworkLabel("vpc-network-name"),
      SubnetworkLabel("vpc-subnetwork-name"),
      FirewallAllowHttpsLabelKey("leonardo-allow-https-firewall-name"),
      FirewallAllowInternalLabelKey("leonardo-allow-internal-firewall-name"),
      NetworkName("leonardo-network"),
      NetworkTag("leonardo"),
      NetworkTag("leonardo-private"),
      false,
      SubnetworkName("leonardo-subnetwork"),
      Map(
        RegionName("us-central1") -> IpRange("10.1.0.0/20"),
        RegionName("northamerica-northeast1") -> IpRange("10.2.0.0/20"),
        RegionName("southamerica-east1") -> IpRange("10.3.0.0/20"),
        RegionName("us-east1") -> IpRange("10.4.0.0/20"),
        RegionName("us-east4") -> IpRange("10.5.0.0/20"),
        RegionName("us-west1") -> IpRange("10.6.0.0/20"),
        RegionName("us-west2") -> IpRange("10.7.0.0/20"),
        RegionName("us-west3") -> IpRange("10.8.0.0/20"),
        RegionName("us-west4") -> IpRange("10.9.0.0/20"),
        RegionName("europe-central2") -> IpRange("10.10.0.0/20"),
        RegionName("europe-north1") -> IpRange("10.11.0.0/20"),
        RegionName("europe-west1") -> IpRange("10.12.0.0/20"),
        RegionName("europe-west2") -> IpRange("10.13.0.0/20"),
        RegionName("europe-west3") -> IpRange("10.14.0.0/20"),
        RegionName("europe-west4") -> IpRange("10.15.0.0/20"),
        RegionName("europe-west6") -> IpRange("10.16.0.0/20"),
        RegionName("asia-east1") -> IpRange("10.17.0.0/20"),
        RegionName("asia-east2") -> IpRange("10.18.0.0/20"),
        RegionName("asia-northeast1") -> IpRange("10.19.0.0/20"),
        RegionName("asia-northeast2") -> IpRange("10.20.0.0/20"),
        RegionName("asia-northeast3") -> IpRange("10.21.0.0/20"),
        RegionName("asia-south1") -> IpRange("10.22.0.0/20"),
        RegionName("asia-southeast1") -> IpRange("10.23.0.0/20"),
        RegionName("asia-southeast2") -> IpRange("10.24.0.0/20"),
        RegionName("australia-southeast1") -> IpRange("10.25.0.0/20"),
        RegionName("northamerica-northeast2") -> IpRange("10.26.0.0/20")
      ),
      List(
        FirewallRuleConfig(
          "leonardo-allow-https",
          allSupportedRegions.map(r => r -> List(IpRange("0.0.0.0/0"))).toMap,
          List(Allowed("tcp", Some("443")))
        ),
        FirewallRuleConfig(
          "leonardo-allow-internal",
          Map(
            RegionName("asia-east1") -> List(IpRange("10.140.0.0/20")),
            RegionName("asia-east2") -> List(IpRange("10.170.0.0/20")),
            RegionName("asia-northeast1") -> List(IpRange("10.146.0.0/20")),
            RegionName("asia-northeast2") -> List(IpRange("10.174.0.0/20")),
            RegionName("asia-northeast3") -> List(IpRange("10.178.0.0/20")),
            RegionName("asia-south1") -> List(IpRange("10.160.0.0/20")),
            RegionName("asia-southeast1") -> List(IpRange("10.148.0.0/20")),
            RegionName("asia-southeast2") -> List(IpRange("10.184.0.0/20")),
            RegionName("australia-southeast1") -> List(IpRange("10.152.0.0/20")),
            RegionName("europe-central2") -> List(IpRange("10.186.0.0/20")),
            RegionName("europe-north1") -> List(IpRange("10.166.0.0/20")),
            RegionName("europe-west1") -> List(IpRange("10.132.0.0/20")),
            RegionName("europe-west2") -> List(IpRange("10.154.0.0/20")),
            RegionName("europe-west3") -> List(IpRange("10.156.0.0/20")),
            RegionName("europe-west4") -> List(IpRange("10.164.0.0/20")),
            RegionName("europe-west6") -> List(IpRange("10.172.0.0/20")),
            RegionName("northamerica-northeast1") -> List(IpRange("10.162.0.0/20")),
            RegionName("northamerica-northeast2") -> List(IpRange("10.188.0.0/20")),
            RegionName("southamerica-east1") -> List(IpRange("10.158.0.0/20")),
            RegionName("us-central1") -> List(IpRange("10.128.0.0/20")),
            RegionName("us-east1") -> List(IpRange("10.142.0.0/20")),
            RegionName("us-east4") -> List(IpRange("10.150.0.0/20")),
            RegionName("us-west1") -> List(IpRange("10.138.0.0/20")),
            RegionName("us-west2") -> List(IpRange("10.168.0.0/20")),
            RegionName("us-west3") -> List(IpRange("10.180.0.0/20")),
            RegionName("us-west4") -> List(IpRange("10.182.0.0/20"))
          ),
          List(Allowed("tcp", Some("0-65535")), Allowed("udp", Some("0-65535")), Allowed("icmp", None))
        ),
        FirewallRuleConfig(
          "leonardo-allow-broad-ssh",
          allSupportedRegions
            .map(r =>
              r -> List(
                IpRange("69.173.127.0/25"),
                IpRange("69.173.124.0/23"),
                IpRange("69.173.126.0/24"),
                IpRange("69.173.127.230/31"),
                IpRange("69.173.64.0/19"),
                IpRange("69.173.127.224/30"),
                IpRange("69.173.127.192/27"),
                IpRange("69.173.120.0/22"),
                IpRange("69.173.127.228/32"),
                IpRange("69.173.127.232/29"),
                IpRange("69.173.127.128/26"),
                IpRange("69.173.96.0/20"),
                IpRange("69.173.127.240/28"),
                IpRange("69.173.112.0/21")
              )
            )
            .toMap,
          List(Allowed("tcp", Some("22")))
        )
      ),
      List(FirewallRuleName("default-allow-rdp"),
           FirewallRuleName("default-allow-icmp"),
           FirewallRuleName("allow-icmp")),
      5 seconds,
      24
    )
    val expectedConfig = AppConfig(
      TerraAppSetupChartConfig(ChartName("/leonardo/terra-app-setup"), ChartVersion("0.0.2")),
      PersistentDiskConfig(
        DiskSize(30),
        DiskType.Standard,
        BlockSize(4096),
        ZoneName("us-central1-a"),
        DiskSize(250)
      ),
      expectedVPCConfig
    )

    config shouldBe expectedConfig
  }
}
