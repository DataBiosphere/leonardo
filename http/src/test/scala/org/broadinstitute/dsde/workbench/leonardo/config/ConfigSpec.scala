package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{ServiceAccountName, ServiceName}
import org.broadinstitute.dsde.workbench.google2.{Location, MachineTypeName, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{galaxyChartName, galaxyChartVersion}
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorConfig.GceMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  CreateDiskTimeout,
  InterruptablePollMonitorConfig,
  LeoPubsubMessageSubscriberConfig,
  PersistentDiskMonitorConfig,
  PollMonitorConfig
}
import org.broadinstitute.dsp.ChartVersion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

final class ConfigSpec extends AnyFlatSpec with Matchers {
  it should "read LeoPubsubMessageSubscriberConfig properly" in {
    val expectedResult = LeoPubsubMessageSubscriberConfig(
      100,
      595 seconds,
      PersistentDiskMonitorConfig(
        CreateDiskTimeout(5, 20),
        PollMonitorConfig(2 seconds, 5, 3 seconds),
        PollMonitorConfig(2 seconds, 5, 3 seconds)
      ),
      GalaxyDiskConfig(
        "nfs-disk",
        DiskSize(100),
        "postgres-disk",
        "gxy-postres-disk",
        DiskSize(10),
        BlockSize(4096)
      ),
      1 seconds
    )

    Config.leoPubsubMessageSubscriberConfig shouldBe expectedResult
  }

  it should "read gce.monitor properly" in {
    val expected = GceMonitorConfig(
      20 seconds,
      PollMonitorConfig(2 seconds, 120, 15 seconds),
      Map(
        RuntimeStatus.Creating -> 30.minutes,
        RuntimeStatus.Starting -> 20.minutes,
        RuntimeStatus.Deleting -> 30.minutes
      ),
      InterruptablePollMonitorConfig(75, 8 seconds, 10 minutes),
      Config.clusterBucketConfig,
      Config.imageConfig
    )

    Config.gceMonitorConfig shouldBe expected
  }

  it should "read PrometheusConfig properly" in {
    val expected = PrometheusConfig(9098)
    Config.prometheusConfig shouldBe expected
  }

  "GKE config" should "read ClusterConfig properly" in {
    val expectedResult = KubernetesClusterConfig(
      Location("us-central1-a"),
      RegionName("us-central1"),
      List(
        "69.173.127.0/25",
        "69.173.124.0/23",
        "69.173.126.0/24",
        "69.173.127.230/31",
        "69.173.64.0/19",
        "69.173.127.224/30",
        "69.173.127.192/27",
        "69.173.120.0/22",
        "69.173.127.228/32",
        "69.173.127.232/29",
        "69.173.127.128/26",
        "69.173.96.0/20",
        "69.173.127.240/28",
        "69.173.112.0/21"
      ).map(CidrIP),
      KubernetesClusterVersion("1.28"),
      1 hour,
      200,
      AutopilotConfig(AutopilotResource(500, 3, 1), AutopilotResource(500, 3, 1))
    )
    Config.gkeClusterConfig shouldBe expectedResult
  }

  it should "read DefaultNodepoolConfig properly" in {
    val expectedResult =
      DefaultNodepoolConfig(MachineTypeName("n1-standard-1"), NumNodes(1), false, MaxNodepoolsPerDefaultNode(16))
    Config.gkeDefaultNodepoolConfig shouldBe expectedResult
  }

  it should "read GalaxyNodepoolConfig properly" in {
    val expectedResult = GalaxyNodepoolConfig(MachineTypeName("n1-highmem-8"),
                                              NumNodes(1),
                                              false,
                                              AutoscalingConfig(AutoscalingMin(0), AutoscalingMax(2))
    )
    Config.gkeGalaxyNodepoolConfig shouldBe expectedResult
  }

  it should "read GalaxyAppConfig properly" in {
    val expectedResult = GalaxyAppConfig(
      ReleaseNameSuffix("gxy-rls"),
      galaxyChartName,
      galaxyChartVersion,
      NamespaceNameSuffix("gxy-ns"),
      List(ServiceConfig(ServiceName("galaxy"), KubernetesServiceKindName("ClusterIP"))),
      ServiceAccountName("gxy-ksa"),
      true,
      DbPassword("replace-me"),
      GalaxyOrchUrl("https://firecloud-orchestration.dsde-dev.broadinstitute.org/api/"),
      GalaxyDrsUrl("https://drshub.dsde-dev.broadinstitute.org/api/v4/drs/resolve"),
      5,
      3,
      true,
      List(
        ChartVersion("0.7.3"),
        ChartVersion("0.8.0"),
        ChartVersion("1.2.0"),
        ChartVersion("1.2.1"),
        ChartVersion("1.2.2"),
        ChartVersion("1.6.0"),
        ChartVersion("1.6.1"),
        ChartVersion("2.1.0"),
        ChartVersion("2.4.4"),
        ChartVersion("2.4.6"),
        ChartVersion("2.4.7"),
        ChartVersion("2.4.8"),
        ChartVersion("2.4.9"),
        ChartVersion("2.5.0"),
        ChartVersion("2.5.1"),
        ChartVersion("2.5.2"),
        ChartVersion("2.8.0"),
        ChartVersion("2.8.1"),
        ChartVersion("2.9.0"),
        ChartVersion("2.10.0")
      )
    )
    Config.gkeGalaxyAppConfig shouldBe expectedResult
  }

  it should "read GalaxyDiskConfig properly" in {
    val expectedResult =
      GalaxyDiskConfig("nfs-disk", DiskSize(100), "postgres-disk", "gxy-postres-disk", DiskSize(10), BlockSize(4096))
    Config.gkeGalaxyDiskConfig shouldBe expectedResult
  }

  it should "read gkeGalaxyAppConfig properly" in {
    val expectedResult =
      AppMonitorConfig(
        PollMonitorConfig(10 seconds, 90, 10 seconds),
        PollMonitorConfig(10 seconds, 90, 10 seconds),
        PollMonitorConfig(30 seconds, 120, 15 seconds),
        PollMonitorConfig(30 seconds, 120, 15 seconds),
        PollMonitorConfig(2 seconds, 100, 3 seconds),
        InterruptablePollMonitorConfig(120, 0 seconds, 30 seconds),
        PollMonitorConfig(0 days, 120, 0 days),
        PollMonitorConfig(0 days, 10, 2 seconds),
        PollMonitorConfig(0 days, 10, 2 seconds),
        InterruptablePollMonitorConfig(5, 1 seconds, 10 minutes),
        InterruptablePollMonitorConfig(5, 1 seconds, 10 minutes),
        PollMonitorConfig(1 second, 1, 1 second)
      )
    Config.appMonitorConfig shouldBe expectedResult
  }
}
