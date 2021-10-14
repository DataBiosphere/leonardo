package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.{ServiceAccountName, ServiceName}
import org.broadinstitute.dsde.workbench.google2.{Location, MachineTypeName, RegionName}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{galaxyChartName, galaxyChartVersion}
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorConfig.GceMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  InterruptablePollMonitorConfig,
  LeoPubsubMessageSubscriberConfig,
  PersistentDiskMonitorConfig,
  PollMonitorConfig
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

final class ConfigSpec extends AnyFlatSpec with Matchers {
  it should "read LeoPubsubMessageSubscriberConfig properly" in {
    val expectedResult = LeoPubsubMessageSubscriberConfig(
      100,
      295 seconds,
      PersistentDiskMonitorConfig(
        PollMonitorConfig(5, 3 seconds),
        PollMonitorConfig(5, 3 seconds),
        PollMonitorConfig(5, 3 seconds)
      ),
      GalaxyDiskConfig(
        "nfs-disk",
        DiskSize(100),
        "postgres-disk",
        "gxy-postres-disk",
        DiskSize(10),
        BlockSize(4096)
      )
    )

    Config.leoPubsubMessageSubscriberConfig shouldBe expectedResult
  }

  it should "read gce.monitor properly" in {
    val expected = GceMonitorConfig(
      20 seconds,
      PollMonitorConfig(120, 15 seconds),
      Map(
        RuntimeStatus.Creating -> 30.minutes,
        RuntimeStatus.Starting -> 20.minutes,
        RuntimeStatus.Deleting -> 30.minutes
      ),
      InterruptablePollMonitorConfig(75, 8 seconds, 10 minutes),
      Config.clusterBucketConfig,
      Config.imageConfig
    )

    Config.gceMonitorConfig shouldBe (expected)
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
      KubernetesClusterVersion("1.20"),
      1 hour,
      200
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
                                              AutoscalingConfig(AutoscalingMin(0), AutoscalingMax(2)))
    Config.gkeGalaxyNodepoolConfig shouldBe expectedResult
  }

  it should "read GalaxyAppConfig properly" in {
    val expectedResult = GalaxyAppConfig(
      "gxy-rls",
      galaxyChartName,
      galaxyChartVersion,
      "gxy-ns",
      List(ServiceConfig(ServiceName("galaxy"), KubernetesServiceKindName("ClusterIP"))),
      ServiceAccountName("gxy-ksa"),
      true,
      "replace-me",
      "https://firecloud-orchestration.dsde-dev.broadinstitute.org/api/",
      "https://us-central1-broad-dsde-dev.cloudfunctions.net/martha_v3"
    )
    Config.gkeGalaxyAppConfig shouldBe expectedResult
  }

  it should "read GalaxyDiskConfig properly" in {
    val expectedResult =
      GalaxyDiskConfig("nfs-disk", DiskSize(100), "postgres-disk", "gxy-postres-disk", DiskSize(10), BlockSize(4096))
    Config.gkeGalaxyDiskConfig shouldBe expectedResult
  }
}
