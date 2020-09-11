package org.broadinstitute.dsde.workbench.leonardo
package config

import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.google2.{Location, MachineTypeName, RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.monitor.MonitorConfig.GceMonitorConfig
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  LeoPubsubMessageSubscriberConfig,
  PersistentDiskMonitorConfig,
  PollMonitorConfig
}
import org.broadinstitute.dsp.{ChartName, ChartVersion}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ConfigSpec extends AnyFlatSpec with Matchers {
  it should "read PersistentDiskConfig properly" in {
    val expectedResult = PersistentDiskConfig(
      DiskSize(30),
      DiskType.Standard,
      BlockSize(4096),
      ZoneName("us-central1-a")
    )

    Config.persistentDiskConfig shouldBe expectedResult
  }

  it should "read LeoPubsubMessageSubscriberConfig properly" in {
    val expectedResult = LeoPubsubMessageSubscriberConfig(
      100,
      295 seconds,
      PersistentDiskMonitorConfig(
        PollMonitorConfig(5, 3 seconds),
        PollMonitorConfig(5, 3 seconds),
        PollMonitorConfig(5, 3 seconds)
      )
    )

    Config.leoPubsubMessageSubscriberConfig shouldBe expectedResult
  }

  it should "read gce.monitor properly" in {
    val expected = GceMonitorConfig(
      20 seconds,
      15 seconds,
      120,
      8 seconds,
      75,
      Config.clusterBucketConfig,
      Map(
        RuntimeStatus.Creating -> 30.minutes,
        RuntimeStatus.Starting -> 20.minutes,
        RuntimeStatus.Deleting -> 30.minutes
      ),
      ZoneName("us-central1-a"),
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
      ).map(CidrIP)
    )
    Config.gkeClusterConfig shouldBe expectedResult
  }

  it should "read DefaultNodepoolConfig properly" in {
    val expectedResult =
      DefaultNodepoolConfig(MachineTypeName("n1-standard-1"), NumNodes(1), false, MaxNodepoolsPerDefaultNode(16))
    Config.gkeDefaultNodepoolConfig shouldBe expectedResult
  }

  it should "read GalaxyNodepoolConfig properly" in {
    val expectedResult = GalaxyNodepoolConfig(MachineTypeName("n1-standard-8"),
                                              NumNodes(1),
                                              true,
                                              AutoscalingConfig(AutoscalingMin(0), AutoscalingMax(1)))
    Config.gkeGalaxyNodepoolConfig shouldBe expectedResult
  }

  it should "read GalaxyAppConfig properly" in {
    val expectedResult = GalaxyAppConfig(
      "galaxy-rls",
      ChartName("galaxy/galaxykubeman"),
      ChartVersion("0.5.3"),
      "galaxy-ns",
      List(ServiceConfig(ServiceName("galaxy"), KubernetesServiceKindName("ClusterIP"))),
      "galaxy-ksa",
      true
    )
    Config.gkeGalaxyAppConfig shouldBe expectedResult
  }
}
