package org.broadinstitute.dsde.workbench.leonardo.config

import org.broadinstitute.dsde.workbench.google2.ZoneName
import org.broadinstitute.dsde.workbench.leonardo.monitor.{
  LeoPubsubMessageSubscriberConfig,
  PersistentDiskMonitor,
  PersistentDiskMonitorConfig
}
import org.broadinstitute.dsde.workbench.leonardo.{BlockSize, DiskSize, DiskType}

import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
        PersistentDiskMonitor(5, 3 seconds),
        PersistentDiskMonitor(5, 3 seconds),
        PersistentDiskMonitor(5, 3 seconds)
      )
    )

    Config.leoPubsubMessageSubscriberConfig shouldBe expectedResult
  }
}
