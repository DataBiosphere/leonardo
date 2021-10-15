package org.broadinstitute.dsde.workbench.leonardo.db

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.{MachineTypeName, RegionName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.makePersistentDisk
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.{DiskSize, GpuConfig, GpuType, LeonardoTestSuite, RuntimeConfig}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class RuntimeConfigQueriesSpec extends AnyFlatSpecLike with TestComponent with LeonardoTestSuite {
  "RuntimeConfigQueries" should "save cluster with properties properly" in isolatedDbTest {
    val runtimeConfig = RuntimeConfig.DataprocConfig(
      numberOfWorkers = 0,
      masterMachineType = MachineTypeName("n1-standard-4"),
      workerMachineType = None,
      masterDiskSize = DiskSize(500),
      workerDiskSize = None,
      numberOfPreemptibleWorkers = Some(0),
      numberOfWorkerLocalSSDs = None,
      properties = Map("spark:spark.executor.memory" -> "10g"),
      region = RegionName("us-central1"),
      componentGatewayEnabled = true
    )
    val res = for {
      now <- IO.realTimeInstant
      id <- RuntimeConfigQueries.insertRuntimeConfig(runtimeConfig, now).transaction
      rc <- RuntimeConfigQueries.getRuntimeConfig(id).transaction
    } yield {
      rc shouldBe runtimeConfig
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "save gceConfig properly" in isolatedDbTest {
    val runtimeConfig1 = RuntimeConfig.GceConfig(
      MachineTypeName("n1-standard-4"),
      DiskSize(100),
      Some(DiskSize(50)),
      ZoneName("us-west2-b"),
      None
    )
    val runtimeConfig2 = RuntimeConfig.GceConfig(
      MachineTypeName("n1-standard-4"),
      DiskSize(100),
      None,
      ZoneName("us-west2-b"),
      Some(GpuConfig(GpuType.NvidiaTeslaT4, 2))
    )
    val res = for {
      now <- IO.realTimeInstant
      id <- RuntimeConfigQueries.insertRuntimeConfig(runtimeConfig1, now).transaction
      rc <- RuntimeConfigQueries.getRuntimeConfig(id).transaction

      id2 <- RuntimeConfigQueries.insertRuntimeConfig(runtimeConfig2, now).transaction
      rc2 <- RuntimeConfigQueries.getRuntimeConfig(id2).transaction
    } yield {
      rc shouldBe runtimeConfig1
      rc2 shouldBe runtimeConfig2
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "save gceWithPdConfig properly" in isolatedDbTest {
    val res = for {
      now <- IO.realTimeInstant
      savedDisk <- makePersistentDisk(None).save()
      runtimeConfig = RuntimeConfig.GceWithPdConfig(
        MachineTypeName("n1-standard-4"),
        Some(savedDisk.id),
        DiskSize(50),
        ZoneName("us-west2-b"),
        Some(GpuConfig(GpuType.NvidiaTeslaT4, 2))
      )
      id <- RuntimeConfigQueries.insertRuntimeConfig(runtimeConfig, now).transaction
      rc <- RuntimeConfigQueries.getRuntimeConfig(id).transaction
    } yield {
      rc shouldBe runtimeConfig
    }
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
