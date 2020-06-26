package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import java.util.concurrent.TimeUnit

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.makePersistentDisk
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.{DiskId, DiskSize, LeonardoTestSuite, RuntimeConfig}

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

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
      properties = Map("spark:spark.executor.memory" -> "10g")
    )
    val res = for {
      now <- testTimer.clock.realTime(TimeUnit.MILLISECONDS)
      id <- RuntimeConfigQueries.insertRuntimeConfig(runtimeConfig, Instant.ofEpochMilli(now)).transaction
      rc <- RuntimeConfigQueries.getRuntimeConfig(id).transaction
    } yield {
      rc shouldBe runtimeConfig
    }
    res.unsafeRunSync()
  }

  it should "save gceConfig properly" in isolatedDbTest {
    val runtimeConfig1 = RuntimeConfig.GceConfig(
      MachineTypeName("n1-standard-4"),
      DiskSize(100),
      Some(DiskSize(50))
    )
    val runtimeConfig2 = RuntimeConfig.GceConfig(
      MachineTypeName("n1-standard-4"),
      DiskSize(100),
      None
    )
    val res = for {
      now <- testTimer.clock.realTime(TimeUnit.MILLISECONDS)
      id <- RuntimeConfigQueries.insertRuntimeConfig(runtimeConfig1, Instant.ofEpochMilli(now)).transaction
      rc <- RuntimeConfigQueries.getRuntimeConfig(id).transaction

      id2 <- RuntimeConfigQueries.insertRuntimeConfig(runtimeConfig2, Instant.ofEpochMilli(now)).transaction
      rc2 <- RuntimeConfigQueries.getRuntimeConfig(id2).transaction
    } yield {
      rc shouldBe runtimeConfig1
      rc2 shouldBe runtimeConfig2
    }
    res.unsafeRunSync()
  }

  it should "save gceWithPdConfig properly" in isolatedDbTest {
    val persistentDiskId = DiskId(1)
    val res = for {
      now <- testTimer.clock.realTime(TimeUnit.MILLISECONDS)
      savedDisk <- makePersistentDisk(persistentDiskId).save()
      runtimeConfig = RuntimeConfig.GceWithPdConfig(
        MachineTypeName("n1-standard-4"),
        Some(savedDisk.id),
        DiskSize(50)
      )
      id <- RuntimeConfigQueries.insertRuntimeConfig(runtimeConfig, Instant.ofEpochMilli(now)).transaction
      rc <- RuntimeConfigQueries.getRuntimeConfig(id).transaction
    } yield {
      rc shouldBe runtimeConfig
    }
    res.unsafeRunSync()
  }
}
