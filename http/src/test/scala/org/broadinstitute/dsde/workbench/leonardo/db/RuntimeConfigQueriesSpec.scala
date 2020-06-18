package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import java.util.concurrent.TimeUnit

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.{DiskSize, LeonardoTestSuite, RuntimeConfig}

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
}
