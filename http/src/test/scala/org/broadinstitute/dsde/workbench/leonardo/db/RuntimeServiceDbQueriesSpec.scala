package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import java.time.Instant

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{makeCluster, _}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.{
  clusterQuery,
  labelQuery,
  LabelResourceType,
  RuntimeServiceDbQueries,
  TestComponent
}
import org.broadinstitute.dsde.workbench.leonardo.http.api.ListRuntimeResponse2
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeServiceDbQueries._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpecLike

class RuntimeServiceDbQueriesSpec extends AnyFlatSpecLike with TestComponent with GcsPathUtils with ScalaFutures {
  val maxElapsed = 5.seconds

  it should "getStatusByName" in isolatedDbTest {
    val r = makeCluster(1)
    val res = for {
      statusBeforeCreating <- getStatusByName(r.googleProject, r.runtimeName).transaction
      runtime <- IO(r.save())
      status <- getStatusByName(runtime.googleProject, runtime.runtimeName).transaction
      _ <- clusterQuery.markPendingDeletion(runtime.id, Instant.now).transaction
      statusAfterDeletion <- getStatusByName(runtime.googleProject, runtime.runtimeName).transaction
    } yield {
      statusBeforeCreating shouldBe None
      runtime.status shouldBe (status.get)
      statusAfterDeletion shouldBe Some(RuntimeStatus.Deleting)
    }

    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "list runtimes" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      list1 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, false, None).transaction
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      d1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig)
      c1 <- IO(
        makeCluster(1).saveWithRuntimeConfig(d1RuntimeConfig)
      )
      list2 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, false, None).transaction
      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      d2RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d2.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig)
      c2 <- IO(
        makeCluster(2).saveWithRuntimeConfig(d2RuntimeConfig)
      )
      list3 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, false, None).transaction
      end <- IO.realTimeInstant
      elapsed = (end.toEpochMilli - start.toEpochMilli).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      list1 shouldEqual List.empty
      val c1Expected = toListRuntimeResponse(c1, Map.empty, d1RuntimeConfig)
      val c2Expected = toListRuntimeResponse(c2, Map.empty, d2RuntimeConfig)
      list2 shouldEqual List(c1Expected)
      list3.toSet shouldEqual Set(c1Expected, c2Expected)
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "list runtimes by labels" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig)
      c1 <- IO(makeCluster(1).saveWithRuntimeConfig(c1RuntimeConfig))
      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d2.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      None)
      c2 <- IO(makeCluster(2).saveWithRuntimeConfig(c2RuntimeConfig))
      labels1 = Map("googleProject" -> c1.googleProject.value,
                    "clusterName" -> c1.runtimeName.asString,
                    "creator" -> c1.auditInfo.creator.value)
      labels2 = Map("googleProject" -> c2.googleProject.value,
                    "clusterName" -> c2.runtimeName.asString,
                    "creator" -> c2.auditInfo.creator.value)
      list1 <- RuntimeServiceDbQueries.listRuntimes(labels1, false, None).transaction
      list2 <- RuntimeServiceDbQueries.listRuntimes(labels2, false, None).transaction
      _ <- labelQuery.saveAllForResource(c1.id, LabelResourceType.Runtime, labels1).transaction
      _ <- labelQuery.saveAllForResource(c2.id, LabelResourceType.Runtime, labels2).transaction
      list3 <- RuntimeServiceDbQueries.listRuntimes(labels1, false, None).transaction
      list4 <- RuntimeServiceDbQueries.listRuntimes(labels2, false, None).transaction
      list5 <- RuntimeServiceDbQueries
        .listRuntimes(Map("googleProject" -> c1.googleProject.value), false, None)
        .transaction
      end <- IO.realTimeInstant
      elapsed = (end.toEpochMilli - start.toEpochMilli).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      list1 shouldEqual List.empty
      list2 shouldEqual List.empty
      val c1Expected = toListRuntimeResponse(c1, labels1, c1RuntimeConfig)
      val c2Expected = toListRuntimeResponse(c2, labels2, c2RuntimeConfig)
      list3 shouldEqual List(c1Expected)
      list4 shouldEqual List(c2Expected)
      list5.toSet shouldEqual Set(c1Expected, c2Expected)
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "list runtimes by project" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig)
      c1 <- IO(
        makeCluster(1).saveWithRuntimeConfig(c1RuntimeConfig)
      )
      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d2.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      None)
      c2 <- IO(
        makeCluster(2).saveWithRuntimeConfig(c2RuntimeConfig)
      )
      list1 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, false, Some(project)).transaction
      list2 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, false, Some(project2)).transaction
      end <- IO.realTimeInstant
      elapsed = (end.toEpochMilli - start.toEpochMilli).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      val c1Expected = toListRuntimeResponse(c1, Map.empty, c1RuntimeConfig)
      val c2Expected = toListRuntimeResponse(c2, Map.empty, c2RuntimeConfig)
      list1.toSet shouldEqual Set(c1Expected, c2Expected)
      list2 shouldEqual List.empty
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "list runtimes including deleted" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      None)
      c1 <- IO(
        makeCluster(1)
          .copy(status = RuntimeStatus.Deleted)
          .saveWithRuntimeConfig(c1RuntimeConfig)
      )
      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d2.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig)
      c2 <- IO(
        makeCluster(2)
          .copy(status = RuntimeStatus.Deleted)
          .saveWithRuntimeConfig(c2RuntimeConfig)
      )
      d3 <- makePersistentDisk(None).save()
      c3RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d3.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig)
      c3 <- IO(
        makeCluster(3).saveWithRuntimeConfig(
          RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                        Some(d3.id),
                                        bootDiskSize = DiskSize(50),
                                        zone = ZoneName("us-west2-b"),
                                        CommonTestData.gpuConfig)
        )
      )
      list1 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, true, None).transaction
      list2 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, false, None).transaction
      end <- IO.realTimeInstant
      elapsed = (end.toEpochMilli - start.toEpochMilli).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      val c1Expected = toListRuntimeResponse(c1, Map.empty, c1RuntimeConfig)
      val c2Expected = toListRuntimeResponse(c2, Map.empty, c2RuntimeConfig)
      val c3Expected = toListRuntimeResponse(c3, Map.empty, c3RuntimeConfig)
      list1.toSet shouldEqual Set(c1Expected, c2Expected, c3Expected)
      list2 shouldEqual List(c3Expected)
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "get a runtime" in isolatedDbTest {
    val res = for {
      disk <- makePersistentDisk(None).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(disk.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig)
      c1 <- IO(makeCluster(1).saveWithRuntimeConfig(c1RuntimeConfig))
      get1 <- RuntimeServiceDbQueries.getRuntime(c1.googleProject, c1.runtimeName).transaction
      get2 <- RuntimeServiceDbQueries.getRuntime(c1.googleProject, RuntimeName("does-not-exist")).transaction.attempt
    } yield {
      get1 shouldBe GetRuntimeResponse.fromRuntime(c1, c1RuntimeConfig, Some(DiskConfig.fromPersistentDisk(disk)))
      get2.isLeft shouldBe true
    }

    res.unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  private def toListRuntimeResponse(
    runtime: Runtime,
    labels: LabelMap,
    runtimeConfig: RuntimeConfig = CommonTestData.defaultDataprocRuntimeConfig
  ): ListRuntimeResponse2 =
    ListRuntimeResponse2(
      runtime.id,
      runtime.samResource,
      runtime.runtimeName,
      runtime.googleProject,
      runtime.auditInfo,
      runtimeConfig,
      Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase,
                          runtime.googleProject,
                          runtime.runtimeName,
                          Set.empty,
                          labels),
      runtime.status,
      labels,
      false
    )

}
