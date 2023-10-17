package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{makeCluster, _}
import org.broadinstitute.dsde.workbench.leonardo.LeonardoTestTags.SlickPlainQueryTest
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeServiceDbQueries._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.model.{IP, WorkbenchEmail}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike

import java.time.Instant
import java.util.UUID
import scala.collection.immutable.List
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RuntimeServiceDbQueriesSpec extends AnyFlatSpecLike with TestComponent with GcsPathUtils with ScalaFutures {
  val maxElapsed = 5.seconds

  it should "getStatusByName" in isolatedDbTest {
    val r = makeCluster(1)
    val res = for {
      statusBeforeCreating <- getStatusByName(r.cloudContext, r.runtimeName).transaction
      runtime <- IO(r.save())
      status <- getStatusByName(runtime.cloudContext, runtime.runtimeName).transaction
      _ <- clusterQuery.markPendingDeletion(runtime.id, Instant.now).transaction
      statusAfterDeletion <- getStatusByName(runtime.cloudContext, runtime.runtimeName).transaction
    } yield {
      statusBeforeCreating shouldBe None
      runtime.status shouldBe (status.get)
      statusAfterDeletion shouldBe Some(RuntimeStatus.Deleting)
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      list1 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, List(RuntimeStatus.Deleted), None).transaction
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      d1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig
      )
      c1 <- IO(
        makeCluster(1).saveWithRuntimeConfig(d1RuntimeConfig)
      )
      list2 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, List(RuntimeStatus.Deleted), None).transaction
      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      d2RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d2.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig
      )
      c2 <- IO(
        makeCluster(2).saveWithRuntimeConfig(d2RuntimeConfig)
      )
      list3 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, List(RuntimeStatus.Deleted), None).transaction
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

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes by labels" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig
      )
      c1 <- IO(makeCluster(1).saveWithRuntimeConfig(c1RuntimeConfig))
      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d2.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      None
      )
      c2 <- IO(makeCluster(2).saveWithRuntimeConfig(c2RuntimeConfig))
      labels1 = Map("googleProject" -> c1.cloudContext.asString,
                    "clusterName" -> c1.runtimeName.asString,
                    "creator" -> c1.auditInfo.creator.value
      )
      labels2 = Map("googleProject" -> c2.cloudContext.asString,
                    "clusterName" -> c2.runtimeName.asString,
                    "creator" -> c2.auditInfo.creator.value
      )
      list1 <- RuntimeServiceDbQueries.listRuntimes(labels1, List(RuntimeStatus.Deleted), None).transaction
      list2 <- RuntimeServiceDbQueries.listRuntimes(labels2, List(RuntimeStatus.Deleted), None).transaction
      _ <- labelQuery.saveAllForResource(c1.id, LabelResourceType.Runtime, labels1).transaction
      _ <- labelQuery.saveAllForResource(c2.id, LabelResourceType.Runtime, labels2).transaction
      list3 <- RuntimeServiceDbQueries.listRuntimes(labels1, List(RuntimeStatus.Deleted), None).transaction
      list4 <- RuntimeServiceDbQueries.listRuntimes(labels2, List(RuntimeStatus.Deleted), None).transaction
      list5 <- RuntimeServiceDbQueries
        .listRuntimes(Map("googleProject" -> c1.cloudContext.asString), List(RuntimeStatus.Deleted), None)
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

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  "listRuntimesForWorkspace" should "list runtimes by labels properly" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig
      )
      c1 <- IO(makeCluster(1).saveWithRuntimeConfig(c1RuntimeConfig))
      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d2.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      None
      )
      c2 <- IO(makeCluster(2).saveWithRuntimeConfig(c2RuntimeConfig))
      labels1 = Map("googleProject" -> c1.cloudContext.asString,
                    "clusterName" -> c1.runtimeName.asString,
                    "creator" -> c1.auditInfo.creator.value
      )
      labels2 = Map("googleProject" -> c2.cloudContext.asString,
                    "clusterName" -> c2.runtimeName.asString,
                    "creator" -> c2.auditInfo.creator.value
      )
      list1 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(labels1, List(RuntimeStatus.Deleted), None, None, None)
        .transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(labels2, List(RuntimeStatus.Deleted), None, None, None)
        .transaction
      _ <- labelQuery.saveAllForResource(c1.id, LabelResourceType.Runtime, labels1).transaction
      _ <- labelQuery.saveAllForResource(c2.id, LabelResourceType.Runtime, labels2).transaction
      list3 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(labels1, List(RuntimeStatus.Deleted), None, None, None)
        .transaction
      list4 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(labels2, List(RuntimeStatus.Deleted), None, None, None)
        .transaction
      list5 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(Map("googleProject" -> c1.cloudContext.asString),
                                  List(RuntimeStatus.Deleted),
                                  None,
                                  None,
                                  None
        )
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

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes by project" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig
      )
      c1 <- IO(
        makeCluster(1).saveWithRuntimeConfig(c1RuntimeConfig)
      )
      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d2.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      None
      )
      c2 <- IO(
        makeCluster(2).saveWithRuntimeConfig(c2RuntimeConfig)
      )
      list1 <- RuntimeServiceDbQueries
        .listRuntimes(Map.empty, List(RuntimeStatus.Deleted), None, Some(cloudContextGcp))
        .transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimes(Map.empty, List(RuntimeStatus.Deleted), None, Some(cloudContext2Gcp))
        .transaction
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

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes including deleted" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      None
      )
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
                                                      CommonTestData.gpuConfig
      )
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
                                                      CommonTestData.gpuConfig
      )
      c3 <- IO(
        makeCluster(3).saveWithRuntimeConfig(
          RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                        Some(d3.id),
                                        bootDiskSize = DiskSize(50),
                                        zone = ZoneName("us-west2-b"),
                                        CommonTestData.gpuConfig
          )
        )
      )
      list1 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, List.empty, None).transaction
      list2 <- RuntimeServiceDbQueries.listRuntimes(Map.empty, List(RuntimeStatus.Deleted), None).transaction
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

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimesV2 including deleted" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      None
      )
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
                                                      CommonTestData.gpuConfig
      )
      c2 <- IO(
        makeCluster(2)
          .copy(status = RuntimeStatus.Deleted)
          .saveWithRuntimeConfig(c2RuntimeConfig)
      )
      d3 <- makePersistentDisk(Some(DiskName("d3"))).save()
      c3RuntimeConfig = RuntimeConfig.AzureConfig(defaultMachineType, Some(d3.id), None)
      c3 <- IO(
        makeCluster(3).saveWithRuntimeConfig(
          c3RuntimeConfig
        )
      )
      list1 <- RuntimeServiceDbQueries.listRuntimesForWorkspace(Map.empty, List.empty, None, None, None).transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(Map.empty, List(RuntimeStatus.Deleted), None, None, None)
        .transaction
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

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimesV2 by workspace" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant

      workspaceId1 = WorkspaceId(UUID.randomUUID())
      workspaceId2 = WorkspaceId(UUID.randomUUID())

      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      None
      )
      c1 <- IO(
        makeCluster(1)
          .copy(workspaceId = Some(workspaceId1))
          .saveWithRuntimeConfig(c1RuntimeConfig)
      )

      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d2.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig
      )
      c2 <- IO(
        makeCluster(2)
          .copy(workspaceId = Some(workspaceId1), status = RuntimeStatus.Deleted)
          .saveWithRuntimeConfig(c2RuntimeConfig)
      )

      d3 <- makePersistentDisk(None).save()
      c3RuntimeConfig = RuntimeConfig.AzureConfig(defaultMachineType, Some(d3.id), None)
      c3 <- IO(
        makeCluster(3)
          .copy(workspaceId = Some(workspaceId2))
          .saveWithRuntimeConfig(
            c3RuntimeConfig
          )
      )
      list1 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(Map.empty, List.empty, None, Some(workspaceId1), None)
        .transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(Map.empty, List(RuntimeStatus.Deleted), None, Some(workspaceId2), None)
        .transaction
      end <- IO.realTimeInstant
      elapsed = (end.toEpochMilli - start.toEpochMilli).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      val c1Expected = toListRuntimeResponse(c1, Map.empty, c1RuntimeConfig)
      val c2Expected = toListRuntimeResponse(c2, Map.empty, c2RuntimeConfig)
      val c3Expected = toListRuntimeResponse(c3, Map.empty, c3RuntimeConfig)
      list1.toSet shouldEqual Set(c1Expected, c2Expected)
      list2 shouldEqual List(c3Expected)
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimesV2 by cloudProvider and workspace" taggedAs SlickPlainQueryTest in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant

      workspaceId1 = WorkspaceId(UUID.randomUUID())
      workspaceId2 = WorkspaceId(UUID.randomUUID())

      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(d1.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      None
      )
      c1 <- IO(
        makeCluster(1)
          .copy(workspaceId = Some(workspaceId1))
          .saveWithRuntimeConfig(c1RuntimeConfig)
      )

      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      c2RuntimeConfig = RuntimeConfig.AzureConfig(defaultMachineType, Some(d2.id), None)
      c2 <- IO(
        makeCluster(2)
          .copy(workspaceId = Some(workspaceId1), cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext))
          .saveWithRuntimeConfig(
            c2RuntimeConfig
          )
      )
      c2ClusterRecord <- clusterQuery.getActiveClusterRecordByName(c2.cloudContext, c2.runtimeName).transaction

      d3 <- makePersistentDisk(None).save()
      c3RuntimeConfig = RuntimeConfig.AzureConfig(defaultMachineType, Some(d3.id), None)
      c3 <- IO(
        makeCluster(3)
          .copy(workspaceId = Some(workspaceId2), cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext))
          .saveWithRuntimeConfig(
            c3RuntimeConfig
          )
      )
      c3ClusterRecord <- clusterQuery.getActiveClusterRecordByName(c3.cloudContext, c3.runtimeName).transaction

      d4 <- makePersistentDisk(Some(DiskName("d4"))).save()
      c4RuntimeConfig = RuntimeConfig.AzureConfig(defaultMachineType, Some(d4.id), None)
      c4 <- IO(
        makeCluster(4)
          .copy(workspaceId = Some(workspaceId1), cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext))
          .saveWithRuntimeConfig(
            c4RuntimeConfig
          )
      )
      c4ClusterRecord <- clusterQuery.getActiveClusterRecordByName(c4.cloudContext, c4.runtimeName).transaction

      d5 <- makePersistentDisk(Some(DiskName("d5"))).save()
      c5RuntimeConfig = RuntimeConfig.AzureConfig(defaultMachineType, Some(d5.id), None)
      c5 <- IO(
        makeCluster(5, Some(WorkbenchEmail("different@gmail.com")))
          .copy(workspaceId = Some(workspaceId2), cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext))
          .saveWithRuntimeConfig(
            c5RuntimeConfig
          )
      )
      c5ClusterRecord <- clusterQuery.getActiveClusterRecordByName(c5.cloudContext, c5.runtimeName).transaction

      list1 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(Map.empty,
                                  List(RuntimeStatus.Deleted),
                                  None,
                                  Some(workspaceId1),
                                  Some(CloudProvider.Azure)
        )
        .transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(Map.empty,
                                  List(RuntimeStatus.Deleted),
                                  None,
                                  Some(workspaceId2),
                                  Some(CloudProvider.Azure)
        )
        .transaction
      list3 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(Map.empty,
                                  List(RuntimeStatus.Deleted),
                                  None,
                                  Some(workspaceId1),
                                  Some(CloudProvider.Gcp)
        )
        .transaction
      list4 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(Map.empty, List(RuntimeStatus.Deleted), None, None, Some(CloudProvider.Azure))
        .transaction
      list5 <- RuntimeServiceDbQueries
        .listRuntimesForWorkspace(Map.empty,
                                  List(RuntimeStatus.Deleted),
                                  Some(c5ClusterRecord.get.auditInfo.creator),
                                  Some(workspaceId2),
                                  Some(CloudProvider.Azure)
        )
        .transaction
      end <- IO.realTimeInstant
      elapsed = (end.toEpochMilli - start.toEpochMilli).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      val c1Expected = toListRuntimeResponse(c1, Map.empty, c1RuntimeConfig)
      val c2Expected = toListRuntimeResponse(c2, Map.empty, c2RuntimeConfig, c2ClusterRecord.get.hostIp)
      val c3Expected = toListRuntimeResponse(c3, Map.empty, c3RuntimeConfig, c3ClusterRecord.get.hostIp)
      val c4Expected = toListRuntimeResponse(c4, Map.empty, c4RuntimeConfig, c4ClusterRecord.get.hostIp)
      val c5Expected = toListRuntimeResponse(c5, Map.empty, c5RuntimeConfig, c5ClusterRecord.get.hostIp)
      list1.toSet shouldEqual Set(c2Expected, c4Expected)
      list2 should contain theSameElementsAs List(c3Expected, c5Expected)
      list3 shouldEqual List(c1Expected)
      list4.toSet shouldEqual Set(c2Expected, c3Expected, c4Expected, c5Expected)
      list5 shouldEqual List(c5Expected)

      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "get a runtime" in isolatedDbTest {
    val res = for {
      disk <- makePersistentDisk(None).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(defaultMachineType,
                                                      Some(disk.id),
                                                      bootDiskSize = DiskSize(50),
                                                      zone = ZoneName("us-west2-b"),
                                                      CommonTestData.gpuConfig
      )
      c1 <- IO(makeCluster(1).saveWithRuntimeConfig(c1RuntimeConfig))
      get1 <- RuntimeServiceDbQueries.getRuntime(c1.cloudContext, c1.runtimeName).transaction
      get2 <- RuntimeServiceDbQueries.getRuntime(c1.cloudContext, RuntimeName("does-not-exist")).transaction.attempt
    } yield {
      get1 shouldBe GetRuntimeResponse.fromRuntime(c1, c1RuntimeConfig, Some(DiskConfig.fromPersistentDisk(disk)))
      get2.isLeft shouldBe true
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  private def toListRuntimeResponse(
    runtime: Runtime,
    labels: LabelMap,
    runtimeConfig: RuntimeConfig = CommonTestData.defaultDataprocRuntimeConfig,
    hostIp: Option[IP] = None
  ): ListRuntimeResponse2 =
    ListRuntimeResponse2(
      runtime.id,
      runtime.workspaceId,
      runtime.samResource,
      runtime.runtimeName,
      runtime.cloudContext,
      runtime.auditInfo,
      runtimeConfig,
      Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase,
                          runtime.cloudContext,
                          runtime.runtimeName,
                          Set.empty,
                          hostIp,
                          labels
      ),
      runtime.status,
      labels,
      false
    )

}
