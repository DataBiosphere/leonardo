package org.broadinstitute.dsde.workbench.leonardo
package db

import cats.effect.IO
import org.broadinstitute.dsde.workbench.google2.{DiskName, ZoneName}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{makeCluster, _}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{
  ProjectSamResourceId,
  RuntimeSamResourceId,
  WorkspaceResourceSamResourceId
}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeServiceDbQueries._
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
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

  it should "list runtimes" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant

      // No runtimes exist
      list1 <- RuntimeServiceDbQueries.listRuntimes(excludeStatuses = List(RuntimeStatus.Deleted)).transaction

      // One runtime exists: c1
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      d1RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
        Some(d1.id),
        bootDiskSize = DiskSize(50),
        zone = ZoneName("us-west2-b"),
        CommonTestData.gpuConfig
      )
      c1 <- IO(
        makeCluster(1).saveWithRuntimeConfig(d1RuntimeConfig)
      )
      c1WorkspaceIds = c1.workspaceId match {
        case Some(workspaceId) => Set(WorkspaceResourceSamResourceId(workspaceId))
        case None              => Set.empty[WorkspaceResourceSamResourceId]
      }
      list2 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = Set(c1.samResource),
          readerWorkspaceIds = c1WorkspaceIds,
          excludeStatuses = List(RuntimeStatus.Deleted)
        )
        .transaction
      // Two runtimes exist: c1, c2
      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      d2RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
        Some(d2.id),
        bootDiskSize = DiskSize(50),
        zone = ZoneName("us-west2-b"),
        CommonTestData.gpuConfig
      )
      c2 <- IO(
        makeCluster(2).saveWithRuntimeConfig(d2RuntimeConfig)
      )
      bothWorkspaceIds = Set(c1.workspaceId, c2.workspaceId).collect { case Some(workspaceId) =>
        WorkspaceResourceSamResourceId(workspaceId)
      }
      list3 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = Set(c1.samResource, c2.samResource),
          readerWorkspaceIds = bothWorkspaceIds,
          excludeStatuses = List(RuntimeStatus.Deleted)
        )
        .transaction

      // no authorizations => no runtimes
      list4 <- RuntimeServiceDbQueries
        .listRuntimes()
        .transaction
      end <- IO.realTimeInstant
      elapsed = (end.toEpochMilli - start.toEpochMilli).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      list1 shouldEqual Vector.empty
      val c1Expected = toListRuntimeResponse(c1, Map.empty, d1RuntimeConfig)
      val c2Expected = toListRuntimeResponse(c2, Map.empty, d2RuntimeConfig)
      list2 shouldEqual Vector(c1Expected)
      list3.toSet shouldEqual Set(c1Expected, c2Expected)
      list4 shouldEqual Vector.empty
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes with many filters" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant

      workspaceId1 = WorkspaceId(UUID.randomUUID())
      workspaceId2 = WorkspaceId(UUID.randomUUID())
      runtimeId1 = UUID.randomUUID.toString
      runtimeId2 = UUID.randomUUID.toString
      runtimeId3 = UUID.randomUUID.toString

      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
        Some(d1.id),
        bootDiskSize = DiskSize(50),
        zone = ZoneName("us-west2-b"),
        None
      )
      c1 <- IO(
        makeCluster(1, samResource = RuntimeSamResourceId(runtimeId1))
          .copy(workspaceId = Some(workspaceId1))
          .saveWithRuntimeConfig(c1RuntimeConfig)
      )
      labels1 = Map(
        "googleProject" -> c1.cloudContext.asString,
        "clusterName" -> c1.runtimeName.asString,
        "creator" -> c1.auditInfo.creator.value
      )
      _ <- labelQuery.saveAllForResource(c1.id, LabelResourceType.Runtime, labels1).transaction

      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      c2RuntimeConfig = RuntimeConfig.AzureConfig(defaultMachineType, Some(d2.id), None)
      c2 <- IO(
        makeCluster(2,
                    samResource = RuntimeSamResourceId(runtimeId2),
                    cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext)
        )
          .copy(workspaceId = Some(workspaceId2))
          .saveWithRuntimeConfig(
            c2RuntimeConfig
          )
      )
      labels2 = Map(
        "clusterName" -> c2.runtimeName.asString,
        "creator" -> c2.auditInfo.creator.value
      )
      _ <- labelQuery.saveAllForResource(c2.id, LabelResourceType.Runtime, labels2).transaction

      d3 <- makePersistentDisk(None).save()
      c3RuntimeConfig = RuntimeConfig.AzureConfig(defaultMachineType, Some(d3.id), None)
      c3 <- IO(
        makeCluster(3,
                    samResource = RuntimeSamResourceId(runtimeId3),
                    cloudContext = CloudContext.Azure(CommonTestData.azureCloudContext)
        )
          .copy(workspaceId = Some(workspaceId2))
          .saveWithRuntimeConfig(
            c3RuntimeConfig
          )
      )

      // Note that c3 exists but is not visible
      googleProject = GoogleProject(c1.cloudContext.asString)
      projectIds = Set(ProjectSamResourceId(googleProject))
      runtimeIds = Set(c1.samResource: SamResourceId, c2.samResource: SamResourceId)
      workspaceIds = Set(workspaceId1, workspaceId2).map(WorkspaceResourceSamResourceId)

      list0 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerWorkspaceIds = workspaceIds,
          readerGoogleProjectIds = projectIds
        )
        .transaction
      list1 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerWorkspaceIds = workspaceIds,
          readerGoogleProjectIds = projectIds,
          cloudContext = Some(CloudContext.Gcp(googleProject)),
          cloudProvider = Some(CloudProvider.Gcp),
          creatorEmail = Some(c1.auditInfo.creator),
          excludeStatuses = List(RuntimeStatus.Deleted),
          labelMap = labels1,
          workspaceId = Some(workspaceId1)
        )
        .transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerWorkspaceIds = workspaceIds,
          readerGoogleProjectIds = projectIds,
          cloudProvider = Some(CloudProvider.Azure),
          creatorEmail = Some(c2.auditInfo.creator),
          excludeStatuses = List(RuntimeStatus.Deleted),
          labelMap = labels2,
          workspaceId = Some(workspaceId2)
        )
        .transaction
      end <- IO.realTimeInstant
      elapsed = (end.toEpochMilli - start.toEpochMilli).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      val c1Expected = toListRuntimeResponse(c1, labels1, c1RuntimeConfig, defaultHostIp)
      val c2Expected = toListRuntimeResponse(c2, labels2, c2RuntimeConfig, defaultHostIp)
      list0 should contain theSameElementsAs Vector(c1Expected, c2Expected)
      list1 shouldEqual Vector(c1Expected)
      list2 shouldEqual Vector(c2Expected)
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes by labels" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
        Some(d1.id),
        bootDiskSize = DiskSize(50),
        zone = ZoneName("us-west2-b"),
        CommonTestData.gpuConfig
      )
      c1 <- IO(makeCluster(1).saveWithRuntimeConfig(c1RuntimeConfig))
      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
        Some(d2.id),
        bootDiskSize = DiskSize(50),
        zone = ZoneName("us-west2-b"),
        None
      )
      c2 <- IO(makeCluster(2).saveWithRuntimeConfig(c2RuntimeConfig))
      labels1 = Map(
        "googleProject" -> c1.cloudContext.asString,
        "clusterName" -> c1.runtimeName.asString,
        "creator" -> c1.auditInfo.creator.value
      )
      labels2 = Map(
        "googleProject" -> c2.cloudContext.asString,
        "clusterName" -> c2.runtimeName.asString,
        "creator" -> c2.auditInfo.creator.value
      )
      runtimeIds = Set(c1.samResource: SamResourceId, c2.samResource: SamResourceId)
      bothProjectIds = Set(
        ProjectSamResourceId(GoogleProject(c1.cloudContext.asString)),
        ProjectSamResourceId(GoogleProject(c2.cloudContext.asString))
      )
      list0 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerGoogleProjectIds = bothProjectIds,
          excludeStatuses = List(RuntimeStatus.Deleted)
        )
        .transaction
      list1 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerGoogleProjectIds = bothProjectIds,
          labelMap = labels1,
          excludeStatuses = List(RuntimeStatus.Deleted)
        )
        .transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerGoogleProjectIds = bothProjectIds,
          labelMap = labels2,
          excludeStatuses = List(RuntimeStatus.Deleted)
        )
        .transaction
      _ <- labelQuery.saveAllForResource(c1.id, LabelResourceType.Runtime, labels1).transaction
      _ <- labelQuery.saveAllForResource(c2.id, LabelResourceType.Runtime, labels2).transaction
      list3 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerGoogleProjectIds = bothProjectIds,
          labelMap = labels1,
          excludeStatuses = List(RuntimeStatus.Deleted)
        )
        .transaction
      list4 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerGoogleProjectIds = bothProjectIds,
          labelMap = labels2,
          excludeStatuses = List(RuntimeStatus.Deleted)
        )
        .transaction
      list5 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerGoogleProjectIds = bothProjectIds,
          labelMap = Map("googleProject" -> c1.cloudContext.asString),
          excludeStatuses = List(RuntimeStatus.Deleted)
        )
        .transaction
      end <- IO.realTimeInstant
      elapsed = (end.toEpochMilli - start.toEpochMilli).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      val c1Expected = toListRuntimeResponse(c1, labels1, c1RuntimeConfig)
      val c2Expected = toListRuntimeResponse(c2, labels2, c2RuntimeConfig)
      list0.map(_.id) should contain theSameElementsAs Set(c1Expected.id, c2Expected.id)
      list1 shouldEqual List.empty
      list2 shouldEqual List.empty
      list3 shouldEqual Vector(c1Expected)
      list4 shouldEqual Vector(c2Expected)
      list5 should contain theSameElementsAs Set(c1Expected, c2Expected)
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "list runtimes by project" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
        Some(d1.id),
        bootDiskSize = DiskSize(50),
        zone = ZoneName("us-west2-b"),
        CommonTestData.gpuConfig
      )
      c1 <- IO(
        makeCluster(1).saveWithRuntimeConfig(c1RuntimeConfig)
      )
      d2 <- makePersistentDisk(Some(DiskName("d2"))).save()
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
        Some(d2.id),
        bootDiskSize = DiskSize(50),
        zone = ZoneName("us-west2-b"),
        None
      )
      c2 <- IO(
        makeCluster(2).saveWithRuntimeConfig(c2RuntimeConfig)
      )
      runtimeIds = Set(c1.samResource: SamResourceId, c2.samResource: SamResourceId)
      bothProjectIds = Set(
        ProjectSamResourceId(GoogleProject(c1.cloudContext.asString)),
        ProjectSamResourceId(GoogleProject(c2.cloudContext.asString))
      )
      list1 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerGoogleProjectIds = bothProjectIds,
          excludeStatuses = List(RuntimeStatus.Deleted),
          cloudContext = Some(cloudContextGcp)
        )
        .transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerGoogleProjectIds = bothProjectIds,
          excludeStatuses = List(RuntimeStatus.Deleted),
          cloudContext = Some(cloudContext2Gcp)
        )
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

  it should "list runtimes including deleted" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
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
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
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
      c3RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
        Some(d3.id),
        bootDiskSize = DiskSize(50),
        zone = ZoneName("us-west2-b"),
        CommonTestData.gpuConfig
      )
      c3 <- IO(
        makeCluster(3).saveWithRuntimeConfig(
          RuntimeConfig.GceWithPdConfig(
            defaultMachineType,
            Some(d3.id),
            bootDiskSize = DiskSize(50),
            zone = ZoneName("us-west2-b"),
            CommonTestData.gpuConfig
          )
        )
      )
      runtimeIds = Set(c1.samResource: SamResourceId, c2.samResource: SamResourceId, c3.samResource: SamResourceId)
      projectIds = Set(
        ProjectSamResourceId(GoogleProject(c1.cloudContext.asString)),
        ProjectSamResourceId(GoogleProject(c2.cloudContext.asString)),
        ProjectSamResourceId(GoogleProject(c3.cloudContext.asString))
      )

      list1 <- RuntimeServiceDbQueries
        .listRuntimes(readerRuntimeIds = runtimeIds, readerGoogleProjectIds = projectIds)
        .transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerGoogleProjectIds = projectIds,
          excludeStatuses = List(RuntimeStatus.Deleted)
        )
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

  it should "list runtimesV2 including deleted" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant
      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
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
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
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
      runtimeIds = Set(c1.samResource: SamResourceId, c2.samResource: SamResourceId, c3.samResource: SamResourceId)
      projectIds = Set(
        ProjectSamResourceId(GoogleProject(c1.cloudContext.asString)),
        ProjectSamResourceId(GoogleProject(c2.cloudContext.asString))
      )
      workspaceIds = Set(c3.workspaceId).collect { case Some(workspaceId) =>
        WorkspaceResourceSamResourceId(workspaceId)
      }
      list1 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerGoogleProjectIds = projectIds,
          readerWorkspaceIds = workspaceIds
        )
        .transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerGoogleProjectIds = projectIds,
          readerWorkspaceIds = workspaceIds,
          excludeStatuses = List(RuntimeStatus.Deleted)
        )
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

  it should "list runtimesV2 by workspace" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant

      workspaceId1 = WorkspaceId(UUID.randomUUID())
      workspaceId2 = WorkspaceId(UUID.randomUUID())

      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
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
      c2RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
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
      runtimeIds = Set(c1.samResource: SamResourceId, c2.samResource: SamResourceId, c3.samResource: SamResourceId)
      workspaceIds = Set(workspaceId1, workspaceId2).map(WorkspaceResourceSamResourceId)

      list1 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerWorkspaceIds = workspaceIds,
          workspaceId = Some(workspaceId1)
        )
        .transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerWorkspaceIds = workspaceIds,
          excludeStatuses = List(RuntimeStatus.Deleted),
          workspaceId = Some(workspaceId2)
        )
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

  it should "list runtimesV2 by cloudProvider and workspace" in isolatedDbTest {
    val res = for {
      start <- IO.realTimeInstant

      workspaceId1 = WorkspaceId(UUID.randomUUID())
      workspaceId2 = WorkspaceId(UUID.randomUUID())

      d1 <- makePersistentDisk(Some(DiskName("d1"))).save()
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
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
      runtimeIds = Set(c1, c2, c3, c4, c5).map(_.samResource: SamResourceId)
      workspaceIds = Set(workspaceId1, workspaceId2).map(WorkspaceResourceSamResourceId)

      list1 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerWorkspaceIds = workspaceIds,
          excludeStatuses = List(RuntimeStatus.Deleted),
          workspaceId = Some(workspaceId1),
          cloudProvider = Some(CloudProvider.Azure)
        )
        .transaction
      list2 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerWorkspaceIds = workspaceIds,
          excludeStatuses = List(RuntimeStatus.Deleted),
          workspaceId = Some(workspaceId2),
          cloudProvider = Some(CloudProvider.Azure)
        )
        .transaction
      list3 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerWorkspaceIds = workspaceIds,
          excludeStatuses = List(RuntimeStatus.Deleted),
          workspaceId = Some(workspaceId1),
          cloudProvider = Some(CloudProvider.Gcp)
        )
        .transaction
      list4 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerWorkspaceIds = workspaceIds,
          excludeStatuses = List(RuntimeStatus.Deleted),
          cloudProvider = Some(CloudProvider.Azure)
        )
        .transaction
      list5 <- RuntimeServiceDbQueries
        .listRuntimes(
          readerRuntimeIds = runtimeIds,
          readerWorkspaceIds = workspaceIds,
          excludeStatuses = List(RuntimeStatus.Deleted),
          creatorEmail = Some(c5ClusterRecord.get.auditInfo.creator),
          workspaceId = Some(workspaceId2),
          cloudProvider = Some(CloudProvider.Azure)
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
      c1RuntimeConfig = RuntimeConfig.GceWithPdConfig(
        defaultMachineType,
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
      Runtime.getProxyUrl(
        Config.proxyConfig.proxyUrlBase,
        runtime.cloudContext,
        runtime.runtimeName,
        Set.empty,
        hostIp,
        labels
      ),
      runtime.status,
      labels,
      patchInProgress = false
    )

}
