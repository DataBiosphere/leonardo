package org.broadinstitute.dsde.workbench.leonardo.db

import cats.effect.unsafe.IORuntime
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.{AppStatus, CloudContext, KubernetesClusterStatus, NodepoolStatus, WorkspaceId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class KubernetesServiceDbQueriesSpec extends AnyFlatSpecLike with TestComponent {
  //listFullApps
  //listAppsByWorkspaceId
  //listMonitoredApps
  //saveOrGetClusterForApp
  //getActiveFullAppByWorkspaceIdAndAppName
  //getActiveFullAppByName
  //getFullAppByName
  //hasClusterOperationInProgress
  //markPendingCreating
  //markPreDeleting
  //markPendingAppDeletion

  "getActiveFullAppByName" should "get active app properly" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1, status = KubernetesClusterStatus.Deleted).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id, status = NodepoolStatus.Deleted).save()
    val savedApp1 = makeApp(2, savedNodepool1.id, status = AppStatus.Error).save()
    val res = KubernetesServiceDbQueries
      .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName, Map.empty)
      .transaction
      .unsafeRunSync()(IORuntime.global)

    res.isDefined shouldBe true
  }

  "getActiveFullAppByWorkspaceIdAndAppName" should "get active app properly" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1, status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id, status = NodepoolStatus.Running).save()
    val workspaceId = WorkspaceId(UUID.randomUUID())
    val savedApp1 = makeApp(1, savedNodepool1.id, status = AppStatus.Running, workspaceId = workspaceId).save()
    val res = KubernetesServiceDbQueries
      .getActiveFullAppByWorkspaceIdAndAppName(
        workspaceId,
        savedApp1.appName,
        Map.empty
      )
      .transaction
      .unsafeRunSync()(IORuntime.global)

    res.isDefined shouldBe true
  }

  "listFullApps" should "get list active apps properly " in isolatedDbTest {
    val cloudContext = CloudContext.Gcp(GoogleProject(project.value + 1))
    val savedCluster1 = makeKubeCluster(1, status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id, status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id, status = AppStatus.Running).save()
    val res = KubernetesServiceDbQueries
      .listFullApps(
        Some(cloudContext),
        Map.empty,
        includeDeleted = false
      )
      .transaction

    // .transaction
    // .unsafeRunSync()(IORuntime.global)

    // res.isDefined shouldBe true
  }

  "listAppsByWorkspaceId" should "get list active app properly " in isolatedDbTest {
    val workspaceId = WorkspaceId(UUID.randomUUID())
    val savedCluster1 = makeKubeCluster(1, status = KubernetesClusterStatus.Running, workspaceId = workspaceId).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id, status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id, status = AppStatus.Running).save()

    val savedCluster2 = makeKubeCluster(2, status = KubernetesClusterStatus.Running, workspaceId = workspaceId).save()
    val savedNodepool2 = makeNodepool(2, savedCluster2.id, status = NodepoolStatus.Running).save()
    val savedApp2 = makeApp(2, savedNodepool2.id, status = AppStatus.Running).save()
    val res = KubernetesServiceDbQueries
      .listAppsByWorkspaceId(
        // savedCluster1.workspaceId,
        Some(workspaceId),
        Map.empty
      )
      .transaction
      .unsafeRunSync()(IORuntime.global)

    res.length shouldBe 2

//    val res2 = KubernetesServiceDbQueries
//      .listAppsByWorkspaceId(
//        savedCluster2.workspaceId,
//        Map.empty
//      )
//      .transaction
//      .unsafeRunSync()(IORuntime.global)
//
//    res2.length shouldBe 1
  }
}
