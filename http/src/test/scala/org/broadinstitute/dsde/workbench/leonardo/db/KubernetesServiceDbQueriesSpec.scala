package org.broadinstitute.dsde.workbench.leonardo
package db

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.makePersistentDisk
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.scalatest.flatspec.AnyFlatSpecLike

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class KubernetesServiceDbQueriesSpec extends AnyFlatSpecLike with TestComponent {
  "listFullApps" should "list apps" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val nodepool1 = makeNodepool(1, cluster1.id).save()
    val nodepool2 = makeNodepool(2, cluster1.id).save()

    val cluster2 = makeKubeCluster(2).save()
    val nodepool3 = makeNodepool(3, cluster2.id).save()

    makeApp(1, nodepool1.id).save()
    makeApp(2, nodepool1.id).save()
    makeApp(3, nodepool2.id).save()
    makeApp(4, nodepool3.id).save()

    val listWithNoProject = dbFutureValue(KubernetesServiceDbQueries.listFullApps(None))
    listWithNoProject.length shouldEqual 2
    listWithNoProject.flatMap(_.nodepools).length shouldEqual 3
    listWithNoProject.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 4

    val listWithProject1 = dbFutureValue(KubernetesServiceDbQueries.listFullApps(Some(cluster1.cloudContext)))
    listWithProject1.length shouldEqual 1
    listWithProject1.flatMap(_.nodepools).length shouldEqual 2
    listWithProject1.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 3

    val listWithProject2 = dbFutureValue(KubernetesServiceDbQueries.listFullApps(Some(cluster2.cloudContext)))
    listWithProject2.length shouldEqual 1
    listWithProject2.flatMap(_.nodepools).length shouldEqual 1
    listWithProject2.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 1
  }

  it should "list apps with labels" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val nodepool1 = makeNodepool(1, cluster1.id).save()

    val pair1 = "foo" -> "bar"
    val pair2 = "fizz" -> "buzz"
    val labels = Map(pair1, pair2)
    val app1 = makeApp(1, nodepool1.id).copy(labels = labels).save()
    val app2 = makeApp(2, nodepool1.id).save()

    val listWithLabels1 = dbFutureValue(KubernetesServiceDbQueries.listFullApps(None, Map(pair1)))
    listWithLabels1.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 1
    listWithLabels1.flatMap(_.nodepools).flatMap(_.apps).head shouldEqual app1
  }

  it should "list deleted apps when includeDeleted is true" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val savedNodepool = makeNodepool(1, cluster1.id).save()
    val app1 = makeApp(1, savedNodepool.id).save()

    val destroyedDate = Instant.now().truncatedTo(ChronoUnit.MICROS)
    // delete app
    dbFutureValue(appQuery.markAsDeleted(app1.id, destroyedDate)) shouldBe 1

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cluster1.cloudContext, app1.appName)
    }
    getApp shouldBe None

    val listAppsWithDeleted = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(cluster1.cloudContext), includeDeleted = true)
    }
    listAppsWithDeleted.length shouldEqual 1
    listAppsWithDeleted.head.nodepools.length shouldEqual 1
    val nodepool = listAppsWithDeleted.head.nodepools.head
    nodepool.apps.length shouldEqual 1
    nodepool.apps.head shouldEqual app1.copy(status = AppStatus.Deleted,
                                             auditInfo = app1.auditInfo.copy(destroyedDate = Some(destroyedDate))
    )

    // delete nodepool for deleted app
    dbFutureValue(nodepoolQuery.markAsDeleted(savedNodepool.id, destroyedDate)) shouldBe 1
    val listAppsWithDeleted2 = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(cluster1.cloudContext), includeDeleted = true)
    }
    listAppsWithDeleted2.length shouldEqual 1
    listAppsWithDeleted2.head.nodepools.length shouldEqual 1
    val deletedNodepool = listAppsWithDeleted2.head.nodepools.head
    deletedNodepool.copy(apps = List()) shouldEqual savedNodepool.copy(
      status = NodepoolStatus.Deleted,
      auditInfo = savedNodepool.auditInfo.copy(destroyedDate = Some(destroyedDate)),
      apps = List()
    )
    deletedNodepool.apps.length shouldEqual 1
    nodepool.apps.head shouldEqual app1.copy(status = AppStatus.Deleted,
                                             auditInfo = app1.auditInfo.copy(destroyedDate = Some(destroyedDate))
    )
  }

  it should "not list deleted apps when includeDeleted is false" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val nodepool1 = makeNodepool(1, cluster1.id).save()
    val app1 = makeApp(1, nodepool1.id).save()
    val app2 = makeApp(2, nodepool1.id).save()

    val destroyedDate = Instant.now()
    dbFutureValue(appQuery.markAsDeleted(app1.id, destroyedDate)) shouldBe 1

    val listAppsWithoutDeleted = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(cluster1.cloudContext), includeDeleted = false)
    }
    listAppsWithoutDeleted.length shouldEqual 1
    listAppsWithoutDeleted.head.nodepools.length shouldEqual 1
    val nodepool = listAppsWithoutDeleted.head.nodepools.head
    nodepool.apps.length shouldEqual 1
    nodepool.apps.head shouldEqual app2
  }

  it should "list Error'd apps if the underlying cluster is deleted" in isolatedDbTest {
    val cluster = LeoLenses.kubernetesClusterToDestroyedDate
      .modify(_ => Some(Instant.now.truncatedTo(ChronoUnit.MICROS)))(
        makeKubeCluster(1).copy(status = KubernetesClusterStatus.Deleted)
      )
      .save()
    val nodepool = LeoLenses.nodepoolToDestroyedDate
      .modify(_ => Some(Instant.now.truncatedTo(ChronoUnit.MICROS)))(
        makeNodepool(1, cluster.id).copy(status = NodepoolStatus.Deleted)
      )
      .save()

    val app = makeApp(1, nodepool.id).copy(status = AppStatus.Error).save()

    val listWithNoProject = dbFutureValue(KubernetesServiceDbQueries.listFullApps(None))
    listWithNoProject.length shouldEqual 1
    listWithNoProject.flatMap(_.nodepools).length shouldEqual 1
    listWithNoProject.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 1
    listWithNoProject.flatMap(_.nodepools).flatMap(_.apps).head shouldEqual app

    val listWithProject = dbFutureValue(KubernetesServiceDbQueries.listFullApps(Some(cluster.cloudContext)))
    listWithProject.length shouldEqual 1
    listWithProject.flatMap(_.nodepools).length shouldEqual 1
    listWithProject.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 1
    listWithProject.flatMap(_.nodepools).flatMap(_.apps).head shouldEqual app

    val getActiveApp =
      dbFutureValue(KubernetesServiceDbQueries.getActiveFullAppByName(cluster.cloudContext, app.appName))
    getActiveApp.isDefined shouldBe true
    getActiveApp.get.cluster.cloudContext shouldEqual cluster.cloudContext
    getActiveApp.get.cluster.clusterName shouldEqual cluster.clusterName
    getActiveApp.get.nodepool.copy(apps = List()) shouldEqual nodepool
    getActiveApp.get.app shouldEqual app

    val getFullApp = dbFutureValue(KubernetesServiceDbQueries.getFullAppById(cluster.cloudContext, app.id))
    getFullApp.isDefined shouldBe true
    getFullApp.get.cluster.cloudContext shouldEqual cluster.cloudContext
    getFullApp.get.cluster.clusterName shouldEqual cluster.clusterName
    getFullApp.get.nodepool.copy(apps = List()) shouldEqual nodepool
    getFullApp.get.app shouldEqual app
  }

  it should "filter clusters and nodepools that have no apps" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    makeNodepool(1, cluster1.id).save()

    val listApps1 = dbFutureValue(KubernetesServiceDbQueries.listFullApps(Some(cluster1.cloudContext)))
    listApps1.length shouldEqual 0
  }

  "listAppsByWorkspaceId" should "list apps based on a workspaceId" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val nodepool1 = makeNodepool(1, cluster1.id).save()

    val workspace1 = WorkspaceId(UUID.randomUUID())
    val workspace2 = WorkspaceId(UUID.randomUUID())
    val workspace3 = WorkspaceId(UUID.randomUUID())
    makeApp(1, nodepool1.id, workspaceId = workspace1).save()
    makeApp(2, nodepool1.id, workspaceId = workspace1).save()
    makeApp(3, nodepool1.id, workspaceId = workspace2).save()
    makeApp(4, nodepool1.id, workspaceId = workspace2).save()

    val listWithNoProject = dbFutureValue(KubernetesServiceDbQueries.listFullAppsByWorkspaceId(None))
    listWithNoProject.length shouldEqual 1
    listWithNoProject.flatMap(_.nodepools).length shouldEqual 1
    listWithNoProject.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 4

    val listWithWorkspace1 = dbFutureValue(KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspace1)))
    listWithWorkspace1.length shouldEqual 1
    listWithWorkspace1.flatMap(_.nodepools).length shouldEqual 1
    listWithWorkspace1.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 2

    val listWithWorkspace2 = dbFutureValue(KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspace2)))
    listWithWorkspace2.length shouldEqual 1
    listWithWorkspace2.flatMap(_.nodepools).length shouldEqual 1
    listWithWorkspace2.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 2

    val listWithWorkspace3 = dbFutureValue(KubernetesServiceDbQueries.listFullAppsByWorkspaceId(Some(workspace3)))
    listWithWorkspace3.length shouldEqual 0
    listWithWorkspace3.flatMap(_.nodepools).length shouldEqual 0
    listWithWorkspace3.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 0
  }

  "saveOrGetClusterForApp" should "get cluster if exists for project" in isolatedDbTest {
    val makeCluster1 = makeKubeCluster(1)
    val makeCluster2 = makeCluster1.copy(clusterName = kubeName0)
    makeCluster1.save()
    val saveCluster2 = SaveKubernetesCluster(
      makeCluster2.cloudContext,
      makeCluster2.clusterName,
      makeCluster2.location,
      makeCluster2.region,
      makeCluster2.status,
      makeCluster2.ingressChart,
      makeCluster2.auditInfo,
      DefaultNodepool.fromNodepool(makeCluster2.nodepools.headOption.get)
    )
    val saveClusterResult = dbFutureValue(KubernetesServiceDbQueries.saveOrGetClusterForApp(saveCluster2))
    saveClusterResult shouldBe a[ClusterExists]
    saveClusterResult.minimalCluster shouldEqual makeCluster1
  }

  it should "save cluster when one doesn't exist for project" in isolatedDbTest {
    val makeCluster1 = makeKubeCluster(1)
    val saveCluster1 = SaveKubernetesCluster(
      makeCluster1.cloudContext,
      makeCluster1.clusterName,
      makeCluster1.location,
      makeCluster1.region,
      makeCluster1.status,
      makeCluster1.ingressChart,
      makeCluster1.auditInfo,
      DefaultNodepool.fromNodepool(makeCluster1.nodepools.headOption.get)
    )
    val saveResult = dbFutureValue(KubernetesServiceDbQueries.saveOrGetClusterForApp(saveCluster1))
    saveResult shouldBe a[ClusterDoesNotExist]
    saveResult.minimalCluster shouldEqual makeCluster1
  }

  it should "allow creation of a cluster if a deleted one exists in the same project" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Deleted).save
    val makeCluster2 = makeKubeCluster(2)
    val saveCluster2 = SaveKubernetesCluster(
      makeCluster2.cloudContext,
      makeCluster2.clusterName,
      makeCluster2.location,
      makeCluster2.region,
      makeCluster2.status,
      makeCluster2.ingressChart,
      makeCluster2.auditInfo,
      DefaultNodepool.fromNodepool(makeCluster2.nodepools.headOption.get)
    )
    val saveResult = dbFutureValue(KubernetesServiceDbQueries.saveOrGetClusterForApp(saveCluster2))
    saveResult shouldBe a[ClusterDoesNotExist]
    saveResult.minimalCluster shouldEqual makeCluster2
  }

  it should "error if cluster exists in creating status" in isolatedDbTest {
    val makeCluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Provisioning).save()
    val makeCluster2 = makeKubeCluster(2).copy(status = KubernetesClusterStatus.Precreating).save()
    val saveCluster1 =
      SaveKubernetesCluster(
        makeCluster1.cloudContext,
        makeCluster1.clusterName,
        makeCluster1.location,
        makeCluster1.region,
        makeCluster1.status,
        makeCluster1.ingressChart,
        makeCluster1.auditInfo,
        DefaultNodepool.fromNodepool(makeCluster1.nodepools.headOption.get)
      )
    val saveCluster2 =
      SaveKubernetesCluster(
        makeCluster2.cloudContext,
        makeCluster2.clusterName,
        makeCluster2.location,
        makeCluster2.region,
        makeCluster2.status,
        makeCluster2.ingressChart,
        makeCluster2.auditInfo,
        DefaultNodepool.fromNodepool(makeCluster2.nodepools.headOption.get)
      )
    val saveResult1IO = KubernetesServiceDbQueries.saveOrGetClusterForApp(saveCluster1).transaction
    val saveResult2IO = KubernetesServiceDbQueries.saveOrGetClusterForApp(saveCluster2).transaction
    the[KubernetesAppCreationException] thrownBy {
      saveResult1IO.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
    the[KubernetesAppCreationException] thrownBy {
      saveResult2IO.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  "getActiveFullAppByName" should "get an active app" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val nodepool1 = makeNodepool(1, cluster1.id).save()
    val app1 = makeApp(1, nodepool1.id).save()

    val getApp1 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cluster1.cloudContext, app1.appName)
    }
    getApp1 shouldBe defined
    getApp1.get.cluster.cloudContext shouldEqual cluster1.cloudContext
    getApp1.get.cluster.clusterName shouldEqual cluster1.clusterName
    getApp1.get.nodepool.copy(apps = List()) shouldEqual nodepool1
    getApp1.get.app shouldEqual app1
  }

  it should "return None when there is no active app" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val nodepool1 = makeNodepool(1, cluster1.id).save()
    makeApp(1, nodepool1.id).save()

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cluster1.cloudContext, AppName("fakeApp"))
    }
    getApp shouldBe None
  }

  it should "get app errors" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val nodepool1 = makeNodepool(1, cluster1.id).save()
    val app1 = makeApp(1, nodepool1.id).save()
    val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
    val error1 = AppError("error1", now, ErrorAction.CreateApp, ErrorSource.App, Some(1))
    val error2 = AppError("error2", now, ErrorAction.DeleteApp, ErrorSource.Nodepool, Some(2))
    appErrorQuery.save(app1.id, error1).transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    appErrorQuery.save(app1.id, error2).transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(cluster1.cloudContext, app1.appName)
    }

    getApp shouldBe defined
    getApp.get.app.errors should contain(error1)
    getApp.get.app.errors should contain(error2)
  }

  it should "get full app with disk and services" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val disk = makePersistentDisk(None).save().unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val basicApp = makeApp(1, savedNodepool1.id)
    val complexApp = basicApp.copy(appResources =
      basicApp.appResources.copy(
        disk = Some(disk),
        services = List(makeService(1), makeService(2))
      )
    )
    val savedApp = complexApp.save()
    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(savedCluster1.cloudContext, savedApp.appName)
    }
    getApp.get.app shouldEqual savedApp
    getApp.get.app.appResources.services.size shouldBe 2
  }

  "getActiveFullAppByWorkspaceIdAndAppName" should "get an active app by workspace id" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val nodepool1 = makeNodepool(1, cluster1.id).save()
    val workspaceId = WorkspaceId(UUID.randomUUID())
    val app1 = makeApp(1, nodepool1.id, workspaceId = workspaceId).save()

    val getApp1 = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, app1.appName)
    }
    getApp1 shouldBe defined
    getApp1.get.cluster.cloudContext shouldEqual cluster1.cloudContext
    getApp1.get.cluster.clusterName shouldEqual cluster1.clusterName
    getApp1.get.nodepool.copy(apps = List()) shouldEqual nodepool1
    getApp1.get.app shouldEqual app1
  }

  it should "return None when there is no active app" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val nodepool1 = makeNodepool(1, cluster1.id).save()
    val workspaceId = WorkspaceId(UUID.randomUUID())
    makeApp(1, nodepool1.id, workspaceId = workspaceId).save()

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(WorkspaceId(UUID.randomUUID()),
                                                                         AppName("fakeApp")
      )
    }
    getApp shouldBe None
  }

  it should "get app errors" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val nodepool1 = makeNodepool(1, cluster1.id).save()
    val workspaceId = WorkspaceId(UUID.randomUUID())
    val app1 = makeApp(1, nodepool1.id, workspaceId = workspaceId).save()
    val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
    val error1 = AppError("error1", now, ErrorAction.CreateApp, ErrorSource.App, Some(1))
    val error2 = AppError("error2", now, ErrorAction.DeleteApp, ErrorSource.Nodepool, Some(2))
    appErrorQuery.save(app1.id, error1).transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    appErrorQuery.save(app1.id, error2).transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByWorkspaceIdAndAppName(workspaceId, app1.appName)
    }

    getApp shouldBe defined
    getApp.get.app.errors should contain(error1)
    getApp.get.app.errors should contain(error2)
  }

  "hasClusterOperationInProgress" should "correctly identify if theres an operation in progress" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).copy(status = KubernetesClusterStatus.Running).save()
    makeNodepool(1, cluster1.id).copy(status = NodepoolStatus.Provisioning).save()
    val nodepool2 = makeNodepool(2, cluster1.id).copy(status = NodepoolStatus.Running).save()
    makeApp(1, nodepool2.id).copy(status = AppStatus.Running).save()

    KubernetesServiceDbQueries
      .hasClusterOperationInProgress(cluster1.id)
      .transaction
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe true
  }
}
