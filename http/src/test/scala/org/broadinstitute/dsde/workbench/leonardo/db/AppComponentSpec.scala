package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.SQLIntegrityConstraintViolationException
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.AppSamResourceId
import org.broadinstitute.dsde.workbench.leonardo.{AppName, AppStatus, AppType, NodepoolLeoId}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils._
import org.broadinstitute.dsde.workbench.leonardo.monitor.AppToAutoDelete

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

import java.time.Instant

import java.time.temporal.ChronoUnit

import java.util.UUID

class AppComponentSpec extends AnyFlatSpecLike with TestComponent {

  it should "save basic app" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = app1.save()
    val app2 = makeApp(2, savedNodepool1.id)
    val savedApp2 = app2.save()

    val savedCluster2 = makeKubeCluster(2).save()
    val savedNodepool2 = makeNodepool(3, savedCluster2.id).save()
    val savedNodepool3 = makeNodepool(4, savedCluster2.id).save()
    val app3 = makeApp(3, savedNodepool2.id)
    val app4 = makeApp(4, savedNodepool3.id)
    val savedApp3 = app3.save()
    val savedApp4 = app4.save()

    savedApp1 shouldEqual app1
    savedApp2 shouldEqual app2
    savedApp3 shouldEqual app3
    savedApp4 shouldEqual app4

    val appType = dbFutureValue(appQuery.getAppType(app1.appName))
    appType shouldBe Some(AppType.Galaxy)
  }

  it should "save complex app" in isolatedDbTest {
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
    complexApp shouldEqual savedApp
  }

  it should "update status" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = app1.save()

    savedApp1.status shouldEqual app1.status
    dbFutureValue(appQuery.updateStatus(savedApp1.id, AppStatus.Running)) shouldEqual 1

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName)
    }
    getApp.get.app.status shouldEqual AppStatus.Running
  }

  it should "update dateAccessed properly" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app1 = makeApp(1, savedNodepool1.id)
    val savedApp1 = app1.save()

    val deletedApp = makeApp(1, savedNodepool1.id).copy(auditInfo = auditInfo.copy(destroyedDate = Some(Instant.now())),
                                                        status = AppStatus.Deleted
    )
    val savedDeletedApp = deletedApp.save()

    val dateAccessed = Instant.ofEpochMilli(2000)

    dbFutureValue(appQuery.updateDateAccessed(app1.appName, savedCluster1.cloudContext, dateAccessed)) shouldEqual 1

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getFullAppById(savedCluster1.cloudContext, savedApp1.id)
    }
    getApp.get.app.auditInfo.dateAccessed shouldBe dateAccessed

    val getDeletedApp = dbFutureValue {
      KubernetesServiceDbQueries.getFullAppById(savedCluster1.cloudContext, savedDeletedApp.id)
    }
    getDeletedApp.get.app.auditInfo.dateAccessed should not be dateAccessed
  }

  it should "fail to save an app without a nodepool" in isolatedDbTest {
    val appName = AppName("test")
    // this is important because we short-circuit the saveApp function with this instead of letting the DB throw it
    val caught = the[SQLIntegrityConstraintViolationException] thrownBy {
      makeApp(1, NodepoolLeoId(1)).copy(appName = appName).save()
    }

    caught.getMessage should include("FK_APP_NODEPOOL_ID")
  }

  it should "enforce uniqueness on (name, cloudContext) for v1 apps" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedNodepool2 = makeNodepool(2, savedCluster1.id).save()

    val appName = AppName("test")
    makeApp(1, savedNodepool1.id).copy(appName = appName, workspaceId = None).save()

    an[AppExistsException] shouldBe thrownBy {
      makeApp(2, savedNodepool2.id).copy(appName = appName, workspaceId = None).save()
    }
  }

  it should "enforce uniqueness on (name, workspace) for v2 apps" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedNodepool2 = makeNodepool(2, savedCluster1.id).save()

    val appName = AppName("test")
    val workspaceId = WorkspaceId(UUID.randomUUID)
    makeApp(1, savedNodepool1.id).copy(appName = appName, workspaceId = Some(workspaceId)).save()

    an[AppExistsException] shouldBe thrownBy {
      makeApp(2, savedNodepool2.id).copy(appName = appName, workspaceId = Some(workspaceId)).save()
    }
  }

  it should "get all apps for a given nodepool" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()
    val savedNodepool2 = makeNodepool(2, savedCluster1.id).save()

    val samResourceId1 = AppSamResourceId("r1", None)
    val samResourceId2 = AppSamResourceId("r2", None)
    val samResourceId3 = AppSamResourceId("r3", None)
    val samResourceId4 = AppSamResourceId("r4", None)
    val app1 = makeApp(1, savedNodepool1.id).copy(status = AppStatus.Error, samResourceId = samResourceId1).save()
    val app2 = makeApp(2, savedNodepool1.id).copy(status = AppStatus.Running, samResourceId = samResourceId2).save()
    makeApp(3, savedNodepool1.id).copy(status = AppStatus.Deleted, samResourceId = samResourceId3).save()
    makeApp(4, savedNodepool2.id).copy(status = AppStatus.Running, samResourceId = samResourceId4).save()

    val res = dbFutureValue(appQuery.getNonDeletedAppsByNodepool(savedNodepool1.id))
    res should contain theSameElementsAs (List(
      GetAppsByNodepoolResult(app1.samResourceId, app1.auditInfo.creator),
      GetAppsByNodepoolResult(app2.samResourceId, app2.auditInfo.creator)
    ))
  }

  it should "get all apps ready for auto delete" in isolatedDbTest {
    val savedCluster = makeKubeCluster(1).save()
    val savedNodepool = makeNodepool(1, savedCluster.id).save()

    val samResourceId1 = AppSamResourceId("r1", None)
    val now = Instant.now
    // App1, Ready to delete:  Last accessed 5 minutes ago, auto delete threshold is 1 minute.
    val app1 = makeApp(1, savedNodepool.id)
      .copy(
        auditInfo = auditInfo.copy(dateAccessed = now.minus(5, ChronoUnit.MINUTES)),
        status = AppStatus.Running,
        autodeleteThreshold = Some(AutodeleteThreshold(1)),
        samResourceId = samResourceId1,
        autodeleteEnabled = true
      )
      .save()
    // App2, Not ready to delete:  Last accessed 5 minutes ago, auto delete threshold is 10 minute.
    makeApp(2, savedNodepool.id)
      .copy(
        auditInfo = auditInfo.copy(dateAccessed = now.minus(5, ChronoUnit.MINUTES)),
        status = AppStatus.Running,
        autodeleteThreshold = Some(AutodeleteThreshold(10)),
        autodeleteEnabled = true
      )
      .save()

    // App3, Not ready to delete:  Status is not deletable
    makeApp(3, savedNodepool.id)
      .copy(
        auditInfo = auditInfo.copy(dateAccessed = now.minus(5, ChronoUnit.MINUTES)),
        status = AppStatus.Deleting,
        autodeleteThreshold = Some(AutodeleteThreshold(1)),
        autodeleteEnabled = true
      )
      .save()

    // App4, Not ready to delete:  AutodeleteEnabled is false
    makeApp(4, savedNodepool.id)
      .copy(
        auditInfo = auditInfo.copy(dateAccessed = now.minus(5, ChronoUnit.MINUTES)),
        status = AppStatus.Running,
        autodeleteThreshold = Some(AutodeleteThreshold(1)),
        samResourceId = samResourceId1,
        autodeleteEnabled = false
      )
      .save()

    // App5, Not ready to delete:  AutodeleteEnabled is null (default value)
    makeApp(5, savedNodepool.id)
      .copy(
        auditInfo = auditInfo.copy(dateAccessed = now.minus(5, ChronoUnit.MINUTES)),
        status = AppStatus.Running,
        autodeleteThreshold = Some(AutodeleteThreshold(1)),
        samResourceId = samResourceId1
      )
      .save()

    val res = dbFutureValue(appQuery.getAppsReadyToAutoDelete)
    res should contain theSameElementsAs (List(
      AppToAutoDelete(app1.id,
                      app1.appName,
                      app1.status,
                      app1.samResourceId,
                      app1.auditInfo.creator,
                      app1.chart.name,
                      savedCluster.cloudContext
      )
    ))
  }

}
