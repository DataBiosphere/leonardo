package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLIntegrityConstraintViolationException
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.{AppName, AppStatus, AppType, NodepoolLeoId}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils._

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

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

  it should "fail to save an app without a nodepool" in isolatedDbTest {
    val appName = AppName("test")
    // this is important because we short-circuit the saveApp function with this instead of letting the DB throw it
    val caught = the[SQLIntegrityConstraintViolationException] thrownBy {
      makeApp(1, NodepoolLeoId(1)).copy(appName = appName).save()
    }

    caught.getMessage should include("FK_APP_NODEPOOL_ID")
  }

}
