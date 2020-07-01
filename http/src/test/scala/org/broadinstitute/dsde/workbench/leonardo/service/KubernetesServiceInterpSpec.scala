package org.broadinstitute.dsde.workbench.leonardo.service

import cats.effect.IO
import fs2.concurrent.InspectableQueue
import org.broadinstitute.dsde.workbench.google2.DiskName
import org.broadinstitute.dsde.workbench.leonardo.http.service.{
  AppAlreadyExistsException,
  AppCannotBeDeletedException,
  AppNotFoundException,
  AppRequiresDiskException,
  DeleteAppParams,
  DiskAlreadyAttachedException,
  LeoKubernetesServiceInterp
}
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.{
  AppName,
  AppStatus,
  AppType,
  CreateCluster,
  DiskId,
  KubernetesClusterStatus,
  LabelMap,
  LeonardoTestSuite,
  NodepoolStatus
}
import org.broadinstitute.dsde.workbench.leonardo.db.{
  appQuery,
  kubernetesClusterQuery,
  persistentDiskQuery,
  KubernetesServiceDbQueries,
  TestComponent
}
import org.broadinstitute.dsde.workbench.leonardo.http.PersistentDiskRequest
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.{CreateAppMessage, DeleteAppMessage}
import org.broadinstitute.dsde.workbench.leonardo.monitor.{LeoPubsubMessage, LeoPubsubMessageType}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpec

class KubernetesServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {

  //used when we care about queue state
  def makeInterp(queue: InspectableQueue[IO, LeoPubsubMessage]) =
    new LeoKubernetesServiceInterp[IO](whitelistAuthProvider, serviceAccountProvider, leoKubernetesConfig, queue)
  val kubeServiceInterp = new LeoKubernetesServiceInterp[IO](whitelistAuthProvider,
                                                             serviceAccountProvider,
                                                             leoKubernetesConfig,
                                                             QueueFactory.makePublisherQueue())

  it should "create an app and a new disk" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()

    val clusters = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(project))
    }
    clusters.length shouldEqual 1
    clusters.flatMap(_.nodepools).length shouldEqual 1
    val cluster = clusters.head
    cluster.auditInfo.creator shouldEqual userInfo.userEmail

    val nodepool = clusters.flatMap(_.nodepools).head
    nodepool.machineType shouldEqual appReq.kubernetesRuntimeConfig.get.machineType
    nodepool.numNodes shouldEqual appReq.kubernetesRuntimeConfig.get.numNodes
    nodepool.autoscalingEnabled shouldEqual appReq.kubernetesRuntimeConfig.get.autoscalingEnabled
    nodepool.auditInfo.creator shouldEqual userInfo.userEmail

    clusters.flatMap(_.nodepools).flatMap(_.apps).length shouldEqual 1
    val app = clusters.flatMap(_.nodepools).flatMap(_.apps).head
    app.appName shouldEqual appName
    app.auditInfo.creator shouldEqual userInfo.userEmail

    val savedDisk = dbFutureValue {
      persistentDiskQuery.getById(app.appResources.disk.get.id)
    }
    savedDisk.map(_.name) shouldEqual Some(diskName)
  }

  it should "queue the proper message when creating an app and a new disk" in isolatedDbTest {

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)

    kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()

    val getApp = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }.get

    val getMinimalCluster = dbFutureValue {
      kubernetesClusterQuery.getMinimalClusterById(getApp.cluster.id)
    }.get

    val defaultNodepools = getMinimalCluster.nodepools.filter(_.isDefault)
    defaultNodepools.length shouldBe 1
    val defaultNodepool = defaultNodepools.head

    val message = publisherQueue.dequeue1.unsafeRunSync()
    message.messageType shouldBe LeoPubsubMessageType.CreateApp
    val createAppMessage = message.asInstanceOf[CreateAppMessage]
    createAppMessage.appId shouldBe getApp.app.id
    createAppMessage.nodepoolId shouldBe getApp.nodepool.id
    createAppMessage.project shouldBe project
    createAppMessage.createDisk shouldBe true
    createAppMessage.cluster shouldBe Some(
      CreateCluster(
        getMinimalCluster.id,
        defaultNodepool.id
      )
    )
  }

  it should "create an app with an existing disk" in isolatedDbTest {
    val disk = makePersistentDisk(DiskId(1)).copy(googleProject = project).save().unsafeRunSync()

    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()

    val message = publisherQueue.dequeue1.unsafeRunSync()
    message.messageType shouldBe LeoPubsubMessageType.CreateApp
    message.asInstanceOf[CreateAppMessage].createDisk shouldBe false

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }

    appResult.flatMap(_.app.appResources.disk.map(_.name)) shouldEqual Some(disk.name)
    appResult.map(_.app.appName) shouldEqual Some(appName)
  }

  it should "error on creation of a galaxy app without a disk" in isolatedDbTest {
    val appName = AppName("app1")
    val appReq = createAppRequest.copy(diskConfig = None, appType = AppType.Galaxy)

    the[AppRequiresDiskException] thrownBy {
      kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()
    }
  }

  it should "error on creation if a disk is attached to another app" in isolatedDbTest {
    val id = DiskId(1)
    val disk = makePersistentDisk(id).copy(googleProject = project).save().unsafeRunSync()
    val appName1 = AppName("app1")
    val appName2 = AppName("app2")

    val createDiskConfig = PersistentDiskRequest(disk.name, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    kubeServiceInterp.createApp(userInfo, project, appName1, appReq).unsafeRunSync()
    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName1)
    }
    appResult.flatMap(_.app.appResources.disk.map(_.name)) shouldEqual Some(disk.name)
    appResult.map(_.app.appName) shouldEqual Some(appName1)

    //we need to update status from creating because we don't allow creation of apps while cluster is creating
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    the[DiskAlreadyAttachedException] thrownBy {
      kubeServiceInterp.createApp(userInfo, project, appName2, appReq).unsafeRunSync()
    }
  }

  it should "error on creation if an app with that name exists" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()

    the[AppAlreadyExistsException] thrownBy {
      kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()
    }
  }

  it should "delete an app and update status appropriately" in isolatedDbTest {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val kubeServiceInterp = makeInterp(publisherQueue)
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()

    val appResultPreStatusUpdate = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }

    //we can't delete while its creating, so set it to Running
    dbFutureValue(appQuery.updateStatus(appResultPreStatusUpdate.get.app.id, AppStatus.Running))

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }
    appResultPreDelete.get.app.status shouldEqual AppStatus.Running
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    val params = DeleteAppParams(userInfo, project, appName, false)
    kubeServiceInterp.deleteApp(params).unsafeRunSync()
    val clusterPostDelete = dbFutureValue {
      KubernetesServiceDbQueries.listFullApps(Some(project), includeDeleted = true)
    }

    clusterPostDelete.length shouldEqual 1
    val nodepool = clusterPostDelete.head.nodepools.head
    nodepool.status shouldEqual NodepoolStatus.Predeleting
    val app = nodepool.apps.head
    app.status shouldEqual AppStatus.Predeleting

    //throw away create message
    publisherQueue.dequeue1.unsafeRunSync()

    val message = publisherQueue.dequeue1.unsafeRunSync()
    message.messageType shouldBe LeoPubsubMessageType.DeleteApp
    val deleteAppMessage = message.asInstanceOf[DeleteAppMessage]
    deleteAppMessage.appId shouldBe app.id
    deleteAppMessage.nodepoolId shouldBe nodepool.id
    deleteAppMessage.project shouldBe project
    deleteAppMessage.deleteDisk shouldBe false
  }

  it should "error on delete if app is in a status that cannot be deleted" in isolatedDbTest {
    val appName = AppName("app1")
    val createDiskConfig = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq = createAppRequest.copy(diskConfig = Some(createDiskConfig))

    kubeServiceInterp.createApp(userInfo, project, appName, appReq).unsafeRunSync()

    val appResultPreDelete = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName)
    }

    //TODO: update this once create publishes pubsub message
    appResultPreDelete.get.app.status shouldEqual AppStatus.Precreating
    appResultPreDelete.get.app.auditInfo.destroyedDate shouldBe None

    val params = DeleteAppParams(userInfo, project, appName, false)
    the[AppCannotBeDeletedException] thrownBy {
      kubeServiceInterp.deleteApp(params).unsafeRunSync()
    }
  }

  it should "list apps" in isolatedDbTest {
    val appName1 = AppName("app1")
    val appName2 = AppName("app2")
    val appName3 = AppName("app3")
    val createDiskConfig1 = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq1 = createAppRequest.copy(diskConfig = Some(createDiskConfig1))
    val diskName2 = DiskName("newDiskName")
    val createDiskConfig2 = PersistentDiskRequest(diskName2, None, None, Map.empty)
    val appReq2 = createAppRequest.copy(diskConfig = Some(createDiskConfig2))

    kubeServiceInterp.createApp(userInfo, project, appName1, appReq1).unsafeRunSync()

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    kubeServiceInterp.createApp(userInfo, project, appName2, appReq2).unsafeRunSync()
    kubeServiceInterp.createApp(userInfo, project2, appName3, appReq1).unsafeRunSync()

    val listAllApps = kubeServiceInterp.listApp(userInfo, None, Map()).unsafeRunSync()
    listAllApps.length shouldEqual 3
    listAllApps.map(_.appName) should contain(appName1)
    listAllApps.map(_.appName) should contain(appName2)
    listAllApps.map(_.appName) should contain(appName3)
    listAllApps.map(_.diskName).sortBy(_.get.value) shouldBe Vector(Some(diskName), Some(diskName), Some(diskName2))
      .sortBy(_.get.value)

    val listProject1Apps = kubeServiceInterp.listApp(userInfo, Some(project), Map()).unsafeRunSync()
    listProject1Apps.length shouldBe 2
    listProject1Apps.map(_.appName) should contain(appName1)
    listProject1Apps.map(_.appName) should contain(appName2)

    val listProject2Apps = kubeServiceInterp.listApp(userInfo, Some(project2), Map()).unsafeRunSync()
    listProject2Apps.length shouldBe 1
    listProject2Apps.map(_.appName) should contain(appName3)

    val listProject3Apps =
      kubeServiceInterp.listApp(userInfo, Some(GoogleProject("fakeProject")), Map()).unsafeRunSync()
    listProject3Apps.length shouldBe 0
  }

  it should "list apps with labels" in isolatedDbTest {
    val appName1 = AppName("app1")
    val appName2 = AppName("app2")
    val appName3 = AppName("app3")
    val createDiskConfig1 = PersistentDiskRequest(diskName, None, None, Map.empty)
    val label1 = "a" -> "b"
    val label2 = "c" -> "d"
    val labels: LabelMap = Map(label1, label2)
    val appReq1 = createAppRequest.copy(diskConfig = Some(createDiskConfig1))
    val diskName2 = DiskName("newDiskName")
    val createDiskConfig2 = PersistentDiskRequest(diskName2, None, None, Map.empty)
    val appReq2 = createAppRequest.copy(diskConfig = Some(createDiskConfig2), labels = labels)

    kubeServiceInterp.createApp(userInfo, project, appName1, appReq1).unsafeRunSync()

    val app1Result = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName1)
    }

    dbFutureValue(kubernetesClusterQuery.updateStatus(app1Result.get.cluster.id, KubernetesClusterStatus.Running))

    kubeServiceInterp.createApp(userInfo, project, appName2, appReq2).unsafeRunSync()

    val app2Result = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName2)
    }
    app2Result.map(_.app.labels).get.toList should contain(label1)
    app2Result.map(_.app.labels).get.toList should contain(label2)

    kubeServiceInterp.createApp(userInfo, project2, appName3, appReq1).unsafeRunSync()

    val listLabelApp = kubeServiceInterp.listApp(userInfo, None, labels).unsafeRunSync()
    listLabelApp.length shouldEqual 1
    listLabelApp.map(_.appName) should contain(appName2)

    val listPartialLabelApp1 = kubeServiceInterp.listApp(userInfo, None, Map(label1)).unsafeRunSync()
    listPartialLabelApp1.length shouldEqual 1
    listPartialLabelApp1.map(_.appName) should contain(appName2)

    val listPartialLabelApp2 = kubeServiceInterp.listApp(userInfo, None, Map(label2)).unsafeRunSync()
    listPartialLabelApp2.length shouldEqual 1
    listPartialLabelApp2.map(_.appName) should contain(appName2)
  }

  it should "get app" in isolatedDbTest {
    val appName1 = AppName("app1")
    val appName2 = AppName("app2")
    val appName3 = AppName("app3")
    val createDiskConfig1 = PersistentDiskRequest(diskName, None, None, Map.empty)
    val appReq1 = createAppRequest.copy(diskConfig = Some(createDiskConfig1))
    val diskName2 = DiskName("newDiskName")
    val createDiskConfig2 = PersistentDiskRequest(diskName2, None, None, Map.empty)
    val appReq2 = createAppRequest.copy(diskConfig = Some(createDiskConfig2))

    kubeServiceInterp.createApp(userInfo, project, appName1, appReq1).unsafeRunSync()

    val appResult = dbFutureValue {
      KubernetesServiceDbQueries.getActiveFullAppByName(project, appName1)
    }
    dbFutureValue(kubernetesClusterQuery.updateStatus(appResult.get.cluster.id, KubernetesClusterStatus.Running))

    kubeServiceInterp.createApp(userInfo, project, appName2, appReq2).unsafeRunSync()

    val diskName3 = DiskName("newDiskName2")
    val createDiskConfig3 = PersistentDiskRequest(diskName3, None, None, Map.empty)
    kubeServiceInterp
      .createApp(userInfo, project2, appName3, appReq1.copy(diskConfig = Some(createDiskConfig3)))
      .unsafeRunSync()

    val getApp1 = kubeServiceInterp.getApp(userInfo, project, appName1).unsafeRunSync()
    getApp1.diskName shouldBe Some(diskName)

    val getApp2 = kubeServiceInterp.getApp(userInfo, project, appName2).unsafeRunSync()
    getApp2.diskName shouldBe Some(diskName2)

    val getApp3 = kubeServiceInterp.getApp(userInfo, project2, appName3).unsafeRunSync()
    getApp3.diskName shouldBe Some(diskName3)
  }

  it should "error on get app if an app does not exist" in isolatedDbTest {
    the[AppNotFoundException] thrownBy {
      kubeServiceInterp.getApp(userInfo, project, AppName("schrodingersApp")).unsafeRunSync()
    }
  }
}
