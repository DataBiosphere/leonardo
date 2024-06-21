package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.{updateAppLogQuery, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.{NoMatchingAppError, NotAnAdminError}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.UpdateAppMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessageType
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsp.ChartVersion
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

final class AdminServiceInterpSpec extends AnyFlatSpec with LeonardoTestSuite with TestComponent {

  val mockAdminAuthProvider = new BaseMockAuthProvider {
    override def isAdminUser(userInfo: UserInfo)(implicit ev: Ask[IO, TraceId]): IO[Boolean] = IO.pure(true)
  }

  val mockNonAdminAuthProvider = new BaseMockAuthProvider {
    override def isAdminUser(userInfo: UserInfo)(implicit ev: Ask[IO, TraceId]): IO[Boolean] = IO.pure(false)
  }

  val updateAppsRequest = UpdateAppsRequest(
    None,
    appType = AppType.Cromwell,
    cloudProvider = CloudProvider.Gcp,
    appVersionsInclude = List(),
    appVersionsExclude = List(),
    googleProject = None,
    workspaceId = None,
    appNames = List(),
    dryRun = true
  )

  it should "not queue a message when doing a dry run of an update" in isolatedDbTest {
    val v1Chart = Config.gkeCromwellAppConfig.chart.copy(version = ChartVersion("0.1.0"))
    val v2Chart = Config.gkeCromwellAppConfig.chart

    val cluster1 = makeKubeCluster(1).save()
    val savedNodepool = makeNodepool(1, cluster1.id).save()
    makeApp(1, savedNodepool.id, status = AppStatus.Running, appType = AppType.Cromwell, chart = v1Chart).save()
    makeApp(2, savedNodepool.id, status = AppStatus.Running, appType = AppType.Cromwell, chart = v2Chart).save()

    val publisherQueue = QueueFactory.makePublisherQueue()
    val interp = new AdminServiceInterp[IO](
      mockAdminAuthProvider,
      publisherQueue
    )

    val res = interp
      .updateApps(userInfo, updateAppsRequest.copy(dryRun = true))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.length shouldEqual 1
    res.map(_.appName.value).contains("app1") shouldBe true

    // Verify that no update message was sent
    publisherQueue.tryTake.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe None
  }

  it should "properly queue a message when doing an update" in isolatedDbTest {
    val v1Chart = Config.gkeCromwellAppConfig.chart.copy(version = ChartVersion("0.1.0"))
    val v2Chart = Config.gkeCromwellAppConfig.chart

    val cluster1 = makeKubeCluster(1).save()
    val savedNodepool = makeNodepool(1, cluster1.id).save()
    val app1 =
      makeApp(1, savedNodepool.id, status = AppStatus.Running, appType = AppType.Cromwell, chart = v1Chart).save()
    val app2 =
      makeApp(2, savedNodepool.id, status = AppStatus.Running, appType = AppType.Cromwell, chart = v2Chart).save()

    val publisherQueue = QueueFactory.makePublisherQueue()
    val interp = new AdminServiceInterp[IO](
      mockAdminAuthProvider,
      publisherQueue
    )

    val jobId = UpdateAppJobId(UUID.randomUUID())
    val res = interp
      .updateApps(userInfo, updateAppsRequest.copy(dryRun = false, jobId = Some(jobId)))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.length shouldEqual 1
    res.map(_.appName) shouldBe Vector(app1.appName)

    // Verify that the update message was sent
    val message = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message.messageType shouldBe LeoPubsubMessageType.UpdateApp
    val updateMsg = message.asInstanceOf[UpdateAppMessage]
    updateMsg.appName shouldBe app1.appName
    updateMsg.jobId shouldBe jobId

    // Verify database has log records for update
    val dbTest = for {
      updateLog1 <- updateAppLogQuery.get(app1.id, jobId)
    } yield {
      updateLog1.isDefined shouldBe true
      updateLog1.map(_.jobId) shouldBe Some(jobId)
      updateLog1.map(_.appId) shouldBe Some(app1.id)
      updateLog1.map(_.status) shouldBe Some(UpdateAppJobStatus.Running)
      updateLog1.map(_.endTime) shouldBe Some(None)
      updateLog1.map(_.errorId) shouldBe Some(None)
    }
    dbTest.transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // Verify that no extra update message was sent
    publisherQueue.tryTake.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe None
  }

  it should "properly queue a message when doing an update for multiple apps" in isolatedDbTest {
    val v1Chart = Config.gkeCromwellAppConfig.chart.copy(version = ChartVersion("0.1.0"))

    val cluster1 = makeKubeCluster(1).save()
    val savedNodepool = makeNodepool(1, cluster1.id).save()
    val app1 =
      makeApp(1, savedNodepool.id, status = AppStatus.Running, appType = AppType.Cromwell, chart = v1Chart).save()
    val app2 =
      makeApp(2, savedNodepool.id, status = AppStatus.Running, appType = AppType.Cromwell, chart = v1Chart).save()

    val publisherQueue = QueueFactory.makePublisherQueue()
    val interp = new AdminServiceInterp[IO](
      mockAdminAuthProvider,
      publisherQueue
    )
    val jobId = UpdateAppJobId(UUID.randomUUID())

    val res = interp
      .updateApps(
        userInfo,
        updateAppsRequest.copy(dryRun = false, jobId = Some(jobId), appNames = List(app1.appName, app2.appName))
      )
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.length shouldEqual 2
    res.map(_.appName.value).sorted shouldBe List(app1, app2).map(_.appName.value).sorted

    // Verify that the update messages were sent
    val message1 = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val message2 = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message1.messageType shouldBe LeoPubsubMessageType.UpdateApp
    val updateMsg1 = message1.asInstanceOf[UpdateAppMessage]
    val updateMsg2 = message2.asInstanceOf[UpdateAppMessage]
    // Use a list because the order of messages is not guaranteed
    val msgList = List(updateMsg1, updateMsg2)
    msgList.map(_.appName.value).sorted shouldBe List(app1, app2).map(_.appName.value).sorted
    msgList.map(_.jobId).distinct shouldBe List(jobId)

    // Verify database has log records for update
    val dbTest = for {
      updateLog1 <- updateAppLogQuery.get(app1.id, jobId)
      updateLog2 <- updateAppLogQuery.get(app2.id, jobId)
    } yield {
      updateLog1.isDefined shouldBe true
      updateLog1.map(_.jobId) shouldBe Some(jobId)
      updateLog1.map(_.appId) shouldBe Some(app1.id)
      updateLog1.map(_.status) shouldBe Some(UpdateAppJobStatus.Running)
      updateLog1.map(_.endTime) shouldBe Some(None)
      updateLog1.map(_.errorId) shouldBe Some(None)

      updateLog2.isDefined shouldBe true
      updateLog2.map(_.jobId) shouldBe Some(jobId)
      updateLog2.map(_.appId) shouldBe Some(app2.id)
      updateLog2.map(_.status) shouldBe Some(UpdateAppJobStatus.Running)
      updateLog2.map(_.endTime) shouldBe Some(None)
      updateLog2.map(_.errorId) shouldBe Some(None)
    }
    dbTest.transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    // Verify that no extra update message was sent
    publisherQueue.tryTake.unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe None
  }

  it should "fail when the user is not an admin" in {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val interp = new AdminServiceInterp[IO](
      mockNonAdminAuthProvider,
      publisherQueue
    )

    an[NotAnAdminError] should be thrownBy {
      interp
        .updateApps(userInfo, updateAppsRequest)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "fail when the app type and cloud provider do not match" in {
    val publisherQueue = QueueFactory.makePublisherQueue()
    val interp = new AdminServiceInterp[IO](
      mockAdminAuthProvider,
      publisherQueue
    )

    val request = updateAppsRequest.copy(
      appType = AppType.Galaxy,
      cloudProvider = CloudProvider.Azure
    )

    an[NoMatchingAppError] should be thrownBy {
      interp
        .updateApps(userInfo, request)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }
}
