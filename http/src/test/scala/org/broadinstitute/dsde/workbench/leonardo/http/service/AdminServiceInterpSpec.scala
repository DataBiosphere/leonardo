package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.effect.IO
import cats.mtl.Ask
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.model.{NoMatchingAppError, NotAnAdminError}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.UpdateAppMessage
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessageType
import org.broadinstitute.dsde.workbench.leonardo.util.QueueFactory
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo}
import org.broadinstitute.dsp.ChartVersion
import org.scalatest.flatspec.AnyFlatSpec

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
    makeApp(1, savedNodepool.id, status = AppStatus.Running, appType = AppType.Cromwell, chart = v1Chart).save()
    makeApp(2, savedNodepool.id, status = AppStatus.Running, appType = AppType.Cromwell, chart = v2Chart).save()

    val publisherQueue = QueueFactory.makePublisherQueue()
    val interp = new AdminServiceInterp[IO](
      mockAdminAuthProvider,
      publisherQueue
    )

    val res = interp
      .updateApps(userInfo, updateAppsRequest.copy(dryRun = false))
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

    res.length shouldEqual 1
    res.map(_.appName.value).contains("app1") shouldBe true

    // Verify that the update message was sent
    val message = publisherQueue.take.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    message.messageType shouldBe LeoPubsubMessageType.UpdateApp
    val updateMsg = message.asInstanceOf[UpdateAppMessage]
    updateMsg.appName shouldBe AppName("app1")
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
