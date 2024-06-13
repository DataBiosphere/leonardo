package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import cats.effect.IO
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeKubeCluster, makeApp, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.UpdateAppJobId
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.scalatest.flatspec.AnyFlatSpecLike
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._
import org.broadinstitute.dsde.workbench.leonardo.db.{TestComponent, updateAppLogQuery, appErrorQuery}

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

class UpdateAppLogComponentSpec extends AnyFlatSpecLike with TestComponent {
  it should "save, get" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app = makeApp(1, savedNodepool1.id)
    val savedApp = app.save()

    val jobId1 = UpdateAppJobId(UUID.randomUUID())
    val startTime = Instant.now()

    val test = for {
      _ <- updateAppLogQuery.save(jobId1, savedApp.id, startTime)
      appUpdateLogRecordOpt <- updateAppLogQuery.get(savedApp.id, jobId1)
    } yield {
      appUpdateLogRecordOpt.isDefined shouldBe true
      appUpdateLogRecordOpt.map(_.errorId) shouldBe Some(None)
      appUpdateLogRecordOpt.map(_.endTime) shouldBe Some(None)
      appUpdateLogRecordOpt.map(_.jobId) shouldBe Some(jobId1)
      appUpdateLogRecordOpt.map(_.appId) shouldBe Some(savedApp.id)
      appUpdateLogRecordOpt.map(_.status) shouldBe Some(UpdateAppJobStatus.Running)
      appUpdateLogRecordOpt.map(_.startTime) shouldBe Some(startTime)
    }
    test.transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "save, update, get" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app = makeApp(1, savedNodepool1.id)
    val savedApp = app.save()

    val jobId1 = UpdateAppJobId(UUID.randomUUID())
    val startTime = Instant.now()

    val appErrorTime = Instant.now()
    val appError = AppError("error msg", appErrorTime, ErrorAction.UpdateApp, ErrorSource.App, None, None)
    println("startTime", startTime)
    println("appErrorTime", appErrorTime)

    val test = for {
      errorId <- appErrorQuery.save(savedApp.id, appError)
      _ <- updateAppLogQuery.save(jobId1, savedApp.id, startTime)
      endTime = Instant.now()
      _ =     println("endtime", endTime)
      _ <- updateAppLogQuery.update(savedApp.id, jobId1, UpdateAppJobStatus.Error, Some(errorId), Some(endTime))
      appUpdateLogRecordOpt <- updateAppLogQuery.get(savedApp.id, jobId1)
    } yield {
      appUpdateLogRecordOpt.isDefined shouldBe true
      appUpdateLogRecordOpt.map(_.startTime) shouldBe Some(startTime)
      appUpdateLogRecordOpt.map(_.errorId) shouldBe Some(Some(errorId))
      appUpdateLogRecordOpt.map(_.endTime) shouldBe Some(Some(endTime))
      appUpdateLogRecordOpt.map(_.jobId) shouldBe Some(jobId1)
      appUpdateLogRecordOpt.map(_.appId) shouldBe Some(savedApp.id)
      appUpdateLogRecordOpt.map(_.status) shouldBe Some(UpdateAppJobStatus.Error)
    }
    test.transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
