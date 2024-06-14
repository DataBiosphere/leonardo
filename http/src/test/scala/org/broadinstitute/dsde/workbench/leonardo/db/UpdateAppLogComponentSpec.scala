package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.UpdateAppJobId
import org.scalatest.flatspec.AnyFlatSpecLike
import org.broadinstitute.dsde.workbench.leonardo.db.{appErrorQuery, updateAppLogQuery, TestComponent}

import java.sql.SQLDataException
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
      appUpdateLogRecordOpt.map(_.startTime.toEpochMilli) shouldBe Some(startTime.toEpochMilli)
    }
    test.transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "save, update with error, get" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app = makeApp(1, savedNodepool1.id)
    val savedApp = app.save()

    val jobId1 = UpdateAppJobId(UUID.randomUUID())
    val startTime = Instant.now()

    val appErrorTime = Instant.now()
    val appError = AppError("error msg", appErrorTime, ErrorAction.UpdateApp, ErrorSource.App, None, None)

    val test = for {
      errorId <- appErrorQuery.save(savedApp.id, appError)
      _ <- updateAppLogQuery.save(jobId1, savedApp.id, startTime)
      endTime = Instant.now()
      _ <- updateAppLogQuery.update(savedApp.id, jobId1, UpdateAppJobStatus.Error, Some(errorId), Some(endTime))
      appUpdateLogRecordOpt <- updateAppLogQuery.get(savedApp.id, jobId1)
    } yield {
      appUpdateLogRecordOpt.isDefined shouldBe true
      appUpdateLogRecordOpt.map(_.startTime.toEpochMilli) shouldBe Some(startTime.toEpochMilli)
      appUpdateLogRecordOpt.map(_.errorId) shouldBe Some(Some(errorId))
      appUpdateLogRecordOpt.map(_.endTime.map(_.toEpochMilli)) shouldBe Some(Some(endTime.toEpochMilli))
      appUpdateLogRecordOpt.map(_.jobId) shouldBe Some(jobId1)
      appUpdateLogRecordOpt.map(_.appId) shouldBe Some(savedApp.id)
      appUpdateLogRecordOpt.map(_.status) shouldBe Some(UpdateAppJobStatus.Error)
    }
    test.transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "save, update with no error, get" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app = makeApp(1, savedNodepool1.id)
    val savedApp = app.save()

    val jobId1 = UpdateAppJobId(UUID.randomUUID())
    val startTime = Instant.now()

    val test = for {
      _ <- updateAppLogQuery.save(jobId1, savedApp.id, startTime)
      endTime = Instant.now()
      _ <- updateAppLogQuery.update(savedApp.id, jobId1, UpdateAppJobStatus.Success, None, Some(endTime))
      appUpdateLogRecordOpt <- updateAppLogQuery.get(savedApp.id, jobId1)
    } yield {
      appUpdateLogRecordOpt.isDefined shouldBe true
      appUpdateLogRecordOpt.map(_.startTime.toEpochMilli) shouldBe Some(startTime.toEpochMilli)
      appUpdateLogRecordOpt.map(_.errorId) shouldBe Some(None)
      appUpdateLogRecordOpt.map(_.endTime.map(_.toEpochMilli)) shouldBe Some(Some(endTime.toEpochMilli))
      appUpdateLogRecordOpt.map(_.jobId) shouldBe Some(jobId1)
      appUpdateLogRecordOpt.map(_.appId) shouldBe Some(savedApp.id)
      appUpdateLogRecordOpt.map(_.status) shouldBe Some(UpdateAppJobStatus.Success)
    }
    test.transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "update to non-existent log should fail" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app = makeApp(1, savedNodepool1.id)
    val savedApp = app.save()

    val jobId1 = UpdateAppJobId(UUID.randomUUID())
    val endTime = Instant.now()

    val test = for {
      _ <- updateAppLogQuery.update(savedApp.id, jobId1, UpdateAppJobStatus.Error, None, Some(endTime))
    } yield ()

    val thrown = the[SQLDataException] thrownBy {
      test.transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    }
  }

  it should "save, update with error, get for multiple apps with same job id" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app1 = makeApp(1, savedNodepool1.id)
    val app2 = makeApp(2, savedNodepool1.id)
    val app3 = makeApp(3, savedNodepool1.id)
    val savedApp1 = app1.save()
    val savedApp2 = app2.save()
    val savedApp3 = app3.save()

    val jobId1 = UpdateAppJobId(UUID.randomUUID())
    val startTime = Instant.now()

    val app1ErrorTime = Instant.now()
    val app1Error = AppError("error msg", app1ErrorTime, ErrorAction.UpdateApp, ErrorSource.App, None, None)
    val app3ErrorTime = Instant.now()
    val app3Error = AppError("error msg", app3ErrorTime, ErrorAction.UpdateApp, ErrorSource.App, None, None)

    val test = for {
      error1Id <- appErrorQuery.save(savedApp1.id, app1Error)
      error3Id <- appErrorQuery.save(savedApp3.id, app3Error)
      _ <- updateAppLogQuery.save(jobId1, savedApp1.id, startTime)
      _ <- updateAppLogQuery.save(jobId1, savedApp2.id, startTime)
      _ <- updateAppLogQuery.save(jobId1, savedApp3.id, startTime)
      endTime1 = Instant.now()
      endTime2 = Instant.now()
      endTime3 = Instant.now()
      _ <- updateAppLogQuery.update(savedApp1.id, jobId1, UpdateAppJobStatus.Error, Some(error1Id), Some(endTime1))
      _ <- updateAppLogQuery.update(savedApp2.id, jobId1, UpdateAppJobStatus.Success, None, Some(endTime2))
      _ <- updateAppLogQuery.update(savedApp3.id, jobId1, UpdateAppJobStatus.Error, Some(error3Id), Some(endTime3))
      appUpdateLogRecordOpt1 <- updateAppLogQuery.get(savedApp1.id, jobId1)
      appUpdateLogRecordOpt2 <- updateAppLogQuery.get(savedApp2.id, jobId1)
      appUpdateLogRecordOpt3 <- updateAppLogQuery.get(savedApp3.id, jobId1)
    } yield {
      appUpdateLogRecordOpt1.isDefined shouldBe true
      appUpdateLogRecordOpt1.map(_.startTime.toEpochMilli) shouldBe Some(startTime.toEpochMilli)
      appUpdateLogRecordOpt1.map(_.errorId) shouldBe Some(Some(error1Id))
      appUpdateLogRecordOpt1.map(_.endTime.map(_.toEpochMilli)) shouldBe Some(Some(endTime1.toEpochMilli))
      appUpdateLogRecordOpt1.map(_.jobId) shouldBe Some(jobId1)
      appUpdateLogRecordOpt1.map(_.appId) shouldBe Some(savedApp1.id)
      appUpdateLogRecordOpt1.map(_.status) shouldBe Some(UpdateAppJobStatus.Error)

      appUpdateLogRecordOpt2.isDefined shouldBe true
      appUpdateLogRecordOpt2.map(_.startTime.toEpochMilli) shouldBe Some(startTime.toEpochMilli)
      appUpdateLogRecordOpt2.map(_.errorId) shouldBe Some(None)
      appUpdateLogRecordOpt2.map(_.endTime.map(_.toEpochMilli)) shouldBe Some(Some(endTime2.toEpochMilli))
      appUpdateLogRecordOpt2.map(_.jobId) shouldBe Some(jobId1)
      appUpdateLogRecordOpt2.map(_.appId) shouldBe Some(savedApp2.id)
      appUpdateLogRecordOpt2.map(_.status) shouldBe Some(UpdateAppJobStatus.Success)

      appUpdateLogRecordOpt3.isDefined shouldBe true
      appUpdateLogRecordOpt3.map(_.startTime.toEpochMilli) shouldBe Some(startTime.toEpochMilli)
      appUpdateLogRecordOpt3.map(_.errorId) shouldBe Some(Some(error3Id))
      appUpdateLogRecordOpt3.map(_.endTime.map(_.toEpochMilli)) shouldBe Some(Some(endTime3.toEpochMilli))
      appUpdateLogRecordOpt3.map(_.jobId) shouldBe Some(jobId1)
      appUpdateLogRecordOpt3.map(_.appId) shouldBe Some(savedApp3.id)
      appUpdateLogRecordOpt3.map(_.status) shouldBe Some(UpdateAppJobStatus.Error)
    }
    test.transaction.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
