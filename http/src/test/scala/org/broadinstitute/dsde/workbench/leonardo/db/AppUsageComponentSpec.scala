package org.broadinstitute.dsde.workbench.leonardo.db

import cats.effect.IO
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.scalatest.flatspec.AnyFlatSpecLike

import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global

class AppUsageComponentSpec extends AnyFlatSpecLike with TestComponent {
  it should "save, get" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app = makeApp(1, savedNodepool1.id)
    val savedApp = app.save()

    val test = for {
      recordStopRes <- appUsageQuery.recordStop(savedApp.id, Instant.now()).attempt
      _ = recordStopRes.leftMap(_.getMessage) shouldBe Left(
        s"Cannot record stopTime because there's no existing unresolved startTime for ${savedApp.id.id}"
      )
      startTime <- IO.realTimeInstant
      appUsageId <- appUsageQuery.recordStart(savedApp.id, startTime)
      appAfterRecordingStart <- testDbRef
        .inTransaction(appUsageQuery.get(appUsageId))
      _ = appAfterRecordingStart shouldBe (Some(AppUsageRecord(appUsageId, savedApp.id, startTime, dummyDate)))

      recordStartRes <- appUsageQuery.recordStart(savedApp.id, startTime).attempt
      _ = recordStartRes.leftMap(_.getMessage) shouldBe (Left(
        s"app(${savedApp.id.id}) usage startTime was recorded previously with no endTime recorded"
      ))

      stopTime <- IO.realTimeInstant
      _ <- appUsageQuery.recordStop(savedApp.id, stopTime)
      appAfterRecordingStop <- testDbRef
        .inTransaction(appUsageQuery.get(appUsageId))
      _ = appAfterRecordingStop shouldBe (Some(AppUsageRecord(appUsageId, savedApp.id, startTime, stopTime)))

      stopTime2 <- IO.realTimeInstant
      secondStopTimeRecordingAttempt <- appUsageQuery.recordStop(savedApp.id, stopTime2).attempt
      _ = secondStopTimeRecordingAttempt.leftMap(_.getMessage) shouldBe
        Left(s"Cannot record stopTime because there's no existing unresolved startTime for ${savedApp.id.id}")
    } yield succeed
    test.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  it should "recordStop even if there are multiple rows of unresolved startTime for the same app" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.api._
    import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.mappedColumnImplicits._

    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app = makeApp(1, savedNodepool1.id)
    val savedApp = app.save()

    def persistStartTime(startTime: Instant) =
      appUsageQuery returning appUsageQuery.map(_.id) += AppUsageRecord(AppUsageId(-1),
                                                                        savedApp.id,
                                                                        startTime,
                                                                        dummyDate
      )

    val test = for {
      startTime1 <- IO.realTimeInstant
      appUsageId1 <- testDbRef.inTransaction(persistStartTime(startTime1))
      startTime2 = startTime1.plusSeconds(100)
      appUsageId2 <- testDbRef.inTransaction(persistStartTime(startTime2))
      stopTime = startTime1.plusSeconds(300)
      _ <- appUsageQuery.recordStop(savedApp.id, stopTime)
      appAfterRecordingStop <- testDbRef
        .inTransaction(appUsageQuery.get(appUsageId1))
      _ = appAfterRecordingStop.get.stopTime shouldBe stopTime
      appAfterRecordingStop2 <- testDbRef
        .inTransaction(appUsageQuery.get(appUsageId2))
      _ = appAfterRecordingStop.get.stopTime shouldBe stopTime
    } yield succeed
    test.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
