package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.broadinstitute.dsde.workbench.leonardo.db.LeoProfile.dummyDate
import org.scalatest.flatspec.AnyFlatSpecLike

import java.sql.SQLDataException
import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.global

class AppUsageComponentSpec extends AnyFlatSpecLike with TestComponent {
  it should "save, get" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id).save()

    val app = makeApp(1, savedNodepool1.id)
    val savedApp = app.save()

    dbFailure(
      appUsageQuery.recordStop(savedApp.id, Instant.now())
    ).getMessage shouldBe s"Cannot record stopTime because there's no existing startTime for ${savedApp.id.id}"

    val startTime = Instant.now()
    val appUsageId = dbFutureValue(appUsageQuery.recordStart(savedApp.id, startTime))
    dbFutureValue(appUsageQuery.get(appUsageId)) shouldBe (Some(
      AppUsageRecord(appUsageId, savedApp.id, startTime, dummyDate)
    ))

    dbFailure(appUsageQuery.recordStart(savedApp.id, startTime)).isInstanceOf[SQLDataException] shouldBe true

    val stopTime = Instant.now()
    dbFutureValue(appUsageQuery.recordStop(savedApp.id, stopTime))

    val updatedAppUsageRecord = dbFutureValue(appUsageQuery.get(appUsageId))
    updatedAppUsageRecord shouldBe (Some(
      AppUsageRecord(appUsageId, savedApp.id, startTime, stopTime)
    ))
  }
}
