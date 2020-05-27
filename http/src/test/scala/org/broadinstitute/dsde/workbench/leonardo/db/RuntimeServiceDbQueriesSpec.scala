package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{makeCluster, _}
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.db.{
  clusterQuery,
  labelQuery,
  LabelResourceType,
  RuntimeServiceDbQueries,
  TestComponent
}
import org.broadinstitute.dsde.workbench.leonardo.http.api.ListRuntimeResponse2
import org.scalatest.FlatSpecLike
import org.broadinstitute.dsde.workbench.leonardo.db.RuntimeServiceDbQueries._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RuntimeServiceDbQueriesSpec extends FlatSpecLike with TestComponent with GcsPathUtils {
  val maxElapsed = 5.seconds

  it should "getStatusByName" in isolatedDbTest {
    val r = makeCluster(1)
    val res = for {
      statusBeforeCreating <- getStatusByName(r.googleProject, r.runtimeName).transaction
      runtime <- IO(r.save())
      status <- getStatusByName(runtime.googleProject, runtime.runtimeName).transaction
      _ <- clusterQuery.markPendingDeletion(runtime.id, Instant.now).transaction
      statusAfterDeletion <- getStatusByName(runtime.googleProject, runtime.runtimeName).transaction
    } yield {
      statusBeforeCreating shouldBe None
      runtime.status shouldBe (status.get)
      statusAfterDeletion shouldBe Some(RuntimeStatus.Deleting)
    }

    res.unsafeRunSync()
  }

  it should "list runtimes" in isolatedDbTest {
    val res = for {
      start <- testTimer.clock.monotonic(TimeUnit.MILLISECONDS)
      list1 <- RuntimeServiceDbQueries.listClusters(Map.empty, false, None).transaction
      c1 <- IO(makeCluster(1).save())
      list2 <- RuntimeServiceDbQueries.listClusters(Map.empty, false, None).transaction
      c2 <- IO(makeCluster(2).save())
      list3 <- RuntimeServiceDbQueries.listClusters(Map.empty, false, None).transaction
      end <- testTimer.clock.monotonic(TimeUnit.MILLISECONDS)
      elapsed = (end - start).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      list1 shouldEqual List.empty
      val c1Expected = toListRuntimeResponse(c1, Map.empty)
      val c2Expected = toListRuntimeResponse(c2, Map.empty)
      list2 shouldEqual List(c1Expected)
      list3.toSet shouldEqual Set(c1Expected, c2Expected)
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()
  }

  it should "list runtimes by labels" in isolatedDbTest {
    val res = for {
      start <- testTimer.clock.monotonic(TimeUnit.MILLISECONDS)
      c1 <- IO(makeCluster(1).save())
      c2 <- IO(makeCluster(2).save())
      labels1 = Map("googleProject" -> c1.googleProject.value,
                    "clusterName" -> c1.runtimeName.asString,
                    "creator" -> c1.auditInfo.creator.value)
      labels2 = Map("googleProject" -> c2.googleProject.value,
                    "clusterName" -> c2.runtimeName.asString,
                    "creator" -> c2.auditInfo.creator.value)
      list1 <- RuntimeServiceDbQueries.listClusters(labels1, false, None).transaction
      list2 <- RuntimeServiceDbQueries.listClusters(labels2, false, None).transaction
      _ <- labelQuery.saveAllForResource(c1.id, LabelResourceType.Runtime, labels1).transaction
      _ <- labelQuery.saveAllForResource(c2.id, LabelResourceType.Runtime, labels2).transaction
      list3 <- RuntimeServiceDbQueries.listClusters(labels1, false, None).transaction
      list4 <- RuntimeServiceDbQueries.listClusters(labels2, false, None).transaction
      list5 <- RuntimeServiceDbQueries
        .listClusters(Map("googleProject" -> c1.googleProject.value), false, None)
        .transaction
      end <- testTimer.clock.monotonic(TimeUnit.MILLISECONDS)
      elapsed = (end - start).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      list1 shouldEqual List.empty
      list2 shouldEqual List.empty
      val c1Expected = toListRuntimeResponse(c1, labels1)
      val c2Expected = toListRuntimeResponse(c2, labels2)
      list3 shouldEqual List(c1Expected)
      list4 shouldEqual List(c2Expected)
      list5.toSet shouldEqual Set(c1Expected, c2Expected)
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()
  }

  it should "list runtimes by project" in isolatedDbTest {
    val res = for {
      start <- testTimer.clock.monotonic(TimeUnit.MILLISECONDS)
      c1 <- IO(makeCluster(1).save())
      c2 <- IO(makeCluster(2).save())
      list1 <- RuntimeServiceDbQueries.listClusters(Map.empty, false, Some(project)).transaction
      list2 <- RuntimeServiceDbQueries.listClusters(Map.empty, false, Some(project2)).transaction
      end <- testTimer.clock.monotonic(TimeUnit.MILLISECONDS)
      elapsed = (end - start).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      val c1Expected = toListRuntimeResponse(c1, Map.empty)
      val c2Expected = toListRuntimeResponse(c2, Map.empty)
      list1.toSet shouldEqual Set(c1Expected, c2Expected)
      list2 shouldEqual List.empty
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()
  }

  it should "list runtimes including deleted" in isolatedDbTest {
    val res = for {
      start <- testTimer.clock.monotonic(TimeUnit.MILLISECONDS)
      c1 <- IO(makeCluster(1).copy(status = RuntimeStatus.Deleted).save())
      c2 <- IO(makeCluster(2).copy(status = RuntimeStatus.Deleted).save())
      c3 <- IO(makeCluster(3).save())
      list1 <- RuntimeServiceDbQueries.listClusters(Map.empty, true, None).transaction
      list2 <- RuntimeServiceDbQueries.listClusters(Map.empty, false, None).transaction
      end <- testTimer.clock.monotonic(TimeUnit.MILLISECONDS)
      elapsed = (end - start).millis
      _ <- loggerIO.info(s"listClusters took $elapsed")
    } yield {
      val c1Expected = toListRuntimeResponse(c1, Map.empty)
      val c2Expected = toListRuntimeResponse(c2, Map.empty)
      val c3Expected = toListRuntimeResponse(c3, Map.empty)
      list1.toSet shouldEqual Set(c1Expected, c2Expected, c3Expected)
      list2 shouldEqual List(c3Expected)
      elapsed should be < maxElapsed
    }

    res.unsafeRunSync()
  }

  private def toListRuntimeResponse(runtime: Runtime, labels: LabelMap): ListRuntimeResponse2 =
    ListRuntimeResponse2(
      runtime.id,
      runtime.samResource,
      runtime.runtimeName,
      runtime.googleProject,
      runtime.auditInfo,
      CommonTestData.defaultDataprocRuntimeConfig,
      Runtime.getProxyUrl(Config.proxyConfig.proxyUrlBase,
                          runtime.googleProject,
                          runtime.runtimeName,
                          Set.empty,
                          labels),
      runtime.status,
      labels,
      false
    )

}
