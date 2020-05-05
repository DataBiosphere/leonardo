package org.broadinstitute.dsde.workbench.leonardo
package http
package db

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.db.{LabelResourceType, RuntimeServiceDbQueries, TestComponent, labelQuery}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.scalatest.FlatSpecLike
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global

class RuntimeServiceDbQueriesSpec extends FlatSpecLike with TestComponent with GcsPathUtils with ScalaFutures {

  "RuntimeServiceDbQueries" should "list runtimes" in isolatedDbTest {
    val res = for {
      list1 <- RuntimeServiceDbQueries.listClusters(Map.empty, false, None).transaction
      c1 <- IO(makeCluster(1).save())
      list2 <- RuntimeServiceDbQueries.listClusters(Map.empty, false, None).transaction
      c2 <- IO(makeCluster(2).save())
      list3 <- RuntimeServiceDbQueries.listClusters(Map.empty, false, None).transaction
    } yield {
      list1 shouldEqual List.empty
      list2 shouldEqual List(c1)
      list3.toSet shouldEqual Set(c1, c2)
    }

    res.unsafeRunSync()
  }

  it should "list by labels" in isolatedDbTest {
    val res = for {
      c1 <- IO(makeCluster(1).save())
      c2 <- IO(makeCluster(2).save())
      labels1 = Map("googleProject" -> c1.googleProject.value, "clusterName" -> c1.runtimeName.asString, "creator" -> c1.auditInfo.creator.value)
      labels2 = Map("googleProject" -> c2.googleProject.value, "clusterName" -> c2.runtimeName.asString, "creator" -> c2.auditInfo.creator.value)
      list1 <- RuntimeServiceDbQueries.listClusters(labels1, false, None).transaction
      list2 <- RuntimeServiceDbQueries.listClusters(labels2, false, None).transaction
      _ <- labelQuery.saveAllForResource(c1.id, LabelResourceType.Runtime, labels1).transaction
      _ <- labelQuery.saveAllForResource(c2.id, LabelResourceType.Runtime, labels2).transaction
      list3 <- RuntimeServiceDbQueries.listClusters(labels1, false, None).transaction
      list4 <- RuntimeServiceDbQueries.listClusters(labels2, false, None).transaction
      list5 <- RuntimeServiceDbQueries.listClusters(Map("googleProject" -> c1.googleProject.value), false, None).transaction
    } yield {
      list1 shouldEqual List.empty
      list2 shouldEqual List.empty
      list3 shouldEqual List(c1)
      list4 shouldEqual List(c2)
      list5.toSet shouldEqual Set(c1, c2)
    }

    res.unsafeRunSync()
  }

  it should "list by project" in isolatedDbTest {

  }

  it should "list including deleted" in isolatedDbTest {

  }

}
