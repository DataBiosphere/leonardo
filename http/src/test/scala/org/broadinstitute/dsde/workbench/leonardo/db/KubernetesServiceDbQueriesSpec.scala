package org.broadinstitute.dsde.workbench.leonardo.db

import cats.effect.unsafe.IORuntime
import org.broadinstitute.dsde.workbench.leonardo.{AppStatus, KubernetesClusterStatus, NodepoolStatus}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class KubernetesServiceDbQueriesSpec extends AnyFlatSpecLike with TestComponent {
  "getActiveFullAppByName" should "get active app properly " in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1, status = KubernetesClusterStatus.Deleted).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id, status = NodepoolStatus.Deleted).save()
    val savedApp1 = makeApp(2, savedNodepool1.id, status = AppStatus.Error).save()
    val res = KubernetesServiceDbQueries
      .getActiveFullAppByName(savedCluster1.cloudContext, savedApp1.appName, Map.empty)
      .transaction
      .unsafeRunSync()(IORuntime.global)

    res.isDefined shouldBe true
  }
}
