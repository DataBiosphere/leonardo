package org.broadinstitute.dsde.workbench.leonardo.db

import cats.effect.unsafe.IORuntime
import org.broadinstitute.dsde.workbench.leonardo.{
  AppStatus,
  KubernetesCluster,
  KubernetesClusterStatus,
  NodepoolStatus,
  WorkspaceId
}
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData.{makeApp, makeKubeCluster, makeNodepool}
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.UUID
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

  "getActiveFullAppByWorkspaceIdAndAppName" should "get active app properly " in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1, status = KubernetesClusterStatus.Running).save()
    val savedNodepool1 = makeNodepool(1, savedCluster1.id, status = NodepoolStatus.Running).save()
    val savedApp1 = makeApp(1, savedNodepool1.id, status = AppStatus.Running).save()
    val res = KubernetesServiceDbQueries
      .getActiveFullAppByWorkspaceIdAndAppName(
        savedCluster1.workspaceId.getOrElse(WorkspaceId(UUID.randomUUID())),
        savedApp1.appName,
        Map.empty
      )
      .transaction
      .unsafeRunSync()(IORuntime.global)

    res.isDefined shouldBe true
  }
}
