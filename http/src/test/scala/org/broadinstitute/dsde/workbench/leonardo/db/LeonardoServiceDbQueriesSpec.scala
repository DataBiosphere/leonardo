package org.broadinstitute.dsde.workbench.leonardo
package db

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.db.LeonardoServiceDbQueries._
import org.broadinstitute.dsde.workbench.leonardo.http.service.ListRuntimeResponse
import org.scalatest.FlatSpecLike
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global

class LeonardoServiceDbQueriesSpec extends FlatSpecLike with TestComponent with GcsPathUtils with ScalaFutures {
  it should "get by labels" in isolatedDbTest {

    val savedCluster1 =
      makeCluster(1).copy(labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar")).save(Some(serviceAccountKey.id))

    val savedCluster2 = makeCluster(2).copy(status = RuntimeStatus.Running).save(Some(serviceAccountKey.id))

    val savedCluster3 =
      makeCluster(3).copy(status = RuntimeStatus.Deleted, labels = Map("a" -> "b", "bam" -> "yes")).save()

    dbFutureValue { listClusters(Map.empty, false) }.toSet shouldEqual Set(savedCluster1, savedCluster2)
      .map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map("bam" -> "yes"), false) }.toSet shouldEqual Set(savedCluster1).map(
      r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig)
    )
    dbFutureValue { listClusters(Map("bam" -> "no"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { listClusters(Map("bam" -> "yes", "vcf" -> "no"), false) }.toSet shouldEqual Set(
      ListRuntimeResponse.fromRuntime(savedCluster1, defaultRuntimeConfig)
    )
    dbFutureValue { listClusters(Map("foo" -> "bar", "vcf" -> "no"), false) }.toSet shouldEqual Set(
      savedCluster1
    ).map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), false) }.toSet shouldEqual Set(
      ListRuntimeResponse.fromRuntime(savedCluster1, defaultRuntimeConfig)
    )
    dbFutureValue { listClusters(Map("a" -> "b"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { listClusters(Map("bam" -> "yes", "a" -> "b"), false) }.toSet shouldEqual Set
      .empty[Cluster]
    dbFutureValue { listClusters(Map("bam" -> "yes", "a" -> "c"), false) }.toSet shouldEqual Set
      .empty[Cluster]
    dbFutureValue { listClusters(Map("bam" -> "yes", "vcf" -> "no"), true) }.toSet shouldEqual Set(
      savedCluster1
    ).map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map("foo" -> "bar", "vcf" -> "no"), true) }.toSet shouldEqual Set(
      savedCluster1
    ).map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), true) }.toSet shouldEqual Set(
      savedCluster1
    ).map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map("a" -> "b"), true) }.toSet shouldEqual Set(savedCluster3).map(
      r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig)
    )
    dbFutureValue { listClusters(Map("bam" -> "yes", "a" -> "b"), true) }.toSet shouldEqual Set(
      savedCluster3
    ).map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map("bam" -> "yes", "a" -> "c"), true) }.toSet shouldEqual Set
      .empty[Cluster]
    dbFutureValue { listClusters(Map("bogus" -> "value"), true) }.toSet shouldEqual Set.empty[Cluster]
  }

  it should "list by labels and project" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .copy(labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"))
      .save(Some(serviceAccountKey.id))

    val savedCluster2 = makeCluster(2)
      .copy(
        status = RuntimeStatus.Running,
        runtimeName = name2,
        googleProject = project2,
        proxyUrl = Runtime.getProxyUrl(proxyUrlBase, project2, name2, Set(jupyterImage), Map("bam" -> "yes")),
        labels = Map("bam" -> "yes")
      )
      .save(Some(serviceAccountKey.id))

    val savedCluster3 = makeCluster(3)
      .copy(status = RuntimeStatus.Deleted, labels = Map("a" -> "b", "bam" -> "yes"))
      .save()

    dbFutureValue { listClusters(Map.empty, false, Some(project)) }.toSet shouldEqual Set(savedCluster1)
      .map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map.empty, true, Some(project)) }.toSet shouldEqual Set(
      savedCluster1,
      savedCluster3
    ).map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map.empty, false, Some(project2)) }.toSet shouldEqual Set(savedCluster2)
      .map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map("bam" -> "yes"), true, Some(project)) }.toSet shouldEqual Set(
      savedCluster1,
      savedCluster3
    ).map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map("bam" -> "yes"), false, Some(project2)) }.toSet shouldEqual Set(
      savedCluster2
    ).map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map("a" -> "b"), true, Some(project)) }.toSet shouldEqual Set(
      savedCluster3
    ).map(r => ListRuntimeResponse.fromRuntime(r, defaultRuntimeConfig))
    dbFutureValue { listClusters(Map("a" -> "b"), true, Some(project2)) }.toSet shouldEqual Set
      .empty[Cluster]
  }
}
