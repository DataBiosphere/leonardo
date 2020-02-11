package org.broadinstitute.dsde.workbench.leonardo
package db

import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.scalatest.FlatSpecLike
import org.scalatest.concurrent.ScalaFutures
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._

import LeonardoServiceDbQueries._
import scala.concurrent.ExecutionContext.Implicits.global

class LeonardoServiceDbQueriesSpec extends TestComponent with FlatSpecLike with GcsPathUtils with ScalaFutures {
  it should "get by labels" in isolatedDbTest {

    val savedCluster1 =
      makeCluster(1).copy(labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar")).save(Some(serviceAccountKey.id))

    val savedCluster2 = makeCluster(2).copy(status = ClusterStatus.Running).save(Some(serviceAccountKey.id))

    val savedCluster3 =
      makeCluster(3).copy(status = ClusterStatus.Deleted, labels = Map("a" -> "b", "bam" -> "yes")).save()

    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map.empty, false) }.toSet shouldEqual Set(savedCluster1, savedCluster2)
      .map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "yes"), false) }.toSet shouldEqual Set(savedCluster1).map(
      _.toListClusterResp(defaultRuntimeConfig)
    )
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "no"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "yes", "vcf" -> "no"), false) }.toSet shouldEqual Set(
      savedCluster1.toListClusterResp(defaultRuntimeConfig)
    )
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("foo" -> "bar", "vcf" -> "no"), false) }.toSet shouldEqual Set(
      savedCluster1
    ).map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), false) }.toSet shouldEqual Set(
      savedCluster1.toListClusterResp(defaultRuntimeConfig)
    )
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("a" -> "b"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "yes", "a" -> "b"), false) }.toSet shouldEqual Set
      .empty[Cluster]
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "yes", "a" -> "c"), false) }.toSet shouldEqual Set
      .empty[Cluster]
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "yes", "vcf" -> "no"), true) }.toSet shouldEqual Set(
      savedCluster1
    ).map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("foo" -> "bar", "vcf" -> "no"), true) }.toSet shouldEqual Set(
      savedCluster1
    ).map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), true) }.toSet shouldEqual Set(
      savedCluster1
    ).map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("a" -> "b"), true) }.toSet shouldEqual Set(savedCluster3).map(
      _.toListClusterResp(defaultRuntimeConfig)
    )
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "yes", "a" -> "b"), true) }.toSet shouldEqual Set(
      savedCluster3
    ).map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "yes", "a" -> "c"), true) }.toSet shouldEqual Set
      .empty[Cluster]
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bogus" -> "value"), true) }.toSet shouldEqual Set.empty[Cluster]
  }

  it should "list by labels and project" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .copy(labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"))
      .save(Some(serviceAccountKey.id))

    val savedCluster2 = makeCluster(2)
      .copy(
        status = ClusterStatus.Running,
        clusterName = name2,
        googleProject = project2,
        clusterUrl = Cluster.getClusterUrl(project2, name2, Set(jupyterImage), Map("bam" -> "yes")),
        labels = Map("bam" -> "yes")
      )
      .save(Some(serviceAccountKey.id))

    val savedCluster3 = makeCluster(3)
      .copy(status = ClusterStatus.Deleted, labels = Map("a" -> "b", "bam" -> "yes"))
      .save()

    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map.empty, false, Some(project)) }.toSet shouldEqual Set(savedCluster1)
      .map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map.empty, true, Some(project)) }.toSet shouldEqual Set(
      savedCluster1,
      savedCluster3
    ).map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map.empty, false, Some(project2)) }.toSet shouldEqual Set(savedCluster2)
      .map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "yes"), true, Some(project)) }.toSet shouldEqual Set(
      savedCluster1,
      savedCluster3
    ).map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("bam" -> "yes"), false, Some(project2)) }.toSet shouldEqual Set(
      savedCluster2
    ).map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("a" -> "b"), true, Some(project)) }.toSet shouldEqual Set(
      savedCluster3
    ).map(_.toListClusterResp(defaultRuntimeConfig))
    dbFutureValue { getClustersByLabelsWithRuntimeConfig(Map("a" -> "b"), true, Some(project2)) }.toSet shouldEqual Set
      .empty[Cluster]
  }
}
