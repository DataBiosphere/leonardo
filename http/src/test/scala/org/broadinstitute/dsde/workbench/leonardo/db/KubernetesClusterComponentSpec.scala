package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils._
import org.broadinstitute.dsde.workbench.leonardo.{
  KubernetesClusterAsyncFields,
  KubernetesClusterStatus,
  NodepoolStatus
}
import org.scalatest.flatspec.AnyFlatSpecLike

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.ExecutionContext.Implicits.global

class KubernetesClusterComponentSpec extends AnyFlatSpecLike with TestComponent {

  it should "save, get, and delete" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1)
    val cluster2 = makeKubeCluster(2)

    val savedCluster1 = cluster1.save()
    val savedCluster2 = cluster2.save()

    savedCluster1 shouldEqual cluster1
    savedCluster2 shouldEqual cluster2

    dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id)) shouldEqual Some(savedCluster1)
    dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster2.id)) shouldEqual Some(savedCluster2)

    dbFutureValue(
      kubernetesClusterQuery.getMinimalActiveClusterByCloudContext(savedCluster1.cloudContext)
    ) shouldEqual Some(savedCluster1)
    dbFutureValue(
      kubernetesClusterQuery.getMinimalActiveClusterByCloudContext(savedCluster2.cloudContext)
    ) shouldEqual Some(savedCluster2)

    // should delete the cluster and initial nodepool, hence '2' records updated
    val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
    dbFutureValue(kubernetesClusterQuery.markAsDeleted(savedCluster1.id, now)) shouldBe 2
    dbFutureValue(kubernetesClusterQuery.markAsDeleted(savedCluster2.id, now)) shouldBe 2

    val getDeletedCluster1 =
      dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id, includeDeletedNodepool = true))
    getDeletedCluster1.map(_.status) shouldEqual Some(KubernetesClusterStatus.Deleted)
    getDeletedCluster1.map(_.auditInfo.destroyedDate) shouldEqual Some(Some(now))
    getDeletedCluster1.map(_.nodepools.map(_.status)) shouldEqual Some(
      savedCluster1.nodepools.map(_.status).map(_ => NodepoolStatus.Deleted)
    )

    dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster2.id)).map(_.status) shouldEqual Some(
      KubernetesClusterStatus.Deleted
    )

  }

  it should "aggregate all sub tables on get, and clean up all tables on delete" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(2, savedCluster1.id).save()

    val getCluster = dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id))
    getCluster.map(c => c.copy(nodepools = c.nodepools.sortBy(_.nodepoolName.value))) shouldEqual Some(
      savedCluster1
        .copy(nodepools = (savedCluster1.nodepools ++ List(savedNodepool1)).sortBy(_.nodepoolName.value))
    )

    // we expect 3 records to be affected by the delete: 2 nodepools, 1 cluster
    dbFutureValue(kubernetesClusterQuery.markAsDeleted(savedCluster1.id, Instant.now())) shouldBe 3
  }

  it should "have 1 nodepool when initialized" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    savedCluster1.nodepools.size shouldBe 1
  }

  it should "prevent duplicate (googleProject, destroyedDate) kubernetes clusters" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1)

    cluster1.save()
    val caught = the[java.sql.SQLIntegrityConstraintViolationException] thrownBy {
      cluster1.save()
    }
    caught.getMessage should include("IDX_KUBERNETES_CLUSTER_UNIQUE")
  }

  it should "update async fields" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()

    val newAsyncFields = KubernetesClusterAsyncFields(loadBalancerIp, apiServerIp, networkFields)
    assert(!savedCluster1.asyncFields.contains(newAsyncFields))

    dbFutureValue(kubernetesClusterQuery.updateAsyncFields(savedCluster1.id, newAsyncFields)) shouldBe 1
    val updatedCluster1 = dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id))

    updatedCluster1 shouldBe Some(savedCluster1.copy(asyncFields = Some(newAsyncFields)))
  }

  it should "update status" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()

    dbFutureValue(kubernetesClusterQuery.updateStatus(savedCluster1.id, KubernetesClusterStatus.Provisioning))
    val updatedCluster1 = dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id))
    updatedCluster1 shouldBe Some(savedCluster1.copy(status = KubernetesClusterStatus.Provisioning))
  }
}
