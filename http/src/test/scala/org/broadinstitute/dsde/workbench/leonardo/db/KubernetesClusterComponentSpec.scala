package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils._
import org.broadinstitute.dsde.workbench.leonardo.{KubernetesClusterAsyncFields, KubernetesClusterStatus}
import org.scalatest.FlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class KubernetesClusterComponentSpec extends FlatSpecLike with TestComponent {

  "KubernetesClusterComponent" should "save, get, and delete" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1)
    val cluster2 = makeKubeCluster(2)

    val savedCluster1 = cluster1.save()
    val savedCluster2 = cluster2.save()

    savedCluster1 shouldEqual cluster1
    savedCluster2 shouldEqual cluster2

    dbFutureValue(kubernetesClusterQuery.getFullClusterById(savedCluster1.id)) shouldEqual Some(savedCluster1)
    dbFutureValue(kubernetesClusterQuery.getFullClusterById(savedCluster2.id)) shouldEqual Some(savedCluster2)

    dbFutureValue(kubernetesClusterQuery.getActiveFullClusterByName(savedCluster1.googleProject, savedCluster1.clusterName)) shouldEqual Some(savedCluster1)
    dbFutureValue(kubernetesClusterQuery.getActiveFullClusterByName(savedCluster2.googleProject, savedCluster2.clusterName)) shouldEqual Some(savedCluster2)

    //should delete the cluster and initial nodepool, hence '2' records deleted
    dbFutureValue(kubernetesClusterQuery.delete(savedCluster1.id)) shouldBe 2
    dbFutureValue(kubernetesClusterQuery.delete(savedCluster2.id)) shouldBe 2
  }

  it should "aggregate all sub tables on get, and clean up all tables on delete" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    val savedNodepool1 = makeNodepool(2, savedCluster1.id).save()
    val namespaceSet = Set(namespace1, namespace0)
    dbFutureValue(namespaceQuery.saveAllForCluster(savedCluster1.id, namespaceSet))

    val getCluster = dbFutureValue(kubernetesClusterQuery.getFullClusterById(savedCluster1.id))
    getCluster shouldEqual Some(savedCluster1
      .copy(
        nodepools = savedCluster1.nodepools + savedNodepool1,
        namespaces = namespaceSet
    ))

    //we expect 5 records to be deleted: 2 namespaces, 2 nodepools, 1 cluster
    dbFutureValue(kubernetesClusterQuery.delete(savedCluster1.id)) shouldBe 5
  }

  it should "have 1 nodepool when initialized" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    savedCluster1.nodepools.size shouldBe 1
  }

  it should "prevent duplicate (googleProject, clusterName, destroyedDate) kubernetes clusters" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1)

    cluster1.save()
    val caught = the[java.sql.SQLIntegrityConstraintViolationException] thrownBy {
      cluster1.save()
    }
    caught.getMessage should include("IDX_KUBERNETES_CLUSTER_UNIQUE")
  }

  it should "update async fields" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()

    val newAsyncFields = KubernetesClusterAsyncFields(apiServerIp, networkFields)
    assert(savedCluster1.asyncFields != Some(newAsyncFields))

    dbFutureValue(kubernetesClusterQuery.updateAsyncFields(savedCluster1.id, newAsyncFields)) shouldBe 1
    val updatedCluster1 = dbFutureValue(kubernetesClusterQuery.getFullClusterById(savedCluster1.id))

    updatedCluster1 shouldBe Some(savedCluster1.copy(asyncFields = Some(newAsyncFields)))
  }

  it should "update destroyed date" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    savedCluster1.auditInfo.destroyedDate shouldBe None

    val newDate = Instant.now()
    dbFutureValue(kubernetesClusterQuery.updateDestroyedDate(savedCluster1.id, newDate))
    val updatedCluster1 = dbFutureValue(kubernetesClusterQuery.getFullClusterById(savedCluster1.id))

    updatedCluster1 shouldBe Some(savedCluster1.copy(auditInfo = savedCluster1.auditInfo.copy(destroyedDate = Some(newDate))))
    dbFutureValue(kubernetesClusterQuery.getActiveFullClusterByName(savedCluster1.googleProject, savedCluster1.clusterName)) shouldBe None
  }

  it should "update status" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()

    dbFutureValue(kubernetesClusterQuery.updateStatus(savedCluster1.id, KubernetesClusterStatus.Provisioning))
    val updatedCluster1 = dbFutureValue(kubernetesClusterQuery.getFullClusterById(savedCluster1.id))
    updatedCluster1 shouldBe Some(savedCluster1.copy(status =KubernetesClusterStatus.Provisioning))
  }
}
