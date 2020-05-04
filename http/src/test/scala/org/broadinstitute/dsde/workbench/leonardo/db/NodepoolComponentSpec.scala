package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils._
import org.broadinstitute.dsde.workbench.leonardo.{NodepoolStatus}
import org.scalatest.FlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class NodepoolComponentSpec extends FlatSpecLike with TestComponent {

  "NodepoolComponent" should "save, get, delete" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    //we never use this, but we want other nodepools in DB to ensure our queries successfully pull the ones associated with this cluster only
    makeKubeCluster(2).save()

    val nodepool1 = makeNodepool(2, savedCluster1.id)
    val nodepool2 = makeNodepool(3, savedCluster1.id)

    val savedNodepool1 = nodepool1.save()
    val savedNodepool2 = nodepool2.save()

    nodepool1 shouldEqual savedNodepool1
    nodepool2 shouldEqual savedNodepool2

    val nodepoolGetAll1 = dbFutureValue(nodepoolQuery.getAllForCluster(savedCluster1.id))

    //the 2 we saved plus initial nodepool
    nodepoolGetAll1.size shouldBe 3
    nodepoolGetAll1 should contain(savedNodepool1)
    nodepoolGetAll1 should contain(savedNodepool2)

    val clusterFromDb = dbFutureValue(kubernetesClusterQuery.getFullClusterById(savedCluster1.id))
    clusterFromDb.map(_.nodepools.size) shouldEqual Some(3)
    clusterFromDb.map(_.nodepools).getOrElse(Set()) should contain(savedNodepool1)
    clusterFromDb.map(_.nodepools).getOrElse(Set()) should contain(savedNodepool2)

    dbFutureValue(nodepoolQuery.delete(savedNodepool2.id)) shouldBe 1
    val nodepoolGetAll2 = dbFutureValue(nodepoolQuery.getAllForCluster(savedCluster1.id))
    nodepoolGetAll2.size shouldBe 2
    nodepoolGetAll2 should not contain(savedNodepool2)
    nodepoolGetAll2 should contain(savedNodepool1)

    dbFutureValue(nodepoolQuery.deleteAllForCluster(savedCluster1.id)) shouldBe 2
    dbFutureValue(nodepoolQuery.getAllForCluster(savedCluster1.id)) shouldBe Set()
  }

  it should "prevent duplicate (clusterId, nodepoolName) nodepools" in isolatedDbTest {
    val clusterId = makeKubeCluster(1).save().id
    val nodepool1 = makeNodepool(2, clusterId)

    nodepool1.save()
    val caught = the[java.sql.SQLIntegrityConstraintViolationException] thrownBy {
      nodepool1.save()
    }

    caught.getMessage should include("IDX_NODEPOOL_UNIQUE")
  }

  it should "update status" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()

    val savedNodepool1 = makeNodepool(3, savedCluster1.id).save()
    savedNodepool1.status shouldBe NodepoolStatus.Status_Unspecified

    dbFutureValue(nodepoolQuery.updateStatus(savedNodepool1.id, NodepoolStatus.Provisioning)) shouldBe 1

    dbFutureValue(nodepoolQuery.getAllForCluster(savedCluster1.id)) should contain(savedNodepool1.copy(status = NodepoolStatus.Provisioning))
  }

  it should "update destroyed date" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()

    val savedNodepool1 = makeNodepool(3, savedCluster1.id).save()
    savedNodepool1.auditInfo.destroyedDate shouldBe None

    val newDestroyedDate = Instant.now()
    dbFutureValue(nodepoolQuery.updateDestroyedDate(savedNodepool1.id, newDestroyedDate)) shouldBe 1

    dbFutureValue(nodepoolQuery.getById(savedNodepool1.id)) shouldEqual Some(savedNodepool1.copy(auditInfo = savedNodepool1.auditInfo.copy(destroyedDate = Some(newDestroyedDate))))
  }
}
