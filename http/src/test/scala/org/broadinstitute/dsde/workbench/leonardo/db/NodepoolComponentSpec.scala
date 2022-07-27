package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.auditInfo
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils._
import org.broadinstitute.dsde.workbench.leonardo.NodepoolStatus
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.leonardo.CloudContext
import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

class NodepoolComponentSpec extends AnyFlatSpecLike with TestComponent {

  it should "save, get, delete" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()
    // we never use this, but we want other nodepools in DB to ensure our queries successfully pull the ones associated with this cluster only
    makeKubeCluster(2).save()

    val nodepool1 = makeNodepool(2, savedCluster1.id)
    val nodepool2 = makeNodepool(3, savedCluster1.id)

    val savedNodepool1 = nodepool1.save()
    val savedNodepool2 = nodepool2.save()

    nodepool1 shouldEqual savedNodepool1
    nodepool2 shouldEqual savedNodepool2

    val clusterFromDb = dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id))

    clusterFromDb.map(_.nodepools.size) shouldEqual Some(3)
    clusterFromDb.map(_.nodepools).getOrElse(List()) should contain(savedNodepool1)
    clusterFromDb.map(_.nodepools).getOrElse(List()) should contain(savedNodepool2)

    val now = Instant.now()
    dbFutureValue(nodepoolQuery.markAsDeleted(savedNodepool2.id, now)) shouldBe 1
    val nodepoolGetAll2 =
      dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id)).map(_.nodepools).get
    nodepoolGetAll2.size shouldBe 2
    nodepoolGetAll2 should contain(savedNodepool1)
    nodepoolGetAll2 should not contain savedNodepool2

    val deletedNodepoolGet =
      dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id, includeDeletedNodepool = true))
    deletedNodepoolGet.get.nodepools should contain
    savedNodepool2.copy(status = NodepoolStatus.Deleted,
                        auditInfo = savedNodepool2.auditInfo.copy(destroyedDate = Some(now))
    )

    dbFutureValue(nodepoolQuery.markActiveAsDeletedForCluster(savedCluster1.id, now)) shouldBe 2
    dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id)).map(_.nodepools) shouldBe Some(List())
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
    savedNodepool1.status shouldBe NodepoolStatus.Unspecified

    dbFutureValue(nodepoolQuery.updateStatus(savedNodepool1.id, NodepoolStatus.Provisioning)) shouldBe 1

    dbFutureValue(kubernetesClusterQuery.getMinimalClusterById(savedCluster1.id)).get.nodepools should contain(
      savedNodepool1.copy(status = NodepoolStatus.Provisioning)
    )
  }

  it should "return a nodepool by user" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val realUser = WorkbenchEmail("real@gmail.com")
    val nodepool1 = makeNodepool(1, cluster1.id).copy(auditInfo = auditInfo.copy(creator = realUser)).save()

    val nodepoolOpt =
      dbFutureValue(nodepoolQuery.getMinimalByUserAndConfig(realUser, cluster1.cloudContext, kubernetesRuntimeConfig))
    nodepoolOpt.isDefined shouldBe true
    nodepoolOpt.map(_.id) shouldBe Some(nodepool1.id)
  }

  it should "return a nodepool by user when the user has 2 apps" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()

    val realUser = WorkbenchEmail("real@gmail.com")
    val nodepool1 = makeNodepool(1, cluster1.id).copy(auditInfo = auditInfo.copy(creator = realUser)).save()

    makeApp(1, nodepool1.id).copy(auditInfo = auditInfo.copy(creator = realUser)).save()
    makeApp(2, nodepool1.id).copy(auditInfo = auditInfo.copy(creator = realUser)).save()

    val nodepoolOpt =
      dbFutureValue(nodepoolQuery.getMinimalByUserAndConfig(realUser, cluster1.cloudContext, kubernetesRuntimeConfig))
    nodepoolOpt.isDefined shouldBe true
    nodepoolOpt.map(_.id) shouldBe Some(nodepool1.id)
  }

  it should "not return a nodepool by user when it does not exist" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()

    val fakeUser = WorkbenchEmail("fake@gmail.com")

    val nodepoolOpt =
      dbFutureValue(nodepoolQuery.getMinimalByUserAndConfig(fakeUser, cluster1.cloudContext, kubernetesRuntimeConfig))
    nodepoolOpt.isDefined shouldBe false
  }

  it should "not return a nodepool in a different project from a user" in isolatedDbTest {
    val cluster1 = makeKubeCluster(1).save()
    val realUser = WorkbenchEmail("real@gmail.com")
    makeNodepool(1, cluster1.id).copy(auditInfo = auditInfo.copy(creator = realUser)).save()

    val nodepoolOpt = dbFutureValue(
      nodepoolQuery.getMinimalByUserAndConfig(realUser,
                                              CloudContext.Gcp(GoogleProject("fake project")),
                                              kubernetesRuntimeConfig
      )
    )
    nodepoolOpt.isDefined shouldBe false
  }

}
