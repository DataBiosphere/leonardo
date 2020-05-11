package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.scalatest.FlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class NamespaceComponentSpec extends FlatSpecLike with TestComponent {

  "NamespaceComponent" should "save, getAll, delete" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()

    dbFutureValue(namespaceQuery.save(savedCluster1.id, namespace0)) shouldBe 1
    dbFutureValue(namespaceQuery.getAllForCluster(savedCluster1.id)) shouldBe Set(namespace0)

    dbFutureValue(namespaceQuery.delete(savedCluster1.id, namespace0)) shouldBe 1
  }

  "NamespaceComponent" should "saveAll, deleteAll" in isolatedDbTest {
    val savedCluster1 = makeKubeCluster(1).save()

    val namespaces = Set(namespace0, namespace1)

    dbFutureValue(namespaceQuery.saveAllForCluster(savedCluster1.id, namespaces))
    dbFutureValue(namespaceQuery.getAllForCluster(savedCluster1.id)) shouldEqual namespaces

    dbFutureValue(namespaceQuery.deleteAllForCluster(savedCluster1.id)) shouldBe 2
    dbFutureValue(namespaceQuery.getAllForCluster(savedCluster1.id)) shouldEqual Set()
  }
}
