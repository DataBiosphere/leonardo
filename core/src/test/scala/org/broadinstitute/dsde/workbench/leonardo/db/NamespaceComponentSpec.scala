package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

class NamespaceComponentSpec extends AnyFlatSpecLike with TestComponent {

  "NamespaceComponent" should "save, getAll, delete" in isolatedDbTest { implicit dbRef =>
    val savedCluster1 = makeKubeCluster(1).save()

    dbFutureValue(namespaceQuery.save(savedCluster1.id, namespace0))
    dbFutureValue(namespaceQuery.getAllForCluster(savedCluster1.id)).map(_.name) shouldBe List(namespace0)

    dbFutureValue(namespaceQuery.delete(savedCluster1.id, namespace0)) shouldBe 1
  }

  "NamespaceComponent" should "saveAll, deleteAll" in isolatedDbTest { implicit dbRef =>
    val savedCluster1 = makeKubeCluster(1).save()

    val namespaceNames = List(namespace0, namespace1)

    dbFutureValue(namespaceQuery.saveAllForCluster(savedCluster1.id, namespaceNames))
    dbFutureValue(namespaceQuery.getAllForCluster(savedCluster1.id)).map(_.name) shouldEqual namespaceNames

    dbFutureValue(namespaceQuery.deleteAllForCluster(savedCluster1.id)) shouldBe 2
    dbFutureValue(namespaceQuery.getAllForCluster(savedCluster1.id)) shouldEqual List()
  }
}
