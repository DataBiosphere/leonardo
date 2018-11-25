package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool.{Jupyter, RStudio}
import org.scalatest.FlatSpecLike

class ClusterImageComponentSpec extends TestComponent with FlatSpecLike with CommonTestData {

  "ClusterImageComponent" should "save and get" in isolatedDbTest {
    val cluster = makeCluster(1).save()

    dbFutureValue { _.clusterImageQuery.save(cluster.id, jupyterImage) }
    dbFutureValue { _.clusterImageQuery.get(cluster.id, Jupyter) } shouldBe Some(jupyterImage)
    dbFutureValue { _.clusterImageQuery.get(cluster.id, RStudio) } shouldBe None
  }

  it should "save and get all for cluster" in isolatedDbTest {
    val cluster = makeCluster(1).save()

    dbFutureValue { _.clusterImageQuery.saveAllForCluster(cluster.id, Seq(jupyterImage, rstudioImage)) }
    dbFutureValue { _.clusterImageQuery.getAllForCluster(cluster.id) }.toSet shouldBe Set(jupyterImage, rstudioImage)
    dbFutureValue { _.clusterImageQuery.getAllForCluster(-1) }.toSet shouldBe Set.empty
  }

}
