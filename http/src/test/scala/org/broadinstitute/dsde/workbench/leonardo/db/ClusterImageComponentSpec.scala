package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterImageType.{Jupyter, RStudio}
import org.scalatest.FlatSpecLike
import CommonTestData._

class ClusterImageComponentSpec extends TestComponent with FlatSpecLike {

  "ClusterImageComponent" should "save and get" in isolatedDbTest {
    val cluster = makeCluster(1).save()

    dbFutureValue { dbRef.dataAccess.clusterImageQuery.get(cluster.id, Jupyter) } shouldBe Some(jupyterImage)
    dbFutureValue { dbRef.dataAccess.clusterImageQuery.get(cluster.id, RStudio) } shouldBe None
    dbFutureValue { dbRef.dataAccess.clusterImageQuery.save(cluster.id, rstudioImage) }
    dbFutureValue { dbRef.dataAccess.clusterImageQuery.get(cluster.id, Jupyter) } shouldBe Some(jupyterImage)
    dbFutureValue { dbRef.dataAccess.clusterImageQuery.get(cluster.id, RStudio) } shouldBe Some(rstudioImage)
  }

  it should "save and get all for cluster" in isolatedDbTest {
    val cluster = makeCluster(1).save()

    dbFutureValue { dbRef.dataAccess.clusterImageQuery.getAllForCluster(cluster.id) }.toSet shouldBe Set(jupyterImage)
    dbFutureValue { dbRef.dataAccess.clusterImageQuery.saveAllForCluster(cluster.id, Seq(rstudioImage)) }
    dbFutureValue { dbRef.dataAccess.clusterImageQuery.getAllForCluster(cluster.id) }.toSet shouldBe Set(jupyterImage, rstudioImage)
    dbFutureValue { dbRef.dataAccess.clusterImageQuery.getAllForCluster(-1) }.toSet shouldBe Set.empty
  }

  it should "upsert" in isolatedDbTest {
    val cluster = makeCluster(1).save()

    dbFutureValue { dbRef.dataAccess.clusterImageQuery.getAllForCluster(cluster.id) }.toSet shouldBe Set(jupyterImage)

    val newImage = jupyterImage.copy(imageUrl = "newImageString")
    dbFutureValue { dbRef.dataAccess.clusterImageQuery.upsert(cluster.id, newImage) }

    dbFutureValue { dbRef.dataAccess.clusterImageQuery.getAllForCluster(cluster.id) }.toSet shouldBe Set(newImage)
  }

}
