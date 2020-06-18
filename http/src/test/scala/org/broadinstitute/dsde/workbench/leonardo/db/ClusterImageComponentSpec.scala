package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, RStudio}
import CommonTestData._
import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

class ClusterImageComponentSpec extends AnyFlatSpecLike with TestComponent {

  "ClusterImageComponent" should "save and get" in isolatedDbTest {
    val cluster = makeCluster(1).save()

    dbFutureValue(clusterImageQuery.get(cluster.id, Jupyter)) shouldBe Some(jupyterImage)
    dbFutureValue(clusterImageQuery.get(cluster.id, RStudio)) shouldBe None
    dbFutureValue(clusterImageQuery.save(cluster.id, rstudioImage))
    dbFutureValue(clusterImageQuery.get(cluster.id, Jupyter)) shouldBe Some(jupyterImage)
    dbFutureValue(clusterImageQuery.get(cluster.id, RStudio)) shouldBe Some(rstudioImage)
  }

  it should "save and get all for cluster" in isolatedDbTest {
    val cluster = makeCluster(1).save()

    dbFutureValue(clusterImageQuery.getAllForCluster(cluster.id)).toSet shouldBe cluster.runtimeImages
    dbFutureValue(clusterImageQuery.saveAllForCluster(cluster.id, Seq(rstudioImage)))
    dbFutureValue(clusterImageQuery.getAllForCluster(cluster.id)).toSet shouldBe (cluster.runtimeImages + rstudioImage)
    dbFutureValue(clusterImageQuery.getAllForCluster(-1)).toSet shouldBe Set.empty
  }

  it should "upsert" in isolatedDbTest {
    val cluster = makeCluster(1).save()

    dbFutureValue(clusterImageQuery.getAllForCluster(cluster.id)).toSet shouldBe cluster.runtimeImages

    val newImage = jupyterImage.copy(imageUrl = "newImageString")
    dbFutureValue(clusterImageQuery.upsert(cluster.id, newImage))

    val expectedImages = (cluster.runtimeImages - jupyterImage) + newImage
    dbFutureValue(clusterImageQuery.getAllForCluster(cluster.id)).toSet shouldBe expectedImages
  }

}
