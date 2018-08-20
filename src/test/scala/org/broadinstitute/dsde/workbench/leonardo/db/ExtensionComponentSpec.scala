package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterStatus, IP, MachineConfig, OperationName}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.FlatSpecLike

import scala.util.Random

class ExtensionComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils{
  "ExtensionComponent" should "save, get,and delete" in isolatedDbTest {
    val cluster1 = getCluster(1).copy(jupyterExtensionUri = Some(jupyterExtensionUri),
                                       jupyterUserScriptUri = Some(jupyterUserScriptUri))

    val cluster2 = getCluster(2)

    val missingId = Random.nextLong()
    dbFutureValue { _.extensionQuery.getAllForCluster(missingId) } shouldEqual UserJupyterExtensionConfig(Map(), Map(), Map())
    dbFailure { _.extensionQuery.save(missingId, ExtensionType.NBExtension.toString, "extName", "extValue") } shouldBe a [SQLException]

    val savedCluster1 = dbFutureValue { _.clusterQuery.save(cluster1, Option(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) }
    savedCluster1 shouldEqual cluster1
    dbFutureValue { _.extensionQuery.saveAllForCluster(savedCluster1.id, Some(userExtConfig)) }
    dbFutureValue { _.extensionQuery.getAllForCluster(savedCluster1.id) } shouldEqual userExtConfig

    val savedCluster2 = dbFutureValue { _.clusterQuery.save(cluster2, Option(gcsPath("gs://bucket2")), Some(serviceAccountKey.id)) }
    savedCluster2 shouldEqual cluster2
    dbFutureValue { _.extensionQuery.save(savedCluster2.id, ExtensionType.NBExtension.toString, "extName", "extValue") } shouldBe 1
  }
}
