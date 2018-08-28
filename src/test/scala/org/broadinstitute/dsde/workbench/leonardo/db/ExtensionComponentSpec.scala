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
    val savedCluster1 = makeCluster(1).copy(jupyterExtensionUri = Some(jupyterExtensionUri),
                                       jupyterUserScriptUri = Some(jupyterUserScriptUri)).save()

    val savedCluster2 = makeCluster(2).save()

    val missingId = Random.nextLong()
    dbFutureValue { _.extensionQuery.getAllForCluster(missingId) } shouldEqual UserJupyterExtensionConfig(Map(), Map(), Map())
    dbFailure { _.extensionQuery.save(missingId, ExtensionType.NBExtension.toString, "extName", "extValue") } shouldBe a [SQLException]

    dbFutureValue { _.extensionQuery.saveAllForCluster(savedCluster1.id, Some(userExtConfig)) }
    dbFutureValue { _.extensionQuery.getAllForCluster(savedCluster1.id) } shouldEqual userExtConfig

    dbFutureValue { _.extensionQuery.save(savedCluster2.id, ExtensionType.NBExtension.toString, "extName", "extValue") } shouldBe 1
  }
}
