package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException

import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.GcsPathUtils
import org.scalatest.FlatSpecLike
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

class ExtensionComponentSpec extends TestComponent with FlatSpecLike with GcsPathUtils {
  "ExtensionComponent" should "save, get,and delete" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .copy(jupyterExtensionUri = Some(jupyterExtensionUri), jupyterUserScriptUri = Some(jupyterUserScriptUri))
      .save()

    val savedCluster2 = makeCluster(2).save()

    val missingId = Random.nextLong()
    dbFutureValue { extensionQuery.getAllForCluster(missingId) } shouldEqual UserJupyterExtensionConfig(Map(),
                                                                                                        Map(),
                                                                                                        Map())
    dbFailure { extensionQuery.save(missingId, ExtensionType.NBExtension.toString, "extName", "extValue") } shouldBe a[
      SQLException
    ]

    dbFutureValue { extensionQuery.saveAllForCluster(savedCluster1.id, Some(userExtConfig)) }
    dbFutureValue { extensionQuery.getAllForCluster(savedCluster1.id) } shouldEqual userExtConfig

    dbFutureValue { extensionQuery.save(savedCluster2.id, ExtensionType.NBExtension.toString, "extName", "extValue") } shouldBe 1
  }
}
