package org.broadinstitute.dsde.workbench.leonardo.db

import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GceInstanceStatus, GcsPathUtils, RuntimeConfigId}
import CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.clusterEq
import org.broadinstitute.dsde.workbench.model.IP

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

/**
 * Created by rtitle on 2/19/18.
 */
class InstanceComponentSpec extends AnyFlatSpecLike with TestComponent with GcsPathUtils {

  val cluster1 = makeCluster(1)

  "InstanceComponent" should "save and get instances" in isolatedDbTest {
    val savedCluster1 = cluster1.save()
    savedCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual cluster1

    dbFutureValue(instanceQuery.save(savedCluster1.id, masterInstance)) shouldEqual 1
    dbFutureValue(instanceQuery.getInstanceByKey(masterInstance.key)) shouldEqual Some(masterInstance)
  }

  it should "update status and ip" in isolatedDbTest {
    val savedCluster1 = cluster1.save()
    savedCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual cluster1

    dbFutureValue(instanceQuery.save(savedCluster1.id, masterInstance)) shouldEqual 1
    dbFutureValue {
      instanceQuery.updateStatusAndIpForCluster(savedCluster1.id, GceInstanceStatus.Provisioning, Some(IP("4.5.6.7")))
    } shouldEqual 1
    val updated = dbFutureValue(instanceQuery.getInstanceByKey(masterInstance.key))
    updated shouldBe defined
    updated.get.status shouldBe GceInstanceStatus.Provisioning
    updated.get.ip shouldBe Some(IP("4.5.6.7"))
  }

}
