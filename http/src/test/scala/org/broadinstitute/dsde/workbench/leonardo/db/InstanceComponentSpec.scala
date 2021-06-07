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
    dbFutureValue(instanceQuery.getAllForCluster(savedCluster1.id)) shouldEqual Seq(masterInstance)
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

  it should "merge instances" in isolatedDbTest {
    val savedCluster1 = cluster1.save()
    savedCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual cluster1
    dbFutureValue(instanceQuery.save(savedCluster1.id, masterInstance)) shouldEqual 1

    val addedWorkers = Seq(masterInstance, workerInstance1, workerInstance2)
    dbFutureValue(instanceQuery.mergeForCluster(savedCluster1.id, addedWorkers)) shouldEqual 3
    dbFutureValue(instanceQuery.getAllForCluster(savedCluster1.id)) shouldBe addedWorkers

    val noChange = Seq(masterInstance, workerInstance1, workerInstance2)
    dbFutureValue(instanceQuery.mergeForCluster(savedCluster1.id, noChange)) shouldEqual 3
    dbFutureValue(instanceQuery.getAllForCluster(savedCluster1.id)) shouldBe noChange

    val updatedStatus = Seq(
      masterInstance.copy(status = GceInstanceStatus.Terminated),
      workerInstance1.copy(status = GceInstanceStatus.Terminated),
      workerInstance2.copy(status = GceInstanceStatus.Terminated)
    )
    dbFutureValue(instanceQuery.mergeForCluster(savedCluster1.id, updatedStatus)) shouldEqual 3
    dbFutureValue(instanceQuery.getAllForCluster(savedCluster1.id)) shouldBe updatedStatus

    val removedOne = Seq(masterInstance.copy(status = GceInstanceStatus.Terminated),
                         workerInstance1.copy(status = GceInstanceStatus.Terminated))
    dbFutureValue(instanceQuery.mergeForCluster(savedCluster1.id, removedOne)) shouldEqual 3
    dbFutureValue(instanceQuery.getAllForCluster(savedCluster1.id)) shouldBe removedOne

    val removedAll = Seq.empty
    dbFutureValue(instanceQuery.mergeForCluster(savedCluster1.id, removedAll)) shouldEqual 2
    dbFutureValue(instanceQuery.getAllForCluster(savedCluster1.id)) shouldBe removedAll
  }

}
