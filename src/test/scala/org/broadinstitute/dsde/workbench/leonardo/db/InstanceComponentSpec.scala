package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ServiceAccountInfo}
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.FlatSpecLike

/**
  * Created by rtitle on 2/19/18.
  */
class InstanceComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {

  val c1 = Cluster(
    clusterName = name1,
    googleId = UUID.randomUUID(),
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
    operationName = OperationName("op1"),
    status = ClusterStatus.Unknown,
    hostIp = Some(IP("numbers.and.dots")),
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = None,
    jupyterUserScriptUri = None,
    Some(GcsBucketName("testStagingBucket1")),
    List.empty,
    Set.empty
  )

  "InstanceComponent" should "save and get instances" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) } shouldEqual c1
    dbFutureValue { _.instanceQuery.save(getClusterId(c1.googleId), masterInstance) } shouldEqual 1
    dbFutureValue { _.instanceQuery.getInstanceByKey(masterInstance.key) } shouldEqual Some(masterInstance)
    dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(c1.googleId)) } shouldEqual Seq(masterInstance)
  }

  it should "update status and ip" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) } shouldEqual c1
    dbFutureValue { _.instanceQuery.save(getClusterId(c1.googleId), masterInstance) } shouldEqual 1
    dbFutureValue { _.instanceQuery.updateStatusAndIpForCluster(getClusterId(c1.googleId), InstanceStatus.Provisioning, Some(IP("4.5.6.7"))) } shouldEqual 1
    val updated = dbFutureValue { _.instanceQuery.getInstanceByKey(masterInstance.key) }
    updated shouldBe 'defined
    updated.get.status shouldBe InstanceStatus.Provisioning
    updated.get.ip shouldBe Some(IP("4.5.6.7"))
  }

  it should "merge instances" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) } shouldEqual c1
    dbFutureValue { _.instanceQuery.save(getClusterId(c1.googleId), masterInstance) } shouldEqual 1

    val addedWorkers = Seq(masterInstance, workerInstance1, workerInstance2)
    dbFutureValue { _.instanceQuery.mergeForCluster(getClusterId(c1.googleId), addedWorkers) } shouldEqual 3
    dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(c1.googleId)) } shouldBe addedWorkers

    val noChange = Seq(masterInstance, workerInstance1, workerInstance2)
    dbFutureValue { _.instanceQuery.mergeForCluster(getClusterId(c1.googleId), noChange) } shouldEqual 3
    dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(c1.googleId)) } shouldBe noChange

    val updatedStatus = Seq(masterInstance.copy(status = InstanceStatus.Terminated), workerInstance1.copy(status = InstanceStatus.Terminated), workerInstance2.copy(status = InstanceStatus.Terminated))
    dbFutureValue { _.instanceQuery.mergeForCluster(getClusterId(c1.googleId), updatedStatus) } shouldEqual 3
    dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(c1.googleId)) } shouldBe updatedStatus

    val removedOne = Seq(masterInstance.copy(status = InstanceStatus.Terminated), workerInstance1.copy(status = InstanceStatus.Terminated))
    dbFutureValue { _.instanceQuery.mergeForCluster(getClusterId(c1.googleId), removedOne) } shouldEqual 3
    dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(c1.googleId)) } shouldBe removedOne

    val removedAll = Seq.empty
    dbFutureValue { _.instanceQuery.mergeForCluster(getClusterId(c1.googleId), removedAll) } shouldEqual 2
    dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId(c1.googleId)) } shouldBe removedAll
  }

}
