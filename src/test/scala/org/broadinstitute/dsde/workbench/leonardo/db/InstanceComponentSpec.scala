package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.model.{AuditInfo, Cluster, DataprocInfo, ServiceAccountInfo}
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
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
    dataprocInfo = DataprocInfo(Option(UUID.randomUUID()), Option(OperationName("op1")), Some(GcsBucketName("testStagingBucket1")), Some(IP("numbers.and.dots"))),
    auditInfo = AuditInfo(userEmail, Instant.now(), None, Instant.now()),
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
    status = ClusterStatus.Unknown,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = None,
    jupyterUserScriptUri = None,
    errors = List.empty,
    instances = Set.empty,
    userJupyterExtensionConfig = None,
    autopauseThreshold = 0)

  "InstanceComponent" should "save and get instances" in isolatedDbTest {
    val savedC1 = dbFutureValue { _.clusterQuery.save(c1, Option(gcsPath("gs://bucket1")), None) }
    savedC1 shouldEqual c1

    dbFutureValue { _.instanceQuery.save(savedC1.id, masterInstance) } shouldEqual 1
    dbFutureValue { _.instanceQuery.getInstanceByKey(masterInstance.key) } shouldEqual Some(masterInstance)
    dbFutureValue { _.instanceQuery.getAllForCluster(savedC1.id) } shouldEqual Seq(masterInstance)
  }

  it should "update status and ip" in isolatedDbTest {
    val savedC1 = dbFutureValue { _.clusterQuery.save(c1, Option(gcsPath("gs://bucket1")), None) }
    savedC1 shouldEqual c1

    dbFutureValue { _.instanceQuery.save(savedC1.id, masterInstance) } shouldEqual 1
    dbFutureValue { _.instanceQuery.updateStatusAndIpForCluster(savedC1.id, InstanceStatus.Provisioning, Some(IP("4.5.6.7"))) } shouldEqual 1
    val updated = dbFutureValue { _.instanceQuery.getInstanceByKey(masterInstance.key) }
    updated shouldBe 'defined
    updated.get.status shouldBe InstanceStatus.Provisioning
    updated.get.ip shouldBe Some(IP("4.5.6.7"))
  }

  it should "merge instances" in isolatedDbTest {
    val savedC1 = dbFutureValue { _.clusterQuery.save(c1, Option(gcsPath("gs://bucket1")), None) }
    savedC1 shouldEqual c1
    dbFutureValue { _.instanceQuery.save(savedC1.id, masterInstance) } shouldEqual 1

    val addedWorkers = Seq(masterInstance, workerInstance1, workerInstance2)
    dbFutureValue { _.instanceQuery.mergeForCluster(savedC1.id, addedWorkers) } shouldEqual 3
    dbFutureValue { _.instanceQuery.getAllForCluster(savedC1.id) } shouldBe addedWorkers

    val noChange = Seq(masterInstance, workerInstance1, workerInstance2)
    dbFutureValue { _.instanceQuery.mergeForCluster(savedC1.id, noChange) } shouldEqual 3
    dbFutureValue { _.instanceQuery.getAllForCluster(savedC1.id) } shouldBe noChange

    val updatedStatus = Seq(masterInstance.copy(status = InstanceStatus.Terminated), workerInstance1.copy(status = InstanceStatus.Terminated), workerInstance2.copy(status = InstanceStatus.Terminated))
    dbFutureValue { _.instanceQuery.mergeForCluster(savedC1.id, updatedStatus) } shouldEqual 3
    dbFutureValue { _.instanceQuery.getAllForCluster(savedC1.id) } shouldBe updatedStatus

    val removedOne = Seq(masterInstance.copy(status = InstanceStatus.Terminated), workerInstance1.copy(status = InstanceStatus.Terminated))
    dbFutureValue { _.instanceQuery.mergeForCluster(savedC1.id, removedOne) } shouldEqual 3
    dbFutureValue { _.instanceQuery.getAllForCluster(savedC1.id) } shouldBe removedOne

    val removedAll = Seq.empty
    dbFutureValue { _.instanceQuery.mergeForCluster(savedC1.id, removedAll) } shouldEqual 2
    dbFutureValue { _.instanceQuery.getAllForCluster(savedC1.id) } shouldBe removedAll
  }

}
