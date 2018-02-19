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
    Set.empty
  )

  private def getClusterId: Long = {
    dbFutureValue { _.clusterQuery.getClusterId(c1.googleId) }.get
  }

  "InstanceComponent" should "save and get instances" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) } shouldEqual c1
    dbFutureValue { _.instanceQuery.save(getClusterId, masterInstance) } shouldEqual 1
    dbFutureValue { _.instanceQuery.getInstanceByKey(masterInstance.key) } shouldEqual Some(masterInstance)
    dbFutureValue { _.instanceQuery.getAllForCluster(getClusterId) } shouldEqual Seq(masterInstance)
  }

  it should "update status and ip" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) } shouldEqual c1
    dbFutureValue { _.instanceQuery.save(getClusterId, masterInstance) } shouldEqual 1
    dbFutureValue { _.instanceQuery.updateStatusAndIpForCluster(getClusterId, InstanceStatus.Provisioning, Some(IP("4.5.6.7"))) } shouldEqual 1
    val updated = dbFutureValue { _.instanceQuery.getInstanceByKey(masterInstance.key) }
    updated shouldBe 'defined
    updated.get.status shouldBe InstanceStatus.Provisioning
    updated.get.ip shouldBe Some(IP("4.5.6.7"))
  }

  it should "mark pending deletion" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) } shouldEqual c1
    dbFutureValue { _.instanceQuery.save(getClusterId, masterInstance) } shouldEqual 1
    dbFutureValue { _.instanceQuery.markPendingDeletionForCluster(getClusterId) } shouldEqual 1
    val updated = dbFutureValue { _.instanceQuery.getInstanceByKey(masterInstance.key) }
    updated shouldBe 'defined
    updated.get.status shouldBe InstanceStatus.Deleting
    updated.get.ip shouldBe None
    updated.get.destroyedDate shouldBe 'defined
  }

  it should "complete deletion" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) } shouldEqual c1
    dbFutureValue { _.instanceQuery.save(getClusterId, masterInstance) } shouldEqual 1
    dbFutureValue { _.instanceQuery.completeDeletionForCluster(getClusterId) } shouldEqual 1
    val updated = dbFutureValue { _.instanceQuery.getInstanceByKey(masterInstance.key) }
    updated shouldBe 'defined
    updated.get.status shouldBe InstanceStatus.Deleted
  }

}
