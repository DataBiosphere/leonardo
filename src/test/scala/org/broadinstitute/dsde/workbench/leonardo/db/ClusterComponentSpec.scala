package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.{clusterEq, clusterSeqEq, clusterSetEq}
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.FlatSpecLike
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.time.{Seconds, Span}
import scala.concurrent.duration._

class ClusterComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {
  "ClusterComponent" should "list, save, get, and delete" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.list() } shouldEqual Seq()

    lazy val err1 = ClusterError("some failure", 10, Instant.now().truncatedTo(ChronoUnit.SECONDS))
    lazy val cluster1UUID = UUID.randomUUID()
    val createdDate, dateAccessed = Instant.now()
    val cluster1 = getCluster(1).copy(dataprocInfo = getDataprocInfo(1).copy(googleId = Some(cluster1UUID)),
                                auditInfo = auditInfo.copy(createdDate = createdDate, dateAccessed = dateAccessed),
                                instances = Set(masterInstance, workerInstance1, workerInstance2))

    val cluster1WithErr = getCluster(1).copy(dataprocInfo = getDataprocInfo(1).copy(googleId = Some(cluster1UUID)),
                                       auditInfo = auditInfo.copy(createdDate = createdDate, dateAccessed = dateAccessed),
                                       errors = List(err1),
                                       instances = Set(masterInstance, workerInstance1, workerInstance2))

    val cluster2 = getCluster(2).copy(status = ClusterStatus.Creating)

    val cluster3 = getCluster(3).copy(machineConfig = MachineConfig(Some(3), Some("test-master-machine-type"), Some(500), Some("test-worker-machine-type"), Some(200), Some(2), Some(1)),
                                      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
                                      status = ClusterStatus.Running)

    val cluster4 = getCluster(4).copy(clusterName = cluster1.clusterName,
                                      googleProject = cluster1.googleProject)


    val savedCluster1 = dbFutureValue { _.clusterQuery.save(cluster1, Option(gcsPath("gs://bucket1")), None) }
    savedCluster1 shouldEqual cluster1

    val savedCluster2 = dbFutureValue { _.clusterQuery.save(cluster2, Option(gcsPath("gs://bucket2")), Some(serviceAccountKey.id)) }
    savedCluster2 shouldEqual cluster2

    val savedCluster3 = dbFutureValue { _.clusterQuery.save(cluster3, Option(gcsPath("gs://bucket3")), Some(serviceAccountKey.id)) }
    savedCluster3 shouldEqual cluster3

    // instances are returned by list* methods
    val expectedClusters123 = Seq(savedCluster1, savedCluster2, savedCluster3).map(_.copy(instances = Set.empty))
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs expectedClusters123

    // instances are returned by get* methods
    dbFutureValue { _.clusterQuery.getActiveClusterByName(cluster1.googleProject, cluster1.clusterName) } shouldEqual Some(savedCluster1)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(cluster2.googleProject, cluster2.clusterName) } shouldEqual Some(savedCluster2)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(cluster3.googleProject, cluster3.clusterName) } shouldEqual Some(savedCluster3)

    dbFutureValue { _.clusterErrorQuery.save(savedCluster1.id, err1) }
    val cluster1WithErrAssignedId = cluster1WithErr.copy(id = savedCluster1.id)

    dbFutureValue { _.clusterQuery.getClusterById(savedCluster1.id) } shouldEqual Some(cluster1WithErrAssignedId)
    dbFutureValue { _.clusterQuery.getClusterById(savedCluster2.id) } shouldEqual Some(savedCluster2)
    dbFutureValue { _.clusterQuery.getClusterById(savedCluster3.id) } shouldEqual Some(savedCluster3)

    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(cluster1.googleProject, cluster1.clusterName) } shouldEqual None
    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(cluster2.googleProject, cluster2.clusterName) } shouldEqual Some(serviceAccountKey.id)
    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(cluster3.googleProject, cluster3.clusterName) } shouldEqual Some(serviceAccountKey.id)

    dbFutureValue { _.clusterQuery.countByClusterServiceAccountAndStatus(clusterServiceAccount.get, ClusterStatus.Creating) } shouldEqual 1
    dbFutureValue { _.clusterQuery.countByClusterServiceAccountAndStatus(clusterServiceAccount.get, ClusterStatus.Running) } shouldEqual 0

    // (project, name) unique key test

    dbFailure { _.clusterQuery.save(cluster4, Option(gcsPath("gs://bucket3")), Some(serviceAccountKey.id)) } shouldBe a[SQLException]

    dbFutureValue { _.clusterQuery.markPendingDeletion(savedCluster1.id) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } should contain theSameElementsAs Seq(cluster2, cluster3)

    val cluster1status = dbFutureValue { _.clusterQuery.getClusterById(savedCluster1.id) }.get
    cluster1status.status shouldEqual ClusterStatus.Deleting
    cluster1status.auditInfo.destroyedDate shouldBe None
    cluster1status.dataprocInfo.hostIp shouldBe None
    cluster1status.instances shouldBe cluster1.instances

    dbFutureValue { _.clusterQuery.markPendingDeletion(savedCluster2.id) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq(cluster3)
    val cluster2status = dbFutureValue { _.clusterQuery.getClusterById(savedCluster2.id) }.get
    cluster2status.status shouldEqual ClusterStatus.Deleting
    cluster2status.auditInfo.destroyedDate shouldBe None
    cluster2status.dataprocInfo.hostIp shouldBe None

    dbFutureValue { _.clusterQuery.markPendingDeletion(savedCluster3.id) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq()
    val cluster3status = dbFutureValue { _.clusterQuery.getClusterById(savedCluster3.id) }.get
    cluster3status.status shouldEqual ClusterStatus.Deleting
    cluster3status.auditInfo.destroyedDate shouldBe None
    cluster3status.dataprocInfo.hostIp shouldBe None
  }

  it should "get by labels" in isolatedDbTest {

    val cluster1 = getCluster(1).copy(labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"))

    val cluster2 = getCluster(2).copy(status = ClusterStatus.Running)

    val cluster3 = getCluster(3).copy(status = ClusterStatus.Deleted,
                                       labels = Map("a" -> "b", "bam" -> "yes"))

    dbFutureValue { _.clusterQuery.save(cluster1, Option(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) } shouldEqual cluster1
    dbFutureValue { _.clusterQuery.save(cluster2, Option(gcsPath("gs://bucket2")), Some(serviceAccountKey.id)) } shouldEqual cluster2
    dbFutureValue { _.clusterQuery.save(cluster3, Option(gcsPath("gs://bucket3")), Some(serviceAccountKey.id)) } shouldEqual cluster3
    dbFutureValue { _.clusterQuery.listByLabels(Map.empty, false) }.toSet shouldEqual Set(cluster1, cluster2)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes"), false) }.toSet shouldEqual Set(cluster1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "no"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), false) }.toSet shouldEqual Set(cluster1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), false) }.toSet shouldEqual Set(cluster1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), false) }.toSet shouldEqual Set(cluster1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), true) }.toSet shouldEqual Set(cluster1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), true) }.toSet shouldEqual Set(cluster1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), true) }.toSet shouldEqual Set(cluster1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), true) }.toSet shouldEqual Set(cluster3)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), true) }.toSet shouldEqual Set(cluster3)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), true) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bogus" -> "value"), true) }.toSet shouldEqual Set.empty[Cluster]
  }

  it should "stop and start a cluster" in isolatedDbTest {
    val dateAccessed = Instant.now()
    val initialCluster = getCluster(1).copy(status = ClusterStatus.Running)

    val savedInitialCluster = dbFutureValue { _.clusterQuery.save(initialCluster, Option(gcsPath( "gs://bucket1")), Some(serviceAccountKey.id)) }
    savedInitialCluster shouldEqual initialCluster

    // note: this does not update the instance records
    dbFutureValue { _.clusterQuery.setToStopping(savedInitialCluster.id) } shouldEqual 1
    val stoppedCluster = dbFutureValue { _.clusterQuery.getClusterById(savedInitialCluster.id) }.get
    val expectedStoppedCluster = initialCluster.copy(
      dataprocInfo = initialCluster.dataprocInfo.copy(hostIp = None),
      auditInfo = initialCluster.auditInfo.copy(dateAccessed = dateAccessed),
      status = ClusterStatus.Stopping)
    stoppedCluster.copy(auditInfo = stoppedCluster.auditInfo.copy(dateAccessed = dateAccessed)) shouldEqual expectedStoppedCluster
    stoppedCluster.auditInfo.dateAccessed should be > initialCluster.auditInfo.dateAccessed

    dbFutureValue { _.clusterQuery.setToRunning(savedInitialCluster.id, initialCluster.dataprocInfo.hostIp.get) } shouldEqual 1
    val runningCluster = dbFutureValue { _.clusterQuery.getClusterById(savedInitialCluster.id) }.get
    val expectedRunningCluster = initialCluster.copy(
      id = savedInitialCluster.id,
      auditInfo = initialCluster.auditInfo.copy(dateAccessed = dateAccessed))
    runningCluster.copy(auditInfo = runningCluster.auditInfo.copy(dateAccessed = dateAccessed)) shouldEqual expectedRunningCluster
    runningCluster.auditInfo.dateAccessed should be > stoppedCluster.auditInfo.dateAccessed
  }

  it should "merge instances" in isolatedDbTest {
    val cluster1 = getCluster(1).copy(instances = Set(masterInstance))

    val savedCluster1 = dbFutureValue { _.clusterQuery.save(cluster1, Option(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) }
    savedCluster1 shouldEqual cluster1

    val updatedCluster1 = cluster1.copy(
      id = savedCluster1.id,
      instances = Set(
        masterInstance.copy(status = InstanceStatus.Provisioning),
        workerInstance1.copy(status = InstanceStatus.Provisioning),
        workerInstance2.copy(status = InstanceStatus.Provisioning)))

    dbFutureValue { _.clusterQuery.mergeInstances(updatedCluster1) } shouldEqual updatedCluster1
    dbFutureValue { _.clusterQuery.getClusterById(savedCluster1.id) }.get shouldEqual updatedCluster1

    val updatedCluster1Again = cluster1.copy(
      instances = Set(
        masterInstance.copy(status = InstanceStatus.Terminated),
        workerInstance1.copy(status = InstanceStatus.Terminated))
    )

    dbFutureValue { _.clusterQuery.mergeInstances(updatedCluster1Again) } shouldEqual updatedCluster1Again
    dbFutureValue { _.clusterQuery.getClusterById(savedCluster1.id) }.get shouldEqual updatedCluster1
  }

  it should "get list of clusters to auto freeze" in isolatedDbTest {
    val runningCluster1 = getCluster(1).copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS)),
                                                    status = ClusterStatus.Running)

    val runningCluster2 = getCluster(2).copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS)),
                                                    status = ClusterStatus.Stopped)

    val stoppedCluster = getCluster(3).copy(status = ClusterStatus.Stopped)

    dbFutureValue { _.clusterQuery.save(runningCluster1, Some(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) } shouldEqual runningCluster1
    dbFutureValue { _.clusterQuery.save(runningCluster2, Some(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) } shouldEqual runningCluster2
    dbFutureValue { _.clusterQuery.save(stoppedCluster, Some(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) } shouldEqual stoppedCluster

    val autoFreezeList = dbFutureValue { _.clusterQuery.getClustersReadyToAutoFreeze() }
    autoFreezeList should contain (runningCluster1)
    //cluster2 is already stopped
    autoFreezeList should not contain stoppedCluster
    autoFreezeList should not contain runningCluster2
  }
}
