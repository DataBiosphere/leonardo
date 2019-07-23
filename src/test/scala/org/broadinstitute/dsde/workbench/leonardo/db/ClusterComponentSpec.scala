package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.{clusterEq, clusterSeqEq, clusterSetEq}
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.stripFieldsForListCluster
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.scalatest.FlatSpecLike

class ClusterComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {
  "ClusterComponent" should "save cluster with properties properly" in isolatedDbTest {
    val cluster = makeCluster(1).copy(properties = Map("spark:spark.executor.memory" -> "10g"))
    val savedCluster = cluster.save()
    savedCluster shouldEqual(cluster)
  }

  "ClusterComponent" should "list, save, get, and delete" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.list() } shouldEqual Seq()

    lazy val err1 = ClusterError("some failure", 10, Instant.now().truncatedTo(ChronoUnit.SECONDS))
    lazy val cluster1UUID = UUID.randomUUID()
    val cluster1 = makeCluster(1).copy(dataprocInfo = makeDataprocInfo(1).copy(googleId = Some(cluster1UUID)),
                                      instances = Set(masterInstance, workerInstance1, workerInstance2),
                                      stopAfterCreation = true)

    val cluster1WithErr = makeCluster(1).copy(dataprocInfo = makeDataprocInfo(1).copy(googleId = Some(cluster1UUID)),
                                       errors = List(err1),
                                       instances = Set(masterInstance, workerInstance1, workerInstance2))

    val cluster2 = makeCluster(2).copy(status = ClusterStatus.Creating)

    val cluster3 = makeCluster(3).copy(machineConfig = MachineConfig(Some(3), Some("test-master-machine-type"), Some(500), Some("test-worker-machine-type"), Some(200), Some(2), Some(1)),
                                      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
                                      status = ClusterStatus.Running)

    val cluster4 = makeCluster(4).copy(clusterName = cluster1.clusterName,
                                      googleProject = cluster1.googleProject)

    val savedCluster1 = cluster1.save(None)
    savedCluster1 shouldEqual cluster1

    val savedCluster2 = cluster2.save()
    savedCluster2 shouldEqual cluster2

    val savedCluster3 = cluster3.save()
    savedCluster3 shouldEqual cluster3

    // instances are returned by list* methods
    val expectedClusters123 = Seq(savedCluster1, savedCluster2, savedCluster3).map(_.copy(instances = Set.empty))
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs expectedClusters123.map(stripFieldsForListCluster)

    // instances are returned by get* methods
    dbFutureValue { _.clusterQuery.getActiveClusterByName(cluster1.googleProject, cluster1.clusterName) } shouldEqual Some(savedCluster1)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(cluster2.googleProject, cluster2.clusterName) } shouldEqual Some(savedCluster2)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(cluster3.googleProject, cluster3.clusterName) } shouldEqual Some(savedCluster3)

    dbFutureValue { _.clusterErrorQuery.save(savedCluster1.id, err1) }
    val cluster1WithErrAssignedId = cluster1WithErr.copy(id = savedCluster1.id, stopAfterCreation = true)

    dbFutureValue { _.clusterQuery.getClusterById(savedCluster1.id) } shouldEqual Some(cluster1WithErrAssignedId)
    dbFutureValue { _.clusterQuery.getClusterById(savedCluster2.id) } shouldEqual Some(savedCluster2)
    dbFutureValue { _.clusterQuery.getClusterById(savedCluster3.id) } shouldEqual Some(savedCluster3)

    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(cluster1.googleProject, cluster1.clusterName) } shouldEqual None
    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(cluster2.googleProject, cluster2.clusterName) } shouldEqual Some(serviceAccountKey.id)
    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(cluster3.googleProject, cluster3.clusterName) } shouldEqual Some(serviceAccountKey.id)

    dbFutureValue { _.clusterQuery.countByClusterServiceAccountAndStatuses(clusterServiceAccount.get, Set(ClusterStatus.Creating)) } shouldEqual 1
    dbFutureValue { _.clusterQuery.countByClusterServiceAccountAndStatuses(clusterServiceAccount.get, Set(ClusterStatus.Running)) } shouldEqual 0

    // (project, name) unique key test

    dbFailure { _.clusterQuery.save(cluster4, Option(gcsPath("gs://bucket3")), Some(serviceAccountKey.id)) } shouldBe a[SQLException]

    dbFutureValue { _.clusterQuery.markPendingDeletion(savedCluster1.id) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } should contain theSameElementsAs Seq(cluster2, cluster3).map(stripFieldsForListCluster)

    val cluster1status = dbFutureValue { _.clusterQuery.getClusterById(savedCluster1.id) }.get
    cluster1status.status shouldEqual ClusterStatus.Deleting
    cluster1status.auditInfo.destroyedDate shouldBe None
    cluster1status.dataprocInfo.hostIp shouldBe None
    cluster1status.instances shouldBe cluster1.instances

    dbFutureValue { _.clusterQuery.markPendingDeletion(savedCluster2.id) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq(cluster3).map(stripFieldsForListCluster)
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

    val savedCluster1 = makeCluster(1).copy(labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar")).save(Some(serviceAccountKey.id))

    val savedCluster2 = makeCluster(2).copy(status = ClusterStatus.Running).save(Some(serviceAccountKey.id))

    val savedCluster3 = makeCluster(3).copy(status = ClusterStatus.Deleted,
                                       labels = Map("a" -> "b", "bam" -> "yes")).save()

    dbFutureValue { _.clusterQuery.listByLabels(Map.empty, false) }.toSet shouldEqual Set(savedCluster1, savedCluster2).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes"), false) }.toSet shouldEqual Set(savedCluster1).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "no"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), false) }.toSet shouldEqual Set(savedCluster1).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), false) }.toSet shouldEqual Set(savedCluster1).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), false) }.toSet shouldEqual Set(savedCluster1).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), true) }.toSet shouldEqual Set(savedCluster1).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), true) }.toSet shouldEqual Set(savedCluster1).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), true) }.toSet shouldEqual Set(savedCluster1).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), true) }.toSet shouldEqual Set(savedCluster3).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), true) }.toSet shouldEqual Set(savedCluster3).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), true) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bogus" -> "value"), true) }.toSet shouldEqual Set.empty[Cluster]
  }

  it should "stop and start a cluster" in isolatedDbTest {
    val dateAccessed = Instant.now()
    val initialCluster = makeCluster(1).copy(status = ClusterStatus.Running).save()

    // note: this does not update the instance records
    dbFutureValue { _.clusterQuery.setToStopping(initialCluster.id) } shouldEqual 1
    val stoppedCluster = dbFutureValue { _.clusterQuery.getClusterById(initialCluster.id) }.get
    val expectedStoppedCluster = initialCluster.copy(
      dataprocInfo = initialCluster.dataprocInfo.copy(hostIp = None),
      auditInfo = initialCluster.auditInfo.copy(dateAccessed = dateAccessed),
      status = ClusterStatus.Stopping)
    stoppedCluster.copy(auditInfo = stoppedCluster.auditInfo.copy(dateAccessed = dateAccessed)) shouldEqual expectedStoppedCluster
    stoppedCluster.auditInfo.dateAccessed should be > initialCluster.auditInfo.dateAccessed

    dbFutureValue { _.clusterQuery.setToRunning(initialCluster.id, initialCluster.dataprocInfo.hostIp.get) } shouldEqual 1
    val runningCluster = dbFutureValue { _.clusterQuery.getClusterById(initialCluster.id) }.get
    val expectedRunningCluster = initialCluster.copy(
      id = initialCluster.id,
      auditInfo = initialCluster.auditInfo.copy(dateAccessed = dateAccessed))
    runningCluster.copy(auditInfo = runningCluster.auditInfo.copy(dateAccessed = dateAccessed)) shouldEqual expectedRunningCluster
    runningCluster.auditInfo.dateAccessed should be > stoppedCluster.auditInfo.dateAccessed
  }

  it should "merge instances" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).copy(instances = Set(masterInstance)).save()

    val updatedCluster1 = savedCluster1.copy(
      id = savedCluster1.id,
      instances = Set(
        masterInstance.copy(status = InstanceStatus.Provisioning),
        workerInstance1.copy(status = InstanceStatus.Provisioning),
        workerInstance2.copy(status = InstanceStatus.Provisioning)))

    dbFutureValue { _.clusterQuery.mergeInstances(updatedCluster1) } shouldEqual updatedCluster1
    dbFutureValue { _.clusterQuery.getClusterById(savedCluster1.id) }.get shouldEqual updatedCluster1

    val updatedCluster1Again = savedCluster1.copy(
      instances = Set(
        masterInstance.copy(status = InstanceStatus.Terminated),
        workerInstance1.copy(status = InstanceStatus.Terminated))
    )

    dbFutureValue { _.clusterQuery.mergeInstances(updatedCluster1Again) } shouldEqual updatedCluster1Again
  }

  it should "get list of clusters to auto freeze" in isolatedDbTest {
    val runningCluster1 = makeCluster(1).copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS)),
                                                    status = ClusterStatus.Running).save()

    val runningCluster2 = makeCluster(2).copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS)),
                                                    status = ClusterStatus.Stopped).save()

    val stoppedCluster = makeCluster(3).copy(status = ClusterStatus.Stopped).save()

    val autopauseDisabledCluster = makeCluster(4).copy(
      auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS)),
      status = ClusterStatus.Running,
      autopauseThreshold = 0).save()

    val autoFreezeList = dbFutureValue { _.clusterQuery.getClustersReadyToAutoFreeze() }
    autoFreezeList should contain (runningCluster1)
    //cluster2 is already stopped
    autoFreezeList should not contain stoppedCluster
    autoFreezeList should not contain runningCluster2
    autoFreezeList should not contain autopauseDisabledCluster
  }

  it should "list by labels and project" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .copy(labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"))
      .save(Some(serviceAccountKey.id))

    val savedCluster2 = makeCluster(2)
      .copy(status = ClusterStatus.Running,
        clusterName = name2,
        googleProject = project2,
        clusterUrl = Cluster.getClusterUrl(project2, name2, clusterUrlBase),
        labels = Map("bam" -> "yes"))
      .save(Some(serviceAccountKey.id))

    val savedCluster3 = makeCluster(3)
      .copy(status = ClusterStatus.Deleted,
        labels = Map("a" -> "b", "bam" -> "yes"))
      .save()

    dbFutureValue { _.clusterQuery.listByLabels(Map.empty, false, Some(project)) }.toSet shouldEqual Set(savedCluster1).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map.empty, true, Some(project)) }.toSet shouldEqual Set(savedCluster1, savedCluster3).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map.empty, false, Some(project2)) }.toSet shouldEqual Set(savedCluster2).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes"), true, Some(project)) }.toSet shouldEqual Set(savedCluster1, savedCluster3).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes"), false, Some(project2)) }.toSet shouldEqual Set(savedCluster2).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), true, Some(project)) }.toSet shouldEqual Set(savedCluster3).map(stripFieldsForListCluster)
    dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), true, Some(project2)) }.toSet shouldEqual Set.empty[Cluster]
  }

  it should "get for dns cache" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .copy(
        labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"),
        instances = Set(masterInstance, workerInstance1, workerInstance2))
      .save(Some(serviceAccountKey.id))

    // Result should not include labels or instances
    dbFutureValue { _.clusterQuery.getActiveClusterByNameMinimal(savedCluster1.googleProject, savedCluster1.clusterName) } shouldEqual
      Some(savedCluster1).map(stripFieldsForListCluster andThen (_.copy(labels = Map.empty)))
  }

  it should "update master machine type" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .copy(machineConfig =
        MachineConfig(Some(3), Some("test-master-machine-type"), Some(500), Some("test-worker-machine-type"), Some(200), Some(2), Some(1)))
      .save()

    val newMachineType = MachineType("this-is-a-new-machine-type")
    dbFutureValue { _.clusterQuery.updateMasterMachineType(savedCluster1.id, newMachineType) }

    dbFutureValue { _.clusterQuery.getClusterById(savedCluster1.id) }.flatMap(_.machineConfig.masterMachineType) shouldBe
      Option(newMachineType.value)
  }

  it should "update master disk size" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .copy(machineConfig =
        MachineConfig(Some(3), Some("test-master-machine-type"), Some(500), Some("test-worker-machine-type"), Some(200), Some(2), Some(1)))
      .save()

    val newDiskSize = 1000
    dbFutureValue { _.clusterQuery.updateMasterDiskSize(savedCluster1.id, newDiskSize) }

    dbFutureValue { _.clusterQuery.getClusterById(savedCluster1.id) }.flatMap(_.machineConfig.masterDiskSize) shouldBe
      Option(newDiskSize)
  }

  it should "list monitored clusters" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).save()
    val savedCluster2 = makeCluster(2).copy(dataprocInfo = DataprocInfo(None, None, None, None)).save()

    // listMonitored should only return clusters that have google info defined
    dbFutureValue { _.clusterQuery.listMonitoredClusterOnly() }.map(_.id) shouldBe Seq(1)
    dbFutureValue { _.clusterQuery.listMonitored() }.map(_.id) shouldBe Seq(1)
    dbFutureValue { _.clusterQuery.listMonitoredFullCluster() }.map(_.id) shouldBe Seq(1)
  }
}
