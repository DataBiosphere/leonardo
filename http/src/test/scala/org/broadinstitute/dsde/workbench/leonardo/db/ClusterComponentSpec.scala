package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.SQLException
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.{clusterEq, clusterSeqEq, clusterSetEq, stripFieldsForListCluster}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.scalatest.FlatSpecLike
import org.scalatest.concurrent.ScalaFutures

class ClusterComponentSpec extends TestComponent with FlatSpecLike  with GcsPathUtils with ScalaFutures {
  "ClusterComponent" should "save cluster with properties properly" in isolatedDbTest {
    val cluster = makeCluster(1).copy(properties = Map("spark:spark.executor.memory" -> "10g"))
    val savedCluster = cluster.save()
    savedCluster shouldEqual (cluster)
  }

  "ClusterComponent" should "list, save, get, and delete" in isolatedDbTest {
    dbFutureValue { dbRef.dataAccess.clusterQuery.listWithLabels() } shouldEqual Seq()

    lazy val err1 = ClusterError("some failure", 10, Instant.now().truncatedTo(ChronoUnit.SECONDS))
    lazy val cluster1UUID = UUID.randomUUID()
    val cluster1 = makeCluster(1).copy(
      dataprocInfo = Some(makeDataprocInfo(1).copy(googleId = cluster1UUID)),
      instances = Set(masterInstance, workerInstance1, workerInstance2),
      stopAfterCreation = true
    )

    val cluster1WithErr = makeCluster(1).copy(dataprocInfo = Some(makeDataprocInfo(1).copy(googleId = cluster1UUID)),
                                              errors = List(err1),
                                              instances = Set(masterInstance, workerInstance1, workerInstance2))

    val cluster2 = makeCluster(2).copy(status = ClusterStatus.Creating)

    val cluster3 = makeCluster(3).copy(
      machineConfig = MachineConfig(Some(3),
                                    Some("test-master-machine-type"),
                                    Some(500),
                                    Some("test-worker-machine-type"),
                                    Some(200),
                                    Some(2),
                                    Some(1)),
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      status = ClusterStatus.Running
    )

    val cluster4 = makeCluster(4).copy(clusterName = cluster1.clusterName, googleProject = cluster1.googleProject)

    val savedCluster1 = cluster1.save(None)
    savedCluster1 shouldEqual cluster1

    val savedCluster2 = cluster2.save()
    savedCluster2 shouldEqual cluster2

    val savedCluster3 = cluster3.save()
    savedCluster3 shouldEqual cluster3

    // instances are returned by list* methods
    val expectedClusters123 = Seq(savedCluster1, savedCluster2, savedCluster3).map(_.copy(instances = Set.empty))
    dbFutureValue { dbRef.dataAccess.clusterQuery.listWithLabels() } should contain theSameElementsAs expectedClusters123.map(
      stripFieldsForListCluster
    )

    // instances are returned by get* methods
    dbFutureValue { dbRef.dataAccess.clusterQuery.getActiveClusterByName(cluster1.googleProject, cluster1.clusterName) } shouldEqual Some(
      savedCluster1
    )
    dbFutureValue { dbRef.dataAccess.clusterQuery.getActiveClusterByName(cluster2.googleProject, cluster2.clusterName) } shouldEqual Some(
      savedCluster2
    )
    dbFutureValue { dbRef.dataAccess.clusterQuery.getActiveClusterByName(cluster3.googleProject, cluster3.clusterName) } shouldEqual Some(
      savedCluster3
    )

    dbFutureValue { dbRef.dataAccess.clusterErrorQuery.save(savedCluster1.id, err1) }
    val cluster1WithErrAssignedId = cluster1WithErr.copy(id = savedCluster1.id, stopAfterCreation = true)

    dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(savedCluster1.id) } shouldEqual Some(cluster1WithErrAssignedId)
    dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(savedCluster2.id) } shouldEqual Some(savedCluster2)
    dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(savedCluster3.id) } shouldEqual Some(savedCluster3)

    dbFutureValue { dbRef.dataAccess.clusterQuery.getServiceAccountKeyId(cluster1.googleProject, cluster1.clusterName) } shouldEqual None
    dbFutureValue { dbRef.dataAccess.clusterQuery.getServiceAccountKeyId(cluster2.googleProject, cluster2.clusterName) } shouldEqual Some(
      serviceAccountKey.id
    )
    dbFutureValue { dbRef.dataAccess.clusterQuery.getServiceAccountKeyId(cluster3.googleProject, cluster3.clusterName) } shouldEqual Some(
      serviceAccountKey.id
    )

    dbFutureValue { dbRef.dataAccess.clusterQuery.countActiveByClusterServiceAccount(clusterServiceAccount.get) } shouldEqual 2
    dbFutureValue { dbRef.dataAccess.clusterQuery.countActiveByProject(project) } shouldEqual 3

    // (project, name) unique key test

    dbFailure { dbRef.dataAccess.clusterQuery.save(cluster4, Option(gcsPath("gs://bucket3")), Some(serviceAccountKey.id)) } shouldBe a[
      SQLException
    ]

    dbFutureValue { dbRef.dataAccess.clusterQuery.markPendingDeletion(savedCluster1.id, Instant.now) } shouldEqual 1
    dbFutureValue { dbRef.dataAccess.clusterQuery.listActiveWithLabels() } should contain theSameElementsAs Seq(cluster2, cluster3)
      .map(stripFieldsForListCluster)

    val cluster1status = dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(savedCluster1.id) }.get
    cluster1status.status shouldEqual ClusterStatus.Deleting
    cluster1status.auditInfo.destroyedDate shouldBe None
    cluster1status.dataprocInfo.flatMap(_.hostIp) shouldBe None
    cluster1status.instances shouldBe cluster1.instances

    dbFutureValue { dbRef.dataAccess.clusterQuery.markPendingDeletion(savedCluster2.id, Instant.now) } shouldEqual 1
    dbFutureValue { dbRef.dataAccess.clusterQuery.listActiveWithLabels() } shouldEqual Seq(cluster3).map(stripFieldsForListCluster)
    val cluster2status = dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(savedCluster2.id) }.get
    cluster2status.status shouldEqual ClusterStatus.Deleting
    cluster2status.auditInfo.destroyedDate shouldBe None
    cluster2status.dataprocInfo.flatMap(_.hostIp) shouldBe None

    dbFutureValue { dbRef.dataAccess.clusterQuery.markPendingDeletion(savedCluster3.id, Instant.now) } shouldEqual 1
    dbFutureValue { dbRef.dataAccess.clusterQuery.listActiveWithLabels() } shouldEqual Seq()
    val cluster3status = dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(savedCluster3.id) }.get
    cluster3status.status shouldEqual ClusterStatus.Deleting
    cluster3status.auditInfo.destroyedDate shouldBe None
    cluster3status.dataprocInfo.flatMap(_.hostIp) shouldBe None
  }

  it should "get by labels" in isolatedDbTest {

    val savedCluster1 =
      makeCluster(1).copy(labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar")).save(Some(serviceAccountKey.id))

    val savedCluster2 = makeCluster(2).copy(status = ClusterStatus.Running).save(Some(serviceAccountKey.id))

    val savedCluster3 =
      makeCluster(3).copy(status = ClusterStatus.Deleted, labels = Map("a" -> "b", "bam" -> "yes")).save()

    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map.empty, false) }.toSet shouldEqual Set(savedCluster1, savedCluster2)
      .map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "yes"), false) }.toSet shouldEqual Set(savedCluster1).map(
      stripFieldsForListCluster
    )
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "no"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), false) }.toSet shouldEqual Set(
      stripFieldsForListCluster(savedCluster1)
    )
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), false) }.toSet shouldEqual Set(
      savedCluster1
    ).map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), false) }.toSet shouldEqual Set(
      savedCluster1
    ).map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("a" -> "b"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), false) }.toSet shouldEqual Set
      .empty[Cluster]
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), false) }.toSet shouldEqual Set
      .empty[Cluster]
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), true) }.toSet shouldEqual Set(
      savedCluster1
    ).map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), true) }.toSet shouldEqual Set(
      savedCluster1
    ).map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), true) }.toSet shouldEqual Set(
      savedCluster1
    ).map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("a" -> "b"), true) }.toSet shouldEqual Set(savedCluster3).map(
      stripFieldsForListCluster
    )
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), true) }.toSet shouldEqual Set(
      savedCluster3
    ).map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), true) }.toSet shouldEqual Set
      .empty[Cluster]
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bogus" -> "value"), true) }.toSet shouldEqual Set.empty[Cluster]
  }

  it should "stop and start a cluster" in isolatedDbTest {
    val dateAccessed = Instant.now()
    val initialCluster = makeCluster(1).copy(status = ClusterStatus.Running).save()

    // note: this does not update the instance records
    dbFutureValue { dbRef.dataAccess.clusterQuery.setToStopping(initialCluster.id, Instant.now) } shouldEqual 1
    val stoppedCluster = dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(initialCluster.id) }.get
    val expectedStoppedCluster = initialCluster.copy(
      dataprocInfo = initialCluster.dataprocInfo.map(_.copy(hostIp = None)),
      auditInfo = initialCluster.auditInfo.copy(dateAccessed = dateAccessed),
      status = ClusterStatus.Stopping
    )
    stoppedCluster.copy(auditInfo = stoppedCluster.auditInfo.copy(dateAccessed = dateAccessed)) shouldEqual expectedStoppedCluster
    stoppedCluster.auditInfo.dateAccessed should be > initialCluster.auditInfo.dateAccessed

    dbFutureValue {
      dbRef.dataAccess.clusterQuery.setToRunning(initialCluster.id, initialCluster.dataprocInfo.flatMap(_.hostIp).get, Instant.now)
    } shouldEqual 1
    val runningCluster = dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(initialCluster.id) }.get
    val expectedRunningCluster = initialCluster.copy(id = initialCluster.id,
                                                     auditInfo =
                                                       initialCluster.auditInfo.copy(dateAccessed = dateAccessed))
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
        workerInstance2.copy(status = InstanceStatus.Provisioning)
      )
    )

    dbFutureValue { dbRef.dataAccess.clusterQuery.mergeInstances(updatedCluster1) } shouldEqual updatedCluster1
    dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(savedCluster1.id) }.get shouldEqual updatedCluster1

    val updatedCluster1Again = savedCluster1.copy(
      instances = Set(masterInstance.copy(status = InstanceStatus.Terminated),
                      workerInstance1.copy(status = InstanceStatus.Terminated))
    )

    dbFutureValue { dbRef.dataAccess.clusterQuery.mergeInstances(updatedCluster1Again) } shouldEqual updatedCluster1Again
  }

  it should "get list of clusters to auto freeze" in isolatedDbTest {
    val runningCluster1 = makeCluster(1)
      .copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS)),
            status = ClusterStatus.Running)
      .save()

    val runningCluster2 = makeCluster(2)
      .copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS)),
            status = ClusterStatus.Stopped)
      .save()

    val stoppedCluster = makeCluster(3).copy(status = ClusterStatus.Stopped).save()

    val autopauseDisabledCluster = makeCluster(4)
      .copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS)),
            status = ClusterStatus.Running,
            autopauseThreshold = 0)
      .save()

    val autoFreezeList = dbFutureValue { dbRef.dataAccess.clusterQuery.getClustersReadyToAutoFreeze() }
    autoFreezeList should contain(runningCluster1)
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
      .copy(
        status = ClusterStatus.Running,
        clusterName = name2,
        googleProject = project2,
        clusterUrl = Cluster.getClusterUrl(project2, name2, Set(jupyterImage), Map("bam" -> "yes")),
        labels = Map("bam" -> "yes")
      )
      .save(Some(serviceAccountKey.id))

    val savedCluster3 = makeCluster(3)
      .copy(status = ClusterStatus.Deleted, labels = Map("a" -> "b", "bam" -> "yes"))
      .save()

    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map.empty, false, Some(project)) }.toSet shouldEqual Set(savedCluster1)
      .map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map.empty, true, Some(project)) }.toSet shouldEqual Set(
      savedCluster1,
      savedCluster3
    ).map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map.empty, false, Some(project2)) }.toSet shouldEqual Set(savedCluster2)
      .map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "yes"), true, Some(project)) }.toSet shouldEqual Set(
      savedCluster1,
      savedCluster3
    ).map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("bam" -> "yes"), false, Some(project2)) }.toSet shouldEqual Set(
      savedCluster2
    ).map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("a" -> "b"), true, Some(project)) }.toSet shouldEqual Set(
      savedCluster3
    ).map(stripFieldsForListCluster)
    dbFutureValue { dbRef.dataAccess.clusterQuery.listByLabels(Map("a" -> "b"), true, Some(project2)) }.toSet shouldEqual Set
      .empty[Cluster]
  }

  it should "get for dns cache" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .copy(labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"),
            instances = Set(masterInstance, workerInstance1, workerInstance2))
      .save(Some(serviceAccountKey.id))

    // Result should not include labels or instances
    dbFutureValue {
      dbRef.dataAccess.clusterQuery.getActiveClusterByNameMinimal(savedCluster1.googleProject, savedCluster1.clusterName)
    } shouldEqual
      Some(stripFieldsForListCluster(savedCluster1).copy(labels = Map.empty))
  }

  it should "update master machine type" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .copy(
        machineConfig = MachineConfig(Some(3),
                                      Some("test-master-machine-type"),
                                      Some(500),
                                      Some("test-worker-machine-type"),
                                      Some(200),
                                      Some(2),
                                      Some(1))
      )
      .save()

    val newMachineType = MachineType("this-is-a-new-machine-type")
    dbFutureValue { dbRef.dataAccess.clusterQuery.updateMasterMachineType(savedCluster1.id, newMachineType, Instant.now) }

    dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(savedCluster1.id) }
      .flatMap(_.machineConfig.masterMachineType) shouldBe
      Option(newMachineType.value)
  }

  it should "update master disk size" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .copy(
        machineConfig = MachineConfig(Some(3),
                                      Some("test-master-machine-type"),
                                      Some(500),
                                      Some("test-worker-machine-type"),
                                      Some(200),
                                      Some(2),
                                      Some(1))
      )
      .save()

    val newDiskSize = 1000
    dbFutureValue { dbRef.dataAccess.clusterQuery.updateMasterDiskSize(savedCluster1.id, newDiskSize, Instant.now) }

    dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(savedCluster1.id) }.flatMap(_.machineConfig.masterDiskSize) shouldBe
      Option(newDiskSize)
  }

  it should "list monitored clusters" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).save()
    val savedCluster2 = makeCluster(2).copy(dataprocInfo = None).save()

    dbFutureValue { dbRef.dataAccess.clusterQuery.listMonitoredClusterOnly() }.toSet shouldBe Set(savedCluster1, savedCluster2)
      .map(stripFieldsForListCluster)
      .map(_.copy(labels = Map.empty))
    dbFutureValue { dbRef.dataAccess.clusterQuery.listMonitored() }.toSet shouldBe Set(savedCluster1, savedCluster2).map(
      stripFieldsForListCluster
    )
  }

  it should "persist custom environment variables" in isolatedDbTest {
    val expectedEvs = Map("foo" -> "bar", "test" -> "this is a test")
    val savedCluster = makeCluster(1).copy(customClusterEnvironmentVariables = expectedEvs).save()

    val retrievedCluster = dbFutureValue { dbRef.dataAccess.clusterQuery.getClusterById(savedCluster.id) }

    retrievedCluster shouldBe 'defined
    retrievedCluster.get.customClusterEnvironmentVariables shouldBe expectedEvs
    retrievedCluster.get shouldBe savedCluster
  }
}
