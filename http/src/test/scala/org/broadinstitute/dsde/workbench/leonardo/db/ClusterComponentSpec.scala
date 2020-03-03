package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.SQLException
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.{
  clusterEq,
  clusterSeqEq,
  stripFieldsForListCluster
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.scalatest.FlatSpecLike
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global

class ClusterComponentSpec extends FlatSpecLike with TestComponent with GcsPathUtils with ScalaFutures {
  "ClusterComponent" should "save cluster with properties properly" in isolatedDbTest {
    val cluster = makeCluster(1).copy(properties = Map("spark:spark.executor.memory" -> "10g"))
    val savedCluster = cluster.save()
    savedCluster.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual (cluster) //input cluster's runtimeConfigId is fake -1
  }

  "ClusterComponent" should "list, save, get, and delete" in isolatedDbTest {
    dbFutureValue { clusterQuery.listWithLabels } shouldEqual Seq()

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
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      status = ClusterStatus.Running
    )

    val cluster4 = makeCluster(4).copy(clusterName = cluster1.clusterName, googleProject = cluster1.googleProject)

    val savedCluster1 = cluster1.save(None)
    savedCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual cluster1

    val savedCluster2 = cluster2.save()
    savedCluster2.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual cluster2

    val savedCluster3 = cluster3.saveWithRuntimeConfig(
      RuntimeConfig.DataprocConfig(3,
                                   "test-master-machine-type",
                                   500,
                                   Some("test-worker-machine-type"),
                                   Some(200),
                                   Some(2),
                                   Some(1))
    )
    savedCluster3.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual cluster3

    // instances are returned by list* methods
    val expectedClusters123 = Seq(savedCluster1, savedCluster2, savedCluster3).map(_.copy(instances = Set.empty))
    dbFutureValue { clusterQuery.listWithLabels } should contain theSameElementsAs expectedClusters123.map(
      stripFieldsForListCluster
    )

    // instances are returned by get* methods
    dbFutureValue { clusterQuery.getActiveClusterByName(cluster1.googleProject, cluster1.clusterName) } shouldEqual Some(
      savedCluster1
    )
    dbFutureValue { clusterQuery.getActiveClusterByName(cluster2.googleProject, cluster2.clusterName) } shouldEqual Some(
      savedCluster2
    )
    dbFutureValue { clusterQuery.getActiveClusterByName(cluster3.googleProject, cluster3.clusterName) } shouldEqual Some(
      savedCluster3
    )

    dbFutureValue { clusterErrorQuery.save(savedCluster1.id, err1) }
    val cluster1WithErrAssignedId = cluster1WithErr.copy(id = savedCluster1.id,
                                                         stopAfterCreation = true,
                                                         runtimeConfigId = savedCluster1.runtimeConfigId)

    dbFutureValue { clusterQuery.getClusterById(savedCluster1.id) } shouldEqual Some(cluster1WithErrAssignedId)
    dbFutureValue { clusterQuery.getClusterById(savedCluster2.id) } shouldEqual Some(savedCluster2)
    dbFutureValue { clusterQuery.getClusterById(savedCluster3.id) } shouldEqual Some(savedCluster3)

    dbFutureValue { clusterQuery.getServiceAccountKeyId(cluster1.googleProject, cluster1.clusterName) } shouldEqual None
    dbFutureValue { clusterQuery.getServiceAccountKeyId(cluster2.googleProject, cluster2.clusterName) } shouldEqual Some(
      serviceAccountKey.id
    )
    dbFutureValue { clusterQuery.getServiceAccountKeyId(cluster3.googleProject, cluster3.clusterName) } shouldEqual Some(
      serviceAccountKey.id
    )

    dbFutureValue { clusterQuery.countActiveByClusterServiceAccount(clusterServiceAccount.get) } shouldEqual 2
    dbFutureValue { clusterQuery.countActiveByProject(project) } shouldEqual 3

    // (project, name) unique key test
    val saveCluster = SaveCluster(cluster4,
                                  Some(gcsPath("gs://bucket3")),
                                  Some(serviceAccountKey.id),
                                  defaultRuntimeConfig,
                                  Instant.now())
    dbFailure { clusterQuery.save(saveCluster) } shouldBe a[
      SQLException
    ]

    dbFutureValue { clusterQuery.markPendingDeletion(savedCluster1.id, Instant.now) } shouldEqual 1
    dbFutureValue { clusterQuery.listActiveWithLabels }
      .map(x => x.copy(runtimeConfigId = RuntimeConfigId(-1))) should contain theSameElementsAs Seq(cluster2, cluster3)
      .map(stripFieldsForListCluster)

    val cluster1status = dbFutureValue { clusterQuery.getClusterById(savedCluster1.id) }.get
    cluster1status.status shouldEqual ClusterStatus.Deleting
    cluster1status.auditInfo.destroyedDate shouldBe None
    cluster1status.dataprocInfo.flatMap(_.hostIp) shouldBe None
    cluster1status.instances shouldBe cluster1.instances

    dbFutureValue { clusterQuery.markPendingDeletion(savedCluster2.id, Instant.now) } shouldEqual 1
    dbFutureValue { clusterQuery.listActiveWithLabels }
      .map(_.copy(runtimeConfigId = RuntimeConfigId(-1))) shouldEqual Seq(cluster3).map(stripFieldsForListCluster)
    val cluster2status = dbFutureValue { clusterQuery.getClusterById(savedCluster2.id) }.get
    cluster2status.status shouldEqual ClusterStatus.Deleting
    cluster2status.auditInfo.destroyedDate shouldBe None
    cluster2status.dataprocInfo.flatMap(_.hostIp) shouldBe None

    dbFutureValue { clusterQuery.markPendingDeletion(savedCluster3.id, Instant.now) } shouldEqual 1
    dbFutureValue { clusterQuery.listActiveWithLabels } shouldEqual Seq()
    val cluster3status = dbFutureValue { clusterQuery.getClusterById(savedCluster3.id) }.get
    cluster3status.status shouldEqual ClusterStatus.Deleting
    cluster3status.auditInfo.destroyedDate shouldBe None
    cluster3status.dataprocInfo.flatMap(_.hostIp) shouldBe None
  }

  it should "stop and start a cluster" in isolatedDbTest {
    val dateAccessed = Instant.now()
    val initialCluster = makeCluster(1).copy(status = ClusterStatus.Running).save()

    // note: this does not update the instance records
    dbFutureValue { clusterQuery.setToStopping(initialCluster.id, Instant.now) } shouldEqual 1
    val stoppedCluster = dbFutureValue { clusterQuery.getClusterById(initialCluster.id) }.get
    val expectedStoppedCluster = initialCluster.copy(
      dataprocInfo = initialCluster.dataprocInfo.map(_.copy(hostIp = None)),
      auditInfo = initialCluster.auditInfo.copy(dateAccessed = dateAccessed),
      status = ClusterStatus.Stopping
    )
    stoppedCluster.copy(auditInfo = stoppedCluster.auditInfo.copy(dateAccessed = dateAccessed)) shouldEqual expectedStoppedCluster
    stoppedCluster.auditInfo.dateAccessed should be > initialCluster.auditInfo.dateAccessed

    dbFutureValue {
      clusterQuery.setToRunning(initialCluster.id, initialCluster.dataprocInfo.flatMap(_.hostIp).get, Instant.now)
    } shouldEqual 1
    val runningCluster = dbFutureValue { clusterQuery.getClusterById(initialCluster.id) }.get
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

    dbFutureValue { clusterQuery.mergeInstances(updatedCluster1) } shouldEqual updatedCluster1
    dbFutureValue { clusterQuery.getClusterById(savedCluster1.id) }.get shouldEqual updatedCluster1

    val updatedCluster1Again = savedCluster1.copy(
      instances = Set(masterInstance.copy(status = InstanceStatus.Terminated),
                      workerInstance1.copy(status = InstanceStatus.Terminated))
    )

    dbFutureValue { clusterQuery.mergeInstances(updatedCluster1Again) } shouldEqual updatedCluster1Again
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

    val autoFreezeList = dbFutureValue { clusterQuery.getClustersReadyToAutoFreeze }
    autoFreezeList should contain(runningCluster1)
    //cluster2 is already stopped
    autoFreezeList should not contain stoppedCluster
    autoFreezeList should not contain runningCluster2
    autoFreezeList should not contain autopauseDisabledCluster
  }

  it should "get for dns cache" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .copy(labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"),
            instances = Set(masterInstance, workerInstance1, workerInstance2))
      .save(Some(serviceAccountKey.id))

    // Result should not include labels or instances
    dbFutureValue {
      clusterQuery.getActiveClusterByNameMinimal(savedCluster1.googleProject, savedCluster1.clusterName)
    } shouldEqual
      Some(stripFieldsForListCluster(savedCluster1).copy(labels = Map.empty))
  }

  it should "update master machine type" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .saveWithRuntimeConfig(
        RuntimeConfig.DataprocConfig(3,
                                     "test-master-machine-type",
                                     500,
                                     Some("test-worker-machine-type"),
                                     Some(200),
                                     Some(2),
                                     Some(1))
      )

    val newMachineType = MachineType("this-is-a-new-machine-type")
    dbFutureValue { RuntimeConfigQueries.updateMachineType(savedCluster1.runtimeConfigId, newMachineType, Instant.now) }

    dbFutureValue { RuntimeConfigQueries.getRuntime(savedCluster1.runtimeConfigId) }.machineType shouldBe
      newMachineType
  }

  it should "update master disk size" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).saveWithRuntimeConfig(
      RuntimeConfig.DataprocConfig(3,
                                   "test-master-machine-type",
                                   500,
                                   Some("test-worker-machine-type"),
                                   Some(200),
                                   Some(2),
                                   Some(1))
    )

    val newDiskSize = 1000
    dbFutureValue { RuntimeConfigQueries.updateDiskSize(savedCluster1.runtimeConfigId, newDiskSize, Instant.now) }

    dbFutureValue { RuntimeConfigQueries.getRuntime(savedCluster1.runtimeConfigId) }.diskSize shouldBe
      newDiskSize
  }

  it should "list monitored clusters" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).save()
    val savedCluster2 = makeCluster(2).copy(dataprocInfo = None).save()

    dbFutureValue { clusterQuery.listMonitoredClusterOnly }.toSet shouldBe Set(savedCluster1, savedCluster2)
      .map(stripFieldsForListCluster)
      .map(_.copy(labels = Map.empty))
    dbFutureValue { clusterQuery.listMonitored }.toSet shouldBe Set(savedCluster1, savedCluster2).map(
      stripFieldsForListCluster
    )
  }

  it should "persist custom environment variables" in isolatedDbTest {
    val expectedEvs = Map("foo" -> "bar", "test" -> "this is a test")
    val savedCluster = makeCluster(1).copy(customClusterEnvironmentVariables = expectedEvs).save()

    val retrievedCluster = dbFutureValue { clusterQuery.getClusterById(savedCluster.id) }

    retrievedCluster shouldBe 'defined
    retrievedCluster.get.customClusterEnvironmentVariables shouldBe expectedEvs
    retrievedCluster.get shouldBe savedCluster
  }

  it should "persist runtimeConfig properly" in isolatedDbTest {
    val runtimeConfig = RuntimeConfig.DataprocConfig(3,
                                                     "test-master-machine-type",
                                                     500,
                                                     Some("test-worker-machine-type"),
                                                     Some(200),
                                                     Some(2),
                                                     Some(1))

    val savedCluster = makeCluster(1)
      .saveWithRuntimeConfig(runtimeConfig)

    dbFutureValue { RuntimeConfigQueries.getRuntime(savedCluster.runtimeConfigId) } shouldBe runtimeConfig
  }
}
