package org.broadinstitute.dsde.workbench.leonardo
package db

import java.sql.SQLException
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import org.broadinstitute.dsde.workbench.google2.MachineTypeName
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.{
  clusterEq,
  clusterSeqEq,
  stripFieldsForListCluster
}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.scalatest.FlatSpecLike
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global

class ClusterComponentSpec extends FlatSpecLike with TestComponent with GcsPathUtils with ScalaFutures {
  "ClusterComponent" should "save cluster with properties properly" in isolatedDbTest {
    val cluster = makeCluster(1).copy(dataprocProperties = Map("spark:spark.executor.memory" -> "10g"))
    val savedCluster = cluster.save()
    savedCluster.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual (cluster) //input cluster's runtimeConfigId is fake -1
  }

  "ClusterComponent" should "list, save, get, and delete" in isolatedDbTest {
    dbFutureValue { clusterQuery.listWithLabels } shouldEqual Seq()

    lazy val err1 = RuntimeError("some failure", 10, Instant.now().truncatedTo(ChronoUnit.SECONDS))
    lazy val cluster1UUID = UUID.randomUUID()
    val cluster1 = makeCluster(1).copy(
      asyncRuntimeFields = Some(makeDataprocInfo(1).copy(googleId = cluster1UUID)),
      dataprocInstances = Set(masterInstance, workerInstance1, workerInstance2),
      stopAfterCreation = true
    )

    val cluster1WithErr = makeCluster(1).copy(
      asyncRuntimeFields = Some(makeDataprocInfo(1).copy(googleId = cluster1UUID)),
      errors = List(err1),
      dataprocInstances = Set(masterInstance, workerInstance1, workerInstance2)
    )

    val cluster2 = makeCluster(2).copy(status = RuntimeStatus.Creating)

    val cluster3 = makeCluster(3).copy(
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      status = RuntimeStatus.Running
    )

    val cluster4 = makeCluster(4).copy(runtimeName = cluster1.runtimeName, googleProject = cluster1.googleProject)

    val savedCluster1 = cluster1.save(None)
    savedCluster1.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual cluster1

    val savedCluster2 = cluster2.save()
    savedCluster2.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual cluster2

    val savedCluster3 = cluster3.saveWithRuntimeConfig(
      RuntimeConfig.DataprocConfig(3,
                                   MachineTypeName("test-master-machine-type"),
                                   500,
                                   Some(MachineTypeName("test-worker-machine-type")),
                                   Some(200),
                                   Some(2),
                                   Some(1))
    )
    savedCluster3.copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual cluster3

    // instances are returned by list* methods
    val expectedClusters123 =
      Seq(savedCluster1, savedCluster2, savedCluster3).map(_.copy(dataprocInstances = Set.empty))
    dbFutureValue { clusterQuery.listWithLabels } should contain theSameElementsAs expectedClusters123.map(
      stripFieldsForListCluster
    )

    // instances are returned by get* methods
    dbFutureValue { clusterQuery.getActiveClusterByName(cluster1.googleProject, cluster1.runtimeName) } shouldEqual Some(
      savedCluster1
    )
    dbFutureValue { clusterQuery.getActiveClusterByName(cluster2.googleProject, cluster2.runtimeName) } shouldEqual Some(
      savedCluster2
    )
    dbFutureValue { clusterQuery.getActiveClusterByName(cluster3.googleProject, cluster3.runtimeName) } shouldEqual Some(
      savedCluster3
    )

    dbFutureValue { clusterErrorQuery.save(savedCluster1.id, err1) }
    val cluster1WithErrAssignedId = cluster1WithErr.copy(id = savedCluster1.id,
                                                         stopAfterCreation = true,
                                                         runtimeConfigId = savedCluster1.runtimeConfigId)

    dbFutureValue { clusterQuery.getClusterById(savedCluster1.id) } shouldEqual Some(cluster1WithErrAssignedId)
    dbFutureValue { clusterQuery.getClusterById(savedCluster2.id) } shouldEqual Some(savedCluster2)
    dbFutureValue { clusterQuery.getClusterById(savedCluster3.id) } shouldEqual Some(savedCluster3)

    dbFutureValue { clusterQuery.getServiceAccountKeyId(cluster1.googleProject, cluster1.runtimeName) } shouldEqual None
    dbFutureValue { clusterQuery.getServiceAccountKeyId(cluster2.googleProject, cluster2.runtimeName) } shouldEqual Some(
      serviceAccountKey.id
    )
    dbFutureValue { clusterQuery.getServiceAccountKeyId(cluster3.googleProject, cluster3.runtimeName) } shouldEqual Some(
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
    cluster1status.status shouldEqual RuntimeStatus.Deleting
    cluster1status.auditInfo.destroyedDate shouldBe None
    cluster1status.asyncRuntimeFields.flatMap(_.hostIp) shouldBe None
    cluster1status.dataprocInstances shouldBe cluster1.dataprocInstances

    dbFutureValue { clusterQuery.markPendingDeletion(savedCluster2.id, Instant.now) } shouldEqual 1
    dbFutureValue { clusterQuery.listActiveWithLabels }
      .map(_.copy(runtimeConfigId = RuntimeConfigId(-1))) shouldEqual Seq(cluster3).map(stripFieldsForListCluster)
    val cluster2status = dbFutureValue { clusterQuery.getClusterById(savedCluster2.id) }.get
    cluster2status.status shouldEqual RuntimeStatus.Deleting
    cluster2status.auditInfo.destroyedDate shouldBe None
    cluster2status.asyncRuntimeFields.flatMap(_.hostIp) shouldBe None

    dbFutureValue { clusterQuery.markPendingDeletion(savedCluster3.id, Instant.now) } shouldEqual 1
    dbFutureValue { clusterQuery.listActiveWithLabels } shouldEqual Seq()
    val cluster3status = dbFutureValue { clusterQuery.getClusterById(savedCluster3.id) }.get
    cluster3status.status shouldEqual RuntimeStatus.Deleting
    cluster3status.auditInfo.destroyedDate shouldBe None
    cluster3status.asyncRuntimeFields.flatMap(_.hostIp) shouldBe None
  }

  it should "stop and start a cluster" in isolatedDbTest {
    val dateAccessed = Instant.now()
    val initialCluster = makeCluster(1).copy(status = RuntimeStatus.Running).save()

    // note: this does not update the instance records
    dbFutureValue { clusterQuery.setToStopping(initialCluster.id, Instant.now) } shouldEqual 1
    val stoppedCluster = dbFutureValue { clusterQuery.getClusterById(initialCluster.id) }.get
    val expectedStoppedCluster = initialCluster.copy(
      asyncRuntimeFields = initialCluster.asyncRuntimeFields.map(_.copy(hostIp = None)),
      auditInfo = initialCluster.auditInfo.copy(dateAccessed = dateAccessed),
      status = RuntimeStatus.Stopping
    )
    stoppedCluster.copy(auditInfo = stoppedCluster.auditInfo.copy(dateAccessed = dateAccessed)) shouldEqual expectedStoppedCluster
    stoppedCluster.auditInfo.dateAccessed should be > initialCluster.auditInfo.dateAccessed

    dbFutureValue {
      clusterQuery.setToRunning(initialCluster.id, initialCluster.asyncRuntimeFields.flatMap(_.hostIp).get, Instant.now)
    } shouldEqual 1
    val runningCluster = dbFutureValue { clusterQuery.getClusterById(initialCluster.id) }.get
    val expectedRunningCluster = initialCluster.copy(id = initialCluster.id,
                                                     auditInfo =
                                                       initialCluster.auditInfo.copy(dateAccessed = dateAccessed))
    runningCluster.copy(auditInfo = runningCluster.auditInfo.copy(dateAccessed = dateAccessed)) shouldEqual expectedRunningCluster
    runningCluster.auditInfo.dateAccessed should be > stoppedCluster.auditInfo.dateAccessed
  }

  it should "merge instances" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).copy(dataprocInstances = Set(masterInstance)).save()

    val updatedCluster1 = savedCluster1.copy(
      id = savedCluster1.id,
      dataprocInstances = Set(
        masterInstance.copy(status = InstanceStatus.Provisioning),
        workerInstance1.copy(status = InstanceStatus.Provisioning),
        workerInstance2.copy(status = InstanceStatus.Provisioning)
      )
    )

    dbFutureValue { clusterQuery.mergeInstances(updatedCluster1) } shouldEqual updatedCluster1
    dbFutureValue { clusterQuery.getClusterById(savedCluster1.id) }.get shouldEqual updatedCluster1

    val updatedCluster1Again = savedCluster1.copy(
      dataprocInstances = Set(masterInstance.copy(status = InstanceStatus.Terminated),
                              workerInstance1.copy(status = InstanceStatus.Terminated))
    )

    dbFutureValue { clusterQuery.mergeInstances(updatedCluster1Again) } shouldEqual updatedCluster1Again
  }

  it should "get list of clusters to auto freeze" in isolatedDbTest {
    val runningCluster1 = makeCluster(1)
      .copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS)),
            status = RuntimeStatus.Running)
      .save()

    val runningCluster2 = makeCluster(2)
      .copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS)),
            status = RuntimeStatus.Stopped)
      .save()

    val stoppedCluster = makeCluster(3).copy(status = RuntimeStatus.Stopped).save()

    val autopauseDisabledCluster = makeCluster(4)
      .copy(auditInfo = auditInfo.copy(dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS)),
            status = RuntimeStatus.Running,
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
            dataprocInstances = Set(masterInstance, workerInstance1, workerInstance2))
      .save(Some(serviceAccountKey.id))

    // Result should not include labels or instances
    dbFutureValue {
      clusterQuery.getActiveClusterByNameMinimal(savedCluster1.googleProject, savedCluster1.runtimeName)
    } shouldEqual
      Some(stripFieldsForListCluster(savedCluster1).copy(labels = Map.empty))
  }

  it should "update master machine type" in isolatedDbTest {
    val savedCluster1 = makeCluster(1)
      .saveWithRuntimeConfig(
        RuntimeConfig.DataprocConfig(3,
                                     MachineTypeName("test-master-machine-type"),
                                     500,
                                     Some(MachineTypeName("test-worker-machine-type")),
                                     Some(200),
                                     Some(2),
                                     Some(1))
      )

    val newMachineType = MachineTypeName("this-is-a-new-machine-type")
    dbFutureValue { RuntimeConfigQueries.updateMachineType(savedCluster1.runtimeConfigId, newMachineType, Instant.now) }

    dbFutureValue { RuntimeConfigQueries.getRuntimeConfig(savedCluster1.runtimeConfigId) }.machineType shouldBe
      newMachineType
  }

  it should "update master disk size" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).saveWithRuntimeConfig(
      RuntimeConfig.DataprocConfig(3,
                                   MachineTypeName("test-master-machine-type"),
                                   500,
                                   Some(MachineTypeName("test-worker-machine-type")),
                                   Some(200),
                                   Some(2),
                                   Some(1))
    )

    val newDiskSize = 1000
    dbFutureValue { RuntimeConfigQueries.updateDiskSize(savedCluster1.runtimeConfigId, newDiskSize, Instant.now) }

    dbFutureValue { RuntimeConfigQueries.getRuntimeConfig(savedCluster1.runtimeConfigId) }.diskSize shouldBe
      newDiskSize
  }

  it should "list monitored clusters" in isolatedDbTest {
    val savedCluster1 = makeCluster(1).save()
    val savedCluster2 = makeCluster(2).copy(asyncRuntimeFields = None).save()

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
                                                     MachineTypeName("test-master-machine-type"),
                                                     500,
                                                     Some(MachineTypeName("test-worker-machine-type")),
                                                     Some(200),
                                                     Some(2),
                                                     Some(1))

    val savedCluster = makeCluster(1)
      .saveWithRuntimeConfig(runtimeConfig)

    dbFutureValue { RuntimeConfigQueries.getRuntimeConfig(savedCluster.runtimeConfigId) } shouldBe runtimeConfig
  }
}
