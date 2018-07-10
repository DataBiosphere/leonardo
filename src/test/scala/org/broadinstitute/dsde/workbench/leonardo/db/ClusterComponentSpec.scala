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
    lazy val c1UUID = UUID.randomUUID()
    val createdDate, dateAccessed = Instant.now()
    val c1 = Cluster(
      clusterName = name1,
      googleId = Option(c1UUID),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
      operationName = Option(OperationName("op1")),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("numbers.and.dots")),
      creator = userEmail,
      createdDate = createdDate,
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no"),
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      stagingBucket = Some(GcsBucketName("testStagingBucket1")),
      errors = List.empty,
      instances = Set(masterInstance, workerInstance1, workerInstance2),
      userJupyterExtensionConfig = Some(userExtConfig),
      dateAccessed = dateAccessed,
      autopauseThreshold = if (autopause) autopauseThreshold else 0

    )

    val c1WithErr = Cluster(
      clusterName = name1,
      googleId = Option(c1UUID),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1),
      operationName = Option(OperationName("op1")),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("numbers.and.dots")),
      creator = userEmail,
      createdDate = createdDate,
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no"),
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      stagingBucket = Some(GcsBucketName("testStagingBucket1")),
      errors = List(err1),
      instances = Set(masterInstance, workerInstance1, workerInstance2),
      userJupyterExtensionConfig = Some(userExtConfig),
      dateAccessed = dateAccessed,
      autopauseThreshold = if (autopause) autopauseThreshold else 0

    )

    val c2 = Cluster(
      clusterName = name2,
      googleId = Option(UUID.randomUUID()),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name2, clusterUrlBase),
      operationName = Option(OperationName("op2")),
      status = ClusterStatus.Creating,
      hostIp = None,
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      stagingBucket = Some(GcsBucketName("testStagingBucket2")),
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      dateAccessed = dateAccessed,
      autopauseThreshold = if (autopause) autopauseThreshold else 0

    )

    val c3 = Cluster(
      clusterName = name3,
      googleId = Option(UUID.randomUUID()),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(3),Some("test-master-machine-type"), Some(500), Some("test-worker-machine-type"), Some(200), Some(2), Some(1)),
      clusterUrl = Cluster.getClusterUrl(project, name3, clusterUrlBase),
      operationName = Option(OperationName("op3")),
      status = ClusterStatus.Running,
      hostIp = None,
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      stagingBucket = Some(GcsBucketName("testStagingBucket3")),
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      dateAccessed = dateAccessed,
      autopauseThreshold = if (autopause) autopauseThreshold else 0)

    val savedC1 = dbFutureValue { _.clusterQuery.save(c1, Option(gcsPath("gs://bucket1")), None) }
    savedC1 shouldEqual c1

    val savedC2 = dbFutureValue { _.clusterQuery.save(c2, Option(gcsPath("gs://bucket2")), Some(serviceAccountKey.id)) }
    savedC2 shouldEqual c2

    val savedC3 = dbFutureValue { _.clusterQuery.save(c3, Option(gcsPath("gs://bucket3")), Some(serviceAccountKey.id)) }
    savedC3 shouldEqual c3

    // instances are returned by list* methods
    val expectedClusters123 = Seq(savedC1, savedC2, savedC3).map(_.copy(instances = Set.empty))
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs expectedClusters123

    // instances are returned by get* methods
    dbFutureValue { _.clusterQuery.getActiveClusterByName(c1.googleProject, c1.clusterName) } shouldEqual Some(savedC1)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(c2.googleProject, c2.clusterName) } shouldEqual Some(savedC2)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(c3.googleProject, c3.clusterName) } shouldEqual Some(savedC3)

    dbFutureValue { _.clusterErrorQuery.save(savedC1.id, err1) }
    val c1WithErrAssignedId = c1WithErr.copy(id = savedC1.id)

    dbFutureValue { _.clusterQuery.getClusterById(savedC1.id) } shouldEqual Some(c1WithErrAssignedId)
    dbFutureValue { _.clusterQuery.getClusterById(savedC2.id) } shouldEqual Some(savedC2)
    dbFutureValue { _.clusterQuery.getClusterById(savedC3.id) } shouldEqual Some(savedC3)

    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(c1.googleProject, c1.clusterName) } shouldEqual None
    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(c2.googleProject, c2.clusterName) } shouldEqual Some(serviceAccountKey.id)
    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(c3.googleProject, c3.clusterName) } shouldEqual Some(serviceAccountKey.id)

    dbFutureValue { _.clusterQuery.countByClusterServiceAccountAndStatus(serviceAccountEmail, ClusterStatus.Creating) } shouldEqual 1
    dbFutureValue { _.clusterQuery.countByClusterServiceAccountAndStatus(serviceAccountEmail, ClusterStatus.Running) } shouldEqual 0

    // (project, name) unique key test

    val c4 = Cluster(
      clusterName = c1.clusterName,
      googleId = Option(UUID.randomUUID()),
      googleProject = c1.googleProject,
      serviceAccountInfo = ServiceAccountInfo(None, Some(WorkbenchEmail("something-new@google.com"))),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(c1.googleProject, c1.clusterName, clusterUrlBase),
      operationName = Option(OperationName("op3")),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("1.2.3.4")),
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      stagingBucket = Some(GcsBucketName("testStagingBucket4")),
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      dateAccessed = Instant.now(),
      autopauseThreshold = if (autopause) autopauseThreshold else 0)

    dbFailure { _.clusterQuery.save(c4, Option(gcsPath("gs://bucket3")), Some(serviceAccountKey.id)) } shouldBe a[SQLException]


    dbFutureValue { _.clusterQuery.markPendingDeletion(savedC1.id) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } should contain theSameElementsAs Seq(c2, c3)

    val c1status = dbFutureValue { _.clusterQuery.getClusterById(savedC1.id) }.get
    c1status.status shouldEqual ClusterStatus.Deleting
    c1status.destroyedDate shouldBe None
    c1status.hostIp shouldBe None
    c1status.instances shouldBe c1.instances

    dbFutureValue { _.clusterQuery.markPendingDeletion(savedC2.id) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq(c3)
    val c2status = dbFutureValue { _.clusterQuery.getClusterById(savedC2.id) }.get
    c2status.status shouldEqual ClusterStatus.Deleting
    c2status.destroyedDate shouldBe None
    c2status.hostIp shouldBe None

    dbFutureValue { _.clusterQuery.markPendingDeletion(savedC3.id) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq()
    val c3status = dbFutureValue { _.clusterQuery.getClusterById(savedC3.id) }.get
    c3status.status shouldEqual ClusterStatus.Deleting
    c3status.destroyedDate shouldBe None
    c3status.hostIp shouldBe None
  }

  it should "get by labels" in isolatedDbTest {
    val c1 = Cluster(
      clusterName = name1,
      googleId = Option(UUID.randomUUID()),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
      operationName = Option(OperationName("op1")),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("numbers.and.dots")),
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"),
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      stagingBucket = Some(GcsBucketName("testStagingBucket1")),
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      dateAccessed = Instant.now(),
      autopauseThreshold = if (autopause) autopauseThreshold else 0)


    val c2 = Cluster(
      clusterName = name2,
      googleId = Option(UUID.randomUUID()),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name2, clusterUrlBase),
      operationName = Option(OperationName("op2")),
      status = ClusterStatus.Running,
      hostIp = None,
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      stagingBucket = Some(GcsBucketName("testStagingBucket2")),
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      dateAccessed = Instant.now(),
      autopauseThreshold = if (autopause) autopauseThreshold else 0)

    val c3 = Cluster(
      clusterName = name3,
      googleId = Option(UUID.randomUUID()),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name3, clusterUrlBase),
      operationName = Option(OperationName("op3")),
      status = ClusterStatus.Deleted,
      hostIp = None,
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("a" -> "b", "bam" -> "yes"),
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      stagingBucket = Some(GcsBucketName("testStagingBucket3")),
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      dateAccessed = Instant.now(),
      autopauseThreshold = if (autopause) autopauseThreshold else 0)

    dbFutureValue { _.clusterQuery.save(c1, Option(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2, Option(gcsPath("gs://bucket2")), Some(serviceAccountKey.id)) } shouldEqual c2
    dbFutureValue { _.clusterQuery.save(c3, Option(gcsPath("gs://bucket3")), Some(serviceAccountKey.id)) } shouldEqual c3
    dbFutureValue { _.clusterQuery.listByLabels(Map.empty, false) }.toSet shouldEqual Set(c1, c2)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes"), false) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "no"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), false) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), false) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), false) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), false) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), true) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), true) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), true) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), true) }.toSet shouldEqual Set(c3)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), true) }.toSet shouldEqual Set(c3)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), true) }.toSet shouldEqual Set.empty[Cluster]
    dbFutureValue { _.clusterQuery.listByLabels(Map("bogus" -> "value"), true) }.toSet shouldEqual Set.empty[Cluster]
  }

  it should "stop and start a cluster" in isolatedDbTest {
    val dateAccessed = Instant.now()

    val initialCluster =
      Cluster(
        clusterName = name1,
        googleId = Option(UUID.randomUUID()),
        googleProject = project,
        serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
        machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
        clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
        operationName = Option(OperationName("op1")),
        status = ClusterStatus.Running,
        hostIp = Some(IP("numbers.and.dots")),
        creator = userEmail,
        createdDate = Instant.now(),
        destroyedDate = None,
        labels = Map("bam" -> "yes", "vcf" -> "no"),
        jupyterExtensionUri = None,
        jupyterUserScriptUri = None,
        stagingBucket = Some(GcsBucketName("testStagingBucket1")),
        errors = List.empty,
        instances = Set(masterInstance, workerInstance1, workerInstance2),
        userJupyterExtensionConfig = None,
        dateAccessed = dateAccessed,
        autopauseThreshold = if (autopause) autopauseThreshold else 0)

    val savedInitialCluster = dbFutureValue { _.clusterQuery.save(initialCluster, Option(gcsPath( "gs://bucket1")), Some(serviceAccountKey.id)) }
    savedInitialCluster shouldEqual initialCluster

    // note: this does not update the instance records
    dbFutureValue { _.clusterQuery.setToStopping(savedInitialCluster.id) } shouldEqual 1
    val stoppedCluster = dbFutureValue { _.clusterQuery.getClusterById(savedInitialCluster.id) }.get
    val expectedStoppedCluster = initialCluster.copy(
      status = ClusterStatus.Stopping,
      hostIp = None,
      dateAccessed = dateAccessed)
    stoppedCluster.copy(dateAccessed = dateAccessed) shouldEqual expectedStoppedCluster
    stoppedCluster.dateAccessed should be > initialCluster.dateAccessed

    dbFutureValue { _.clusterQuery.setToRunning(savedInitialCluster.id, initialCluster.hostIp.get) } shouldEqual 1
    val runningCluster = dbFutureValue { _.clusterQuery.getClusterById(savedInitialCluster.id) }.get
    val expectedRunningCluster = initialCluster.copy(
      id = savedInitialCluster.id,
      dateAccessed = dateAccessed)
    runningCluster.copy(dateAccessed = dateAccessed) shouldEqual expectedRunningCluster
    runningCluster.dateAccessed should be > stoppedCluster.dateAccessed
  }

  it should "merge instances" in isolatedDbTest {

    val c1 =
      Cluster(
        clusterName = name1,
        googleId = Option(UUID.randomUUID()),
        googleProject = project,
        serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
        machineConfig = MachineConfig(Some(0), Some(""), Some(500)),
        clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
        operationName = Option(OperationName("op1")),
        status = ClusterStatus.Running,
        hostIp = Some(IP("numbers.and.dots")),
        creator = userEmail,
        createdDate = Instant.now(),
        destroyedDate = None,
        labels = Map("bam" -> "yes", "vcf" -> "no"),
        jupyterExtensionUri = None,
        jupyterUserScriptUri = None,
        stagingBucket = Some(GcsBucketName("testStagingBucket1")),
        errors = List.empty,
        instances = Set(masterInstance),
        userJupyterExtensionConfig = None,
        dateAccessed = Instant.now(),
        autopauseThreshold = if (autopause) autopauseThreshold else 0)

    val savedC1 = dbFutureValue { _.clusterQuery.save(c1, Option(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) }
    savedC1 shouldEqual c1

    val updatedC1 = c1.copy(
      id = savedC1.id,
      instances = Set(
        masterInstance.copy(status = InstanceStatus.Provisioning),
        workerInstance1.copy(status = InstanceStatus.Provisioning),
        workerInstance2.copy(status = InstanceStatus.Provisioning)))

    dbFutureValue { _.clusterQuery.mergeInstances(updatedC1) } shouldEqual updatedC1
    dbFutureValue { _.clusterQuery.getClusterById(savedC1.id) }.get shouldEqual updatedC1

    val updatedC1Again = c1.copy(
      instances = Set(
        masterInstance.copy(status = InstanceStatus.Terminated),
        workerInstance1.copy(status = InstanceStatus.Terminated))
    )

    dbFutureValue { _.clusterQuery.mergeInstances(updatedC1Again) } shouldEqual updatedC1Again
    dbFutureValue { _.clusterQuery.getClusterById(savedC1.id) }.get shouldEqual updatedC1
  }

  it should "get list of clusters to auto freeze" in isolatedDbTest {
    val runningCluster1 = Cluster(
      clusterName = name1,
      googleId = Option(UUID.randomUUID()),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0), Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
      operationName = Option(OperationName("op1")),
      status = ClusterStatus.Running,
      hostIp = Some(IP("numbers.and.dots")),
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no"),
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      stagingBucket = Some(GcsBucketName("testStagingBucket1")),
      errors = List.empty,
      instances = Set(masterInstance),
      userJupyterExtensionConfig = None,
      dateAccessed = Instant.now(),
      autopauseThreshold = if (autopause) autopauseThreshold else 0)

    val runningCluster2 = Cluster(
      clusterName = name2,
      googleId = Option(UUID.randomUUID()),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0), Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name2, clusterUrlBase),
      operationName = Option(OperationName("op2")),
      status = ClusterStatus.Stopped,
      hostIp = None,
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      stagingBucket = Some(GcsBucketName("testStagingBucket2")),
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      dateAccessed = Instant.now().minus(100, ChronoUnit.DAYS),
      autopauseThreshold = if (autopause) autopauseThreshold else 0)

    dbFutureValue { _.clusterQuery.save(runningCluster1, gcsPath("gs://bucket1"), Some(serviceAccountKey.id)) } shouldEqual runningCluster1
    dbFutureValue { _.clusterQuery.save(runningCluster2, gcsPath("gs://bucket1"), Some(serviceAccountKey.id)) } shouldEqual runningCluster2
    dbFutureValue { _.clusterQuery.save(stoppedCluster, gcsPath("gs://bucket1"), Some(serviceAccountKey.id)) } shouldEqual stoppedCluster

    val autoFreezeList = dbFutureValue { _.clusterQuery.getClustersReadyToAutoFreeze() }
    autoFreezeList should contain (runningCluster1)
      //c2 is already stopped
    autoFreezeList should not contain stoppedCluster
    autoFreezeList should not contain runningCluster2

  }
}
