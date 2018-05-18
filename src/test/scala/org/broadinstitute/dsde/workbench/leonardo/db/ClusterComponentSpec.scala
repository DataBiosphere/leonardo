package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.FlatSpecLike

class ClusterComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {

  "ClusterComponent" should "list, save, get, and delete" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.list() } shouldEqual Seq()

    lazy val err1 = ClusterError("some failure", 10, Instant.now().truncatedTo(ChronoUnit.SECONDS))
    lazy val c1UUID = UUID.randomUUID()
    val createdDate, dateAccessed = Instant.now()
    val c1 = Cluster(
      clusterName = name1,
      googleId = c1UUID,
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
      operationName = OperationName("op1"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("numbers.and.dots")),
      creator = userEmail,
      createdDate = createdDate,
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no"),
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      Some(GcsBucketName("testStagingBucket1")),
      List.empty,
      Set(masterInstance, workerInstance1, workerInstance2),
      Some(userExtConfig),
      dateAccessed
    )

    val c1witherr1 = Cluster(
      clusterName = name1,
      googleId = c1UUID,
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1),
      operationName = OperationName("op1"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("numbers.and.dots")),
      creator = userEmail,
      createdDate = createdDate,
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no"),
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      Some(GcsBucketName("testStagingBucket1")),
      List(err1),
      Set(masterInstance, workerInstance1, workerInstance2),
      Some(userExtConfig),
      dateAccessed
    )

    val c2 = Cluster(
      clusterName = name2,
      googleId = UUID.randomUUID(),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name2, clusterUrlBase),
      operationName = OperationName("op2"),
      status = ClusterStatus.Creating,
      hostIp = None,
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      Some(GcsBucketName("testStagingBucket2")),
      List.empty,
      Set.empty,
      None,
      dateAccessed
    )

    val c3 = Cluster(
      clusterName = name3,
      googleId = UUID.randomUUID(),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(3),Some("test-master-machine-type"), Some(500), Some("test-worker-machine-type"), Some(200), Some(2), Some(1)),
      clusterUrl = Cluster.getClusterUrl(project, name3, clusterUrlBase),
      operationName = OperationName("op3"),
      status = ClusterStatus.Running,
      hostIp = None,
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      Some(GcsBucketName("testStagingBucket3")),
      List.empty,
      Set.empty,
      None,
      dateAccessed
    )


    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2, gcsPath("gs://bucket2"), Some(serviceAccountKey.id)) } shouldEqual c2
    dbFutureValue { _.clusterQuery.save(c3, gcsPath("gs://bucket3"), Some(serviceAccountKey.id)) } shouldEqual c3
    val c1Id =  dbFutureValue { _.clusterQuery.getIdByGoogleId(c1.googleId)}.get
    // instances not returned by list* methods
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs Seq(c1, c2, c3).map(_.copy(instances = Set.empty))
    // instances are returned by get* methods
    dbFutureValue { _.clusterQuery.getActiveClusterByName(c1.googleProject, c1.clusterName) } shouldEqual Some(c1)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(c2.googleProject, c2.clusterName) } shouldEqual Some(c2)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(c3.googleProject, c3.clusterName) } shouldEqual Some(c3)
    dbFutureValue { _.clusterErrorQuery.save(c1Id, err1) }
    dbFutureValue { _.clusterQuery.getByGoogleId(c1.googleId) } shouldEqual Some(c1witherr1)
    dbFutureValue { _.clusterQuery.getByGoogleId(c2.googleId) } shouldEqual Some(c2)
    dbFutureValue { _.clusterQuery.getByGoogleId(c3.googleId) } shouldEqual Some(c3)
    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(c1.googleProject, c1.clusterName) } shouldEqual None
    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(c2.googleProject, c2.clusterName) } shouldEqual Some(serviceAccountKey.id)
    dbFutureValue { _.clusterQuery.getServiceAccountKeyId(c3.googleProject, c3.clusterName) } shouldEqual Some(serviceAccountKey.id)
    dbFutureValue { _.clusterQuery.countByClusterServiceAccountAndStatus(serviceAccountEmail, ClusterStatus.Creating) } shouldEqual 1
    dbFutureValue { _.clusterQuery.countByClusterServiceAccountAndStatus(serviceAccountEmail, ClusterStatus.Running) } shouldEqual 0

    // (project, name) unique key test

    val c4 = Cluster(
      clusterName = c1.clusterName,
      googleId = UUID.randomUUID(),
      googleProject = c1.googleProject,
      serviceAccountInfo = ServiceAccountInfo(None, Some(WorkbenchEmail("something-new@google.com"))),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(c1.googleProject, c1.clusterName, clusterUrlBase),
      operationName = OperationName("op3"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("1.2.3.4")),
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      Some(GcsBucketName("testStagingBucket4")),
      List.empty,
      Set.empty,
      None,
      Instant.now()
    )
    dbFailure { _.clusterQuery.save(c4, gcsPath("gs://bucket3"), Some(serviceAccountKey.id)) } shouldBe a[SQLException]

    // googleId unique key test

    val name5 = ClusterName("name5")
    val c5 = Cluster(
      clusterName = name5,
      googleId = c1.googleId,
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(WorkbenchEmail("something-new@google.com"))),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name5, clusterUrlBase),
      operationName = OperationName("op4"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("1.2.3.4")),
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      Some(GcsBucketName("testStagingBucket5")),
      List.empty,
      Set.empty,
      None,
      Instant.now()
    )
    dbFailure { _.clusterQuery.save(c5, gcsPath("gs://bucket5"), Some(serviceAccountKey.id)) } shouldBe a[SQLException]

    dbFutureValue { _.clusterQuery.markPendingDeletion(c1.googleId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } should contain theSameElementsAs Seq(c2, c3)
    val c1status = dbFutureValue { _.clusterQuery.getByGoogleId(c1.googleId) }.get
    c1status.status shouldEqual ClusterStatus.Deleting
    c1status.destroyedDate shouldBe None
    c1status.hostIp shouldBe None
    c1status.instances shouldBe c1.instances

    dbFutureValue { _.clusterQuery.markPendingDeletion(c2.googleId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq(c3)
    val c2status = dbFutureValue { _.clusterQuery.getByGoogleId(c2.googleId) }.get
    c2status.status shouldEqual ClusterStatus.Deleting
    c2status.destroyedDate shouldBe None
    c2status.hostIp shouldBe None

    dbFutureValue { _.clusterQuery.markPendingDeletion(c3.googleId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq()
    val c3status = dbFutureValue { _.clusterQuery.getByGoogleId(c3.googleId) }.get
    c3status.status shouldEqual ClusterStatus.Deleting
    c3status.destroyedDate shouldBe None
    c3status.hostIp shouldBe None
  }

  it should "get by labels" in isolatedDbTest {
    val c1 = Cluster(
      clusterName = name1,
      googleId = UUID.randomUUID(),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
      operationName = OperationName("op1"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("numbers.and.dots")),
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"),
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      Some(GcsBucketName("testStagingBucket1")),
      List.empty,
      Set.empty,
      None,
      Instant.now()
    )

    val c2 = Cluster(
      clusterName = name2,
      googleId = UUID.randomUUID(),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name2, clusterUrlBase),
      operationName = OperationName("op2"),
      status = ClusterStatus.Running,
      hostIp = None,
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      Some(GcsBucketName("testStagingBucket2")),
      List.empty,
      Set.empty,
      None,
      Instant.now()
    )

    val c3 = Cluster(
      clusterName = name3,
      googleId = UUID.randomUUID(),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name3, clusterUrlBase),
      operationName = OperationName("op3"),
      status = ClusterStatus.Deleted,
      hostIp = None,
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("a" -> "b", "bam" -> "yes"),
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      Some(GcsBucketName("testStagingBucket3")),
      List.empty,
      Set.empty,
      None,
      Instant.now()
    )

    dbFutureValue { _.clusterQuery.save(c1, gcsPath( "gs://bucket1"), Some(serviceAccountKey.id)) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2, gcsPath("gs://bucket2"), Some(serviceAccountKey.id)) } shouldEqual c2
    dbFutureValue { _.clusterQuery.save(c3, gcsPath("gs://bucket3"), Some(serviceAccountKey.id)) } shouldEqual c3

    dbFutureValue { _.clusterQuery.listByLabels(Map.empty, false) }.toSet shouldEqual Set(c1, c2)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes"), false) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "no"), false) }.toSet shouldEqual Set.empty
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), false) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), false) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), false) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), false) }.toSet shouldEqual Set.empty
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), false) }.toSet shouldEqual Set.empty
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), false) }.toSet shouldEqual Set.empty
    dbFutureValue { _.clusterQuery.listByLabels(Map("bogus" -> "value"), false) }.toSet shouldEqual Set.empty


    dbFutureValue { _.clusterQuery.listByLabels(Map.empty, true) }.toSet shouldEqual Set(c1, c2, c3)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes"), true) }.toSet shouldEqual Set(c1, c3)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "no"), true) }.toSet shouldEqual Set.empty
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), true) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), true) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), true) }.toSet shouldEqual Set(c1)
    dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), true) }.toSet shouldEqual Set(c3)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), true) }.toSet shouldEqual Set(c3)
    dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), true) }.toSet shouldEqual Set.empty
    dbFutureValue { _.clusterQuery.listByLabels(Map("bogus" -> "value"), true) }.toSet shouldEqual Set.empty
  }

  it should "stop and start a cluster" in isolatedDbTest {
    val dateAccessed = Instant.now()
    val initialCluster = Cluster(
      clusterName = name1,
      googleId = UUID.randomUUID(),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
      operationName = OperationName("op1"),
      status = ClusterStatus.Running,
      hostIp = Some(IP("numbers.and.dots")),
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no"),
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      Some(GcsBucketName("testStagingBucket1")),
      List.empty,
      Set(masterInstance, workerInstance1, workerInstance2),
      None,
      dateAccessed
    )

    dbFutureValue { _.clusterQuery.save(initialCluster, gcsPath( "gs://bucket1"), Some(serviceAccountKey.id)) } shouldEqual initialCluster
    // note: this does not update the instance records
    dbFutureValue { _.clusterQuery.setToStopping(initialCluster.googleId) } shouldEqual 1
    val stoppedCluster = dbFutureValue { _.clusterQuery.getByGoogleId(initialCluster.googleId) }
    stoppedCluster.map(_.copy(dateAccessed = dateAccessed)) shouldEqual Some(
      initialCluster.copy(
        status = ClusterStatus.Stopping,
        hostIp = None,
        dateAccessed = dateAccessed
      )
    )

    stoppedCluster.map(_.dateAccessed).get should be > initialCluster.dateAccessed

    dbFutureValue { _.clusterQuery.setToRunning(initialCluster.googleId, initialCluster.hostIp.get) } shouldEqual 1
    val runningCluster = dbFutureValue { _.clusterQuery.getByGoogleId(initialCluster.googleId) }
    runningCluster.map(_.copy(dateAccessed = dateAccessed)) shouldEqual Some(initialCluster.copy(dateAccessed = dateAccessed))
    runningCluster.map(_.dateAccessed) should be > stoppedCluster.map(_.dateAccessed)
  }

  it should "merge instances" in isolatedDbTest {
    val c1 = Cluster(
      clusterName = name1,
      googleId = UUID.randomUUID(),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0), Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
      operationName = OperationName("op1"),
      status = ClusterStatus.Running,
      hostIp = Some(IP("numbers.and.dots")),
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no"),
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      Some(GcsBucketName("testStagingBucket1")),
      List.empty,
      Set(masterInstance),
      None,
      Instant.now()
    )

    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), Some(serviceAccountKey.id)) } shouldEqual c1

    val updatedC1 = c1.copy(
      instances = Set(
        masterInstance.copy(status = InstanceStatus.Provisioning),
        workerInstance1.copy(status = InstanceStatus.Provisioning),
        workerInstance2.copy(status = InstanceStatus.Provisioning))
    )

    dbFutureValue { _.clusterQuery.mergeInstances(updatedC1) } shouldBe updatedC1
    dbFutureValue { _.clusterQuery.getByGoogleId(c1.googleId) } shouldBe Some(updatedC1)

    val updatedC1Again = c1.copy(
      instances = Set(
        masterInstance.copy(status = InstanceStatus.Terminated),
        workerInstance1.copy(status = InstanceStatus.Terminated))
    )

    dbFutureValue { _.clusterQuery.mergeInstances(updatedC1Again) } shouldBe updatedC1Again
    dbFutureValue { _.clusterQuery.getByGoogleId(c1.googleId) } shouldBe Some(updatedC1Again)
  }

}
