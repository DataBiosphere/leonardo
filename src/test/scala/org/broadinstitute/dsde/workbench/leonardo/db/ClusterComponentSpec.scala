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
import org.scalatest.compatible.Assertion

class ClusterComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {
  import ClusterComponentSpec._

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
      dateAccessed = dateAccessed
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
      dateAccessed = dateAccessed
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
      dateAccessed = dateAccessed
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
      dateAccessed = dateAccessed
    )

    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), None) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2, gcsPath("gs://bucket2"), Some(serviceAccountKey.id)) } shouldEqual c2
    dbFutureValue { _.clusterQuery.save(c3, gcsPath("gs://bucket3"), Some(serviceAccountKey.id)) } shouldEqual c3

    // Get the cluster id's assigned by the database (since the id field is auto incremented) to use further below
    val c1Id =  dbFutureValue { _.clusterQuery.getIdByGoogleId(c1.googleId)}.get
    val c2Id =  dbFutureValue { _.clusterQuery.getIdByGoogleId(c2.googleId)}.get
    val c3Id =  dbFutureValue { _.clusterQuery.getIdByGoogleId(c3.googleId)}.get

    val c1WithAssignedId = c1.copy(id = c1Id)
    val c2WithAssignedId = c2.copy(id = c2Id)
    val c3WithAssignedId = c3.copy(id = c3Id)

    // instances are returned by list* methods
    val expectedClusters123 = Seq(c1WithAssignedId, c2WithAssignedId, c3WithAssignedId).map(_.copy(instances = Set.empty))
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs expectedClusters123

    // instances are returned by get* methods
    dbFutureValue { _.clusterQuery.getActiveClusterByName(c1.googleProject, c1.clusterName) } shouldEqual Some(c1WithAssignedId)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(c2.googleProject, c2.clusterName) } shouldEqual Some(c2WithAssignedId)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(c3.googleProject, c3.clusterName) } shouldEqual Some(c3WithAssignedId)

    dbFutureValue { _.clusterErrorQuery.save(c1Id, err1) }
    val c1WithErrId =  dbFutureValue { _.clusterQuery.getIdByGoogleId(c1.googleId)}.get
    val c1WithErrAssignedId = c1WithErr.copy(id = c1WithErrId)

    dbFutureValue { _.clusterQuery.getById(c1Id) } shouldEqual Some(c1WithErrAssignedId)
    dbFutureValue { _.clusterQuery.getById(c2Id) } shouldEqual Some(c2WithAssignedId)
    dbFutureValue { _.clusterQuery.getById(c3Id) } shouldEqual Some(c3WithAssignedId)

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
      dateAccessed = Instant.now())

    dbFailure { _.clusterQuery.save(c4, gcsPath("gs://bucket3"), Some(serviceAccountKey.id)) } shouldBe a[SQLException]

    dbFutureValue { _.clusterQuery.markPendingDeletion(c1Id) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } should contain theSameElementsAs Seq(c2, c3)
    val c1status = dbFutureValue { _.clusterQuery.getById(c1Id) }.get
    c1status.status shouldEqual ClusterStatus.Deleting
    c1status.destroyedDate shouldBe None
    c1status.hostIp shouldBe None
    c1status.instances shouldBe c1.instances

    dbFutureValue { _.clusterQuery.markPendingDeletion(c2Id) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq(c3)
    val c2status = dbFutureValue { _.clusterQuery.getById(c2Id) }.get
    c2status.status shouldEqual ClusterStatus.Deleting
    c2status.destroyedDate shouldBe None
    c2status.hostIp shouldBe None

    dbFutureValue { _.clusterQuery.markPendingDeletion(c3Id) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq()
    val c3status = dbFutureValue { _.clusterQuery.getById(c3Id) }.get
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
      dateAccessed = Instant.now())

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
      dateAccessed = Instant.now())

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
      dateAccessed = Instant.now())

    assertEquivalent(c1) { dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), Some(serviceAccountKey.id)) } }
    assertEquivalent(c2) { dbFutureValue { _.clusterQuery.save(c2, gcsPath("gs://bucket2"), Some(serviceAccountKey.id)) } }
    assertEquivalent(c3) { dbFutureValue { _.clusterQuery.save(c3, gcsPath("gs://bucket3"), Some(serviceAccountKey.id)) } }

    assertEquivalent(Set(c1, c2)) { dbFutureValue { _.clusterQuery.listByLabels(Map.empty, false) }.toSet }
    assertEquivalent(Set(c1)) { dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes"), false) }.toSet }
    assertEquivalent(Set.empty[Cluster]) { dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "no"), false) }.toSet }
    assertEquivalent(Set(c1)) { dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), false) }.toSet }
    assertEquivalent(Set(c1)) { dbFutureValue { _.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), false) }.toSet }
    assertEquivalent(Set(c1)) { dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), false) }.toSet }
    assertEquivalent(Set.empty[Cluster]) { dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), false) }.toSet }
    assertEquivalent(Set.empty[Cluster]) { dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), false) }.toSet }
    assertEquivalent(Set.empty[Cluster]) { dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), false) }.toSet }
    assertEquivalent(Set.empty[Cluster]) { dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no"), true) }.toSet }
    assertEquivalent(Set(c1)) { dbFutureValue { _.clusterQuery.listByLabels(Map("foo" -> "bar", "vcf" -> "no"), true) }.toSet }
    assertEquivalent(Set(c1)) { dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"), true) }.toSet }
    assertEquivalent(Set(c3)) { dbFutureValue { _.clusterQuery.listByLabels(Map("a" -> "b"), true) }.toSet }
    assertEquivalent(Set(c3)) { dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "b"), true) }.toSet }
    assertEquivalent(Set.empty[Cluster]) { dbFutureValue { _.clusterQuery.listByLabels(Map("bam" -> "yes", "a" -> "c"), true) }.toSet }
    assertEquivalent(Set.empty[Cluster]) { dbFutureValue { _.clusterQuery.listByLabels(Map("bogus" -> "value"), true) }.toSet }
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
        dateAccessed = dateAccessed)

    assertEquivalent(initialCluster) { dbFutureValue { _.clusterQuery.save(initialCluster, gcsPath( "gs://bucket1"), Some(serviceAccountKey.id)) } }

    val initialClusterId = dbFutureValue { _.clusterQuery.getIdByGoogleId(initialCluster.googleId)}.get

    // note: this does not update the instance records
    dbFutureValue { _.clusterQuery.setToStopping(initialClusterId) } shouldEqual 1
    val stoppedClusterId = dbFutureValue { _.clusterQuery.getIdByGoogleId(initialCluster.googleId)}.get
    val stoppedCluster = dbFutureValue { _.clusterQuery.getById(stoppedClusterId) }.get
    val expectedStoppedCluster = initialCluster.copy(
      status = ClusterStatus.Stopping,
      hostIp = None,
      dateAccessed = dateAccessed)
    assertEquivalent(expectedStoppedCluster) { stoppedCluster.copy(dateAccessed = dateAccessed) }
    stoppedCluster.dateAccessed should be > initialCluster.dateAccessed

    dbFutureValue { _.clusterQuery.setToRunning(initialClusterId, initialCluster.hostIp.get) } shouldEqual 1
    val runningClusterId = dbFutureValue { _.clusterQuery.getIdByGoogleId(initialCluster.googleId) }.get
    val runningCluster = dbFutureValue { _.clusterQuery.getById(runningClusterId) }.get
    val expectedRunningCluster = initialCluster.copy(
      id = runningClusterId,
      dateAccessed = dateAccessed)
    assertEquivalent(expectedRunningCluster) { runningCluster.copy(dateAccessed = dateAccessed) }
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
        dateAccessed = Instant.now())

    assertEquivalent(c1) { dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), Some(serviceAccountKey.id)) } }

    val updatedC1Id = dbFutureValue { _.clusterQuery.getIdByGoogleId(c1.googleId) }.get
    val updatedC1 = c1.copy(
      id = updatedC1Id,
      instances = Set(
        masterInstance.copy(status = InstanceStatus.Provisioning),
        workerInstance1.copy(status = InstanceStatus.Provisioning),
        workerInstance2.copy(status = InstanceStatus.Provisioning)))

    assertEquivalent(updatedC1) { dbFutureValue { _.clusterQuery.mergeInstances(updatedC1) } }
    assertEquivalent(updatedC1) { dbFutureValue { _.clusterQuery.getById(updatedC1Id) }.get }

    val updatedC1Again = c1.copy(
      instances = Set(
        masterInstance.copy(status = InstanceStatus.Terminated),
        workerInstance1.copy(status = InstanceStatus.Terminated))
    )

    assertEquivalent(updatedC1Again) { dbFutureValue { _.clusterQuery.mergeInstances(updatedC1Again) } }
    assertEquivalent(updatedC1) { dbFutureValue { _.clusterQuery.getById(updatedC1Id) }.get }
  }
}

object ClusterComponentSpec {
  import org.scalatest.Matchers._

  // Equivalence means clusters have the same fields when ignoring the id field
  private[db] def assertEquivalent(cs1: Set[Cluster])(cs2: Set[Cluster]): Assertion = {
    val fixedId = 0

    val cs1WithFixedId = cs1 foreach { _.copy(id = fixedId) }
    val cs2WithFixedId = cs2 foreach { _.copy(id = fixedId) }

    cs1WithFixedId shouldEqual cs2WithFixedId
  }

  private[db] def assertEquivalent(c1: Cluster)(c2: Cluster): Assertion = {
    assertEquivalent(Set(c1))(Set(c2))
  }
}
