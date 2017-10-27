package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.google.gcs.GcsBucketName
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchUserServiceAccountEmail
import org.scalatest.FlatSpecLike

class ClusterComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {

  "ClusterComponent" should "list, save, get, and delete" in isolatedDbTest {
    dbFutureValue { _.clusterQuery.list() } shouldEqual Seq()

    val c1 = Cluster(
      clusterName = name1,
      googleId = UUID.randomUUID(),
      googleProject = project,
      googleServiceAccount = googleServiceAccount,
      googleBucket = GcsBucketName("bucket1"),
      clusterUrl = Cluster.getClusterUrl(project, name1),
      operationName = OperationName("op1"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("numbers.and.dots")),
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no"),
      jupyterExtensionUri = None)

    val c2 = Cluster(
      clusterName = name2,
      googleId = UUID.randomUUID(),
      googleProject = project,
      googleServiceAccount = googleServiceAccount,
      googleBucket = GcsBucketName("bucket2"),
      clusterUrl = Cluster.getClusterUrl(project, name2),
      operationName = OperationName("op2"),
      status = ClusterStatus.Creating,
      hostIp = None,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = jupyterExtensionUri)

    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1")) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2, gcsPath("gs://bucket2")) } shouldEqual c2
    dbFutureValue { _.clusterQuery.list() } should contain theSameElementsAs Seq(c1, c2)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(c1.googleProject, c1.clusterName) } shouldEqual Some(c1)
    dbFutureValue { _.clusterQuery.getActiveClusterByName(c1.googleProject, c2.clusterName) } shouldEqual Some(c2)
    dbFutureValue { _.clusterQuery.getByGoogleId(c1.googleId) } shouldEqual Some(c1)
    dbFutureValue { _.clusterQuery.getByGoogleId(c2.googleId) } shouldEqual Some(c2)

    // (project, name) unique key test

    val c3 = Cluster(
      clusterName = c1.clusterName,
      googleId = UUID.randomUUID(),
      googleProject = c1.googleProject,
      googleServiceAccount = WorkbenchUserServiceAccountEmail("something-new@google.com"),
      googleBucket = GcsBucketName("bucket3"),
      clusterUrl = Cluster.getClusterUrl(c1.googleProject, c1.clusterName),
      operationName = OperationName("op3"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("1.2.3.4")),
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = jupyterExtensionUri)
    dbFailure { _.clusterQuery.save(c3, gcsPath("gs://bucket3")) } shouldBe a[SQLException]

    // googleId unique key test

    val name4 = ClusterName("name4")
    val c4 = Cluster(
      clusterName = name4,
      googleId = c1.googleId,
      googleProject = project,
      googleServiceAccount = WorkbenchUserServiceAccountEmail("something-new@google.com"),
      googleBucket = GcsBucketName("bucket4"),
      clusterUrl = Cluster.getClusterUrl(project, name4),
      operationName = OperationName("op4"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("1.2.3.4")),
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = jupyterExtensionUri)
    dbFailure { _.clusterQuery.save(c4, gcsPath("gs://bucket4")) } shouldBe a[SQLException]

    dbFutureValue { _.clusterQuery.markPendingDeletion(c1.googleId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq(c2)
    val c1status = dbFutureValue { _.clusterQuery.getByGoogleId(c1.googleId) }.get
    c1status.status shouldEqual ClusterStatus.Deleting
    assert(c1status.destroyedDate.nonEmpty)
    c1status.hostIp shouldBe None

    dbFutureValue { _.clusterQuery.markPendingDeletion(c2.googleId) } shouldEqual 1
    dbFutureValue { _.clusterQuery.listActive() } shouldEqual Seq()
    val c2status = dbFutureValue { _.clusterQuery.getByGoogleId(c2.googleId) }.get
    c2status.status shouldEqual ClusterStatus.Deleting
    assert(c2status.destroyedDate.nonEmpty)
    c2status.hostIp shouldBe None
  }

  it should "get by labels" in isolatedDbTest {
    val c1 = Cluster(
      clusterName = name1,
      googleId = UUID.randomUUID(),
      googleProject = project,
      googleServiceAccount = googleServiceAccount,
      googleBucket = GcsBucketName("bucket1"),
      clusterUrl = Cluster.getClusterUrl(project, name1),
      operationName = OperationName("op1"),
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("numbers.and.dots")),
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("bam" -> "yes", "vcf" -> "no", "foo" -> "bar"),
      jupyterExtensionUri = None)

    val c2 = Cluster(
      clusterName = name2,
      googleId = UUID.randomUUID(),
      googleProject = project,
      googleServiceAccount = googleServiceAccount,
      googleBucket = GcsBucketName("bucket2"),
      clusterUrl = Cluster.getClusterUrl(project, name2),
      operationName = OperationName("op2"),
      status = ClusterStatus.Running,
      hostIp = None,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      jupyterExtensionUri = jupyterExtensionUri)

    val c3 = Cluster(
      clusterName = name3,
      googleId = UUID.randomUUID(),
      googleProject = project,
      googleServiceAccount = googleServiceAccount,
      googleBucket = GcsBucketName("bucket3"),
      clusterUrl = Cluster.getClusterUrl(project, name3),
      operationName = OperationName("op3"),
      status = ClusterStatus.Deleted,
      hostIp = None,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map("a" -> "b", "bam" -> "yes"),
      jupyterExtensionUri = jupyterExtensionUri)

    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1")) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2, gcsPath("gs://bucket2")) } shouldEqual c2
    dbFutureValue { _.clusterQuery.save(c3, gcsPath("gs://bucket3")) } shouldEqual c3

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
}
