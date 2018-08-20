package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.FlatSpecLike

class ClusterErrorComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {

  "ClusterErrorComponent" should "save, and get" in isolatedDbTest {
    val c1 =
      Cluster(
      clusterName = name1,
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      dataprocInfo = DataprocInfo(Option(UUID.randomUUID()), Option(OperationName("op1")), Some(GcsBucketName("testStagingBucket1")), None),
      auditInfo = AuditInfo(userEmail, Instant.now(), Option(Instant.now()), Instant.now()),
      machineConfig = MachineConfig(Some(0), Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1),
      status = ClusterStatus.Creating,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      autopauseThreshold = 30,
      defaultClientId = None)

    lazy val timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val clusterError = ClusterError("Some Error", 10, timestamp)

    val savedC1 = dbFutureValue { _.clusterQuery.save(c1, Option(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) }
    savedC1 shouldEqual c1

    dbFutureValue {_.clusterErrorQuery.get(savedC1.id)} shouldEqual List.empty
    dbFutureValue {_.clusterErrorQuery.save(savedC1.id, clusterError)}
    dbFutureValue {_.clusterErrorQuery.get(savedC1.id)} shouldEqual List(clusterError)
  }
}
