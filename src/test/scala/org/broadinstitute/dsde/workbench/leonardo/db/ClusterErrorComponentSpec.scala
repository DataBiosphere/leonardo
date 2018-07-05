package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.FlatSpecLike



class ClusterErrorComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils {

  "ClusterErrorComponent" should "save, and get" in isolatedDbTest {
    val c1 = Cluster(
      clusterName = name1,
      googleId = UUID.randomUUID(),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0), Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1),
      operationName = OperationName("op1"),
      status = ClusterStatus.Creating,
      hostIp = None,
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = Option(Instant.now()),
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      Some(GcsBucketName("testStagingBucket1")),
      List.empty,
      Set.empty,
      None,
      Instant.now(),
      30)

    lazy val timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    val clusterError = ClusterError("Some Error", 10, timestamp)

    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), Some(serviceAccountKey.id)) } shouldEqual c1
    val c1Id = dbFutureValue {
      _.clusterQuery.getIdByGoogleId(c1.googleId)
    }.get

    dbFutureValue {_.clusterErrorQuery.get(c1Id)} shouldEqual List.empty
    dbFutureValue {_.clusterErrorQuery.save(c1Id, clusterError)}
    dbFutureValue {_.clusterErrorQuery.get(c1Id)} shouldEqual List(clusterError)
  }
}
