package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterStatus, IP, MachineConfig, OperationName}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.FlatSpecLike

import scala.util.Random

class ExtensionComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils{
  "ExtensionComponent" should "save, get,and delete" in isolatedDbTest {
    val c1 = Cluster(
      clusterName = name1,
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      dataprocInfo = DataprocInfo(Option(UUID.randomUUID()), Option(OperationName("op1")), Some(GcsBucketName("testStagingBucket1")), None),
      auditInfo = AuditInfo(userEmail, Instant.now(), Option(Instant.now()), Instant.now()),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
      status = ClusterStatus.Creating,
      labels = Map.empty,
      jupyterExtensionUri = Some(jupyterExtensionUri),
      jupyterUserScriptUri = Some(jupyterUserScriptUri),
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      autopauseThreshold = 0,
      defaultClientId = None)

    val c2 = Cluster(
      clusterName = name2,
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      dataprocInfo = DataprocInfo(Option(UUID.randomUUID()), Option(OperationName("op2")), Some(GcsBucketName("testStagingBucket2")), Some(IP("sure, this is an IP address"))),
      auditInfo = AuditInfo(userEmail, Instant.now(),  None, Instant.now()),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name2, clusterUrlBase),
      status = ClusterStatus.Unknown,
      labels = Map.empty,
      jupyterExtensionUri = None,
      jupyterUserScriptUri = None,
      errors = List.empty,
      instances = Set.empty,
      userJupyterExtensionConfig = None,
      autopauseThreshold = 0,
      defaultClientId = None)

    val missingId = Random.nextLong()
    dbFutureValue { _.extensionQuery.getAllForCluster(missingId) } shouldEqual UserJupyterExtensionConfig(Map(), Map(), Map())
    dbFailure { _.extensionQuery.save(missingId, ExtensionType.NBExtension.toString, "extName", "extValue") } shouldBe a [SQLException]

    val savedC1 = dbFutureValue { _.clusterQuery.save(c1, Option(gcsPath("gs://bucket1")), Some(serviceAccountKey.id)) }
    savedC1 shouldEqual c1
    dbFutureValue { _.extensionQuery.saveAllForCluster(savedC1.id, Some(userExtConfig)) }
    dbFutureValue { _.extensionQuery.getAllForCluster(savedC1.id) } shouldEqual userExtConfig

    val savedC2 = dbFutureValue { _.clusterQuery.save(c2, Option(gcsPath("gs://bucket2")), Some(serviceAccountKey.id)) }
    savedC2 shouldEqual c2
    dbFutureValue { _.extensionQuery.save(savedC2.id, ExtensionType.NBExtension.toString, "extName", "extValue") } shouldBe 1
  }
}
