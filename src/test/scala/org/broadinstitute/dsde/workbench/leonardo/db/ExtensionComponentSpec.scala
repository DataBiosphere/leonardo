package org.broadinstitute.dsde.workbench.leonardo.db

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model.ExtensionType
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ServiceAccountInfo, UserJupyterExtensionConfig}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterStatus, IP, MachineConfig, OperationName}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.FlatSpecLike

import scala.util.Random

class ExtensionComponentSpec extends TestComponent with FlatSpecLike with CommonTestData with GcsPathUtils{
  "ExtensionComponent" should "save, get,and delete" in isolatedDbTest {


    val c1 = Cluster(
      clusterName = name1,
      googleId = UUID.randomUUID(),
      googleProject = project,
      serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
      machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
      clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
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
      status = ClusterStatus.Unknown,
      hostIp = Some(IP("sure, this is an IP address")),
      creator = userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = Map.empty,
      None,
      None,
      Some(GcsBucketName("testStagingBucket2")),
      List.empty,
      Set.empty,
      None,
      Instant.now()
    )

    val missingId = Random.nextLong()
    dbFutureValue { _.extensionQuery.getAllForCluster(missingId) } shouldEqual UserJupyterExtensionConfig(Map(), Map(), Map())
    dbFailure { _.extensionQuery.save(missingId, ExtensionType.NBExtension.toString, "extName", "extValue") } shouldBe a [SQLException]

    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), Some(serviceAccountKey.id)) } shouldEqual c1
    val c1Id = dbFutureValue { _.clusterQuery.getIdByGoogleId(c1.googleId) }.get
    dbFutureValue { _.extensionQuery.saveAllForCluster(c1Id, Some(userExtConfig)) }
    dbFutureValue { _.extensionQuery.getAllForCluster(c1Id) } shouldEqual userExtConfig


    dbFutureValue { _.clusterQuery.save(c2, gcsPath("gs://bucket2"), Some(serviceAccountKey.id)) } shouldEqual c2
    val c2Id = dbFutureValue { _.clusterQuery.getIdByGoogleId(c2.googleId) }.get
    dbFutureValue { _.extensionQuery.save(c2Id, ExtensionType.NBExtension.toString, "extName", "extValue") } shouldBe 1
  }
}
