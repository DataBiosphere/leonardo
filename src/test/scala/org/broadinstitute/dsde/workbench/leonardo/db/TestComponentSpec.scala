package org.broadinstitute.dsde.workbench.leonardo.db

import java.time.Instant
import java.util.UUID

import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.FlatSpecLike

final class TestComponentSpec extends TestComponent with FlatSpecLike with CommonTestData {
  val c0 = Cluster(
    clusterName = name0,
    googleId = Option(UUID.randomUUID()),
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(Some(serviceAccountEmail), Some(serviceAccountEmail)),
    machineConfig = MachineConfig(Some(0), Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name0, clusterUrlBase),
    operationName = Option(OperationName("op0")),
    status = ClusterStatus.Unknown,
    hostIp = Some(IP("numbers.and.dots")),
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = None,
    jupyterUserScriptUri = None,
    stagingBucket = Some(GcsBucketName("testStagingBucket0")),
    errors = List.empty,
    instances = Set(masterInstance, workerInstance1, workerInstance2),
    userJupyterExtensionConfig = Some(userExtConfig),
    dateAccessed = Instant.now())

  val c1 = c0.copy(id = 1)

  val c2 = c1.copy(id = 2)

  val c1Copy = c1

  val c3WithDistinctHostIp = c0.copy(hostIp = Some(IP("cluster.3.ip")))

  "assertEquivalent()" should "correctly compare two sets of Cluster's" in {
    assertEquivalent(c1) { c1Copy }
    assertEquivalent(Set(c1)) { Set(c1Copy) }
    assertEquivalent(Set(c1, c2)) { Set(c2, c1Copy) }
    assertEquivalent(Set.empty[Cluster]) { Set.empty }
  }

  "setClusterId()" should "set Cluster.id's to the same value" in {
    val (csAWithId0, csBWithFixedId0) = setClusterId(Set(c0, c1), Set(c1, c2), 0)
    csAWithId0 shouldEqual csBWithFixedId0

    val (csCWithId0, csDWithFixedId0) = setClusterId(Set(c0, c1), Set(c3WithDistinctHostIp, c2), 0)
    csCWithId0 should not equal csDWithFixedId0

    val (csEWithId5, _) = setClusterId(Set(c1, c2), Set.empty, 5)
    csEWithId5 should not equal Set(c0, c0)
  }
}
