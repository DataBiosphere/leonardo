package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.{AuditInfo, Cluster, DataprocInfo, ServiceAccountInfo}
import org.broadinstitute.dsde.workbench.leonardo.model.google.{ClusterStatus, MachineConfig, OperationName}
import org.broadinstitute.dsde.workbench.leonardo.monitor.ClusterDateAccessedActor.UpdateDateAccessed
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.time.{Seconds, Span}


class ClusterDateAccessedSpec extends TestKit(ActorSystem("leonardotest")) with
  FlatSpecLike with BeforeAndAfterAll with TestComponent with CommonTestData with GcsPathUtils { testKit =>

  val testCluster1 = Cluster(
    clusterName = name1,
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
    dataprocInfo = DataprocInfo(Option(UUID.randomUUID()), Option(OperationName("op1")), Some(GcsBucketName("testStagingBucket1")), None),
    auditInfo = AuditInfo(userEmail, Instant.now(), None, Instant.now()),
    machineConfig = MachineConfig(Some(0), Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
    status = ClusterStatus.Running,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = None,
    jupyterUserScriptUri = None,
    errors = List.empty,
    instances = Set.empty,
    userJupyterExtensionConfig = Some(userExtConfig),
    autopauseThreshold = 0,
    defaultClientId = None
  )

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ClusterDateAccessedMonitor" should "update date accessed" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    val savedTestCluster1 = dbFutureValue { _.clusterQuery.save(testCluster1, Option(gcsPath("gs://bucket")), Some(serviceAccountKey.id)) }
    savedTestCluster1 shouldEqual testCluster1

    val currentTime = Instant.now()
    val dateAccessedActor = system.actorOf(ClusterDateAccessedActor.props(autoFreezeConfig, DbSingleton.ref))
    dateAccessedActor ! UpdateDateAccessed(testCluster1.clusterName, testCluster1.googleProject, currentTime)
    eventually(timeout(Span(5, Seconds))) {
      val c1 = dbFutureValue { _.clusterQuery.getClusterById(savedTestCluster1.id) }
      c1.map(_.auditInfo.dateAccessed).get shouldBe currentTime
    }
  }
}
