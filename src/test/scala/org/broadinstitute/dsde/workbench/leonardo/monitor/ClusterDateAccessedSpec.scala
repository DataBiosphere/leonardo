package org.broadinstitute.dsde.workbench.leonardo.monitor

import java.time.Instant
import java.util.UUID
import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ServiceAccountInfo}
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
    googleId = Option(UUID.randomUUID()),
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(clusterServiceAccount(project), notebookServiceAccount(project)),
    machineConfig = MachineConfig(Some(0), Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1, clusterUrlBase),
    operationName = Option(OperationName("op1")),
    status = ClusterStatus.Running,
    hostIp = None,
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = None,
    jupyterUserScriptUri = None,
    stagingBucket = Some(GcsBucketName("testStagingBucket1")),
    errors = List.empty,
    instances = Set.empty,
    userJupyterExtensionConfig = Some(userExtConfig),
    dateAccessed = Instant.now()
  )

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "ClusterDateAccessedMonitor" should "update date accessed" in isolatedDbTest {
    import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq

    dbFutureValue { _.clusterQuery.save(testCluster1, gcsPath("gs://bucket"), Some(serviceAccountKey.id)) } shouldEqual testCluster1

    val currentTime = Instant.now()
    val dateAccessedActor = system.actorOf(ClusterDateAccessedActor.props(autoFreezeconfig, DbSingleton.ref))
    dateAccessedActor ! UpdateDateAccessed(testCluster1.clusterName, testCluster1.googleProject, currentTime)
    eventually(timeout(Span(5, Seconds))) {
      val c1 = dbFutureValue { _.clusterQuery.getByGoogleId(testCluster1.googleId) }
      c1.map(_.dateAccessed).get shouldBe currentTime
    }
  }
}
