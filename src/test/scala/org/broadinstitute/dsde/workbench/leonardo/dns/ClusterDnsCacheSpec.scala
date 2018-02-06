package org.broadinstitute.dsde.workbench.leonardo.dns

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Host
import akka.pattern.ask
import akka.testkit.{TestActorRef, TestKit}
import akka.util.Timeout
import org.broadinstitute.dsde.workbench.google.gcs.GcsBucketName
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by rtitle on 9/1/17.
  */
class ClusterDnsCacheSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with BeforeAndAfterAll with TestComponent with ScalaFutures with Eventually with CommonTestData with GcsPathUtils {

  implicit val timeout = Timeout(5 seconds)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val c1 = Cluster(
    clusterName = name1,
    googleId = UUID.randomUUID(),
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name1),
    operationName = OperationName("op1"),
    status = ClusterStatus.Unknown,
    hostIp = Some(IP("numbers.and.dots")),
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    jupyterExtensionUri = jupyterExtensionUri,
    Some(GcsBucketName("testStagingBucket1")))

  val c2 = Cluster(
    clusterName = name2,
    googleId = UUID.randomUUID(),
    googleProject = project,
    serviceAccountInfo = ServiceAccountInfo(None, Some(serviceAccountEmail)),
    machineConfig = MachineConfig(Some(0),Some(""), Some(500)),
    clusterUrl = Cluster.getClusterUrl(project, name2),
    operationName = OperationName("op2"),
    status = ClusterStatus.Creating,
    hostIp = None,
    creator = userEmail,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map.empty,
    None,
    Some(GcsBucketName("testStagingBucket2")))

  it should "update maps and return clusters" in isolatedDbTest {
    val actorRef = TestActorRef[ClusterDnsCache](ClusterDnsCache.props(proxyConfig, DbSingleton.ref))

    // init maps to empty
    actorRef.underlyingActor.ProjectNameToHost = Map.empty
    ClusterDnsCache.HostToIp = Map.empty

    // save the clusters to the db
    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), Some(serviceAccountKey.id)) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2, gcsPath("gs://bucket2"), Some(serviceAccountKey.id)) } shouldEqual c2

    // maps should be populated
    eventually {
      actorRef.underlyingActor.ProjectNameToHost shouldBe Map(
        (project, name1) -> ClusterReady(Host(s"${c1.googleId.toString}.jupyter.firecloud.org")),
        (project, name2) -> ClusterNotReady
      )
      ClusterDnsCache.HostToIp shouldBe Map(
        Host(s"${c1.googleId.toString}.jupyter.firecloud.org") -> c1.hostIp.get
      )
    }

    // calling GetByProjectAndName should return the correct responses
    (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterReady(Host(s"${c1.googleId.toString}.jupyter.firecloud.org"))
    (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotReady
    (actorRef ? GetByProjectAndName(project, ClusterName("bogus"))).futureValue shouldBe ClusterNotFound
  }

  it should "return a ready cluster after a ProcessReadyCluster message" in isolatedDbTest {
    val actorRef = TestActorRef[ClusterDnsCache](ClusterDnsCache.props(proxyConfig, DbSingleton.ref))

    // init maps to empty
    actorRef.underlyingActor.ProjectNameToHost = Map.empty
    ClusterDnsCache.HostToIp = Map.empty

    // save the clusters to the db
    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1"), Some(serviceAccountKey.id)) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2, gcsPath("gs://bucket2"), Some(serviceAccountKey.id)) } shouldEqual c2

    // we have not yet executed a DNS cache refresh cycle

    (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterNotFound
    (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotFound

    // add one cluster to cache immediately without waiting for the cycle

    actorRef ! ProcessReadyCluster(c1)

    // the cluster is available in the DNS cache but the other is not

    (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterReady(Host(s"${c1.googleId.toString}.jupyter.firecloud.org"))
    (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotFound

    // the cycle still eventually populates normally

    eventually {
      (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterReady(Host(s"${c1.googleId.toString}.jupyter.firecloud.org"))
      (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotReady
    }

    val newname1 = ClusterName("newname1")
    val newname2 = ClusterName("newname2")

    val newC1 = c1.copy(clusterName = newname1, hostIp = Some(IP("a new IP")), googleId = UUID.randomUUID())
    val newC2 = c2.copy(clusterName = newname2, hostIp = Some(IP("another new IP")), googleId = UUID.randomUUID())

    dbFutureValue { _.clusterQuery.save(newC1, gcsPath("gs://newbucket1"), Some(serviceAccountKey.id)) } shouldEqual newC1
    dbFutureValue { _.clusterQuery.save(newC2, gcsPath("gs://newbucket2"), Some(serviceAccountKey.id)) } shouldEqual newC2

    // add one cluster to cache immediately without waiting for the cycle

    actorRef ! ProcessReadyCluster(newC1)

    // the new cluster is available in the DNS cache (as well as the older clusters) but the other is not

    (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterReady(Host(s"${c1.googleId.toString}.jupyter.firecloud.org"))
    (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotReady
    (actorRef ? GetByProjectAndName(project, newname1)).futureValue shouldBe ClusterReady(Host(s"${newC1.googleId.toString}.jupyter.firecloud.org"))
    (actorRef ? GetByProjectAndName(project, newname2)).futureValue shouldBe ClusterNotFound

    // the cycle still eventually populates normally

    eventually {
      (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterReady(Host(s"${c1.googleId.toString}.jupyter.firecloud.org"))
      (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotReady
      (actorRef ? GetByProjectAndName(project, newname1)).futureValue shouldBe ClusterReady(Host(s"${newC1.googleId.toString}.jupyter.firecloud.org"))
      (actorRef ? GetByProjectAndName(project, newname2)).futureValue shouldBe ClusterReady(Host(s"${newC2.googleId.toString}.jupyter.firecloud.org"))
    }

  }

}
