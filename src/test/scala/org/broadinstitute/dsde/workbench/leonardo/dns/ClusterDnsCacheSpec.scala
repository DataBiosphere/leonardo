package org.broadinstitute.dsde.workbench.leonardo.dns

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{TestActorRef, TestKit}
import akka.util.Timeout
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterStatus}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by rtitle on 9/1/17.
  */
class ClusterDnsCacheSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with BeforeAndAfterAll with TestComponent with ScalaFutures with Eventually {

  val proxyConfig = ProxyConfig(jupyterPort = 8001, jupyterProtocol = "tcp", jupyterDomain = ".jupyter.firecloud.org", dnsPollPeriod = 1 second)
  implicit val timeout = Timeout(5 seconds)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val c1 = Cluster(
    clusterName = "name1",
    googleId = UUID.randomUUID(),
    googleProject = "dsp-leo-test",
    googleServiceAccount = "not-a-service-acct@google.com",
    googleBucket = "bucket1",
    clusterUrl = Cluster.getClusterUrl("dsp-leo-test", "name1"),
    operationName = "op1",
    status = ClusterStatus.Unknown,
    hostIp = Some("numbers.and.dots"),
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map("bam" -> "yes", "vcf" -> "no"),
    Some("extension_uri"))

  val c2 = Cluster(
    clusterName = "name2",
    googleId = UUID.randomUUID(),
    googleProject = "dsp-leo-test",
    googleServiceAccount = "not-a-service-acct@google.com",
    googleBucket = "bucket2",
    clusterUrl = Cluster.getClusterUrl("dsp-leo-test", "name2"),
    operationName = "op2",
    status = ClusterStatus.Creating,
    hostIp = None,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = Map.empty,
    None)

  it should "update maps and return clusters" in isolatedDbTest {
    val actorRef = TestActorRef[ClusterDnsCache](ClusterDnsCache.props(proxyConfig, DbSingleton.ref))

    // maps should initially be empty
    actorRef.underlyingActor.ProjectNameToHost shouldBe 'empty
    ClusterDnsCache.HostToIp shouldBe 'empty

    // save the clusters to the db
    dbFutureValue { _.clusterQuery.save(c1) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2) } shouldEqual c2

    // maps should be populated
    eventually {
      actorRef.underlyingActor.ProjectNameToHost shouldBe Map(
        ("dsp-leo-test", "name1") -> ClusterReady(s"${c1.googleId.toString}.jupyter.firecloud.org"),
        ("dsp-leo-test", "name2") -> ClusterNotReady
      )
      ClusterDnsCache.HostToIp shouldBe Map(
        s"${c1.googleId.toString}.jupyter.firecloud.org" -> c1.hostIp.get
      )
    }

    // calling GetByProjectAndName should return the correct response
    (actorRef ? GetByProjectAndName("dsp-leo-test", "name1")).futureValue shouldBe ClusterReady(s"${c1.googleId.toString}.jupyter.firecloud.org")
    (actorRef ? GetByProjectAndName("dsp-leo-test", "name2")).futureValue shouldBe ClusterNotReady
    (actorRef ? GetByProjectAndName("dsp-leo-test", "bogus")).futureValue shouldBe ClusterNotFound
  }

}
