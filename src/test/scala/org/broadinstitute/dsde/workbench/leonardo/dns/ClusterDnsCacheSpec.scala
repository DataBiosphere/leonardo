package org.broadinstitute.dsde.workbench.leonardo.dns

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Host
import akka.pattern.ask
import akka.testkit.{TestActorRef, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.gcs.GcsBucketName
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
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

  val config = ConfigFactory.parseResources("reference.conf").withFallback(ConfigFactory.load())
  val proxyConfig = config.as[ProxyConfig]("proxy")
  implicit val timeout = Timeout(5 seconds)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

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
    jupyterExtensionUri = jupyterExtensionUri)

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
    None)

  it should "update maps and return clusters" in isolatedDbTest {
    val actorRef = TestActorRef[ClusterDnsCache](ClusterDnsCache.props(proxyConfig, DbSingleton.ref))

    // maps should initially be empty
    actorRef.underlyingActor.ProjectNameToHost shouldBe 'empty
    ClusterDnsCache.HostToIp shouldBe 'empty

    // save the clusters to the db
    dbFutureValue { _.clusterQuery.save(c1, gcsPath("gs://bucket1")) } shouldEqual c1
    dbFutureValue { _.clusterQuery.save(c2, gcsPath("gs://bucket2")) } shouldEqual c2

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

    // calling GetByProjectAndName should return the correct response
    (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterReady(Host(s"${c1.googleId.toString}.jupyter.firecloud.org"))
    (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotReady
    (actorRef ? GetByProjectAndName(project, ClusterName("bogus"))).futureValue shouldBe ClusterNotFound
  }

}
