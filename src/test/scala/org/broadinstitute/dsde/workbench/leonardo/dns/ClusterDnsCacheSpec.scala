//package org.broadinstitute.dsde.workbench.leonardo.dns
//
//import java.time.Instant
//import java.util.UUID
//
//import akka.actor.ActorSystem
//import akka.http.scaladsl.model.Uri.Host
//import akka.pattern.ask
//import akka.testkit.{TestActorRef, TestKit}
//import akka.util.Timeout
//import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
//import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
//import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
//import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
//import org.broadinstitute.dsde.workbench.leonardo.model._
//import org.broadinstitute.dsde.workbench.leonardo.model.google._
//import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
//import org.scalatest.concurrent.{Eventually, ScalaFutures}
//import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
//
//import scala.concurrent.duration._
//
///**
//  * Created by rtitle on 9/1/17.
//  */
//class ClusterDnsCacheSpec extends TestKit(ActorSystem("leonardotest")) with FlatSpecLike with Matchers with BeforeAndAfterAll with TestComponent with ScalaFutures with Eventually with CommonTestData with GcsPathUtils {
//
//  implicit val timeout = Timeout(5 seconds)
//
//  override def afterAll(): Unit = {
//    TestKit.shutdownActorSystem(system)
//    super.afterAll()
//  }
//
//  val cluster1 = makeCluster(1).copy()
//
//  val cluster2 = makeCluster(2).copy(status = ClusterStatus.Creating,
//                                     dataprocInfo = makeDataprocInfo(2).copy(hostIp = None))
//
//  val cluster3 = makeCluster(3).copy(status = ClusterStatus.Stopping,
//                                     dataprocInfo = makeDataprocInfo(3).copy(hostIp = None))
//
//  it should "update maps and return clusters" in isolatedDbTest {
//    val actorRef = TestActorRef[ClusterDnsCache](ClusterDnsCache.props(proxyConfig, DbSingleton.ref))
//
//    // init maps to empty
//    actorRef.underlyingActor.ProjectNameToHost = Map.empty
//    ClusterDnsCache.HostToIp = Map.empty
//
//    // save the clusters to the db
//    cluster1.save() shouldEqual cluster1
//    cluster2.save() shouldEqual cluster2
//    cluster3.save() shouldEqual cluster3
//
//    // maps should be populated
//    eventually {
//      actorRef.underlyingActor.ProjectNameToHost shouldBe Map(
//        (project, name1) -> ClusterReady(Host(s"${cluster1.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org")),
//        (project, name2) -> ClusterNotReady,
//        (project, name3) -> ClusterPaused
//      )
//      ClusterDnsCache.HostToIp shouldBe Map(
//        Host(s"${cluster1.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org") -> cluster1.dataprocInfo.hostIp.get
//      )
//    }
//
//    // calling GetByProjectAndName should return the correct responses
//    (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterReady(Host(s"${cluster1.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org"))
//    (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotReady
//    (actorRef ? GetByProjectAndName(project, name3)).futureValue shouldBe ClusterPaused
//    (actorRef ? GetByProjectAndName(project, ClusterName("bogus"))).futureValue shouldBe ClusterNotFound
//    (actorRef ? GetByProjectAndName(project, name0)).futureValue shouldBe ClusterNotFound
//  }
//
//  it should "return a ready cluster after a ProcessReadyCluster message" in isolatedDbTest {
//    val actorRef = TestActorRef[ClusterDnsCache](ClusterDnsCache.props(proxyConfig, DbSingleton.ref))
//
//    // init maps to empty
//    actorRef.underlyingActor.ProjectNameToHost = Map.empty
//    ClusterDnsCache.HostToIp = Map.empty
//
//    // save the clusters to the db
//    cluster1.save() shouldEqual cluster1
//    cluster2.save() shouldEqual cluster2
//    cluster3.save() shouldEqual cluster3
//
//    // we have not yet executed a DNS cache refresh cycle
//
//    (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterNotFound
//    (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotFound
//    (actorRef ? GetByProjectAndName(project, name3)).futureValue shouldBe ClusterNotFound
//
//    // add one cluster to cache immediately without waiting for the cycle
//
//    actorRef ! ProcessReadyCluster(cluster1)
//
//    // the cluster is available in the DNS cache but the other is not
//
//    (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterReady(Host(s"${cluster1.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org"))
//    (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotFound
//    (actorRef ? GetByProjectAndName(project, name3)).futureValue shouldBe ClusterNotFound
//
//    // the cycle still eventually populates normally
//
//    eventually {
//      (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterReady(Host(s"${cluster1.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org"))
//      (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotReady
//      (actorRef ? GetByProjectAndName(project, name3)).futureValue shouldBe ClusterPaused
//    }
//
//    val newName1 = ClusterName("newname1")
//    val newName2 = ClusterName("newname2")
//
//    val newCluster1 = cluster1.copy(clusterName = newName1, dataprocInfo = cluster1.dataprocInfo.copy(hostIp = Some(IP("a new IP")), googleId = Option(UUID.randomUUID())))
//    val newCluster2 = cluster2.copy(clusterName = newName2, dataprocInfo = cluster2.dataprocInfo.copy(hostIp = Some(IP("another new IP")), googleId = Option(UUID.randomUUID())))
//
//    newCluster1.save() shouldEqual newCluster1
//    newCluster2.save() shouldEqual newCluster2
//
//    // add one cluster to cache immediately without waiting for the cycle
//
//    actorRef ! ProcessReadyCluster(newCluster1)
//
//    // the new cluster is available in the DNS cache (as well as the older clusters) but the other is not
//
//    (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterReady(Host(s"${cluster1.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org"))
//    (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotReady
//    (actorRef ? GetByProjectAndName(project, newName1)).futureValue shouldBe ClusterReady(Host(s"${newCluster1.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org"))
//    (actorRef ? GetByProjectAndName(project, newName2)).futureValue shouldBe ClusterNotFound
//
//    // the cycle still eventually populates normally
//
//    eventually {
//      (actorRef ? GetByProjectAndName(project, name1)).futureValue shouldBe ClusterReady(Host(s"${cluster1.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org"))
//      (actorRef ? GetByProjectAndName(project, name2)).futureValue shouldBe ClusterNotReady
//      (actorRef ? GetByProjectAndName(project, newName1)).futureValue shouldBe ClusterReady(Host(s"${newCluster1.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org"))
//      (actorRef ? GetByProjectAndName(project, newName2)).futureValue shouldBe ClusterReady(Host(s"${newCluster2.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org"))
//    }
//
//  }
//
//}
