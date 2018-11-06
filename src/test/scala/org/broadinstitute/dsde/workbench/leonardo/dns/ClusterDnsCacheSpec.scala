package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.{CommonTestData, GcsPathUtils}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

/**
  * Created by rtitle on 9/1/17.
  */
class ClusterDnsCacheSpec extends FlatSpecLike with BeforeAndAfterAll with TestComponent with ScalaFutures with Eventually with CommonTestData {

  override def afterAll(): Unit = {
    super.afterAll()
  }

  val cluster1: Cluster = makeCluster(1).copy()

  val cluster2: Cluster = makeCluster(2).copy(status = ClusterStatus.Creating,
    dataprocInfo = makeDataprocInfo(2).copy(hostIp = None))

  val clusterDnsCache = new ClusterDnsCache(proxyConfig, DbSingleton.ref, dnsCacheConfig)
  it should "update maps and return clusters" in isolatedDbTest {

    // save the clusters to the db
    cluster1.save() shouldEqual cluster1
    cluster2.save() shouldEqual cluster2

    //We test the ProjectNameToHost Cache before the HostToIp map
    //This replicates how the proxy accesses these maps as well.
    //ProjectNameToHost read updates the HostToIP map
    eventually {
      clusterDnsCache.projectNameToHost.get(DnsCacheKey(cluster1.googleProject, cluster1.clusterName)).futureValue shouldEqual
        HostReady(Host(s"${cluster1.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org"))
    }
    eventually {
      clusterDnsCache.projectNameToHost.get(DnsCacheKey(cluster2.googleProject, cluster2.clusterName)).futureValue shouldEqual
        HostNotReady
    }

    ClusterDnsCache.HostToIp.value.get(Host(s"${cluster1.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org")) shouldBe cluster1.dataprocInfo.hostIp

    ClusterDnsCache.HostToIp.value.get(Host(s"${cluster2.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org")) shouldBe None
  }
}
