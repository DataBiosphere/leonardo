package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import org.broadinstitute.dsde.workbench.leonardo.ClusterEnrichments.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

/**
  * Created by rtitle on 9/1/17.
  */
class ClusterDnsCacheSpec extends FlatSpecLike with BeforeAndAfterAll with TestComponent with ScalaFutures with Eventually with CommonTestData {

  override def afterAll(): Unit = {
    super.afterAll()
  }

  val clusterBeingCreated: Cluster = makeCluster(2).copy(status = ClusterStatus.Creating,
    dataprocInfo = makeDataprocInfo(2).copy(hostIp = None))

  val runningCluster: Cluster = makeCluster(1).copy(status = ClusterStatus.Running)

  val stoppedCluster: Cluster = makeCluster(3).copy(status = ClusterStatus.Stopped,
    dataprocInfo = makeDataprocInfo(2).copy(hostIp = None))

  val clusterDnsCache = new ClusterDnsCache(proxyConfig, DbSingleton.ref, dnsCacheConfig)

  it should "update maps and return clusters" in isolatedDbTest {
    // save the clusters to the db
    clusterBeingCreated.save() shouldEqual clusterBeingCreated
    runningCluster.save() shouldEqual runningCluster
    stoppedCluster.save() shouldEqual stoppedCluster

    // We test the projectClusterToHostStatus cache before the hostToIp map
    // This replicates how the proxy accesses these maps as well.
    // projectClusterToHostStatus read updates the HostToIP map
    eventually {
      clusterDnsCache.cache.get(DnsCacheKey(clusterBeingCreated.googleProject, clusterBeingCreated.clusterName)).futureValue shouldEqual
        HostNotReady
    }
    eventually {
      clusterDnsCache.cache.get(DnsCacheKey(runningCluster.googleProject, runningCluster.clusterName)).futureValue shouldEqual
        HostReady(Host(s"${runningCluster.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org"))
    }
    eventually {
      clusterDnsCache.cache.get(DnsCacheKey(stoppedCluster.googleProject, stoppedCluster.clusterName)).futureValue shouldEqual
        HostPaused
    }

    ClusterDnsCache.hostToIp.get(Host(s"${runningCluster.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org")) shouldBe runningCluster.dataprocInfo.hostIp
    ClusterDnsCache.hostToIp.get(Host(s"${clusterBeingCreated.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org")) shouldBe None
    ClusterDnsCache.hostToIp.get(Host(s"${stoppedCluster.dataprocInfo.googleId.get.toString}.jupyter.firecloud.org")) shouldBe None
  }
}
