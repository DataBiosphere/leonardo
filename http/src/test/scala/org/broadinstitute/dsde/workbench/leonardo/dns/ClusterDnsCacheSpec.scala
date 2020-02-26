package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import org.broadinstitute.dsde.workbench.leonardo.{Cluster, RuntimeConfigId, RuntimeStatus}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by rtitle on 9/1/17.
 */
class ClusterDnsCacheSpec
    extends FlatSpecLike
    with BeforeAndAfterAll
    with TestComponent
    with ScalaFutures
    with Eventually {

  override def afterAll(): Unit =
    super.afterAll()

  val clusterBeingCreated: Cluster =
    makeCluster(2)
      .copy(status = RuntimeStatus.Creating, asyncRuntimeFields = Some(makeDataprocInfo(2).copy(hostIp = None)))
  val runningCluster: Cluster = makeCluster(1).copy(status = RuntimeStatus.Running)
  val stoppedCluster: Cluster =
    makeCluster(3)
      .copy(status = RuntimeStatus.Stopped, asyncRuntimeFields = Some(makeDataprocInfo(2).copy(hostIp = None)))

  val cacheKeyForClusterBeingCreated = DnsCacheKey(clusterBeingCreated.googleProject, clusterBeingCreated.runtimeName)
  val cacheKeyForRunningCluster = DnsCacheKey(runningCluster.googleProject, runningCluster.runtimeName)
  val cacheKeyForStoppedCluster = DnsCacheKey(stoppedCluster.googleProject, stoppedCluster.runtimeName)

  val runningClusterHost = Host(
    s"${runningCluster.asyncRuntimeFields.map(_.googleId).get.toString}.jupyter.firecloud.org"
  )
  val clusterBeingCreatedHost = Host(
    s"${clusterBeingCreated.asyncRuntimeFields.map(_.googleId).get.toString}.jupyter.firecloud.org"
  )
  val stoppedClusterHost = Host(
    s"${stoppedCluster.asyncRuntimeFields.map(_.googleId).get.toString}.jupyter.firecloud.org"
  )

  val clusterDnsCache = new ClusterDnsCache(proxyConfig, DbSingleton.dbRef, dnsCacheConfig, blocker)

  it should "update maps and return clusters" in isolatedDbTest {
    // save the clusters to the db
    clusterBeingCreated.save().copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual clusterBeingCreated
    runningCluster.save().copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual runningCluster
    stoppedCluster.save().copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual stoppedCluster

    // We test the projectClusterToHostStatus cache before the hostToIp map.
    // This replicates how the proxy accesses these maps as well.
    // projectClusterToHostStatus read updates the HostToIP map.
    eventually {
      clusterDnsCache.getHostStatus(cacheKeyForClusterBeingCreated).unsafeRunSync() shouldEqual HostNotReady
    }
    eventually {
      clusterDnsCache.getHostStatus(cacheKeyForRunningCluster).unsafeRunSync() shouldEqual HostReady(runningClusterHost)
    }
    eventually { clusterDnsCache.getHostStatus(cacheKeyForStoppedCluster).unsafeRunSync() shouldEqual HostPaused }

    clusterDnsCache.size shouldBe 3
    clusterDnsCache.stats.missCount shouldBe 3
    clusterDnsCache.stats.loadCount shouldBe 3
    clusterDnsCache.stats.evictionCount shouldBe 0

    ClusterDnsCache.hostToIp.get(runningClusterHost) shouldBe runningCluster.asyncRuntimeFields.flatMap(_.hostIp)
    ClusterDnsCache.hostToIp.get(clusterBeingCreatedHost) shouldBe None
    ClusterDnsCache.hostToIp.get(stoppedClusterHost) shouldBe None

    val cacheKeys = Set(cacheKeyForClusterBeingCreated, cacheKeyForRunningCluster, cacheKeyForStoppedCluster)

    // Check that the cache entries are eventually evicted and get re-loaded upon re-reading
    eventually {
      cacheKeys.foreach(x => clusterDnsCache.getHostStatus(x).unsafeRunSync())
      clusterDnsCache.stats.evictionCount shouldBe 3
    }

    clusterDnsCache.size shouldBe 3
    clusterDnsCache.stats.missCount shouldBe 6
    clusterDnsCache.stats.loadCount shouldBe 6
  }
}
