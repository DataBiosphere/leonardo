package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.{Runtime, RuntimeConfigId, RuntimeStatus}
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.{HostNotReady, HostPaused, HostReady}
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

class RuntimeDnsCacheSpec
    extends AnyFlatSpecLike
    with BeforeAndAfterAll
    with TestComponent
    with ScalaFutures
    with Eventually {

  override def afterAll(): Unit =
    super.afterAll()

  val clusterBeingCreated: Runtime =
    makeCluster(2)
      .copy(status = RuntimeStatus.Creating, asyncRuntimeFields = Some(makeAsyncRuntimeFields(2).copy(hostIp = None)))
  val runningCluster: Runtime = makeCluster(1).copy(status = RuntimeStatus.Running)
  val stoppedCluster: Runtime =
    makeCluster(3)
      .copy(status = RuntimeStatus.Stopped, asyncRuntimeFields = Some(makeAsyncRuntimeFields(2).copy(hostIp = None)))

  val cacheKeyForClusterBeingCreated =
    RuntimeDnsCacheKey(clusterBeingCreated.googleProject, clusterBeingCreated.runtimeName)
  val cacheKeyForRunningCluster = RuntimeDnsCacheKey(runningCluster.googleProject, runningCluster.runtimeName)
  val cacheKeyForStoppedCluster = RuntimeDnsCacheKey(stoppedCluster.googleProject, stoppedCluster.runtimeName)

  val runningClusterHost = Host(
    s"${runningCluster.asyncRuntimeFields.map(_.googleId).get.value.toString}.jupyter.firecloud.org"
  )
  val clusterBeingCreatedHost = Host(
    s"${clusterBeingCreated.asyncRuntimeFields.map(_.googleId).get.value.toString}.jupyter.firecloud.org"
  )
  val stoppedClusterHost = Host(
    s"${stoppedCluster.asyncRuntimeFields.map(_.googleId).get.value.toString}.jupyter.firecloud.org"
  )

  val runtimeDnsCache = new RuntimeDnsCache(proxyConfig, testDbRef, Config.runtimeDnsCacheConfig, blocker)

  it should "update maps and return clusters" in isolatedDbTest {
    // save the clusters to the db
    clusterBeingCreated.save().copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual clusterBeingCreated
    runningCluster.save().copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual runningCluster
    stoppedCluster.save().copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual stoppedCluster

    // We test the projectClusterToHostStatus cache before the hostToIp map.
    // This replicates how the proxy accesses these maps as well.
    // projectClusterToHostStatus read updates the HostToIP map.
    eventually {
      runtimeDnsCache.getHostStatus(cacheKeyForClusterBeingCreated).unsafeRunSync() shouldEqual HostNotReady
    }
    eventually {
      runtimeDnsCache.getHostStatus(cacheKeyForRunningCluster).unsafeRunSync() shouldEqual HostReady(runningClusterHost)
    }
    eventually(runtimeDnsCache.getHostStatus(cacheKeyForStoppedCluster).unsafeRunSync() shouldEqual HostPaused)

    runtimeDnsCache.size shouldBe 3
    runtimeDnsCache.stats.missCount shouldBe 3
    runtimeDnsCache.stats.loadCount shouldBe 3
    runtimeDnsCache.stats.evictionCount shouldBe 0

    HostToIpMapping.hostToIp.get(runningClusterHost) shouldBe runningCluster.asyncRuntimeFields.flatMap(_.hostIp)
    HostToIpMapping.hostToIp.get(clusterBeingCreatedHost) shouldBe None
    HostToIpMapping.hostToIp.get(stoppedClusterHost) shouldBe None

    val cacheKeys = Set(cacheKeyForClusterBeingCreated, cacheKeyForRunningCluster, cacheKeyForStoppedCluster)

    // Check that the cache entries are eventually evicted and get re-loaded upon re-reading
    eventually {
      cacheKeys.foreach(x => runtimeDnsCache.getHostStatus(x).unsafeRunSync())
      runtimeDnsCache.stats.evictionCount shouldBe 3
    }

    runtimeDnsCache.size shouldBe 3
    runtimeDnsCache.stats.missCount shouldBe 6
    runtimeDnsCache.stats.loadCount shouldBe 6
  }
}
