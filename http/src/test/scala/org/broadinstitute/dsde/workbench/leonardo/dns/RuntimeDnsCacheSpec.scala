package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.{HostNotReady, HostPaused, HostReady}
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.{Runtime, RuntimeConfigId, RuntimeStatus}
import org.broadinstitute.dsde.workbench.model.IP
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.ExecutionContext.Implicits.global

class RuntimeDnsCacheSpec
    extends AnyFlatSpecLike
    with BeforeAndAfterAll
    with TestComponent
    with ScalaFutures
    with Eventually {

  override def afterAll(): Unit =
    super.afterAll()

  val runningClusterIp = IP("1.2.3.4")
  val runningCluster: Runtime = makeCluster(1).copy(
    status = RuntimeStatus.Running,
    asyncRuntimeFields = Some(makeAsyncRuntimeFields(1).copy(hostIp = Some(runningClusterIp)))
  )
  val clusterBeingCreated: Runtime =
    makeCluster(2)
      .copy(status = RuntimeStatus.Creating, asyncRuntimeFields = Some(makeAsyncRuntimeFields(2).copy(hostIp = None)))
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

  val runtimeDnsCache =
    new RuntimeDnsCache(proxyConfig, testDbRef, Config.runtimeDnsCacheConfig, proxyResolver, blocker)

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
