package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import cats.effect.IO
import com.github.benmanes.caffeine.cache.Caffeine
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.clusterEq
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.{HostNotReady, HostPaused, HostReady}
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.{CloudProvider, Runtime, RuntimeConfigId, RuntimeStatus}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import scalacache.Cache
import scalacache.caffeine.CaffeineCache

import scala.concurrent.ExecutionContext.Implicits.global

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
      .copy(asyncRuntimeFields = Some(makeAsyncRuntimeFields(2).copy(hostIp = None)), status = RuntimeStatus.Creating)
  val runningCluster: Runtime = makeCluster(1).copy(status = RuntimeStatus.Running)
  val stoppedCluster: Runtime =
    makeCluster(3)
      .copy(asyncRuntimeFields = Some(makeAsyncRuntimeFields(2).copy(hostIp = None)), status = RuntimeStatus.Stopped)

  val cacheKeyForClusterBeingCreated =
    RuntimeDnsCacheKey(clusterBeingCreated.cloudContext, clusterBeingCreated.runtimeName)
  val cacheKeyForRunningCluster = RuntimeDnsCacheKey(runningCluster.cloudContext, runningCluster.runtimeName)
  val cacheKeyForStoppedCluster = RuntimeDnsCacheKey(stoppedCluster.cloudContext, stoppedCluster.runtimeName)

  val runningClusterHost = Host(
    s"${runningCluster.asyncRuntimeFields.map(_.proxyHostName).get.value}.jupyter.firecloud.org"
  )
  val clusterBeingCreatedHost = Host(
    s"${clusterBeingCreated.asyncRuntimeFields.map(_.proxyHostName).get.value}.jupyter.firecloud.org"
  )
  val stoppedClusterHost = Host(
    s"${stoppedCluster.asyncRuntimeFields.map(_.proxyHostName).get.value}.jupyter.firecloud.org"
  )
  val underlyingRuntimeDnsCache =
    Caffeine.newBuilder().maximumSize(10000L).recordStats().build[RuntimeDnsCacheKey, scalacache.Entry[HostStatus]]()
  val runtimeDnsCaffeineCache: Cache[IO, RuntimeDnsCacheKey, HostStatus] =
    CaffeineCache[IO, RuntimeDnsCacheKey, HostStatus](underlyingRuntimeDnsCache)
  val runtimeDnsCache =
    new RuntimeDnsCache[IO](proxyConfig, testDbRef, hostToIpMapping, runtimeDnsCaffeineCache)

  it should "update maps and return clusters" in isolatedDbTest {
    // save the clusters to the db
    clusterBeingCreated.save().copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual clusterBeingCreated
    runningCluster.save().copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual runningCluster
    stoppedCluster.save().copy(runtimeConfigId = RuntimeConfigId(-1)) shouldEqual stoppedCluster

    // We test the projectClusterToHostStatus cache before the hostToIp map.
    // This replicates how the proxy accesses these maps as well.
    // projectClusterToHostStatus read updates the HostToIP map.
    eventually {
      runtimeDnsCache
        .getHostStatus(cacheKeyForClusterBeingCreated)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldEqual HostNotReady
    }
    eventually {
      runtimeDnsCache
        .getHostStatus(cacheKeyForRunningCluster)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldEqual HostReady(
        runningClusterHost,
        s"${cloudContextGcp.asString}/${runningCluster.runtimeName.asString}",
        CloudProvider.Gcp
      )
    }
    eventually(
      runtimeDnsCache
        .getHostStatus(cacheKeyForStoppedCluster)
        .unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldEqual HostPaused
    )
    val cacheMap = underlyingRuntimeDnsCache.asMap()
    cacheMap.size() shouldBe 3
    underlyingRuntimeDnsCache.stats.missCount shouldBe 3
//    underlyingRuntimeDnsCache.stats.loadCount shouldBe 3
    underlyingRuntimeDnsCache.stats.evictionCount shouldBe 0

    hostToIpMapping.get
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .get(runningClusterHost.address) shouldBe runningCluster.asyncRuntimeFields.flatMap(_.hostIp)
    hostToIpMapping.get
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .get(clusterBeingCreatedHost.address) shouldBe None
    hostToIpMapping.get
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
      .get(stoppedClusterHost.address) shouldBe None

    val cacheKeys = Set(cacheKeyForClusterBeingCreated, cacheKeyForRunningCluster, cacheKeyForStoppedCluster)

    // Check that the cache entries are eventually evicted and get re-loaded upon re-reading
    eventually {
      cacheKeys.foreach(x => runtimeDnsCache.getHostStatus(x).unsafeRunSync()(cats.effect.unsafe.IORuntime.global))
//      underlyingRuntimeDnsCache.stats.evictionCount shouldBe 3
    }
    val secondCacheMap = underlyingRuntimeDnsCache.asMap()
    secondCacheMap.size() shouldBe 3
//    underlyingRuntimeDnsCache.stats.missCount shouldBe 6
//    underlyingRuntimeDnsCache.stats.loadCount shouldBe 6
  }
}
