package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.IO
import com.github.benmanes.caffeine.cache.Caffeine
import io.circe.parser
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.dao.ExecutionState.Idle
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpJupyterDAO.sessionDecoder
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.dns.RuntimeDnsCache
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalacache.Cache
import scalacache.caffeine.CaffeineCache

import scala.concurrent.ExecutionContext.Implicits.global

class HttpJupyterDAOSpec extends AnyFlatSpec with Matchers with LeonardoTestSuite with TestComponent {
  val underlyingRuntimeDnsCache =
    Caffeine.newBuilder().maximumSize(10000L).build[String, scalacache.Entry[HostStatus]]()
  val runtimeDnsCaffeineCache: Cache[IO, HostStatus] = CaffeineCache[IO, HostStatus](underlyingRuntimeDnsCache)

  override def beforeAll(): Unit = {
    super.beforeAll()
    runtimeDnsCaffeineCache.removeAll.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  override def afterAll(): Unit = {
    runtimeDnsCaffeineCache.close.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    super.afterAll()
  }

  "HttpJupyterDAO" should "decode jupyter list sessions response successfully" in {
    val response =
      """
        |[
        |  {
        |    "id": "5091122d-6067-4dbb-8671-3709523d3aa7",
        |    "kernel": {
        |      "connections": 1,
        |      "execution_state": "idle",
        |      "id": "a6562977-dbf2-40a8-8f87-83fd61c07366",
        |      "last_activity": "2019-05-03T17:52:41.092191Z",
        |      "name": "python3"
        |    },
        |    "name": "",
        |    "notebook": {
        |      "name": "",
        |      "path": "Demo_2017-10-24/testing.ipynb"
        |    },
        |    "path": "Demo_2017-10-24/testing.ipynb",
        |    "type": "notebook"
        |  }
        |]
      """.stripMargin

    val res = for {
      json <- parser.parse(response)
      resp <- json.as[List[Session]]
    } yield resp

    res shouldBe (Right(List(Session(Kernel(Idle)))))
  }

  it should "return true for isAllKernelsIdle if host is down" in {
    val clusterDnsCache =
      new RuntimeDnsCache(proxyConfig, testDbRef, hostToIpMapping, runtimeDnsCaffeineCache)

    val jupyterDAO = new HttpJupyterDAO(clusterDnsCache, FakeHttpClient.client)
    val res = jupyterDAO.isAllKernelsIdle(CloudContext.Gcp(GoogleProject("project1")), RuntimeName("rt"))
    res.map(r => r shouldBe true).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }
}
