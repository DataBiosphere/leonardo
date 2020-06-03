package org.broadinstitute.dsde.workbench.leonardo.dao

import io.circe.parser
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData.{dnsCacheConfig, proxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao.ExecutionState.Idle
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpJupyterDAO.sessionDecoder
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.{FakeHttpClient, LeonardoTestSuite, RuntimeName}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global

class HttpJupyterDAOSpec extends FlatSpec with LeonardoTestSuite with Matchers with TestComponent {
  it should "decode jupyter list sessions response successfully" in {
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
    val clusterDnsCache = new ClusterDnsCache(proxyConfig, testDbRef, dnsCacheConfig, blocker)

    val jupyterDAO = new HttpJupyterDAO(clusterDnsCache, FakeHttpClient.client)
    val res = jupyterDAO.isAllKernelsIdle(GoogleProject("project1"), RuntimeName("rt"))
    res.unsafeRunSync() shouldBe (true)
  }
}
