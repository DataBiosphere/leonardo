package org.broadinstitute.dsde.workbench.leonardo.dao

import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import io.circe.parser
import HttpJupyterDAO.sessionDecoder
import org.broadinstitute.dsde.workbench.leonardo.dao.ExecutionState.Idle

class HttpJupyterDAOSpec extends FlatSpec with Matchers with BeforeAndAfterAll with ScalatestRouteTest with ScalaFutures {
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

    res shouldBe(Right(List(Session(Kernel(Idle)))))
  }
}
