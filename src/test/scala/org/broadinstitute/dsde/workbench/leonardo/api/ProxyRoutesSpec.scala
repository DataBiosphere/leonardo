package org.broadinstitute.dsde.workbench.leonardo.api

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbSingleton, TestComponent}
import org.broadinstitute.dsde.workbench.leonardo.api.ProxyRoutesSpec._
import org.broadinstitute.dsde.workbench.leonardo.model.{Cluster, ClusterStatus}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.Future

/**
  * Created by rtitle on 8/10/17.
  */
class ProxyRoutesSpec extends FlatSpec with Matchers with BeforeAndAfterAll with ScalatestRouteTest with ScalaFutures with TestLeoRoutes with TestComponent {
  implicit val patience = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  val googleProject = "dsp-leo-test"
  val clusterName = "test"

  // For starting/stopping the backend server
  var bindingFuture: Future[ServerBinding] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    bindingFuture = Http().bindAndHandle(backendRoute, "0.0.0.0", 8000)
  }

  override def afterAll(): Unit = {
    bindingFuture.flatMap(_.unbind())
    system.terminate()
    DbSingleton.actorSystem.terminate()
    super.afterAll()
  }

  def withTestCluster[T](testCode: => T): T = {
    isolatedDbTest {
      val c1 = Cluster(
        clusterName = clusterName,
        googleId = UUID.randomUUID(),
        googleProject = googleProject,
        googleServiceAccount = "not-a-service-acct@google.com",
        googleBucket = "bucket1",
        clusterUrl = Cluster.getClusterUrl("dsp-leo-test", "name1"),
        operationName = "op1",
        status = ClusterStatus.Unknown,
        hostIp = Some("localhost"),
        createdDate = Instant.now(),
        destroyedDate = None,
        labels = Map("bam" -> "yes", "vcf" -> "no"))
      dbFutureValue { _.clusterQuery.save(c1) } shouldEqual c1
      testCode
    }
  }

  "ProxyRoutes" should "listen on /notebooks/{project}/{name}/..." in withTestCluster {
    Get(s"/notebooks/$googleProject/$clusterName") ~> leoRoutes.proxyRoutes ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
    }
    Get(s"/notebooks/$googleProject/$clusterName/foo") ~> leoRoutes.proxyRoutes ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
    }
    Get(s"/notebooks/$googleProject/aDifferentClusterName") ~> leoRoutes.proxyRoutes ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.NotFound
    }
    Get("/notebooks/") ~> leoRoutes.proxyRoutes ~> check {
      handled shouldBe false
    }
    Get("/foo") ~> leoRoutes.proxyRoutes ~> check {
      handled shouldBe false
    }
    Get("/") ~> leoRoutes.proxyRoutes ~> check {
      handled shouldBe false
    }
  }

  it should "pass through paths" in {
    Get(s"/notebooks/$googleProject/$clusterName") ~> leoRoutes.proxyRoutes ~> check {
      responseAs[Data].path shouldEqual s"/notebooks/$googleProject/$clusterName"
    }
  }

  it should "pass through query string params" in {
    Get(s"/notebooks/$googleProject/$clusterName") ~> leoRoutes.proxyRoutes ~> check {
      responseAs[Data].qs shouldBe None
    }
    Get(s"/notebooks/$googleProject/$clusterName?foo=bar&baz=biz") ~> leoRoutes.proxyRoutes ~> check {
      responseAs[Data].qs shouldEqual Some("foo=bar&baz=biz")
    }
  }

  it should "pass through http methods" in {
    Get(s"/notebooks/$googleProject/$clusterName") ~> leoRoutes.proxyRoutes ~> check {
      responseAs[Data].method shouldBe "GET"
    }
    Post(s"/notebooks/$googleProject/$clusterName") ~> leoRoutes.proxyRoutes ~> check {
      responseAs[Data].method shouldBe "POST"
    }
    Put(s"/notebooks/$googleProject/$clusterName") ~> leoRoutes.proxyRoutes ~> check {
      responseAs[Data].method shouldBe "PUT"
    }
  }

  it should "pass through headers" in {
    Get(s"/notebooks/$googleProject/$clusterName")
      .addHeader(RawHeader("foo", "bar"))
      .addHeader(RawHeader("baz", "biz")) ~> leoRoutes.proxyRoutes ~> check {
      responseAs[Data].headers should contain allElementsOf Map("foo" -> "bar", "baz" -> "biz")
    }
  }

  /**
    * Akka-http TestKit doesn't seem to support websocket servers.
    * So for websocket tests, manually create a server binding on port 9000 for the proxy.
    */
  def withWebsocketProxy[T](testCode: => T): T = {
    val bindingFuture = Http().bindAndHandle(leoRoutes.proxyRoutes, "0.0.0.0", 9000)
    try {
      testCode
    } finally {
      bindingFuture.flatMap(_.unbind())
    }
  }

  it should "proxy websockets" in withWebsocketProxy {
    // Sink for incoming data from the WebSocket
    val incoming = Sink.head[String]

    // Source outgoing data over the WebSocket
    val outgoing = Source.single(TextMessage("Leonardo"))

    // Flow to hit the proxy server
    val webSocketFlow = Http().webSocketClientFlow(WebSocketRequest(Uri(s"ws://localhost:9000/notebooks/$googleProject/$clusterName/websocket"))).map {
      case m: TextMessage.Strict => m.text
      case _ => throw new IllegalArgumentException("ProxyRoutesSpec only supports strict messages")
    }

    // Glue together the source, sink, and flow. Returns a tuple of:
    //  - `upgradeResponse` is a Future[WebSocketUpgradeResponse] that completes or fails when the connection succeeds or fails.
    //  - `result` is a Future[String] with the stream completion from the incoming sink.
    val (upgradeResponse, result) =
      outgoing
        .viaMat(webSocketFlow)(Keep.right)
        .toMat(incoming)(Keep.both)
        .run()

    // The connection future should have returned the HTTP 101 status code
    upgradeResponse.futureValue.response.status shouldBe StatusCodes.SwitchingProtocols
    // The stream completion future should have greeted Leonardo
    result.futureValue shouldBe "Hello Leonardo!"
  }

  // The backend route (i.e. the route behind the proxy)
  val backendRoute: Route =
    pathPrefix("notebooks" / googleProject / clusterName) {
      extractRequest { request =>
        path("websocket") {
          handleWebSocketMessages(greeter)
        } ~
          complete {
            Data(
              request.method.value,
              request.uri.path.toString,
              request.uri.queryString(),
              request.headers.map(h => h.name -> h.value).toMap
            )
          }
      }
    }

  // A simple websocket handler
  def greeter: Flow[Message, Message, Any] =
    Flow[Message].mapConcat {
      case TextMessage.Strict(text) =>
        TextMessage.Strict("Hello " + text + "!") :: Nil

      case text: TextMessage =>
        TextMessage(Source.single("Hello ") ++ text.textStream ++ Source.single("!")) :: Nil

      case bm: BinaryMessage =>
        // ignore binary messages but drain content to avoid the stream being clogged
        bm.dataStream.runWith(Sink.ignore)
        Nil
    }

}

object ProxyRoutesSpec {
  // Convenience class for capturing HTTP data sent to the backend server and returning it back to the caller
  case class Data(method: String, path: String, qs: Option[String], headers: Map[String, String])
  implicit val DataFormat: RootJsonFormat[Data] = jsonFormat4(Data)
}
