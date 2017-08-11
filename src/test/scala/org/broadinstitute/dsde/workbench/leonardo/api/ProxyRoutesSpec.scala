package org.broadinstitute.dsde.workbench.leonardo.api

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 8/10/17.
  */
class ProxyRoutesSpec extends FlatSpec with Matchers with BeforeAndAfterAll with ScalatestRouteTest with ScalaFutures {
  implicit val patience = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  // ScalatestRouteTest sets the Host header to 'www.example.com' by default.
  // We need to set it to localhost for the proxy to work.
  val LocalHostHeader = akka.http.scaladsl.model.headers.Host("localhost")

  // Convenience class for capturing HTTP data sent to the backend server and returning it back to the caller
  case class Data(method: String, path: String, qs: Option[String], headers: Map[String, String])
  implicit val DataFormat = jsonFormat4(Data)

  // The backend route (i.e. the route behind the proxy)
  val backendRoute: Route =
    pathPrefix("notebooks") {
      extractRequest { request =>
        path("websocket") {
          println("in a websocet")
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

  def greeter: Flow[Message, Message, Any] =
    Flow[Message].mapConcat {
      // only accept strict messages for this test
      case TextMessage.Strict(text) =>
        TextMessage.Strict("Hello " + text + "!") :: Nil

      // ignore non-strict messages but drain content to avoid the stream being clogged
      case _ => throw new IllegalArgumentException("ProxyRoutesSpec only supports strict websocket messages")
    }

  private class TestProxyRoutes(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext)
    extends ProxyRoutes with LazyLogging

  val proxyRoutes = pathPrefix("notebooks") { new TestProxyRoutes proxyRoutes }

  // For starting/stopping the backend server
  var bindingFuture: Future[ServerBinding] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    bindingFuture = Http().bindAndHandle(backendRoute, "0.0.0.0", 8000)
  }

  override def afterAll(): Unit = {
    bindingFuture.flatMap(_.unbind())
    super.afterAll()
  }

  "ProxyRoutes" should "listen on /notebooks/{id}/..." in {
    Get("/notebooks/test").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
    }
    Get("/notebooks/test/foo").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
    }
    Get("/notebooks/").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      handled shouldBe false
    }
    Get("/foo").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      handled shouldBe false
    }
    Get("/").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      handled shouldBe false
    }
  }

  it should "pass through paths" in {
    Get("/notebooks/test").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      responseAs[Data].path shouldEqual "/notebooks/test"
    }
    Get("/notebooks/test2").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      responseAs[Data].path shouldEqual "/notebooks/test2"
    }
    Get("/notebooks/test3").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      responseAs[Data].path shouldEqual "/notebooks/test3"
    }
  }

  it should "pass through query string params" in {
    Get("/notebooks/test").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      responseAs[Data].qs shouldBe None
    }
    Get("/notebooks/test?foo=bar&baz=biz").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      responseAs[Data].qs shouldEqual Some("foo=bar&baz=biz")
    }
  }

  it should "pass through http methods" in {
    Get("/notebooks/test").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      responseAs[Data].method shouldBe "GET"
    }
    Post("/notebooks/test").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      responseAs[Data].method shouldBe "POST"
    }
    Put("/notebooks/test").addHeader(LocalHostHeader) ~> proxyRoutes ~> check {
      responseAs[Data].method shouldBe "PUT"
    }
  }

  it should "pass through headers" in {
    Get("/notebooks/test")
      .addHeader(LocalHostHeader)
      .addHeader(RawHeader("foo", "bar"))
      .addHeader(RawHeader("baz", "biz")) ~> proxyRoutes ~> check {
      responseAs[Data].headers should contain allElementsOf Map("foo" -> "bar", "baz" -> "biz")
    }
  }

  /**
    * Akka-http TestKit doesn't seem to support websocket servers.
    * So for websocket tests, manually create a server binding on port 9000 for the proxy.
    */
  def withWebsocketProxy[T](testCode: => T): T = {
    val bindingFuture = Http().bindAndHandle(proxyRoutes, "0.0.0.0", 9000)
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
    val webSocketFlow = Http().webSocketClientFlow(WebSocketRequest(Uri("ws://localhost:9000/notebooks/websocket"))).map {
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
}
