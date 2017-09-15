package org.broadinstitute.dsde.workbench.leonardo.api

import java.io.InputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Cookie, RawHeader}
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.broadinstitute.dsde.workbench.leonardo.api.ProxyRoutesSpec._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.collection.immutable
import scala.concurrent.Future

/**
  * Created by rtitle on 8/10/17.
  */
class ProxyRoutesSpec extends FlatSpec with Matchers with BeforeAndAfterAll with ScalatestRouteTest with ScalaFutures with TestLeoRoutes {
  implicit val patience = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  val ClusterName = "test"
  val GoogleProject = "dsp-leo-test"
  val TokenCookie = Cookie("FCToken", "me")

  // The backend server behind the proxy
  var bindingFuture: Future[ServerBinding] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val password = "leo-test".toCharArray

    val ks: KeyStore = KeyStore.getInstance("PKCS12")
    val keystore: InputStream = getClass.getClassLoader.getResourceAsStream("test-jupyter-server.p12")

    require(keystore != null, "Keystore required!")
    ks.load(keystore, password)

    val keyManagerFactory: KeyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(ks, password)

    val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    tmf.init(ks)

    val sslContext: SSLContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)
    val https: HttpsConnectionContext = ConnectionContext.https(sslContext)

    bindingFuture = Http().bindAndHandle(backendRoute, "0.0.0.0", proxyConfig.jupyterPort, https)
  }

  override def afterAll(): Unit = {
    bindingFuture.flatMap(_.unbind())
    super.afterAll()
  }

  "ProxyRoutes" should "listen on /notebooks/{project}/{name}/..." in {
    Get(s"/notebooks/$GoogleProject/$ClusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
    }
    Get(s"/notebooks/$GoogleProject/$ClusterName/foo").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
    }
    Get(s"/notebooks/$GoogleProject/aDifferentClusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.NotFound
    }
    Get("/notebooks/").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      handled shouldBe false
    }
    Get("/api/notebooks").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      handled shouldBe false
    }
  }

  it should "reject non-cookied requests" in {
    Get(s"/notebooks/$GoogleProject/$ClusterName") ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "pass through paths" in {
    Get(s"/notebooks/$GoogleProject/$ClusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Data].path shouldEqual s"/notebooks/$GoogleProject/$ClusterName"
    }
  }

  it should "pass through query string params" in {
    Get(s"/notebooks/$GoogleProject/$ClusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      responseAs[Data].qs shouldBe None
    }
    Get(s"/notebooks/$GoogleProject/$ClusterName?foo=bar&baz=biz").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      responseAs[Data].qs shouldEqual Some("foo=bar&baz=biz")
    }
  }

  it should "pass through http methods" in {
    Get(s"/notebooks/$GoogleProject/$ClusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      responseAs[Data].method shouldBe "GET"
    }
    Post(s"/notebooks/$GoogleProject/$ClusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      responseAs[Data].method shouldBe "POST"
    }
    Put(s"/notebooks/$GoogleProject/$ClusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      responseAs[Data].method shouldBe "PUT"
    }
  }

  it should "pass through headers" in {
    Get(s"/notebooks/$GoogleProject/$ClusterName").addHeader(TokenCookie)
      .addHeader(RawHeader("foo", "bar"))
      .addHeader(RawHeader("baz", "biz")) ~> leoRoutes.route ~> check {
      responseAs[Data].headers should contain allElementsOf Map("foo" -> "bar", "baz" -> "biz")
    }
  }

  it should "proxy websockets" in withWebsocketProxy {
    // See comments in ProxyService.handleHttpRequest for more high-level information on Flows, Sources, and Sinks.

    // Sink for incoming data from the WebSocket
    val incoming = Sink.head[String]

    // Source outgoing data over the WebSocket
    val outgoing = Source.single(TextMessage("Leonardo"))

    // Flow to hit the proxy server
    val webSocketFlow = Http().webSocketClientFlow(WebSocketRequest(Uri(s"ws://localhost:9000/notebooks/$GoogleProject/$ClusterName/websocket"), immutable.Seq(TokenCookie))).map {
      case m: TextMessage.Strict => m.text
      case _ => throw new IllegalArgumentException("ProxyRoutesSpec only supports strict messages")
    }

    // Glue together the source, sink, and flow. This materializes the Flow and actually initiates the HTTP request.
    // Returns a tuple of:
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
    pathPrefix("notebooks" / GoogleProject / ClusterName) {
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

  /**
    * Akka-http TestKit doesn't seem to support websocket servers.
    * So for websocket tests, manually create a server binding on port 9000 for the proxy.
    */
  def withWebsocketProxy[T](testCode: => T): T = {
    val bindingFuture = Http().bindAndHandle(leoRoutes.route, "0.0.0.0", 9000)
    try {
      testCode
    } finally {
      bindingFuture.flatMap(_.unbind())
    }
  }

}

object ProxyRoutesSpec {
  // Convenience class for capturing HTTP data sent to the backend server and returning it back to the caller
  case class Data(method: String, path: String, qs: Option[String], headers: Map[String, String])
  implicit val DataFormat: RootJsonFormat[Data] = jsonFormat4(Data)
}
