package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Cookie, RawHeader}
import akka.http.scaladsl.model.ws.{TextMessage, WebSocketRequest}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.Http
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.broadinstitute.dsde.workbench.leonardo.service.TestProxy
import org.broadinstitute.dsde.workbench.leonardo.service.TestProxy.Data
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.immutable

/**
  * Created by rtitle on 8/10/17.
  */
class ProxyRoutesSpec extends FlatSpec with Matchers with BeforeAndAfterAll with ScalatestRouteTest with ScalaFutures with TestLeoRoutes with TestProxy {
  implicit val patience = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  val clusterName = "test"
  val googleProject = "dsp-leo-test"
  val TokenCookie = Cookie("FCtoken", "me")
  val unauthorizedTokenCookie = Cookie("FCtoken", "unauthorized")
  val expiredTokenCookie = Cookie("FCtoken", "expired")
  val routeTest = this

  override def beforeAll(): Unit = {
    super.beforeAll()
    startProxyServer()
  }

  override def afterAll(): Unit = {
    shutdownProxyServer()
    super.afterAll()
  }

  "ProxyRoutes" should "listen on /notebooks/{project}/{name}/..." in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
    }
    Get(s"/notebooks/$googleProject/$clusterName/foo").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
    }
    Get(s"/notebooks/$googleProject/aDifferentClusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
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
    Get(s"/notebooks/$googleProject/$clusterName") ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "401 when using a non-white-listed user" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(unauthorizedTokenCookie) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "401 when using an expired token" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(expiredTokenCookie) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "pass through paths" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Data].path shouldEqual s"/notebooks/$googleProject/$clusterName"
    }
  }

  it should "pass through query string params" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      responseAs[Data].qs shouldBe None
    }
    Get(s"/notebooks/$googleProject/$clusterName?foo=bar&baz=biz").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      responseAs[Data].qs shouldEqual Some("foo=bar&baz=biz")
    }
  }

  it should "pass through http methods" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      responseAs[Data].method shouldBe "GET"
    }
    Post(s"/notebooks/$googleProject/$clusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      responseAs[Data].method shouldBe "POST"
    }
    Put(s"/notebooks/$googleProject/$clusterName").addHeader(TokenCookie) ~> leoRoutes.route ~> check {
      responseAs[Data].method shouldBe "PUT"
    }
  }

  it should "pass through headers" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(TokenCookie)
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
    val webSocketFlow = Http().webSocketClientFlow(WebSocketRequest(Uri(s"ws://localhost:9000/notebooks/$googleProject/$clusterName/websocket"), immutable.Seq(TokenCookie))).map {
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

