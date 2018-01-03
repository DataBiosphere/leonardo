package org.broadinstitute.dsde.workbench.leonardo.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.ws.{TextMessage, WebSocketRequest}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.Http
import akka.stream.scaladsl.{Keep, Sink, Source}
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.GcsPathUtils
import org.broadinstitute.dsde.workbench.leonardo.service.TestProxy
import org.broadinstitute.dsde.workbench.leonardo.service.TestProxy.Data
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.immutable

/**
  * Created by rtitle on 8/10/17.
  */
class ProxyRoutesSpec extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfter with ScalatestRouteTest with ScalaFutures with TestLeoRoutes with TestProxy with TestComponent with GcsPathUtils {
  implicit val patience = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  val clusterName = "test"
  val googleProject = "dsp-leo-test"
  val tokenCookie = HttpCookiePair("FCtoken", "me")
  val unauthorizedTokenCookie = HttpCookiePair("FCtoken", "unauthorized")
  val expiredTokenCookie = HttpCookiePair("FCtoken", "expired")
  val serviceAccountEmail = WorkbenchEmail("pet-1234567890@test-project.iam.gserviceaccount.com")
  val userEmail = WorkbenchEmail("user1@example.com")

  val routeTest = this

  override def beforeAll(): Unit = {
    super.beforeAll()
    startProxyServer()
  }

  override def afterAll(): Unit = {
    shutdownProxyServer()
    super.afterAll()
  }

  before {
    proxyService.googleTokenCache.invalidateAll()
  }

  "ProxyRoutes" should "listen on /notebooks/{project}/{name}/..." in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
    }
    Get(s"/notebooks/$googleProject/$clusterName/foo").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
    }
    Get(s"/notebooks/$googleProject/aDifferentClusterName").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.NotFound
    }
    Get("/notebooks/").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      handled shouldBe false
    }
    Get("/api/notebooks").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      handled shouldBe false
    }
  }

  it should "reject non-cookied requests" in {
    Get(s"/notebooks/$googleProject/$clusterName") ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "404 when using a non-white-listed user" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(Cookie(unauthorizedTokenCookie)) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "401 when using an expired token" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(Cookie(expiredTokenCookie)) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "pass through paths" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Data].path shouldEqual s"/notebooks/$googleProject/$clusterName"
    }
  }

  it should "pass through query string params" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      responseAs[Data].qs shouldBe None
    }
    Get(s"/notebooks/$googleProject/$clusterName?foo=bar&baz=biz").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      responseAs[Data].qs shouldEqual Some("foo=bar&baz=biz")
    }
  }

  it should "pass through http methods" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      responseAs[Data].method shouldBe "GET"
    }
    Post(s"/notebooks/$googleProject/$clusterName").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      responseAs[Data].method shouldBe "POST"
    }
    Put(s"/notebooks/$googleProject/$clusterName").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      responseAs[Data].method shouldBe "PUT"
    }
  }

  it should "pass through headers" in {
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(Cookie(tokenCookie))
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
    val webSocketFlow = Http().webSocketClientFlow(WebSocketRequest(Uri(s"ws://localhost:9000/notebooks/$googleProject/$clusterName/websocket"), immutable.Seq(Cookie(tokenCookie)))).map {
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

  "setCookie" should "set a cookie given a valid Authorization header" in {
    // cache should not initially contain the token
    proxyService.googleTokenCache.asMap().containsKey(tokenCookie.value) shouldBe false

    // login request with Authorization header should succeed and return a Set-Cookie header
    Get(s"/notebooks/$googleProject/$clusterName/setCookie")
      .addHeader(Authorization(OAuth2BearerToken(tokenCookie.value))) ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      header[`Set-Cookie`] shouldBe Some(`Set-Cookie`(
        HttpCookie.fromPair(
          tokenCookie,
          secure = true,
          maxAge = Some(3600),
          domain = None,
          path = Some("/"))))
    }

    // cache should now contain the token
    proxyService.googleTokenCache.asMap().containsKey(tokenCookie.value) shouldBe true
  }

  it should "401 when not given an Authorization header" in {
    Get(s"/notebooks/$googleProject/$clusterName/setCookie") ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "404 when using a non-white-listed user" in {
    Get(s"/notebooks/$googleProject/$clusterName/setCookie")
      .addHeader(Authorization(OAuth2BearerToken(unauthorizedTokenCookie.value))) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "401 when using an expired token" in {
    Get(s"/notebooks/$googleProject/$clusterName")
      .addHeader(Authorization(OAuth2BearerToken(expiredTokenCookie.value))) ~> leoRoutes.route ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  "invalidateToken" should "remove a token from cache" in {
    // cache should not initially contain the token
    proxyService.googleTokenCache.asMap().containsKey(tokenCookie.value) shouldBe false

    // regular request with a cookie should succeed but NOT return a Set-Cookie header
    Get(s"/notebooks/$googleProject/$clusterName").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      header[`Set-Cookie`] shouldBe None
    }

    // cache should now contain the token
    proxyService.googleTokenCache.asMap().containsKey(tokenCookie.value) shouldBe true

    // log out, passing a cookie
    Get(s"/notebooks/invalidateToken").addHeader(Cookie(tokenCookie)) ~> leoRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      header[`Set-Cookie`] shouldBe None
    }

    // cache should not contain the token
    proxyService.googleTokenCache.asMap().containsKey(tokenCookie.value) shouldBe false
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

