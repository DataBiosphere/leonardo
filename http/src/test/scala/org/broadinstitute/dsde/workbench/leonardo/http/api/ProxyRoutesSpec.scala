package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.ws.{TextMessage, WebSocketRequest}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.{Keep, Sink, Source}
import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import cats.mtl.Ask
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.TestUtils.appContext
import org.broadinstitute.dsde.workbench.leonardo.config.{ProxyConfig, RefererConfig}
import org.broadinstitute.dsde.workbench.leonardo.dao._
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.http.service.SamResourceCacheKey.{AppCacheKey, RuntimeCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.http.service.TestProxy.{dataDecoder, Data}
import org.broadinstitute.dsde.workbench.leonardo.http.service._
import org.broadinstitute.dsde.workbench.leonardo.model.AuthenticationError
import org.broadinstitute.dsde.workbench.leonardo.monitor.UpdateDateAccessedMessage
import org.broadinstitute.dsde.workbench.leonardo.util.ServicesRegistry
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatestplus.mockito.MockitoSugar

import java.time.Instant
import scala.collection.immutable
import scala.concurrent.duration._

class ProxyRoutesSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfter
    with ScalatestRouteTest
    with ScalaFutures
    with LeonardoTestSuite
    with TestProxy
    with TestComponent
    with GcsPathUtils
    with TestLeoRoutes
    with MockitoSugar {
  implicit val patience: PatienceConfig = PatienceConfig(timeout = scaled(Span(30, Seconds)))
  override def proxyConfig: ProxyConfig = CommonTestData.proxyConfig

  val clusterName = "test"
  val googleProject = "dsp-leo-test"
  val appName = "app"
  val serviceName = "service"
  val validOrigin = "http://example.com"
  val invalidOrigin = "http://something-else.com"
  val unauthorizedTokenCookie = HttpCookiePair("LeoToken", "unauthorized")
  val expiredTokenCookie = HttpCookiePair("LeoToken", "expired")

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
    val res = for {
      _ <- googleTokenCache.removeAll
      _ <- samResourceCache.put(
        RuntimeCacheKey(CloudContext.Gcp(GoogleProject(googleProject)), RuntimeName(clusterName))
      )(
        (Some(runtimeSamResource.resourceId), None),
        None
      )
      _ <- samResourceCache.put(
        AppCacheKey(CloudContext.Gcp(GoogleProject(googleProject)), AppName(appName), None)
      )(
        (Some(appSamId.resourceId), appSamId.accessScope),
        None
      )
      _ <- samResourceCache.put(
        AppCacheKey(CloudContext.Gcp(GoogleProject(googleProject)), AppName(appName), None)
      )(
        (Some(appSamId.resourceId), appSamId.accessScope),
        None
      )
    } yield ()
    res.unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

  "runtime proxy routes" should "listen on /proxy/{project}/{name} id1" in {
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      validateCors(origin = Some("http://example.com"))
    }
    Get(s"/proxy/$googleProject/$clusterName/foo")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      validateCors(origin = Some("http://example.com"))
    }
    Get(s"/proxy/")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldBe StatusCodes.MethodNotAllowed
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "HTTP method not allowed, supported methods: OPTIONS"
    }
    Get(s"/api/proxy")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldBe StatusCodes.NotFound
      val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
      resp shouldBe "\"API not found. Make sure you're calling the correct endpoint with correct method\""
    }
  }

  it should "404 for non-existent runtimes" in {
    val newName = "aDifferentClusterName"
    // should 404 since the internal id cannot be looked up
    Get(s"/proxy/$googleProject/$newName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
    // should still 404 even if a cache entry is present
    samResourceCache
      .put(
        RuntimeCacheKey(CloudContext.Gcp(GoogleProject(googleProject)), RuntimeName(newName))
      )((Some(runtimeSamResource.resourceId), None), None)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    Get(s"/proxy/$googleProject/$newName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "set CORS headers in runtime proxy requests" in {
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      validateCors(origin = Some("http://example.com"))
    }
  }

  it should "reject non-cookied runtime proxy requests" in {
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Referer(Uri(validRefererUri)))
      .addHeader(Origin(validOrigin)) ~> httpRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "404 when using a non-allowlisted user in a runtime proxy request" in {
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Cookie(unauthorizedTokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "401 when using an expired token in a runtime proxy request" in {
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Cookie(expiredTokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should s"pass through paths in runtime proxy requests" in {
    val queue = Queue.bounded[IO, UpdateDateAccessedMessage](100).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val proxyService =
      new MockProxyService(
        proxyConfig,
        MockJupyterDAO,
        allowListAuthProvider,
        runtimeDnsCache,
        kubernetesDnsCache,
        googleTokenCache,
        samResourceCache,
        MockGoogleOAuth2Service,
        queue = Some(queue)
      )
    samResourceCache.put(
      RuntimeCacheKey(CloudContext.Gcp(GoogleProject(googleProject)), RuntimeName(clusterName))
    )((Some(runtimeSamResource.resourceId), None), None)
    val proxyRoutes = new ProxyRoutes(proxyService, corsSupport, refererConfig)
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> proxyRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Data].path shouldEqual s"/proxy/$googleProject/$clusterName"
      val message = queue.tryTake.unsafeRunSync()(cats.effect.unsafe.IORuntime.global).get
      message.cloudContext.asString shouldBe googleProject
      message.updateTarget.asString shouldBe s"runtime/${clusterName}"
    }
  }

  it should s"pass through paths in app proxy requests" in {
    val queue = Queue.bounded[IO, UpdateDateAccessedMessage](100).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
    val proxyService =
      new MockProxyService(
        proxyConfig,
        MockJupyterDAO,
        new BaseMockAuthProvider(),
        runtimeDnsCache,
        kubernetesDnsCache,
        googleTokenCache,
        samResourceCache,
        MockGoogleOAuth2Service,
        queue = Some(queue)
      )
    samResourceCache.put(
      AppCacheKey(CloudContext.Gcp(GoogleProject(googleProject)), AppName(appName), None)
    )((Some(appSamResourceId.resourceId), None), None)
    val proxyRoutes = new ProxyRoutes(proxyService, corsSupport, refererConfig)
    Get(s"/proxy/google/v1/apps/$googleProject/$appName/sas")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> proxyRoutes.route ~> check {
      val message = queue.tryTake.unsafeRunSync()(cats.effect.unsafe.IORuntime.global).get
      message.cloudContext.asString shouldBe googleProject
      message.updateTarget.asString.contains(s"app/${appName}") shouldBe true
    }
  }

  it should "pass through query string params in runtime proxy requests" in {
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].qs shouldBe None
    }
    Get(s"/proxy/$googleProject/$clusterName?foo=bar&baz=biz")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].qs shouldEqual Some("foo=bar&baz=biz")
    }
  }

  it should "pass through encoded query string params in runtime proxy requests" in {
    Get(s"/proxy/$googleProject/$clusterName?foo=This%20is%20an%20encoded%20param.&baz=biz")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].qs shouldEqual Some("foo=This is an encoded param.&baz=biz")
    }
  }

  it should "pass through http methods in runtime proxy requests" in {
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].method shouldBe "GET"
    }
    Post(s"/proxy/$googleProject/$clusterName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].method shouldBe "POST"
    }
    Put(s"/proxy/$googleProject/$clusterName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].method shouldBe "PUT"
    }
  }

  it should "pass through headers in runtime proxy requests" in {
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(RawHeader("foo", "bar"))
      .addHeader(RawHeader("baz", "biz"))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].headers.toList should contain allElementsOf Map("foo" -> "bar", "baz" -> "biz").toList
    }
  }

  it should "remove utf-8'' from content-disposition header filenames" in {
    // The TestProxy adds the Content-Disposition header to the response, we can't do it from here
    Get(s"/proxy/$googleProject/$clusterName/content-disposition-test")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[HttpResponse].headers should contain(
        `Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> "notebook.ipynb"))
      )
    }
  }

  it should "proxy websockets" in withWebsocketProxy {
    // See comments in ProxyService.handleHttpRequest for more high-level information on Flows, Sources, and Sinks.

    // Sink for incoming data from the WebSocket
    val incoming = Sink.head[String]

    // Source outgoing data over the WebSocket
    val outgoing = Source.single(TextMessage("Leonardo"))

    // Flow to hit the proxy server
    val webSocketFlow = Http()
      .webSocketClientFlow(
        WebSocketRequest(Uri(s"ws://localhost:9000/proxy/$googleProject/$clusterName/websocket"),
                         immutable.Seq(Cookie(tokenCookie), Origin(validOrigin))
        )
      )
      .map {
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

  it should "create a new terminal when trying to access a terminal that was not created yet" in {
    val jupyterDAO = mock[JupyterDAO[IO]]
    when {
      jupyterDAO.terminalExists(GoogleProject(googleProject), RuntimeName(clusterName), TerminalName("1"))
    } thenReturn IO.pure(false)

    when {
      jupyterDAO.createTerminal(GoogleProject(googleProject), RuntimeName(clusterName))
    } thenReturn IO.unit

    val httpRoutes = createHttpRoute(jupyterDAO)

    Get(s"/proxy/$googleProject/$clusterName/jupyter/terminals/1")
      .addHeader(Authorization(OAuth2BearerToken(tokenCookie.value)))
      .addHeader(Origin(validOrigin))
      .addHeader(Origin("http://example.com"))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      verify(jupyterDAO, times(1)).terminalExists(GoogleProject(googleProject),
                                                  RuntimeName(clusterName),
                                                  TerminalName("1")
      )
      verify(jupyterDAO, times(1)).createTerminal(GoogleProject(googleProject), RuntimeName(clusterName))
    }
  }

  "app proxy routes" should "listen on /proxy/google/v1/apps/{project}/{name}/{service}" in {
    Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      validateCors(origin = Some("http://example.com"))
    }
    Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName/foo")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      validateCors(origin = Some("http://example.com"))
    }
  }

  it should "404 for non-existent apps" in {
    val newName = "killerApp"
    // should 404 since the internal id cannot be looked up
    Get(s"/proxy/google/v1/apps/$newName/$serviceName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.NotFound
    }
  }

  it should "set CORS headers in app proxy requests" in {
    Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      validateCors(origin = Some("http://example.com"))
    }
  }

  it should "reject non-cookied requests in app proxy requests" in {
    Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should s"401 when using an expired token in app proxy requests" in {
    Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
      .addHeader(Cookie(expiredTokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should s"pass through paths in app proxy requests" in {
    Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.OK
      responseAs[Data].path shouldEqual s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName"
    }
  }

  it should "pass through query string params in app proxy requests" in {
    Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName?foo=bar&baz=biz")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].qs shouldEqual Some("foo=bar&baz=biz")
    }
  }

  it should "pass through encoded query string params in app proxy requests" in {
    Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName?foo=This%20is%20an%20encoded%20param.&baz=biz")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].qs shouldEqual Some("foo=This is an encoded param.&baz=biz")
    }
  }

  it should "pass through http methods in app proxy requests" in {
    Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].method shouldBe "GET"
    }
    Post(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].method shouldBe "POST"
    }
    Put(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].method shouldBe "PUT"
    }
  }

  it should "pass through headers in app proxy requests" in {
    Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(RawHeader("foo", "bar"))
      .addHeader(RawHeader("baz", "biz"))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      responseAs[Data].headers.toList should contain allElementsOf Map("foo" -> "bar", "baz" -> "biz").toList
    }
  }

  "setCookie" should "set a cookie given a valid Authorization header" in {
    // login request with Authorization header should succeed and return a Set-Cookie header
    Get(s"/proxy/$googleProject/$clusterName/setCookie")
      .addHeader(Authorization(OAuth2BearerToken(tokenCookie.value)))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      validateRawCookie(setCookie = header("Set-Cookie"), age = 3600)
      status shouldEqual StatusCodes.NoContent
      validateCors(origin = Some("http://example.com"))
    }

    // cache should now contain the token
    googleTokenCache
      .get(tokenCookie.value)
      .map(_.isDefined)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe true
  }

  it should "reject setCookie if user is not registered" in {
    val samDao = new MockSamDAO {
      override def getSamUserInfo(token: String)(implicit ev: Ask[IO, TraceId]): IO[Option[SamUserInfo]] =
        IO.pure(None)
    }
    val proxyService =
      new MockProxyService(
        proxyConfig,
        MockJupyterDAO,
        allowListAuthProvider,
        runtimeDnsCache,
        kubernetesDnsCache,
        googleTokenCache,
        samResourceCache,
        MockGoogleOAuth2Service,
        samDAO = Some(samDao)
      )

    val gcpOnlyServicesRegistry = {
      val registry = ServicesRegistry()
      registry.register[ProxyService](proxyService)
      registry.register[RuntimeService[IO]](runtimeService)
      registry.register[DiskService[IO]](MockDiskServiceInterp)
      registry
    }

    val httpRoutes = new HttpRoutes(
      openIdConnectionConfiguration,
      statusService,
      gcpOnlyServicesRegistry,
      MockDiskV2ServiceInterp,
      leoKubernetesService,
      runtimev2Service,
      MockAdminServiceInterp,
      userInfoDirectives,
      contentSecurityPolicy,
      refererConfig
    )
    // cache should not initially contain the token
    googleTokenCache
      .get(tokenCookie.value)
      .map(_.isDefined)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe false
    // login request with Authorization header should succeed and return a Set-Cookie header
    Get(s"/proxy/$googleProject/$clusterName/setCookie")
      .addHeader(Authorization(OAuth2BearerToken(tokenCookie.value)))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "handle preflight OPTIONS requests" in {
    Options(s"/proxy/$googleProject/$clusterName/setCookie")
      .addHeader(Authorization(OAuth2BearerToken(tokenCookie.value)))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.NoContent
      header[`Set-Cookie`] shouldBe None
      validateCors(origin = Some("http://example.com"), optionsRequest = true)
    }
  }

  it should "unset the cookie when not given an Authorization header" in {
    Get(s"/proxy/$googleProject/$clusterName/setCookie")
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      handled shouldBe true
      validateUnsetRawCookie(setCookie = header("Set-Cookie"))
      status shouldEqual StatusCodes.NoContent
      validateCors(origin = Some("http://example.com"))
    }
  }

  it should "401 when using an expired token" in {
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Authorization(OAuth2BearerToken(expiredTokenCookie.value)))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  "invalidateToken" should "remove a token from cache" in {
    // regular request with a cookie should succeed but NOT return a Set-Cookie header
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      header[`Set-Cookie`] shouldBe None
    }

    // cache should now contain the token
    googleTokenCache
      .get(tokenCookie.value)
      .map(_.isDefined)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe true

    // log out, passing a cookie
    Get(s"/proxy/invalidateToken")
      .addHeader(Cookie(tokenCookie))
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      handled shouldBe true
      validateUnsetRawCookie(setCookie = header("Set-Cookie"))
      status shouldEqual StatusCodes.NoContent
      validateCors(origin = Some("http://example.com"))
    }

    // cache should not contain the token
    googleTokenCache
      .get(tokenCookie.value)
      .map(_.isDefined)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global) shouldBe false
  }

  "Proxy routes" should "401 when Referer is not a valid URI" in {
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Origin(validOrigin))
      .addHeader(Referer(Uri("https://notAGoodExample.com"))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.Unauthorized
    }
  }

  it should "403 when Origin is not an allowlisted URI" in {
    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Origin(invalidOrigin))
      .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
      status shouldEqual StatusCodes.Forbidden
    }
  }

  it should "handle all origins, referers when wildcards in allowlist" in {
    val routesWithWildcardReferer =
      createHttpRoute(MockJupyterDAO, RefererConfig(Set("*", "bvdp-saturn-dev.appspot.com/"), enabled = true))
    val routesWithNoRefererConfig =
      createHttpRoute(MockJupyterDAO, RefererConfig(Set("bvdp-saturn-dev.appspot.com/"), enabled = false))

    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Origin(invalidOrigin))
      .addHeader(Cookie(tokenCookie))
      .addHeader(Referer(Uri(validRefererUri))) ~> routesWithWildcardReferer.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      validateCors(origin = Some(invalidOrigin))
    }

    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Origin(validOrigin))
      .addHeader(Cookie(tokenCookie))
      .addHeader(Referer(Uri("http://foo:9099"))) ~> routesWithWildcardReferer.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      validateCors(origin = Some(validOrigin))
    }

    Get(s"/proxy/$googleProject/$clusterName")
      .addHeader(Origin(invalidOrigin))
      .addHeader(Cookie(tokenCookie))
      .addHeader(Referer(Uri("http://foo:9099"))) ~> routesWithNoRefererConfig.route ~> check {
      handled shouldBe true
      status shouldEqual StatusCodes.OK
      validateCors(origin = Some(invalidOrigin))
    }

  }

  "Proxy utils" should "fail to decode b2c token if token expired" ignore {
    val token = "" // use a valid expired token for testing
    val now = Instant.now()
    val res = ProxyService.decodeB2cToken(token, now)
    res shouldBe Left(AccessTokenExpiredException)
  }

  it should "fail to decode b2c token" in {
    val res = proxyService.getCachedUserInfoFromToken("", false).attempt.unsafeRunSync()

    res shouldBe Left(AuthenticationError(extraMessage = "invalid token"))
  }

  def createHttpRoute(jupyterDAO: JupyterDAO[IO], useRefererConfig: RefererConfig = refererConfig): HttpRoutes = {
    val proxyService =
      new MockProxyService(proxyConfig,
                           jupyterDAO,
                           allowListAuthProvider,
                           runtimeDnsCache,
                           kubernetesDnsCache,
                           googleTokenCache,
                           samResourceCache,
                           MockGoogleOAuth2Service
      )

    val gcpOnlyServicesRegistry = {
      val registry = ServicesRegistry()
      registry.register[ProxyService](proxyService)
      registry.register[RuntimeService[IO]](runtimeService)
      registry.register[DiskService[IO]](MockDiskServiceInterp)
      registry
    }

    new HttpRoutes(
      openIdConnectionConfiguration,
      statusService,
      gcpOnlyServicesRegistry,
      MockDiskV2ServiceInterp,
      leoKubernetesService,
      runtimev2Service,
      MockAdminServiceInterp,
      userInfoDirectives,
      contentSecurityPolicy,
      useRefererConfig
    )
  }

  /**
   * Akka-http TestKit doesn't seem to support websocket servers.
   * So for websocket tests, manually create a server binding on port 9000 for the proxy.
   */
  def withWebsocketProxy[T](testCode: => T): T = {
    val bindingFuture = Http().bindAndHandle(httpRoutes.route, "0.0.0.0", 9000)
    try
      testCode
    finally
      bindingFuture.flatMap(_.unbind())
  }

  private def validateCors(origin: Option[String] = None, optionsRequest: Boolean = false): Unit = {
    // Issue 272: CORS headers should not be double set
    headers.count(_.is(`Access-Control-Allow-Origin`.lowercaseName)) shouldBe 1
    header[`Access-Control-Allow-Origin`] shouldBe origin
      .map(`Access-Control-Allow-Origin`(_))
      .orElse(Some(`Access-Control-Allow-Origin`.*))
    header[`Access-Control-Allow-Credentials`] shouldBe Some(`Access-Control-Allow-Credentials`(true))
    header[`Access-Control-Allow-Headers`] shouldBe Some(
      `Access-Control-Allow-Headers`("Authorization", "Content-Type", "Accept", "Origin", "X-App-Id")
    )
    header[`Access-Control-Max-Age`] shouldBe Some(`Access-Control-Max-Age`(1728000))
    header[`Access-Control-Allow-Methods`] shouldBe (
      if (optionsRequest) Some(`Access-Control-Allow-Methods`(OPTIONS, POST, PUT, GET, DELETE, HEAD, PATCH))
      else None
    )
    header("Content-Security-Policy") shouldBe Some(
      RawHeader("Content-Security-Policy", contentSecurityPolicy.asString)
    )
  }
}
