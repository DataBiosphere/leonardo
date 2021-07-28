package org.broadinstitute.dsde.workbench.leonardo
package http
package api

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{ContentDispositionTypes, `Content-Disposition`, _}
import akka.http.scaladsl.model.ws.{TextMessage, WebSocketRequest}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.scaladsl.{Keep, Sink, Source}
import cats.effect.{IO, Resource}
import cats.mtl.Ask
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._
import cats.effect.std.Queue
import org.broadinstitute.dsde.workbench.leonardo.CommonTestData._
import org.broadinstitute.dsde.workbench.leonardo.KubernetesTestData._
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.dao.google.MockGoogleOAuth2Service
import org.broadinstitute.dsde.workbench.leonardo.dao.{
  JupyterDAO,
  MockJupyterDAO,
  MockSamDAO,
  TerminalName,
  UserSubjectId
}
import org.broadinstitute.dsde.workbench.leonardo.db.TestComponent
import org.broadinstitute.dsde.workbench.leonardo.http.service.SamResourceCacheKey.{AppCacheKey, RuntimeCacheKey}
import org.broadinstitute.dsde.workbench.leonardo.http.service.TestProxy.{dataDecoder, Data}
import org.broadinstitute.dsde.workbench.leonardo.http.service.{MockDiskServiceInterp, MockProxyService, TestProxy}
import org.broadinstitute.dsde.workbench.leonardo.monitor.UpdateDateAccessMessage
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.mockito.Mockito.{verify, _}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatestplus.mockito.MockitoSugar

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
  implicit val patience = PatienceConfig(timeout = scaled(Span(30, Seconds)))
  implicit val routeTimeout = RouteTestTimeout(10 seconds)
  override def proxyConfig: ProxyConfig = CommonTestData.proxyConfig

  val clusterName = "test"
  val googleProject = "dsp-leo-test"
  val appName = "app"
  val serviceName = "service"
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

  val httpRoutesResourceWithProxyService = proxyServiceResource.map { proxyService =>
    proxyService.samResourceCache.put(RuntimeCacheKey(GoogleProject(googleProject), RuntimeName(clusterName)),
                                      Some(runtimeSamResource.resourceId))
    proxyService.samResourceCache.put(AppCacheKey(GoogleProject(googleProject), AppName(appName)),
                                      Some(appSamId.resourceId))
    (proxyService,
     new HttpRoutes(
       swaggerConfig,
       statusService,
       proxyService,
       runtimeService,
       MockDiskServiceInterp,
       leoKubernetesService,
       userInfoDirectives,
       contentSecurityPolicy,
       refererConfig
     ))
  }

  override val httpRoutesResource = httpRoutesResourceWithProxyService.map(_._2)

  "runtime proxy routes" should "listen on /proxy/{project}/{name} id1" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO {
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            handled shouldBe true
            status shouldEqual StatusCodes.OK
            validateCors()
          }
          Get(s"/proxy/$googleProject/$clusterName/foo")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            handled shouldBe true
            status shouldEqual StatusCodes.OK
            validateCors()
          }
          Get(s"/proxy/")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            status shouldBe StatusCodes.MethodNotAllowed
            val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
            resp shouldBe "HTTP method not allowed, supported methods: OPTIONS"
          }
          Get(s"/api/proxy")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            status shouldBe StatusCodes.NotFound
            val resp = responseEntity.toStrict(5 seconds).futureValue.data.utf8String
            resp shouldBe "\"API not found. Make sure you're calling the correct endpoint with correct method\""
          }
        }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "404 for non-existent runtimes" in {
    httpRoutesResourceWithProxyService
      .use {
        case (proxyService, httpRoutes) =>
          val newName = "aDifferentClusterName"
          // should 404 since the internal id cannot be looked up
          IO {
            Get(s"/proxy/$googleProject/$newName")
              .addHeader(Cookie(tokenCookie))
              .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
              status shouldEqual StatusCodes.NotFound
            }
            // should still 404 even if a cache entry is present
            proxyService.samResourceCache.put(RuntimeCacheKey(GoogleProject(googleProject), RuntimeName(newName)),
                                              Some(runtimeSamResource.resourceId))
            Get(s"/proxy/$googleProject/$newName")
              .addHeader(Cookie(tokenCookie))
              .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
              status shouldEqual StatusCodes.NotFound
            }
          }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "set CORS headers in runtime proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO {
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Origin("http://example.com"))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            handled shouldBe true
            status shouldEqual StatusCodes.OK
            validateCors(origin = Some("http://example.com"))
          }
        }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "reject non-cookied runtime proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            handled shouldBe true
            status shouldEqual StatusCodes.Unauthorized
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "404 when using a non-white-listed user in a runtime proxy request" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Cookie(unauthorizedTokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            status shouldEqual StatusCodes.NotFound
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "401 when using an expired token in a runtime proxy request" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Cookie(expiredTokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            status shouldEqual StatusCodes.Unauthorized
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should s"pass through paths in runtime proxy requests" in {
    val queue = Queue.bounded[IO, UpdateDateAccessMessage](100).unsafeRunSync()(cats.effect.unsafe.implicits.global)

    val proxyServiceResource = for {
      runtimeDnsCache <- runtimeDnsCacheResource
      kubernetesDnsCache <- kubernetesDnsCacheResource
    } yield new MockProxyService(proxyConfig,
                                 MockJupyterDAO,
                                 whitelistAuthProvider,
                                 runtimeDnsCache,
                                 kubernetesDnsCache,
                                 MockGoogleOAuth2Service,
                                 queue = Some(queue))

    proxyServiceResource
      .use { proxyService =>
        proxyService.samResourceCache.put(RuntimeCacheKey(GoogleProject(googleProject), RuntimeName(clusterName)),
                                          Some(runtimeSamResource.resourceId))
        val proxyRoutes = new ProxyRoutes(proxyService, corsSupport, refererConfig)
        IO {
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> proxyRoutes.route ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Data].path shouldEqual s"/proxy/$googleProject/$clusterName"
            val message = queue.tryTake.unsafeRunSync()(cats.effect.unsafe.implicits.global).get
            message.googleProject.value shouldBe googleProject
            message.runtimeName.asString shouldBe clusterName
          }
        }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)

  }

  it should "pass through query string params in runtime proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].qs shouldBe None
          }
        ) >> IO(
          Get(s"/proxy/$googleProject/$clusterName?foo=bar&baz=biz")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].qs shouldEqual Some("foo=bar&baz=biz")
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "pass through encoded query string params in runtime proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/$googleProject/$clusterName?foo=This%20is%20an%20encoded%20param.&baz=biz")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].qs shouldEqual Some("foo=This is an encoded param.&baz=biz")
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "pass through http methods in runtime proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO {
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].method shouldBe "GET"
          }
          Post(s"/proxy/$googleProject/$clusterName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].method shouldBe "POST"
          }
          Put(s"/proxy/$googleProject/$clusterName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].method shouldBe "PUT"
          }
        }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "pass through headers in runtime proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO {
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(RawHeader("foo", "bar"))
            .addHeader(RawHeader("baz", "biz"))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].headers.toList should contain allElementsOf Map("foo" -> "bar", "baz" -> "biz").toList
          }
        }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "remove utf-8'' from content-disposition header filenames" in {
    httpRoutesResource
      .use { httpRoutes =>
        // The TestProxy adds the Content-Disposition header to the response, we can't do it from here
        IO {
          Get(s"/proxy/$googleProject/$clusterName/content-disposition-test")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[HttpResponse].headers should contain(
              `Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> "notebook.ipynb"))
            )
          }
        }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "proxy websockets" in {
    websocketProxyResource.use { _ =>
      IO {
        // See comments in ProxyService.handleHttpRequest for more high-level information on Flows, Sources, and Sinks.

        // Sink for incoming data from the WebSocket
        val incoming = Sink.head[String]

        // Source outgoing data over the WebSocket
        val outgoing = Source.single(TextMessage("Leonardo"))

        // Flow to hit the proxy server
        val webSocketFlow = Http()
          .webSocketClientFlow(
            WebSocketRequest(Uri(s"ws://localhost:9000/proxy/$googleProject/$clusterName/websocket"),
                             immutable.Seq(Cookie(tokenCookie)))
          )
          .map {
            case m: TextMessage.Strict => m.text
            case _                     => throw new IllegalArgumentException("ProxyRoutesSpec only supports strict messages")
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
    }

  }

  it should "create a new terminal when trying to access a terminal that was not created yet" in {
    val jupyterDAO = mock[JupyterDAO[IO]]
    when {
      jupyterDAO.terminalExists(GoogleProject(googleProject), RuntimeName(clusterName), TerminalName("1"))
    } thenReturn IO.pure(false)

    when {
      jupyterDAO.createTerminal(GoogleProject(googleProject), RuntimeName(clusterName))
    } thenReturn IO.unit

    createHttpRoute(jupyterDAO)
      .use { httpRoutes =>
        IO {
          Get(s"/proxy/$googleProject/$clusterName/jupyter/terminals/1")
            .addHeader(Authorization(OAuth2BearerToken(tokenCookie.value)))
            .addHeader(Origin("http://example.com"))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            status shouldEqual StatusCodes.OK
            verify(jupyterDAO, times(1)).terminalExists(GoogleProject(googleProject),
                                                        RuntimeName(clusterName),
                                                        TerminalName("1"))
            verify(jupyterDAO, times(1)).createTerminal(GoogleProject(googleProject), RuntimeName(clusterName))
          }
        }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)

  }

  "app proxy routes" should "listen on /proxy/google/v1/apps/{project}/{name}/{service}" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO {
          Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            handled shouldBe true
            status shouldEqual StatusCodes.OK
            validateCors()
          }
          Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName/foo")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            handled shouldBe true
            status shouldEqual StatusCodes.OK
            validateCors()
          }
        }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "404 for non-existent apps" in {
    val newName = "killerApp"
    // should 404 since the internal id cannot be looked up
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/google/v1/apps/$newName/$serviceName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            status shouldEqual StatusCodes.NotFound
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "set CORS headers in app proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Origin("http://example.com"))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            handled shouldBe true
            status shouldEqual StatusCodes.OK
            validateCors(origin = Some("http://example.com"))
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "reject non-cookied requests in app proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            handled shouldBe true
            status shouldEqual StatusCodes.Unauthorized
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should s"401 when using an expired token in app proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
            .addHeader(Cookie(expiredTokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            status shouldEqual StatusCodes.Unauthorized
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should s"pass through paths in app proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Data].path shouldEqual s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName"
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "pass through query string params in app proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName?foo=bar&baz=biz")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].qs shouldEqual Some("foo=bar&baz=biz")
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "pass through encoded query string params in app proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(
            s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName?foo=This%20is%20an%20encoded%20param.&baz=biz"
          ).addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].qs shouldEqual Some("foo=This is an encoded param.&baz=biz")
          }
        )
      }
  }

  it should "pass through http methods in app proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].method shouldBe "GET"
          }
        ) >> IO(
          Post(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].method shouldBe "POST"
          }
        ) >> IO(
          Put(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].method shouldBe "PUT"
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "pass through headers in app proxy requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/google/v1/apps/$googleProject/$appName/$serviceName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(RawHeader("foo", "bar"))
            .addHeader(RawHeader("baz", "biz"))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            responseAs[Data].headers.toList should contain allElementsOf Map("foo" -> "bar", "baz" -> "biz").toList
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  "setCookie" should "set a cookie given a valid Authorization header" in {
    httpRoutesResourceWithProxyService
      .use {
        case (proxyService, httpRoutes) =>
          IO {
            // cache should not initially contain the token
            proxyService.googleTokenCache.asMap().containsKey(tokenCookie.value) shouldBe false
            // login request with Authorization header should succeed and return a Set-Cookie header
            Get(s"/proxy/$googleProject/$clusterName/setCookie")
              .addHeader(Authorization(OAuth2BearerToken(tokenCookie.value)))
              .addHeader(Origin("http://example.com"))
              .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
              validateRawCookie(setCookie = header("Set-Cookie"), age = 3600)
              status shouldEqual StatusCodes.NoContent
              validateCors(origin = Some("http://example.com"))
            }

            // cache should now contain the token
            proxyService.googleTokenCache.asMap().containsKey(tokenCookie.value) shouldBe true

          }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)

  }

  it should "reject setCookie if user is not registered" in {
    val samDao = new MockSamDAO {
      override def getUserSubjectIdFromToken(token: String)(implicit ev: Ask[IO, TraceId]): IO[Option[UserSubjectId]] =
        IO.pure(None)
    }
    val httpRoutesWithProxyService = for {
      runtimeDnsCache <- runtimeDnsCacheResource
      kubernetesDnsCache <- kubernetesDnsCacheResource
    } yield {
      val proxyService = new MockProxyService(proxyConfig,
                                              MockJupyterDAO,
                                              whitelistAuthProvider,
                                              runtimeDnsCache,
                                              kubernetesDnsCache,
                                              MockGoogleOAuth2Service,
                                              samDAO = Some(samDao))
      (proxyService,
       new HttpRoutes(
         swaggerConfig,
         statusService,
         proxyService,
         runtimeService,
         MockDiskServiceInterp,
         leoKubernetesService,
         userInfoDirectives,
         contentSecurityPolicy,
         refererConfig
       ))
    }

    httpRoutesWithProxyService
      .use {
        case (proxyService, httpRoutes) =>
          IO {
            // cache should not initially contain the token
            proxyService.googleTokenCache.asMap().containsKey(tokenCookie.value) shouldBe false
            // login request with Authorization header should succeed and return a Set-Cookie header
            Get(s"/proxy/$googleProject/$clusterName/setCookie")
              .addHeader(Authorization(OAuth2BearerToken(tokenCookie.value)))
              .addHeader(Origin("http://example.com"))
              .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
              status shouldEqual StatusCodes.Unauthorized
            }
          }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)

  }

  it should "handle preflight OPTIONS requests" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO {
          Options(s"/proxy/$googleProject/$clusterName/setCookie")
            .addHeader(Authorization(OAuth2BearerToken(tokenCookie.value)))
            .addHeader(Origin("http://example.com"))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            handled shouldBe true
            status shouldEqual StatusCodes.NoContent
            header[`Set-Cookie`] shouldBe None
            validateCors(origin = Some("http://example.com"), optionsRequest = true)
          }
        }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "unset the cookie when not given an Authorization header" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/$googleProject/$clusterName/setCookie")
            .addHeader(Origin("http://example.com"))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            handled shouldBe true
            validateUnsetRawCookie(setCookie = header("Set-Cookie"))
            status shouldEqual StatusCodes.NoContent
            validateCors(origin = Some("http://example.com"))
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "401 when using an expired token" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO {
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Authorization(OAuth2BearerToken(expiredTokenCookie.value)))
            .addHeader(Origin("http://example.com"))
            .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
            status shouldEqual StatusCodes.Unauthorized
          }
        }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)

  }

  "invalidateToken" should "remove a token from cache" in {
    httpRoutesResourceWithProxyService
      .use {
        case (proxyService, httpRoutes) =>
          IO {
            proxyService.googleTokenCache.asMap().containsKey(tokenCookie.value) shouldBe false

            // regular request with a cookie should succeed but NOT return a Set-Cookie header
            Get(s"/proxy/$googleProject/$clusterName")
              .addHeader(Cookie(tokenCookie))
              .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
              handled shouldBe true
              status shouldEqual StatusCodes.OK
              header[`Set-Cookie`] shouldBe None
            }

            // cache should now contain the token
            proxyService.googleTokenCache.asMap().containsKey(tokenCookie.value) shouldBe true

            // log out, passing a cookie
            Get(s"/proxy/invalidateToken")
              .addHeader(Cookie(tokenCookie))
              .addHeader(Origin("http://example.com"))
              .addHeader(Referer(Uri(validRefererUri))) ~> httpRoutes.route ~> check {
              handled shouldBe true
              validateUnsetRawCookie(setCookie = header("Set-Cookie"))
              status shouldEqual StatusCodes.NoContent
              validateCors(origin = Some("http://example.com"))
            }

            // cache should not contain the token
            proxyService.googleTokenCache.asMap().containsKey(tokenCookie.value) shouldBe false
          }
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  def createHttpRoute(jupyterDAO: JupyterDAO[IO]): Resource[IO, HttpRoutes] =
    for {
      runtimeDnsCache <- runtimeDnsCacheResource
      kubernetesDnsCache <- kubernetesDnsCacheResource
    } yield {
      val proxyService =
        new MockProxyService(proxyConfig,
                             jupyterDAO,
                             whitelistAuthProvider,
                             runtimeDnsCache,
                             kubernetesDnsCache,
                             MockGoogleOAuth2Service)
      proxyService.samResourceCache
        .put(RuntimeCacheKey(GoogleProject(googleProject), RuntimeName(clusterName)),
             Some(runtimeSamResource.resourceId))
      proxyService.samResourceCache
        .put(AppCacheKey(GoogleProject(googleProject), AppName(appName)), Some(appSamId.resourceId))

      new HttpRoutes(
        swaggerConfig,
        statusService,
        proxyService,
        runtimeService,
        MockDiskServiceInterp,
        leoKubernetesService,
        userInfoDirectives,
        contentSecurityPolicy,
        refererConfig
      )
    }

  /**
   * Akka-http TestKit doesn't seem to support websocket servers.
   * So for websocket tests, manually create a server binding on port 9000 for the proxy.
   */
  def websocketProxyResource: Resource[IO, Unit] =
    for {
      httpRoutes <- httpRoutesResource
      _ <- Resource.make(IO.fromFuture(IO(Http().bindAndHandle(httpRoutes.route, "0.0.0.0", 9000))))(binding =>
        IO.fromFuture(IO(binding.unbind())).void
      )
    } yield ()

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
    header("Content-Security-Policy") shouldBe Some(RawHeader("Content-Security-Policy", contentSecurityPolicy))
  }

  it should "401 when Referer is not a valid URI" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Referer(Uri("https://notAGoodExample.com"))) ~> httpRoutes.route ~> check {
            status shouldEqual StatusCodes.Unauthorized
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }

  it should "handle wildcards in referer allow list" in {
    httpRoutesResource
      .use { httpRoutes =>
        IO(
          Get(s"/proxy/$googleProject/$clusterName")
            .addHeader(Cookie(tokenCookie))
            .addHeader(Referer(Uri("http://foo:9099"))) ~> httpRoutes.route ~> check {
            handled shouldBe true
            status shouldEqual StatusCodes.OK
            validateCors()
          }
        )
      }
      .unsafeRunSync()(cats.effect.unsafe.implicits.global)
  }
}
