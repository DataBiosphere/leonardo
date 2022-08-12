package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{IO, Resource}
import cats.implicits._
import fs2._
import org.broadinstitute.dsde.workbench.leonardo.BillingProjectFixtureSpec.proxyRedirectServerPortKey
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.client.Client
import org.http4s.dsl.io._
import org.http4s.headers.{Referer, `Content-Type`}
import org.http4s.implicits._
import org.http4s.server.Server
import org.http4s._

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

// This is for setting `REFERER` header in automation tests
object ProxyRedirectClient {
  // serverRef is Singleton http4s server to serve the proxy redirect page.
  // Explanation of the type:
  //   `Ref` is a cats-effect reference, used to cache a single instance of the server
  //   `Server[IO]` is the actual server instance
  //   `IO[Unit]` is used to shut down the server. See documentation for http4s `ServerBuilder.allocated`.
  //
  // There might be more than one server running if there's more than one test using
  // `NewBillingProjectAndWorkspaceBeforeAndAfterAll`
//  private val serverRef: Ref[IO, Map[Int, (Server, IO[Unit])]] =
//    Ref.of[IO, Map[Int, (Server, IO[Unit])]](Map.empty).unsafeRunSync()(cats.effect.unsafe.IORuntime.global)

  // If test is running in headless mode, hostname needs to work in a docker container
  val host = sys.props.get("headless") match {
    case Some("true") => java.net.InetAddress.getLocalHost.getHostName
    case _            => "localhost"
  }

//  def startServer(): IO[Int] = ProxyRedirectClient.server.use(
//    server =>
//      IO.pure(server.address.port.value)
//  )
////    for {
////      serverAndShutDown <- ProxyRedirectClient.server
////      port = serverAndShutDown._1.address.port.value
////      _ <- serverRef.modify(mp => (mp, mp + (port -> serverAndShutDown)))
////    } yield port
//
//  def stopServer(port: Int): IO[Unit] =
//    for {
//      ref <- serverRef.get
//      _ <- ref.get(port).traverse(_._2)
//    } yield ()

  def baseUri: Uri =
    Uri.unsafeFromString(s"http://${host}:${sys.props.get(proxyRedirectServerPortKey).get.toInt}")

  def genRefererHeader(): IO[Referer] =
    IO.pure(Referer(baseUri))

  def get(rurl: String): IO[Uri] =
    IO.pure(baseUri.withPath(Uri.Path.unsafeFromString("proxyRedirectClient")).withQueryParam("rurl", rurl))

  // Tests the server connection by making a simple GET request
  def testConnection(rurl: String)(implicit client: Client[IO]): IO[Unit] =
    for {
      uri <- get(rurl)
      r <- client
        .run(
          Request[IO](
            method = Method.GET,
            uri = uri
          )
        )
        .use { resp =>
          if (!resp.status.isSuccess) {
            onError(s"Could not load proxy redirect page at url: ${get(rurl)}")(resp)
              .flatMap(IO.raiseError)
          } else
            IO.unit
        }
    } yield r

  private def onError(message: String)(response: Response[IO]): IO[Throwable] =
    for {
      body <- response.bodyText.compile.foldMonoid
    } yield RestError(message, response.status, Some(body))

  // Loads redirect-proxy-page.html and replaces templated values
  private def getContent(rurl: String): Stream[IO, Byte] =
    io.readInputStream(IO(getClass.getClassLoader.getResourceAsStream("redirect-proxy-page.html")), 4096)
      .through(text.utf8.decode)
      .through(text.lines)
      .map(_.replace("""$(rurl)""", s"""'$rurl'"""))
      .intersperse("\n")
      .through(text.utf8.encode)

  def server: Resource[IO, Server] =
    for {
      // Instantiate a dedicated execution context for this server
      blockingEc <- Resource.eval(IO(Executors.newCachedThreadPool).map(ExecutionContext.fromExecutor))
      route = HttpRoutes
        .of[IO] { case GET -> Root / "proxyRedirectClient" :? Rurl(rurl) =>
          Ok(getContent(rurl), `Content-Type`(MediaType.text.html))
        }
        .orNotFound
      // Note this uses `bindAny` which will bind to an arbitrary port. We can't use a dedicated port
      // because multiple test suites may be running on the same host in different class loaders.
      server <- BlazeServerBuilder[IO].withExecutionContext(blockingEc).bindAny("0.0.0.0").withHttpApp(route).resource
    } yield server

  private object Rurl extends QueryParamDecoderMatcher[String]("rurl")
}
