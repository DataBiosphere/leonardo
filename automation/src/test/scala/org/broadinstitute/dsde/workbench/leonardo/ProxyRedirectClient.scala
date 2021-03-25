package org.broadinstitute.dsde.workbench.leonardo

import java.util.concurrent.Executors

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, ContextShift, IO, Timer}
import com.typesafe.scalalogging.LazyLogging
import fs2._
import org.broadinstitute.dsde.workbench.service.RestClient
import org.http4s.client.Client
import org.http4s.dsl.io._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.http4s.server.Server
import org.http4s.server.blaze._
import org.http4s.{HttpRoutes, _}

import scala.concurrent.ExecutionContext

object ProxyRedirectClient extends RestClient with LazyLogging {
  def baseUri: IO[Uri] = singletonServer.get.map(_._1.baseUri)

  def get(rurl: String): IO[Uri] =
    baseUri.map(_.withPath("proxyRedirectClient").withQueryParam("rurl", rurl))

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

  def stopServer(): IO[Unit] =
    singletonServer.get.flatMap(_._2)

  private def onError(message: String)(response: Response[IO]): IO[Throwable] =
    for {
      body <- response.bodyText.compile.foldMonoid
    } yield RestError(message, response.status, Some(body))

  private def getContent(rurl: String, blocker: Blocker)(implicit cs: ContextShift[IO]): Stream[IO, Byte] =
    io.readInputStream(IO(getClass.getClassLoader.getResourceAsStream("redirect-proxy-page.html")), 4096, blocker)
      .through(text.utf8Decode)
      .through(text.lines)
      .map(_.replace("""$(rurl)""", s"""'$rurl'"""))
      .intersperse("\n")
      .through(text.utf8Encode)

  private def server: IO[(Server[IO], IO[Unit])] =
    for {
      // Instantiate a dedicated execution context for this server
      blockingEc <- IO(Executors.newCachedThreadPool).map(ExecutionContext.fromExecutor)
      blocker = Blocker.liftExecutionContext(blockingEc)
      implicit0(timer: Timer[IO]) = IO.timer(blockingEc)
      implicit0(cs: ContextShift[IO]) = IO.contextShift(blockingEc)
      route = HttpRoutes
        .of[IO] {
          case GET -> Root / "proxyRedirectClient" :? Rurl(rurl) =>
            Ok(getContent(rurl, blocker), `Content-Type`(MediaType.text.html))
        }
        .orNotFound
      server <- BlazeServerBuilder[IO](blockingEc).bindAny().withHttpApp(route).allocated
    } yield server

  private val singletonServer: Ref[IO, (Server[IO], IO[Unit])] = Ref.unsafe(server.unsafeRunSync())

  private object Rurl extends QueryParamDecoderMatcher[String]("rurl")
}
