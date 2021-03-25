package org.broadinstitute.dsde.workbench.leonardo

import cats.effect.{Blocker, ContextShift, IO, Resource, Timer}
import com.typesafe.scalalogging.LazyLogging
import fs2._
import org.broadinstitute.dsde.workbench.service.RestClient
import org.broadinstitute.dsde.workbench.util2.ExecutionContexts
import org.http4s.dsl.io._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.http4s.server.Server
import org.http4s.server.blaze._
import org.http4s.{HttpRoutes, _}

object ProxyRedirectClient extends RestClient with LazyLogging {
  // Note: change to "localhost" if running tests locally
  val host = java.net.InetAddress.getLocalHost.getHostName
  val port = 9099

  def get(rurl: String): String =
    s"http://$host:$port/proxyRedirectClient?rurl=${rurl}"

  private def getContent(rurl: String, blocker: Blocker)(implicit cs: ContextShift[IO]): Stream[IO, Byte] =
    io.readInputStream(IO(getClass.getClassLoader.getResourceAsStream("redirect-proxy-page.html")), 4096, blocker)
      .through(text.utf8Decode)
      .through(text.lines)
      .map(_.replace("""$(rurl)""", s"""'$rurl'"""))
      .intersperse("\n")
      .through(text.utf8Encode)

  def server: Resource[IO, Server[IO]] =
    for {
      // Instantiate a dedicated execution context for this server
      blockingEc <- ExecutionContexts.cachedThreadPool[IO]
      blocker = Blocker.liftExecutionContext(blockingEc)
      implicit0(timer: Timer[IO]) = IO.timer(blockingEc)
      implicit0(cs: ContextShift[IO]) = IO.contextShift(blockingEc)
      route = HttpRoutes
        .of[IO] {
          case GET -> Root / "proxyRedirectClient" :? Rurl(rurl) =>
            Ok(getContent(rurl, blocker), `Content-Type`(MediaType.text.html))
        }
        .orNotFound
      server <- BlazeServerBuilder[IO](blockingEc).bindHttp(port, host).withHttpApp(route).resource
    } yield server

  object Rurl extends QueryParamDecoderMatcher[String]("rurl")
}
