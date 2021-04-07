package org.broadinstitute.dsde.workbench.leonardo

import akka.http.scaladsl.model.Uri
import cats.effect.IO
import cats.effect.concurrent.Ref
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dns.ProxyResolver
import org.broadinstitute.dsde.workbench.model.IP
import org.http4s.client.RequestKey

import java.net.InetSocketAddress
import scala.concurrent.Future

class MockProxyResolver(host: String, port: Int) extends ProxyResolver[IO] {
  override def resolveHttp4s(requestKey: RequestKey): Either[Throwable, InetSocketAddress] =
    Right(InetSocketAddress.createUnresolved(host, port))

  override def resolveAkka(host: String, port: Int): Future[InetSocketAddress] =
    Future.successful(InetSocketAddress.createUnresolved(host, port))

  val hostToIpMapping: Ref[IO, Map[Uri.Host, IP]] = Ref.unsafe(Map.empty)
}

object LocalProxyResolver extends MockProxyResolver("localhost", Config.proxyConfig.proxyPort)
