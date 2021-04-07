package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import cats.effect.Effect
import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.model.IP
import org.http4s.client.RequestKey

import java.net.{InetAddress, InetSocketAddress}
import scala.concurrent.Future

/**
 * Implements custom DNS resolution for accessing routes through the Leo proxy.
 * Contains APIs for http4s and akka-http.
 */
trait ProxyResolver[F[_]] {
  // http4s API
  // See https://github.com/http4s/http4s/pull/4699
  def resolveHttp4s(requestKey: RequestKey): Either[Throwable, InetSocketAddress]

  // akka-http API
  // See https://doc.akka.io/docs/akka-http/current/client-side/client-transport.html#custom-host-name-resolution-transport
  def resolveAkka(host: String, port: Int): Future[InetSocketAddress]
}

/**
 * Implementation of ProxyResolver using a Map[Host, IP] stored in a Ref.
 */
class ProxyResolverInterp[F[_]](proxyConfig: ProxyConfig, hostToIpMapping: Ref[F, Map[Host, IP]])(
  implicit F: Effect[F]
) extends ProxyResolver[F] {

  override def resolveHttp4s(requestKey: RequestKey): Either[Throwable, InetSocketAddress] =
    resolveInternal(requestKey.authority.host.value, requestKey.authority.port.getOrElse(proxyConfig.proxyPort))
      .map(_.asRight)
      .toIO
      .unsafeRunSync()

  override def resolveAkka(host: String, port: Int): Future[InetSocketAddress] =
    resolveInternal(host, port).toIO.unsafeToFuture()

  private def resolveInternal(host: String, port: Int): F[InetSocketAddress] =
    hostToIpMapping.get.map { mapping =>
      // If we have the mapping stored in the Ref, use IP without resolving the host
      // Otherwise, fall back to default hostname resolution
      mapping.get(Host(host)) match {
        case Some(ip) => InetSocketAddress.createUnresolved(ip.asString, port)
        case _        => new InetSocketAddress(InetAddress.getByName(host), port)
      }
    }
}
