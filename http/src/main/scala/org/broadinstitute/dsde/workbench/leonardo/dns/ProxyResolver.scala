package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import cats.effect.Async
import cats.effect.Ref
import cats.effect.std.Dispatcher
import cats.implicits._
import org.broadinstitute.dsde.workbench.model.IP
import org.http4s.Uri
import org.http4s.client.RequestKey

import java.net.InetSocketAddress
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

object ProxyResolver {
  def apply[F[_]: Async](hostToIpMapping: Ref[F, Map[Host, IP]], dispatcher: Dispatcher[F]): ProxyResolver[F] =
    new ProxyResolverInterp(hostToIpMapping, dispatcher)

  /**
   * Implementation of ProxyResolver using a Map[Host, IP] stored in a Ref.
   */
  private class ProxyResolverInterp[F[_]](hostToIpMapping: Ref[F, Map[Host, IP]], dispatcher: Dispatcher[F])(
    implicit F: Async[F]
  ) extends ProxyResolver[F] {

    override def resolveHttp4s(requestKey: RequestKey): Either[Throwable, InetSocketAddress] =
      requestKey match {
        case RequestKey(s, auth) =>
          val port = auth.port.getOrElse(if (s == Uri.Scheme.https) 443 else 80)
          val host = auth.host.value
          Either.catchNonFatal(dispatcher.unsafeRunSync(resolveInternal(host, port)))
      }

    override def resolveAkka(host: String, port: Int): Future[InetSocketAddress] =
      dispatcher.unsafeToFuture(resolveInternal(host, port))

    private def resolveInternal(host: String, port: Int): F[InetSocketAddress] =
      for {
        mapping <- hostToIpMapping.get
        // Use the IP if we have a mapping for it; otherwise fall back to default host name resolution
        h = mapping.get(Host(host)).map(_.asString).getOrElse(host)
        res <- F.delay(new InetSocketAddress(h, port))
      } yield res
  }
}
