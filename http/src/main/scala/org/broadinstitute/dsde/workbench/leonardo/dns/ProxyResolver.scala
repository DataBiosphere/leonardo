package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import cats.effect.Effect
import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.config.ProxyConfig
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.IP
import org.http4s.client.RequestKey

import java.net.InetSocketAddress
import scala.concurrent.Future

trait ProxyResolver[F[_]] {
  // http4s API
  def resolveHttp4s(requestKey: RequestKey): Either[Throwable, InetSocketAddress]

  // akka-http API
  def resolveAkka(host: String, port: Int): Future[InetSocketAddress]
}

class ProxyResolverInterp[F[_]](proxyConfig: ProxyConfig, hostToIpMapping: Ref[F, Map[Host, IP]])(
  implicit F: Effect[F]
) extends ProxyResolver[F] {

  override def resolveHttp4s(requestKey: RequestKey): Either[Throwable, InetSocketAddress] = {
    val res = hostToIpMapping.get.map { mapping =>
      mapping
        .get(Host(requestKey.authority.host.value))
        .map(ip =>
          InetSocketAddress.createUnresolved(ip.asString, requestKey.authority.port.getOrElse(proxyConfig.proxyPort))
        )
        .toRight(IpNotFoundException(requestKey.authority.host.value))
    }
    res.toIO.unsafeRunSync()
  }

  override def resolveAkka(host: String, port: Int): Future[InetSocketAddress] = {
    val res = for {
      mapping <- hostToIpMapping.get
      ipOpt = mapping.get(Host(host)).map(ip => InetSocketAddress.createUnresolved(ip.asString, port))
      res <- F.fromOption(ipOpt, IpNotFoundException(host))
    } yield res
    res.toIO.unsafeToFuture()
  }
}

case class IpNotFoundException(hostname: String)
    extends LeoException(s"IP mapping not found for host ${hostname}", traceId = None)
