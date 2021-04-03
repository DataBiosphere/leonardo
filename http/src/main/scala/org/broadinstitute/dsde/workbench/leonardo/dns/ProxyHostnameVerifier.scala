package org.broadinstitute.dsde.workbench.leonardo.dns

import akka.http.scaladsl.model.Uri.Host
import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import org.broadinstitute.dsde.workbench.model.IP
import sun.net.util.IPAddressUtil
import sun.security.util.HostnameChecker

import java.security.cert.X509Certificate
import javax.net.ssl.{HostnameVerifier, SSLSession}

class ProxyHostnameVerifier extends HostnameVerifier {
  def hostnameChecker: HostnameChecker = HostnameChecker.getInstance(HostnameChecker.TYPE_TLS)

  override def verify(hostname: String, session: SSLSession): Boolean = {
    val res = for {
      hostOpt <- if (IPAddressUtil.isIPv4LiteralAddress(hostname) || IPAddressUtil.isIPv6LiteralAddress(hostname)) {
        IPToHostMapping.ipToHostMapping.get.map(_.get(IP(hostname)).map(_.address))
      } else {
        IO.pure(Some(hostname))
      }

      res <- hostOpt.flatTraverse { host =>
        val certOpt = session.getPeerCertificates match {
          case Array(cert: X509Certificate, _*) => Some(cert)
          case _                                => None
        }

        certOpt.traverse(cert => IO(hostnameChecker.`match`(host, cert)).attempt.map(_.isRight))

      }
    } yield res.getOrElse(false)

    res.unsafeRunSync()
  }
}

object IPToHostMapping {
  private[dns] val ipToHostMapping: Ref[IO, Map[IP, Host]] = Ref.unsafe(Map.empty)

}
