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

/**
 * Implementation of javax.net.ssl.HostnameVerifier which delegates to
 * sun.security.util.HostnameChecker, but uses the Leo DNS cache to resolve IPs.
 */
class ProxyHostnameVerifier extends HostnameVerifier {
  def hostnameChecker: HostnameChecker = HostnameChecker.getInstance(HostnameChecker.TYPE_TLS)

  override def verify(hostname: String, session: SSLSession): Boolean = {
    val res = for {
      // Consult our cache if the host is an IP; otherwise use it directly.
      hostOpt <- if (IPAddressUtil.isIPv4LiteralAddress(hostname) || IPAddressUtil.isIPv6LiteralAddress(hostname)) {
        val key = IP(hostname)
        IPToHostMapping.ipToHostMapping.get.map(_.get(key).map(_.address))
      } else {
        IO.pure(Some(hostname))
      }

      // Resolve the server certificate being used
      certOpt <- IO(session.getPeerCertificates).attempt.map {
        case Right(Array(cert: X509Certificate, _*)) => Some(cert)
        case _                                       => None
      }

      // Call the JDK HostnameChecker to do the verification
      res <- hostOpt.flatTraverse { host =>
        certOpt.traverse(cert => IO(hostnameChecker.`match`(host, cert)).attempt.map(_.isRight))
      }
    } yield res.getOrElse(false)

    res.unsafeRunSync()
  }
}

object IPToHostMapping {
  private[dns] val ipToHostMapping: Ref[IO, Map[IP, Host]] = Ref.unsafe(Map.empty)
}
