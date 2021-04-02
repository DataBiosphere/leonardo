package org.broadinstitute.dsde.workbench.leonardo.dns

import sun.net.util.IPAddressUtil
import sun.security.util.HostnameChecker

import java.security.cert.{CertificateException, X509Certificate}
import javax.net.ssl.{HostnameVerifier, SSLPeerUnverifiedException, SSLSession}

class ProxyHostnameVerifier extends HostnameVerifier {

  def hostnameChecker: HostnameChecker = HostnameChecker.getInstance(HostnameChecker.TYPE_TLS)

  override def verify(hostname: String, session: SSLSession): Boolean = {
    val checker = hostnameChecker
    if (IPAddressUtil.isIPv4LiteralAddress(hostname) || IPAddressUtil.isIPv6LiteralAddress(hostname)) {
      true // TODO check cache
    } else {
      try {
        session.getPeerCertificates match {
          case Array(cert: X509Certificate, _*) =>
            try {
              checker.`match`(hostname, cert)
              true
            } catch {
              case _: CertificateException =>
                false
            }
          case _ => false
        }
      } catch {
        case _: SSLPeerUnverifiedException => false
      }
    }
  }
}
