package org.broadinstitute.dsde.workbench.leonardo.dns

import java.net.{InetAddress, UnknownHostException}

import akka.http.scaladsl.model.Uri.Host
import cats.effect.IO
import cats.effect.concurrent.Ref
import org.broadinstitute.dsde.workbench.leonardo.IP
import sun.net.spi.nameservice.{NameService, NameServiceDescriptor}

class JupyterNameService extends NameService {

  override def getHostByAddr(addr: Array[Byte]): String =
    // Looking up IP -> hostname is not needed for the Leo use case
    throw new UnknownHostException

  override def lookupAllHostAddr(host: String): Array[InetAddress] =
    HostToIpMapping.hostToIpMapping.get
      .unsafeRunSync()
      .get(Host(host))
      .map(ip => Array(InetAddress.getByName(ip.value)))
      .getOrElse {
        throw new UnknownHostException(s"Unknown address: $host")
      }
}

class JupyterNameServiceDescriptor extends NameServiceDescriptor {
  override def createNameService(): NameService = new JupyterNameService
  override def getProviderName: String = "Jupyter"
  override def getType: String = "dns"
}

// This is used by JupyterNameService to look up an IP given a hostname.
// It is populated by RuntimeDnsCache and KubernetesDnsCache.
//
// We use cats-effect Ref to provide a concurrent, mutable, atomic reference.
// See https://typelevel.org/cats-effect/concurrency/ref.html
//
// This is defined statically in an object via Ref.unsafe() because JupyterNameService
// needs to be initialized with a no-arg constructor. Normally, we'd initialize this
// in Boot and pass via constructor to classes that need it.
object HostToIpMapping {
  private[dns] val hostToIpMapping: Ref[IO, Map[Host, IP]] = Ref.unsafe(Map.empty)
}
