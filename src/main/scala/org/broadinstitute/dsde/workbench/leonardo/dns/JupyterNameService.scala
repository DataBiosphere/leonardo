package org.broadinstitute.dsde.workbench.leonardo.dns

import java.net.{InetAddress, UnknownHostException}

import akka.http.scaladsl.model.Uri.Host
import org.slf4j.LoggerFactory
import sun.net.spi.nameservice.{NameService, NameServiceDescriptor}

/**
  * Created by rtitle on 8/25/17.
  */
class JupyterNameService extends NameService {

  override def getHostByAddr(addr: Array[Byte]): String = {
    // Looking up IP -> hostname is not needed for the Leo use case
    throw new UnknownHostException
  }

  override def lookupAllHostAddr(host: String): Array[InetAddress] = {
    ClusterDnsCache.HostToIp.get(Host(host)).map(ip => Array(InetAddress.getByName(ip.string))).getOrElse {
      throw new UnknownHostException(s"Unknown address: $host")
    }
  }
}

class JupyterNameServiceDescriptor extends NameServiceDescriptor {
  override def createNameService(): NameService = new JupyterNameService
  override def getProviderName: String = "Jupyter"
  override def getType: String = "dns"
}
