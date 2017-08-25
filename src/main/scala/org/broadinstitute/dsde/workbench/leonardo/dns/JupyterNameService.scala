package org.broadinstitute.dsde.workbench.leonardo.dns

import java.net.{InetAddress, UnknownHostException}

import sun.net.spi.nameservice.{NameService, NameServiceDescriptor}

/**
  * Created by rtitle on 8/25/17.
  */
class JupyterNameService extends NameService {

  override def getHostByAddr(addr: Array[Byte]): String = {
    val addrAsString = InetAddress.getByAddress(addr).getHostAddress
    ClusterDnsCache.IpToHost.get(addrAsString).getOrElse {
      throw new UnknownHostException(s"Unknown address: $addrAsString")
    }
  }

  override def lookupAllHostAddr(host: String): Array[InetAddress] = {
    ClusterDnsCache.HostToIp.get(host).map(ip => Array(InetAddress.getByName(ip))).getOrElse {
      throw new UnknownHostException(s"Unknown address: $host")
    }
  }
}

class JupyterNameServiceDescriptor extends NameServiceDescriptor {
  override def createNameService(): NameService = new JupyterNameService
  override def getProviderName: String = "Jupyter"
  override def getType: String = "dns"
}
