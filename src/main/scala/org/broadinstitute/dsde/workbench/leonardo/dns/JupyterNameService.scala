package org.broadinstitute.dsde.workbench.leonardo.dns

import java.net.{InetAddress, UnknownHostException}

import akka.http.scaladsl.model.Uri.Host
import com.typesafe.scalalogging.LazyLogging
import sun.net.spi.nameservice.{NameService, NameServiceDescriptor}

/**
  * Created by rtitle on 8/25/17.
  */
class JupyterNameService extends NameService with LazyLogging {

  override def getHostByAddr(addr: Array[Byte]): String = {
    // Looking up IP -> hostname is not needed for the Leo use case
    throw new UnknownHostException
  }

  override def lookupAllHostAddr(host: String): Array[InetAddress] = {
    logger.info("Looking up ip for " + host)
    ClusterDnsCache.HostToIp.get(Host(host)).map { ip =>
      logger.info("got ip " + ip.value)
      Array(InetAddress.getByName(ip.value))
    }.getOrElse {
      logger.info("Couldn't get ip")
      throw new UnknownHostException(s"Unknown address: $host")
    }
  }
}

class JupyterNameServiceDescriptor extends NameServiceDescriptor {
  override def createNameService(): NameService = new JupyterNameService
  override def getProviderName: String = "Jupyter"
  override def getType: String = "dns"
}
