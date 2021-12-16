package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.Async
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.HostReady
import org.broadinstitute.dsde.workbench.leonardo.dns.RuntimeDnsCache
import org.broadinstitute.dsde.workbench.leonardo.{CloudContext, RuntimeName}
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}

class HttpRStudioDAO[F[_]: Async](val runtimeDnsCache: RuntimeDnsCache[F], client: Client[F])
    extends RStudioDAO[F]
    with LazyLogging {
  def isProxyAvailable(cloudContext: CloudContext, runtimeName: RuntimeName): F[Boolean] =
    Proxy.getRuntimeTargetHost[F](runtimeDnsCache, cloudContext, runtimeName) flatMap {
      case HostReady(targetHost) =>
        client
          .successful(
            Request[F](
              method = Method.GET,
              uri = Uri.unsafeFromString(
                s"https://${targetHost.address}/proxy/${cloudContext.asString}/${runtimeName.asString}/rstudio/"
              )
            )
          )
          .handleError(_ => false)
      case _ => Async[F].pure(false)
    }
}

trait RStudioDAO[F[_]] {
  def isProxyAvailable(cloudContext: CloudContext, runtimeName: RuntimeName): F[Boolean]
}
