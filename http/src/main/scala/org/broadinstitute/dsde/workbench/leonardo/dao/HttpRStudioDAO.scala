package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.RuntimeName
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache.HostReady
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}

class HttpRStudioDAO[F[_]: Timer: ContextShift: Concurrent](val clusterDnsCache: ClusterDnsCache[F], client: Client[F])
    extends RStudioDAO[F]
    with LazyLogging {
  def isProxyAvailable(googleProject: GoogleProject, runtimeName: RuntimeName): F[Boolean] =
    Proxy.getTargetHost[F](clusterDnsCache, googleProject, runtimeName) flatMap {
      case HostReady(targetHost) =>
        client.successful(
          Request[F](
            method = Method.GET,
            uri = Uri.unsafeFromString(
              s"https://${targetHost.toString}/proxy/$googleProject/$runtimeName/rstudio/"
            )
          )
        )
      case _ => Concurrent[F].pure(false)
    }
}

trait RStudioDAO[F[_]] {
  def isProxyAvailable(googleProject: GoogleProject, runtimeName: RuntimeName): F[Boolean]
}
