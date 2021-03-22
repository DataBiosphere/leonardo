package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo.RuntimeName
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.HostReady
import org.broadinstitute.dsde.workbench.leonardo.dns.RuntimeDnsCache
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}

class HttpRStudioDAO[F[_]: Timer: ContextShift: Concurrent](val runtimeDnsCache: RuntimeDnsCache[F], client: Client[F])(
  ) extends RStudioDAO[F]
    with LazyLogging {
  def isProxyAvailable(googleProject: GoogleProject, runtimeName: RuntimeName): F[Boolean] =
    Proxy.getRuntimeTargetHost[F](runtimeDnsCache, googleProject, runtimeName) flatMap {
      case HostReady(targetHost) =>
        client
          .successful(
            Request[F](
              method = Method.GET,
              uri = Uri.unsafeFromString(
                s"https://${targetHost.toString}/proxy/${googleProject.value}/${runtimeName.asString}/rstudio/"
              )
            )
          )
          .handleError(_ => false)

      case _ =>
        Concurrent[F].pure(false)

    }

}

trait RStudioDAO[F[_]] {
  def isProxyAvailable(googleProject: GoogleProject, runtimeName: RuntimeName): F[Boolean]
}
