package org.broadinstitute.dsde.workbench.leonardo.dao

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.AppName
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.HostReady
import org.broadinstitute.dsde.workbench.leonardo.dns.KubernetesDnsCache
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}

class HttpGalaxyDAO[F[_]: Timer: ContextShift: Concurrent](val kubernetesDnsCache: KubernetesDnsCache[F],
                                                           client: Client[F])
    extends GalaxyDAO[F] {

  def isProxyAvailable(googleProject: GoogleProject, appName: AppName): F[Boolean] =
    Proxy.getAppTargetHost[F](kubernetesDnsCache, googleProject, appName) flatMap {
      case HostReady(targetHost) =>
        client
          .successful(
            Request[F](
              method = Method.GET,
              uri = Uri.unsafeFromString(
                s"https://${targetHost.toString}/proxy/google/v1/apps/${googleProject.value}/${appName.value}/galaxy/"
              )
            )
          )
          .handleError(_ => false)
      case _ => Concurrent[F].pure(false)
    }

  // TODO
  def hasJobsRunning(googleProject: GoogleProject, appName: AppName): F[Boolean] = Concurrent[F].pure(false)
}

trait GalaxyDAO[F[_]] {
  def isProxyAvailable(googleProject: GoogleProject, appName: AppName): F[Boolean]
  def hasJobsRunning(googleProject: GoogleProject, appName: AppName): F[Boolean]
}
