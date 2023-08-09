package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.Async
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.HostReady
import org.broadinstitute.dsde.workbench.leonardo.dns.KubernetesDnsCache
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}

class HttpAppDAO[F[_]: Async](val kubernetesDnsCache: KubernetesDnsCache[F], client: Client[F]) extends AppDAO[F] {

  def isProxyAvailable(googleProject: GoogleProject, appName: AppName, serviceName: ServiceName): F[Boolean] =
    Proxy.getAppTargetHost[F](kubernetesDnsCache, CloudContext.Gcp(googleProject), appName) flatMap {
      case HostReady(targetHost, _, _) =>
        val serviceUrl = serviceName match {
          case ServiceName("welder-service") =>
            s"https://${targetHost.address}/proxy/google/v1/apps/${googleProject.value}/${appName.value}/${serviceName.value}/status/"
          case _ =>
            s"https://${targetHost.address}/proxy/google/v1/apps/${googleProject.value}/${appName.value}/${serviceName.value}/"
        }
        client
          .successful(
            Request[F](
              method = Method.GET,
              uri = Uri.unsafeFromString(serviceUrl)
            )
          )
          .handleError(_ => false)
      case _ => Async[F].pure(false) // Update once we support Relay for apps
    }
}

trait AppDAO[F[_]] {
  def isProxyAvailable(googleProject: GoogleProject, appName: AppName, serviceName: ServiceName): F[Boolean]
}
