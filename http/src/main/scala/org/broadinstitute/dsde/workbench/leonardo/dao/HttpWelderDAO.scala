package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache
import org.broadinstitute.dsde.workbench.leonardo.dns.ClusterDnsCache._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterName
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.newrelic.NewRelicMetrics
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}

class HttpWelderDAO[F[_]: Concurrent: Timer: ContextShift: Logger: NewRelicMetrics](val clusterDnsCache: ClusterDnsCache[F], client: Client[F])(
  implicit metrics: NewRelicMetrics[F]
) extends WelderDAO[F] {

  def flushCache(googleProject: GoogleProject, clusterName: ClusterName): F[Unit] =
    for {
      host <- Proxy.getTargetHost(clusterDnsCache, googleProject, clusterName)
      res <- host match {
        case HostReady(targetHost) =>
          client.successful(
            Request[F](
              method = Method.POST,
              uri = Uri.unsafeFromString(
                s"https://${targetHost.toString}/proxy/$googleProject/$clusterName/welder/cache/flush"
              )
            )
          )
        case _ =>
          Logger[F].error(s"fail to get target host name for welder for ${googleProject}/${clusterName}").as(false)
      }
      _ <- if (res)
        metrics.incrementCounter("welder/flushcache/success")
      else
        metrics.incrementCounter("welder/flushcache/failure")
    } yield ()

  def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): F[Boolean] =
    {
      for {
        host <- Proxy.getTargetHost(clusterDnsCache, googleProject, clusterName)
        res <- host match {
          case HostReady(targetHost) =>
            client.successful(
              Request[F](
                method = Method.GET,
                uri =
                  Uri.unsafeFromString(s"https://${targetHost.toString}/proxy/$googleProject/$clusterName/welder/status")
              )
            )
          case _ =>
            Logger[F].error(s"fail to get target host name for welder for ${googleProject}/${clusterName}").as(false)
        }
        _ <- if (res)
          metrics.incrementCounter("welder/status/success")
        else
          metrics.incrementCounter("welder/status/failure")
      } yield res
    }
}

trait WelderDAO[F[_]] {
  def flushCache(googleProject: GoogleProject, clusterName: ClusterName): F[Unit]
  def isProxyAvailable(googleProject: GoogleProject, clusterName: ClusterName): F[Boolean]
}
