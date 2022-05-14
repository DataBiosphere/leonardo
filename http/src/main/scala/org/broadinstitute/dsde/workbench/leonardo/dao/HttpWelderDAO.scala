package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.Async
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.dao.HostStatus.HostReady
import org.broadinstitute.dsde.workbench.leonardo.dns.RuntimeDnsCache
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}
import org.typelevel.log4cats.Logger

class HttpWelderDAO[F[_]: Async: Logger](
  val runtimeDnsCache: RuntimeDnsCache[F],
  client: Client[F]
)(implicit
  metrics: OpenTelemetryMetrics[F]
) extends WelderDAO[F] {

  def flushCache(cloudContext: CloudContext, runtimeName: RuntimeName): F[Unit] =
    for {
      host <- Proxy.getRuntimeTargetHost(runtimeDnsCache, cloudContext, runtimeName)
      res <- host match {
        case HostReady(targetHost) =>
          client.successful(
            Request[F](
              method = Method.POST,
              uri = Uri.unsafeFromString(
                s"https://${targetHost.address}/proxy/${cloudContext.asString}/${runtimeName.asString}/welder/cache/flush"
              )
            )
          )
        case x =>
          Logger[F]
            .error(
              s"fail to get target host name for welder for ${cloudContext.asStringWithProvider}/${runtimeName.asString} when trying to flush cache. Host status ${x}"
            )
            .as(false)
      }
      _ <-
        if (res)
          metrics.incrementCounter("welder/flushcache", tags = Map("result" -> "success"))
        else
          metrics.incrementCounter("welder/flushcache", tags = Map("result" -> "failure"))
    } yield ()

  def isProxyAvailable(cloudContext: CloudContext, runtimeName: RuntimeName): F[Boolean] =
    for {
      host <- Proxy.getRuntimeTargetHost(runtimeDnsCache, cloudContext, runtimeName)
      res <- host match {
        case HostReady(targetHost) =>
          client
            .successful(
              Request[F](
                method = Method.GET,
                uri = Uri.unsafeFromString(
                  s"https://${targetHost.address}/proxy/${cloudContext.asString}/${runtimeName.asString}/welder/status"
                )
              )
            )
            .handleError(_ => false)
        case x =>
          Logger[F]
            .error(
              s"fail to get target host name for welder for ${cloudContext.asString}/${runtimeName.asString}. Host status ${x}"
            )
            .as(false)
      }
      _ <-
        if (res) {
          metrics.incrementCounter("welder/status", tags = Map("result" -> "success"))
        } else
          metrics.incrementCounter("welder/status", tags = Map("result" -> "failure"))
    } yield res
}

trait WelderDAO[F[_]] {
  def flushCache(cloudContext: CloudContext, runtimeName: RuntimeName): F[Unit]
  def isProxyAvailable(cloudContext: CloudContext, runtimeName: RuntimeName): F[Boolean]
}
