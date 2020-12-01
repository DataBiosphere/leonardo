package org.broadinstitute.dsde.workbench.leonardo
package dao

import akka.http.scaladsl.model.Uri.Host
import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Timer}
import org.broadinstitute.dsde.workbench.leonardo.dns._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.duration._

sealed trait HostStatus extends Product with Serializable
object HostStatus {
  final case object HostNotFound extends HostStatus
  final case object HostNotReady extends HostStatus
  final case object HostPaused extends HostStatus
  final case class HostReady(hostname: Host) extends HostStatus
}

object Proxy {
  def getRuntimeTargetHost[F[_]: Timer: ContextShift: Concurrent](runtimeDnsCache: RuntimeDnsCache[F],
                                                                  googleProject: GoogleProject,
                                                                  runtimeName: RuntimeName
  ): F[HostStatus] =
    runtimeDnsCache.getHostStatus(RuntimeDnsCacheKey(googleProject, runtimeName)).timeout(5 seconds)

  def getAppTargetHost[F[_]: Timer: ContextShift: Concurrent](kubernetesDnsCache: KubernetesDnsCache[F],
                                                              googleProject: GoogleProject,
                                                              appName: AppName
  ): F[HostStatus] =
    kubernetesDnsCache.getHostStatus(KubernetesDnsCacheKey(googleProject, appName)).timeout(5 seconds)
}
