package org.broadinstitute.dsde.workbench.leonardo
package dao

import akka.http.scaladsl.model.Uri.Host
import cats.effect.Async
import org.broadinstitute.dsde.workbench.leonardo.dns._
import org.http4s.Uri

sealed trait HostStatus extends Product with Serializable
object HostStatus {
  final case object HostNotFound extends HostStatus
  final case object HostNotReady extends HostStatus
  final case object HostPaused extends HostStatus
  final case class HostReady(hostname: Host, path: String, cloudProvider: CloudProvider) extends HostStatus {
    def toUri: Uri =
      cloudProvider match {
        case CloudProvider.Gcp =>
          Uri.unsafeFromString(s"https://${hostname.address()}/proxy/${path}")
        case CloudProvider.Azure =>
          azureUri
      }

    def toNotebooksUri: Uri =
      cloudProvider match {
        case CloudProvider.Gcp =>
          Uri.unsafeFromString(s"https://${hostname.address()}/notebooks/${path}")
        case CloudProvider.Azure =>
          azureUri
      }

    private def azureUri: Uri = Uri.unsafeFromString(s"https://${hostname.address()}/${path}")
  }
}

object Proxy {

  def getRuntimeTargetHost[F[_]: Async](runtimeDnsCache: RuntimeDnsCache[F],
                                        cloudContext: CloudContext,
                                        runtimeName: RuntimeName
  ): F[HostStatus] =
    runtimeDnsCache
      .getHostStatus(RuntimeDnsCacheKey(cloudContext, runtimeName))

  def getAppTargetHost[F[_]: Async](kubernetesDnsCache: KubernetesDnsCache[F],
                                    cloudContext: CloudContext,
                                    appName: AppName
  ): F[HostStatus] =
    kubernetesDnsCache.getHostStatus(KubernetesDnsCacheKey(cloudContext, appName))
}
