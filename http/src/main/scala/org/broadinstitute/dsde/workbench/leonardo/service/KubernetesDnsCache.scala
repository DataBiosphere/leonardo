package org.broadinstitute.dsde.workbench.leonardo.service

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.model.Uri.Host
import cats.effect.implicits._
import cats.effect.{Blocker, ContextShift, Effect}
import com.google.common.cache.{CacheBuilder, CacheLoader, CacheStats}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.google2.GKEModels.KubernetesClusterId
import org.broadinstitute.dsde.workbench.google2.KubernetesModels.KubernetesApiServerIp
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.AppName
import org.broadinstitute.dsde.workbench.leonardo.config.CacheConfig
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, GetAppResult, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.leonardo.service.KubernetesDnsCache._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject

import scala.concurrent.ExecutionContext

object KubernetesDnsCache {
  sealed trait AppHostStatus
  case object HostAppNotFound extends AppHostStatus
  case object HostServiceNotFound extends AppHostStatus
  case object HostAppNotReady extends AppHostStatus
  case class HostAppReady(hostname: Host, clusterId: KubernetesClusterId) extends AppHostStatus
  case class KubernetesCacheKey(googleProject: GoogleProject, appName: AppName, serviceName: ServiceName)
}

class KubernetesDnsCache[F[_]: Effect: ContextShift](kubernetesProxyConfig: CacheConfig,
                                                  blocker: Blocker)(implicit ec: ExecutionContext, dbRef: DbReference[F])
  extends LazyLogging {

  def getHostStatus(key: KubernetesCacheKey): F[AppHostStatus] =
    blocker.blockOn(Effect[F].delay(projectAppToHostStatus.get(key)))
  def size: Long = projectAppToHostStatus.size
  def stats: CacheStats = projectAppToHostStatus.stats

  private val projectAppToHostStatus = CacheBuilder
    .newBuilder()
    .expireAfterWrite(kubernetesProxyConfig.cacheExpiryTime.toSeconds, TimeUnit.SECONDS)
    .maximumSize(kubernetesProxyConfig.cacheMaxSize)
    .recordStats
    .build(
      new CacheLoader[KubernetesCacheKey, AppHostStatus] {
        def load(key: KubernetesCacheKey): AppHostStatus = {
          logger.debug(s"DNS Cache miss for ${key.googleProject} / ${key.appName} / ${key.serviceName} ...loading from DB...")
          val getApp = dbRef
            .inTransaction {
              KubernetesServiceDbQueries.getActiveFullAppByName(key.googleProject, key.appName)
            }
            .toIO
            .unsafeRunSync()

          getApp match {
            case Some(app) =>
              hostStatusByAppResult(app, key.serviceName)
            case None =>
              HostAppNotFound
          }
        }
      }
    )

  private def readyAppToHost(appResult: GetAppResult, ip: KubernetesApiServerIp, serviceName: ServiceName): AppHostStatus =
    appResult.app.getInternalProxyUrls(ip).get(
      serviceName
    ).fold[AppHostStatus](HostServiceNotFound)(url => HostAppReady(
      Host(url.getPath),
      appResult.cluster.getGkeClusterId
    ))

  private def hostStatusByAppResult(appResult: GetAppResult, serviceName: ServiceName): AppHostStatus =
    appResult.cluster.asyncFields.map(_.apiServerIp)
      .fold[AppHostStatus](HostAppNotReady)(ip =>
        readyAppToHost(appResult, ip, serviceName)
    )

}
