package org.broadinstitute.dsde.workbench.leonardo
package monitor

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import fs2.Stream
import org.broadinstitute.dsde.workbench.google2.KubernetesSerializableName.ServiceName
import org.broadinstitute.dsde.workbench.leonardo.config.Config
import org.broadinstitute.dsde.workbench.leonardo.dao.{AppDAO, SamDAO, WdsDAO}
import org.broadinstitute.dsde.workbench.leonardo.db.{DbReference, KubernetesServiceDbQueries}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger
import org.broadinstitute.dsde.workbench.leonardo.db._
import org.broadinstitute.dsde.workbench.leonardo.http.dbioToIO
import org.broadinstitute.dsde.workbench.leonardo.util.AppCreationException
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.http4s.{AuthScheme, Credentials, Uri}
import org.http4s.headers.Authorization
import org.broadinstitute.dsde.workbench.leonardo.http._

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class AppHealthMonitor[F[_]](config: AppHealthMonitorConfig, appDAO: AppDAO[F], wdsDAO: WdsDAO[F], samDAO: SamDAO[F])(
  implicit
  F: Async[F],
  dbRef: DbReference[F],
  metrics: OpenTelemetryMetrics[F],
  logger: StructuredLogger[F],
  ec: ExecutionContext
) {

  val process: Stream[F, Unit] =
    (Stream.sleep[F](config.checkInterval) ++ Stream.eval(
      healthCheck
        .handleErrorWith(e => logger.error(e)("Unexpected error occurred during app health monitoring"))
    )).repeat

  private[monitor] def healthCheck: F[Unit] =
    for {
      now <- F.realTimeInstant
      traceId = TraceId(s"AppHealthMonitor_${now.toEpochMilli}")
      implicit0(appContext: Ask[F, AppContext]) = Ask.const[F, AppContext](AppContext(traceId, now))
      clusters <- KubernetesServiceDbQueries.listAppsForHealthCheck.transaction
      _ <- recordAppDbStatus(clusters)

    } yield ()

  private[monitor] def recordAppDbStatus(
    allClusters: List[KubernetesCluster]
  )(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val allApps = for {
      c <- allClusters
      n <- c.nodepools
      a <- n.apps
    } yield Map((c.cloudContext.cloudProvider, a.appType, a.status) -> 1)
    allApps.combineAll.toList.traverse_ { case ((cloudProvider, appType, status), count) =>
      metrics.gauge(
        "appDbStatus",
        count,
        Map("cloudProvider" -> cloudProvider.asString, "appType" -> appType.toString, "status" -> status.toString)
      )
    }
  }

  private[monitor] def recordServiceHealth(
    allClusters: List[KubernetesCluster]
  )(implicit ev: Ask[F, AppContext]): F[Unit] = {
    val allServicesByUser = (for {
      c <- allClusters if c.asyncFields.isDefined
      n <- c.nodepools
      a <- n.apps if a.status == AppStatus.Running
      (service, _) <- a.getProxyUrls(c, Config.proxyConfig.proxyUrlBase).toList
    } yield Map(
      a.auditInfo.creator -> List((c.cloudContext, c.asyncFields.get.loadBalancerIp, a.appName, service))
    )).combineAll

    services.traverse_ { case (cloudContext, baseUri, appName, userEmail, serviceName, url) =>
      for {
        ctx <- ev.ask
        tokenOpt <- samDAO.getCachedArbitraryPetAccessToken(userEmail)
        token <- F.fromOption(tokenOpt, AppCreationException(s"Pet not found for user ${userEmail}", Some(ctx.traceId)))
        authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))
        isUp <- (cloudContext, baseUri, serviceName) match {
          case (CloudContext.Gcp(project), _, s) => appDAO.isProxyAvailable(project, appName, s)
          case (CloudContext.Azure(_), Some(relayEndpoint), ServiceName("wds")) =>
            val relayPath = Uri.unsafeFromString(relayEndpoint) / appName.value
            wdsDAO.getStatus(relayPath, authHeader)
        }
      } yield ()
    }

//    val services = allClusters.flatMap(c =>
//      c.nodepools.flatMap(_.apps.filter(_.status == AppStatus.Running).flatMap(_.getProxyUrls())
//    )
//    val x = apps.traverse(a => a._1.getProxyUrls())
  }

  // metrics:
  // (cloud, apptype, dbstatus) -> count

  // (cloud, apptype, healthstatus) -> count

}

case class AppHealthMonitorConfig(checkInterval: FiniteDuration)
