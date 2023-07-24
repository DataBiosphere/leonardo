package org.broadinstitute.dsde.workbench.leonardo
package http
package service

import cats.Parallel
import cats.effect.Async
import org.broadinstitute.dsde.workbench.leonardo.config.KubernetesAppConfig
import org.broadinstitute.dsde.workbench.leonardo.db.KubernetesServiceDbQueries
import org.broadinstitute.dsde.workbench.leonardo.model.NoMatchingAppError
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage.UpdateAppMessage
import cats.effect.std.Queue
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.leonardo.db.DbReference
import org.broadinstitute.dsde.workbench.leonardo.model.{LeoAuthProvider, NotAnAdminError}
import org.broadinstitute.dsde.workbench.leonardo.monitor.LeoPubsubMessage
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.typelevel.log4cats.StructuredLogger

import scala.concurrent.ExecutionContext

final class AdminServiceInterp[F[_] : Parallel](authProvider: LeoAuthProvider[F],
                                                publisherQueue: Queue[F, LeoPubsubMessage],
                                                adminAppConfig: AdminAppConfig
                                               )(implicit
                                                 F: Async[F],
                                                 log: StructuredLogger[F],
                                                 dbReference: DbReference[F],
                                                 ec: ExecutionContext
                                               ) extends AdminService[F] {

  def updateApps(
                  userInfo: UserInfo,
                  req: UpdateAppsRequest
                )(implicit as: Ask[F, AppContext]): F[Vector[ListUpdateableAppResponse]] = {
    for {
      ctx: AppContext <- as.ask

      // Ensure the user is a Terra admin
      hasPermission: Boolean <- authProvider.isAdminUser(userInfo)
      _ <- F.raiseWhen(!hasPermission)(NotAnAdminError(userInfo.userEmail, Option(ctx.traceId)))

      // Find the config relevant to this cloud and app type
      appConfig: KubernetesAppConfig <- adminAppConfig.configForTypeAndCloud(req.appType, req.cloudProvider) match {
        case Some(conf) => F.pure(conf)
        case None => F.raiseError(NoMatchingAppError(req.appType, req.cloudProvider, Option(ctx.traceId)))
      }

      // Query the database for the collection of updateable apps matching filters
      excludedVersions = (appConfig.chartVersionsToExcludeFromUpdates ++ req.appVersionsExclude).distinct
      matchingApps <- KubernetesServiceDbQueries.listAppsForUpdate(appConfig.chart,
        req.appType,
        req.cloudProvider,
        req.appVersionsInclude.map(Chart(appConfig.chartName, _)),
        excludedVersions.map(Chart(appConfig.chartName, _)),
        req.googleProject,
        req.workspaceId,
        req.appNames).transaction
      responseList = ListUpdateableAppResponse.fromClusters(matchingApps).toVector

      // If not a dry run, enqueue messages requesting app update.
      _ <- {
        if (req.dryRun)
          F.unit
        else {
          val appNames = responseList.map(_.appName.value).mkString(", ")
          log.info(s"Triggering update of ${responseList.length} apps of type ${req.cloudProvider}/${req.appType}: ${appNames}")
          responseList
            .map(makeUpdateAppMessage(_, ctx.traceId))
            .map(publisherQueue.offer)
            .traverse(identity)
        }
      }
    } yield responseList
  }

  private def makeUpdateAppMessage(updateableApp: ListUpdateableAppResponse, traceId: TraceId): UpdateAppMessage =
    UpdateAppMessage(updateableApp.appId,
      updateableApp.appName,
      updateableApp.cloudContext,
      updateableApp.workspaceId,
      updateableApp.cloudContext match {
        case CloudContext.Gcp(googleProject) => Option(googleProject)
        case _ => None
      },
      Option(traceId)
    )
}
