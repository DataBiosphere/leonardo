package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, SamResourceType, WorkspaceId}
import org.broadinstitute.dsde.workbench.model.UserInfo
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.typelevel.log4cats.StructuredLogger

import java.util.UUID

class SamServiceInterp[F[_]](apiClientProvider: SamApiClientProvider[F])(implicit
  F: Async[F],
  metrics: OpenTelemetryMetrics[F],
  logger: StructuredLogger[F]
) extends SamService[F] {

  override def lookupWorkspaceParentForGoogleProject(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[WorkspaceId]] = for {
    ctx <- ev.ask

    // Get resource parent from Sam
    resourcesApi <- apiClientProvider.resourcesApi(userInfo.accessToken.token)
    parent <- F.blocking(resourcesApi.getResourceParent(SamResourceType.Project.asString, googleProject.value)).attempt

    // Annotate error cases but don't fail
    workspaceId = parent match {
      case Left(e)     => Left(s"Error retrieving parent resource from Sam: ${e.getMessage}")
      case Right(null) => Left(s"No parent found for google project $googleProject")
      case Right(resource) if resource.getResourceTypeName != SamResourceType.Workspace.asString =>
        Left(s"Unexpected parent resource type ${resource.getResourceTypeName} for google project $googleProject")
      case Right(resource) =>
        Either.catchNonFatal(UUID.fromString(resource.getResourceId)).map(WorkspaceId).leftMap(_.getMessage)
    }

    // Log result and emit metric
    metricName = "lookupWorkspace"
    _ <- workspaceId match {
      case Right(res) =>
        logger.info(ctx.loggingCtx)(s"Populating parent workspace ID $res for google project $googleProject") >> metrics
          .incrementCounter(metricName, tags = Map("succeeded" -> "true"))
      case Left(error) =>
        logger.warn(ctx.loggingCtx)(
          s"Unable to populate workspace ID for google project $googleProject: $error"
        ) >> metrics.incrementCounter(metricName, tags = Map("succeeded" -> "false"))
    }
  } yield workspaceId.toOption
}
