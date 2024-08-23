package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.client.sam.ApiException
import org.broadinstitute.dsde.workbench.client.sam.model.GetOrCreateManagedIdentityRequest
import org.broadinstitute.dsde.workbench.leonardo.auth.CloudAuthTokenProvider
import org.broadinstitute.dsde.workbench.leonardo.model.LeoInternalServerError
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, SamResourceType, WorkspaceId}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s.AuthScheme
import org.http4s.Credentials.Token
import org.typelevel.log4cats.StructuredLogger

import java.util.UUID

class SamServiceInterp[F[_]](apiClientProvider: SamApiClientProvider[F],
                             cloudAuthTokenProvider: CloudAuthTokenProvider[F]
)(implicit
  F: Async[F],
  metrics: OpenTelemetryMetrics[F],
  logger: StructuredLogger[F]
) extends SamService[F] {

  // TODO: pattern for retries

  override def getPetServiceAccount(userInfo: UserInfo, googleProject: GoogleProject)(implicit
    ev: Ask[F, AppContext]
  ): F[WorkbenchEmail] =
    for {
      ctx <- ev.ask
      googleApi <- apiClientProvider.googleApi(userInfo.accessToken.token)
      pet <- F.blocking(googleApi.getPetServiceAccount(googleProject.value)).adaptError { case e: ApiException =>
        SamException.create("Error getting pet service account from Sam", e, ctx.traceId)
      }
    } yield WorkbenchEmail(pet)

  override def getPetManagedIdentity(userInfo: UserInfo, azureCloudContext: AzureCloudContext)(implicit
    ev: Ask[F, AppContext]
  ): F[WorkbenchEmail] =
    for {
      ctx <- ev.ask
      azureApi <- apiClientProvider.azureApi(userInfo.accessToken.token)
      pet <- F
        .blocking(
          azureApi.getPetManagedIdentity(
            new GetOrCreateManagedIdentityRequest()
              .tenantId(azureCloudContext.tenantId.value)
              .subscriptionId(azureCloudContext.subscriptionId.value)
              .managedResourceGroupName(azureCloudContext.managedResourceGroupName.value)
          )
        )
        .adaptError { case e: ApiException =>
          SamException.create("Error getting pet managed identity from Sam", e, ctx.traceId)
        }
    } yield WorkbenchEmail(pet)

  override def getProxyGroup(
    userEmail: WorkbenchEmail
  )(implicit ev: Ask[F, AppContext]): F[WorkbenchEmail] =
    for {
      ctx <- ev.ask
      leoToken <- getLeoAuthToken
      googleApi <- apiClientProvider.googleApi(leoToken)
      proxy <- F.blocking(googleApi.getProxyGroup(userEmail.value)).adaptError { case e: ApiException =>
        SamException.create("Error getting proxy group from Sam", e, ctx.traceId)
      }
    } yield WorkbenchEmail(proxy)

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

  private def getLeoAuthToken(implicit ev: Ask[F, AppContext]): F[String] =
    for {
      ctx <- ev.ask
      authorization <- cloudAuthTokenProvider.getAuthToken
      token <- authorization.credentials match {
        case Token(AuthScheme.Bearer, token) => F.pure(token)
        case _ =>
          F.raiseError(LeoInternalServerError("Internal server error retrieving Leo credentials", Some(ctx.traceId)))
      }
    } yield token
}
