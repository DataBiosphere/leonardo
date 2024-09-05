package org.broadinstitute.dsde.workbench.leonardo.dao.sam

import akka.http.scaladsl.model.StatusCodes
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.api.services.storage.StorageScopes
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.client.sam.ApiException
import org.broadinstitute.dsde.workbench.client.sam.model.{
  AccessPolicyMembershipRequest,
  CreateResourceRequestV2,
  FullyQualifiedResourceId,
  GetOrCreateManagedIdentityRequest
}
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{ProjectSamResourceId, WorkspaceResourceSamResourceId}
import org.broadinstitute.dsde.workbench.leonardo.auth.CloudAuthTokenProvider
import org.broadinstitute.dsde.workbench.leonardo.http._
import org.broadinstitute.dsde.workbench.leonardo.model.LeoInternalServerError
import org.broadinstitute.dsde.workbench.leonardo.{AppContext, SamResourceId, SamResourceType, SamRole, WorkspaceId}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.http4s.AuthScheme
import org.http4s.Credentials.Token
import org.typelevel.log4cats.StructuredLogger

import java.util.UUID
import scala.jdk.CollectionConverters._

class SamServiceInterp[F[_]](apiClientProvider: SamApiClientProvider[F],
                             cloudAuthTokenProvider: CloudAuthTokenProvider[F]
)(implicit
  F: Async[F],
  metrics: OpenTelemetryMetrics[F],
  logger: StructuredLogger[F]
) extends SamService[F] {

  private val saScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    StorageScopes.DEVSTORAGE_READ_ONLY
  )

  override def getPetServiceAccount(bearerToken: String, googleProject: GoogleProject)(implicit
    ev: Ask[F, AppContext]
  ): F[WorkbenchEmail] =
    for {
      ctx <- ev.ask
      googleApi <- apiClientProvider.googleApi(bearerToken)
      userEmail <- getUserEmail(bearerToken)
      pet <- SamRetry.retry(googleApi.getPetServiceAccount(googleProject.value), "getPetServiceAccount").adaptError {
        case e: ApiException =>
          SamException.create("Error getting pet service account from Sam", e, ctx.traceId)
      }
      _ <- logger.info(ctx.loggingCtx)(
        s"Retrieved pet service account $pet for user $userEmail in project $googleProject"
      )
    } yield WorkbenchEmail(pet)

  override def getPetManagedIdentity(bearerToken: String, azureCloudContext: AzureCloudContext)(implicit
    ev: Ask[F, AppContext]
  ): F[WorkbenchEmail] =
    for {
      ctx <- ev.ask
      azureApi <- apiClientProvider.azureApi(bearerToken)
      userEmail <- getUserEmail(bearerToken)
      pet <- SamRetry
        .retry(
          azureApi.getPetManagedIdentity(
            new GetOrCreateManagedIdentityRequest()
              .tenantId(azureCloudContext.tenantId.value)
              .subscriptionId(azureCloudContext.subscriptionId.value)
              .managedResourceGroupName(azureCloudContext.managedResourceGroupName.value)
          ),
          "getPetManagedIdentity"
        )
        .adaptError { case e: ApiException =>
          SamException.create("Error getting pet managed identity from Sam", e, ctx.traceId)
        }
      _ <- logger.info(ctx.loggingCtx)(
        s"Retrieved pet managed identity $pet for user $userEmail in Azure cloud context ${azureCloudContext.asString}"
      )
    } yield WorkbenchEmail(pet)

  override def getProxyGroup(
    userEmail: WorkbenchEmail
  )(implicit ev: Ask[F, AppContext]): F[WorkbenchEmail] =
    for {
      ctx <- ev.ask
      leoToken <- getLeoAuthToken
      googleApi <- apiClientProvider.googleApi(leoToken)
      proxy <- SamRetry.retry(googleApi.getProxyGroup(userEmail.value), "getProxyGroup").adaptError {
        case e: ApiException =>
          SamException.create("Error getting proxy group from Sam", e, ctx.traceId)
      }
      _ <- logger.info(ctx.loggingCtx)(s"Retrieved proxy group $proxy for user $userEmail")
    } yield WorkbenchEmail(proxy)

  override def getPetServiceAccountToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[F, AppContext]
  ): F[String] = for {
    ctx <- ev.ask
    leoToken <- getLeoAuthToken
    googleApi <- apiClientProvider.googleApi(leoToken)
    petToken <- SamRetry
      .retry(googleApi.getUserPetServiceAccountToken(googleProject.value, userEmail.value, saScopes.asJava),
             "getUserPetServiceAccountToken"
      )
      .adaptError { case e: ApiException =>
        SamException.create("Error getting pet service account token from Sam", e, ctx.traceId)
      }
    _ <- logger.info(ctx.loggingCtx)(
      s"Retrieved pet service account token for user $userEmail in project $googleProject"
    )
  } yield petToken

  override def lookupWorkspaceParentForGoogleProject(bearerToken: String, googleProject: GoogleProject)(implicit
    ev: Ask[F, AppContext]
  ): F[Option[WorkspaceId]] = for {
    ctx <- ev.ask

    // Get resource parent from Sam
    resourcesApi <- apiClientProvider.resourcesApi(bearerToken)
    parent <- SamRetry
      .retry(resourcesApi.getResourceParent(SamResourceType.Project.asString, googleProject.value), "getResourceParent")
      .attempt

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

  private def isAuthorized(bearerToken: String, samResourceId: SamResourceId, action: String)(implicit
    ev: Ask[F, AppContext]
  ): F[Boolean] =
    for {
      ctx <- ev.ask
      resourcesApi <- apiClientProvider.resourcesApi(bearerToken)
      isAuthorized <- SamRetry
        .retry(
          resourcesApi.resourcePermissionV2(samResourceId.resourceType.asString, samResourceId.resourceId, action),
          "resourcePermissionV2"
        )
        .adaptError { case e: ApiException =>
          SamException.create("Error checking resource permission in Sam", e, ctx.traceId)
        }

    } yield isAuthorized

  override def checkAuthz(bearerToken: String, samResourceId: SamResourceId, action: String)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    isAuthorized <- isAuthorized(bearerToken, samResourceId, action)
    userEmail <- getUserEmail(bearerToken)
    _ <- F.raiseWhen(!isAuthorized)(
      SamException.create(
        s"User $userEmail is not authorized to perform action $action on ${samResourceId.resourceType} ${samResourceId.resourceId}",
        StatusCodes.Forbidden.intValue,
        ctx.traceId
      )
    )
    _ <- logger.info(ctx.loggingCtx)(
      s"User $userEmail is authorized to $action ${samResourceId.resourceType} ${samResourceId.resourceId}"
    )
  } yield ()

  override def listResources(bearerToken: String, samResourceType: SamResourceType)(implicit
    ev: Ask[F, AppContext]
  ): F[List[String]] = for {
    ctx <- ev.ask
    resourcesApi <- apiClientProvider.resourcesApi(bearerToken)
    resources <- SamRetry
      .retry(resourcesApi.listResourcesAndPoliciesV2(samResourceType.asString), "listResourcesAndPoliciesV2")
      .adaptError { case e: ApiException =>
        SamException.create("Error listing resources from Sam", e, ctx.traceId)
      }
  } yield resources.asScala.toList.map(_.getResourceId)

  override def createResource(bearerToken: String,
                              samResourceId: SamResourceId,
                              projectParent: Option[GoogleProject],
                              workspaceParent: Option[WorkspaceId],
                              creator: Option[WorkbenchEmail]
  )(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] =
    for {
      ctx <- ev.ask
      resourcesApi <- apiClientProvider.resourcesApi(bearerToken)

      // Parent must be google project or workspace, but not both
      parent <- (projectParent, workspaceParent) match {
        case (Some(project), None)   => F.pure(ProjectSamResourceId(project))
        case (None, Some(workspace)) => F.pure(WorkspaceResourceSamResourceId(workspace))
        case _ =>
          F.raiseError(
            LeoInternalServerError(
              "Internal error creating Sam resource: google project or workspace parent is required",
              Some(ctx.traceId)
            )
          )
      }

      // All Leo resources have a creator role
      policies = creator
        .map(c =>
          Map(
            SamRole.Creator.asString -> new AccessPolicyMembershipRequest()
              .addMemberEmailsItem(c.value)
              .addRolesItem(SamRole.Creator.asString)
          )
        )
        .getOrElse(Map.empty)

      body = new CreateResourceRequestV2()
        .resourceId(samResourceId.resourceId)
        .parent(
          new FullyQualifiedResourceId().resourceTypeName(parent.resourceType.asString).resourceId(parent.resourceId)
        )
        .policies(policies.asJava)
      _ <- SamRetry
        .retry(resourcesApi.createResourceV2(samResourceId.resourceType.asString, body), "createResourceV2")
        .adaptError { case e: ApiException =>
          SamException.create(s"Error creating ${samResourceId.resourceType.asString} resource in Sam", e, ctx.traceId)
        }
      _ <- logger.info(ctx.loggingCtx)(
        s"Created Sam resource for ${samResourceId.resourceType.asString} ${samResourceId.resourceId}"
      )
    } yield ()

  override def deleteResource(bearerToken: String, samResourceId: SamResourceId)(implicit
    ev: Ask[F, AppContext]
  ): F[Unit] = for {
    ctx <- ev.ask
    resourcesApi <- apiClientProvider.resourcesApi(bearerToken)
    _ <- SamRetry
      .retry(resourcesApi.deleteResourceV2(samResourceId.resourceType.asString, samResourceId.resourceId),
             "deleteResourceV2"
      )
      .adaptError { case e: ApiException =>
        SamException.create(s"Error deleting ${samResourceId.resourceType.asString} resource in Sam", e, ctx.traceId)
      }
    _ <- logger.info(ctx.loggingCtx)(
      s"Deleted Sam resource for ${samResourceId.resourceType.asString} ${samResourceId.resourceId}"
    )
  } yield ()

  override def getUserEmail(bearerToken: String)(implicit ev: Ask[F, AppContext]): F[WorkbenchEmail] =
    for {
      ctx <- ev.ask
      usersApi <- apiClientProvider.usersApi(bearerToken)
      userStatus <- SamRetry.retry(usersApi.getUserStatusInfo(), "getUserStatusInfo").adaptError {
        case e: ApiException =>
          SamException.create(s"Error getting user status info from Sam", e, ctx.traceId)
      }
    } yield WorkbenchEmail(userStatus.getUserEmail)
}
