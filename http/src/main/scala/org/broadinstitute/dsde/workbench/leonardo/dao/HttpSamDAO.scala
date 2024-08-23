package org.broadinstitute.dsde.workbench.leonardo
package dao

import _root_.fs2._
import _root_.io.circe._
import _root_.org.typelevel.log4cats.StructuredLogger
import akka.http.scaladsl.model.StatusCode._
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import cats.effect.Async
import cats.mtl.Ask
import cats.syntax.all._
import com.google.api.services.storage.StorageScopes
import com.google.auth.oauth2.ServiceAccountCredentials
import org.broadinstitute.dsde.workbench.azure.AzureCloudContext
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.SamResourceId.{ProjectSamResourceId, WorkspaceResourceSamResourceId}
import org.broadinstitute.dsde.workbench.leonardo.auth.CloudAuthTokenProvider
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, UserInfo, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics
import org.broadinstitute.dsde.workbench.util.health.Subsystems.Subsystem
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus, Subsystems}
import org.http4s._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.{`Content-Type`, Authorization}
import scalacache.Cache

import java.io.ByteArrayInputStream
import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.control.NoStackTrace

/**
 * Deprecated. Functionality should be ported to SamService, which uses the generated Sam client.
 */
@Deprecated
class HttpSamDAO[F[_]](httpClient: Client[F],
                       config: HttpSamDaoConfig,
                       petKeyCache: Cache[F, UserEmailAndProject, Option[Json]],
                       cloudAuthTokenProvider: CloudAuthTokenProvider[F]
)(implicit
  logger: StructuredLogger[F],
  F: Async[F],
  metrics: OpenTelemetryMetrics[F]
) extends SamDAO[F]
    with Http4sClientDsl[F] {

  private val saScopes = Seq(
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    StorageScopes.DEVSTORAGE_READ_ONLY
  )
  override def registerLeo(implicit ev: Ask[F, TraceId]): F[Unit] =
    for {
      leoToken <- getLeoAuthToken
      isRegistered <- httpClient.expectOr[RegisterInfoResponse](
        Request[F](
          method = Method.GET,
          uri = config.samUri.withPath(Uri.Path.unsafeFromString(s"/register/user/v2/self/info")),
          headers = Headers(leoToken)
        )
      )(onError)
      _ <- httpClient
        .successful(
          Request[F](
            method = Method.POST,
            uri = config.samUri.withPath(Uri.Path.unsafeFromString(s"/register/user/v2/self")),
            entity = Entity.strict(Chunk.array("app.terra.bio/#terms-of-service".getBytes(UTF_8)).toByteVector),
            headers = Headers(leoToken)
          )
        )
        .whenA(!isRegistered.enabled)
    } yield ()

  override def getStatus(implicit ev: Ask[F, TraceId]): F[StatusCheckResponse] =
    metrics.incrementCounter("sam/status") >>
      httpClient.expectOr[StatusCheckResponse](
        Request[F](
          method = Method.GET,
          uri = config.samUri.withPath(Uri.Path.unsafeFromString(s"/status"))
        )
      )(onError)

  override private[leonardo] def hasResourcePermissionUnchecked(resourceType: SamResourceType,
                                                                resource: String,
                                                                action: String,
                                                                authHeader: Authorization
  )(implicit
    ev: Ask[F, TraceId]
  ): F[Boolean] =
    for {
      _ <- metrics.incrementCounter(s"sam/hasResourcePermission/${resourceType.asString}/${action}")
      res <- httpClient.expectOr[Boolean](
        Request[F](
          method = Method.GET,
          uri = config.samUri
            .withPath(
              Uri.Path.unsafeFromString(s"/api/resources/v2/${resourceType.asString}/${resource}/action/${action}")
            ),
          headers = Headers(authHeader)
        )
      )(onError)
    } yield res

  override def getListOfResourcePermissions[R, A](resource: R, authHeader: Authorization)(implicit
    sr: SamResourceAction[R, A],
    ev: Ask[F, TraceId]
  ): F[List[A]] = {
    implicit val d = sr.decoder
    metrics.incrementCounter(s"sam/getListOfResourcePermissions/${sr.resourceType(resource).asString}") >>
      httpClient.expectOr[List[A]](
        Request[F](
          method = Method.GET,
          uri = config.samUri
            .withPath(
              Uri.Path.unsafeFromString(
                s"/api/resources/v2/${sr.resourceType(resource).asString}/${sr.resourceIdAsString(resource)}/actions"
              )
            ),
          headers = Headers(authHeader)
        )
      )(onError)
  }

  /** For every role the user is granted on a Sam resource, list it and the resource ID. */
  override def listResourceIdsWithRole[R <: SamResourceId](
    authHeader: Authorization
  )(implicit
    resourceDefinition: SamResource[R],
    resourceIdDecoder: Decoder[R],
    ev: Ask[F, TraceId]
  ): F[List[(R, SamRole)]] =
    for {
      ctx <- ev.ask
      resourceType = resourceDefinition.resourceType.asString
      _ <- metrics.incrementCounter(s"sam/getResourcePolicies/${resourceType}")
      response <- httpClient.expectOr[List[ListResourceRolesItem[R]]](
        Request[F](
          method = Method.GET,
          uri = config.samUri.withPath(Uri.Path.unsafeFromString(s"/api/resources/v2/${resourceType}")),
          headers = Headers(authHeader)
        )
      )(onError)
    } yield response.flatMap(item => item.samRoles.map(role => (item.samResourceId, role)))

  /** @deprecated Prefer listResourceIdsWithRole. */
  @Deprecated
  override def getResourcePolicies[R](
    authHeader: Authorization,
    resourceType: SamResourceType
  )(implicit sr: SamResource[R], decoder: Decoder[R], ev: Ask[F, TraceId]): F[List[(R, SamPolicyName)]] =
    for {
      ctx <- ev.ask
      _ <- metrics.incrementCounter(s"sam/getResourcePolicies/${resourceType.asString}")
      resp <- httpClient.expectOr[List[ListResourceResponse[R]]](
        Request[F](
          method = Method.GET,
          uri = config.samUri.withPath(Uri.Path.unsafeFromString(s"/api/resources/v2/${resourceType.asString}")),
          headers = Headers(authHeader)
        )
      )(onError)
    } yield resp.flatMap(r => r.samPolicyNames.map(pn => (r.samResourceId, pn)))

  override def getResourceRoles(authHeader: Authorization, resourceId: SamResourceId)(implicit
    ev: Ask[F, TraceId]
  ): F[Set[SamRole]] = for {
    ctx <- ev.ask
    _ <- metrics.incrementCounter(s"sam/getRoles/${resourceId.resourceType.asString}")
    resp <- httpClient.expectOr[Set[SamRole]](
      Request[F](
        method = Method.GET,
        uri = config.samUri.withPath(
          Uri.Path.unsafeFromString(
            s"/api/resources/v2/${resourceId.resourceType.asString}/${resourceId.resourceId}/roles"
          )
        ),
        headers = Headers(authHeader)
      )
    )(onError)
  } yield resp

  override def createResourceAsGcpPet[R](resource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit
    sr: SamResource[R],
    ev: Ask[F, TraceId]
  ): F[Unit] =
    for {
      traceId <- ev.ask
      tokenOpt <- getCachedPetAccessToken(creatorEmail, googleProject)
      token <- F.fromOption(tokenOpt,
                            AuthProviderException(traceId,
                                                  s"No pet SA found for ${creatorEmail} in ${googleProject}",
                                                  StatusCodes.Unauthorized
                            )
      )
      authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))
      _ <- createResourceInternal(resource, authHeader)
    } yield ()

  override def createResourceWithUserInfo[R](resource: R, userInfo: UserInfo)(implicit
    sr: SamResource[R],
    ev: Ask[F, TraceId]
  ): F[Unit] =
    createResourceInternal(resource, Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token)))

  private def createResourceInternal[R](resource: R, authHeader: Authorization)(implicit
    sr: SamResource[R],
    ev: Ask[F, TraceId]
  ): F[Unit] =
    for {
      traceId <- ev.ask
      loggingCtx = Map("traceId" -> traceId.asString)
      _ <- logger.info(loggingCtx)(
        s"Creating ${sr.resourceType(resource).asString}/${sr.resourceIdAsString(resource)} resource in Sam"
      )
      _ <- metrics.incrementCounter(s"sam/createResource/${sr.resourceType(resource).asString}")
      _ <- httpClient
        .run(
          Request[F](
            method = Method.POST,
            uri = config.samUri
              .withPath(
                Uri.Path
                  .unsafeFromString(
                    s"/api/resources/v2/${sr.resourceType(resource).asString}/${sr.resourceIdAsString(resource)}"
                  )
              ),
            headers = Headers(authHeader)
          )
        )
        .use { resp =>
          if (resp.status.isSuccess)
            F.unit
          else
            onError(resp).flatMap(F.raiseError[Unit])
        }
    } yield ()

  def createResourceWithGoogleProjectParent[R](resource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit
    sr: SamResource[R],
    encoder: Encoder[R],
    ev: Ask[F, TraceId]
  ): F[Unit] =
    for {
      traceId <- ev.ask
      tokenOpt <- getCachedPetAccessToken(creatorEmail, googleProject)
      token <- F.fromOption(tokenOpt,
                            AuthProviderException(traceId,
                                                  s"No pet SA found for ${creatorEmail} in ${googleProject}",
                                                  StatusCodes.Unauthorized
                            )
      )
      authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))
      loggingCtx = Map("traceId" -> traceId.asString)
      _ <- logger.info(loggingCtx)(
        s"Creating ${sr.resourceType(resource).asString} resource in sam v2 for ${googleProject}/${sr.resourceIdAsString(resource)}"
      )
      _ <- metrics.incrementCounter(s"sam/createResource/${sr.resourceType(resource).asString}")
      policies = Map[SamPolicyName, SamPolicyData](
        SamPolicyName.Creator -> SamPolicyData(List(creatorEmail), List(sr.ownerRoleName(resource)))
      )
      parent = SerializableSamResource(SamResourceType.Project, ProjectSamResourceId(googleProject))
      _ <- httpClient
        .run(
          Request[F](
            method = Method.POST,
            uri = config.samUri
              .withPath(Uri.Path.unsafeFromString(s"/api/resources/v2/${sr.resourceType(resource).asString}")),
            headers = Headers(authHeader, `Content-Type`(MediaType.application.json)),
            entity = CreateSamResourceRequest[R](resource, policies, parent)
          )
        )
        .use { resp =>
          if (resp.status.isSuccess)
            F.unit
          else
            onError(resp).flatMap(F.raiseError[Unit])
        }
    } yield ()

  def createResourceWithWorkspaceParent[R](resource: R,
                                           creatorEmail: WorkbenchEmail,
                                           userInfo: UserInfo,
                                           workspaceId: WorkspaceId
  )(implicit
    sr: SamResource[R],
    encoder: Encoder[R],
    ev: Ask[F, TraceId]
  ): F[Unit] =
    for {
      traceId <- ev.ask
      // We can use getCachedArbitraryPetAccessToken instead of using the userInfo if this ever needs to become asynchronous
      authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
      loggingCtx = Map("traceId" -> traceId.asString)
      _ <- logger.info(loggingCtx)(
        s"Creating ${sr.resourceType(resource).asString} resource in sam v2 for ${workspaceId}/${sr.resourceIdAsString(resource)}"
      )
      _ <- metrics.incrementCounter(s"sam/createResource/${sr.resourceType(resource).asString}")
      policies = sr.resourceType(resource) match {
        case SamResourceType.SharedApp =>
          // SamResourceType.SharedApp (kubernetes-app-shared") inherits all its policies from the parent workspace,
          // so should have no direct roles.
          Map.empty[SamPolicyName, SamPolicyData]
        case _ =>
          // Other types set an explicit ownerRoleName() policy. As of this writing, the only other resource type
          // handled by this case clause is SamResourceType.App ("kubernetes-app"); this is controlled by
          // SamAuthProvider.notifyResourceCreatedV2().
          Map[SamPolicyName, SamPolicyData](
            SamPolicyName.Creator -> SamPolicyData(List(creatorEmail), List(sr.ownerRoleName(resource)))
          )
      }
      parent = SerializableSamResource(SamResourceType.Workspace, WorkspaceResourceSamResourceId(workspaceId))
      _ <- httpClient
        .run(
          Request[F](
            method = Method.POST,
            uri = config.samUri
              .withPath(Uri.Path.unsafeFromString(s"/api/resources/v2/${sr.resourceType(resource).asString}")),
            headers = Headers(authHeader, `Content-Type`(MediaType.application.json)),
            entity = CreateSamResourceRequest[R](resource, policies, parent)
          )
        )
        .use { resp =>
          if (resp.status.isSuccess)
            F.unit
          else
            onError(resp).flatMap(F.raiseError[Unit])
        }
    } yield ()

  override def deleteResourceAsGcpPet[R](resource: R, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit
    sr: SamResource[R],
    ev: Ask[F, TraceId]
  ): F[Unit] =
    for {
      traceId <- ev.ask
      tokenOpt <- getCachedPetAccessToken(creatorEmail, googleProject)
      token <- F.fromOption(tokenOpt,
                            AuthProviderException(traceId,
                                                  s"No pet SA found for ${creatorEmail} in ${googleProject}",
                                                  StatusCodes.Unauthorized
                            )
      )
      authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))
      _ <- deleteResourceInternal(resource, authHeader)
    } yield ()

  override def deleteResourceWithUserInfo[R](resource: R, userInfo: UserInfo)(implicit
    sr: SamResource[R],
    ev: Ask[F, TraceId]
  ): F[Unit] =
    deleteResourceInternal(resource, Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token)))

  def deleteResourceInternal[R](resource: R, authHeader: Authorization)(implicit
    sr: SamResource[R],
    ev: Ask[F, TraceId]
  ): F[Unit] =
    for {
      traceId <- ev.ask
      loggingCtx = Map("traceId" -> traceId.asString)
      _ <- logger.info(loggingCtx)(
        s"Deleting ${sr.resourceType(resource).asString}/${sr.resourceIdAsString(resource)} resource in Sam"
      )
      _ <- metrics.incrementCounter(s"sam/deleteResource/${sr.resourceType(resource).asString}")
      _ <- httpClient
        .run(
          Request[F](
            method = Method.DELETE,
            uri = config.samUri
              .withPath(
                Uri.Path
                  .unsafeFromString(
                    s"/api/resources/v2/${sr.resourceType(resource).asString}/${sr.resourceIdAsString(resource)}"
                  )
              ),
            headers = Headers(authHeader)
          )
        )
        .use { resp =>
          resp.status match {
            case Status.NotFound =>
              logger.info(loggingCtx)(
                s"Fail to delete Sam resource ${sr.resourceType(resource).asString}/${sr.resourceIdAsString(resource)} because it doesn't exist in Sam"
              )
            case s if s.isSuccess => F.unit
            case _                => onError(resp).flatMap(F.raiseError[Unit])
          }
        }
    } yield ()

  override def getPetServiceAccount(authorization: Authorization, googleProject: GoogleProject)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[WorkbenchEmail]] =
    metrics.incrementCounter("sam/getPetServiceAccount") >>
      httpClient.expectOptionOr[WorkbenchEmail](
        Request[F](
          method = Method.GET,
          uri = config.samUri
            .withPath(Uri.Path.unsafeFromString(s"/api/google/v1/user/petServiceAccount/${googleProject.value}")),
          headers = Headers(authorization)
        )
      )(onError)

  override def getPetManagedIdentity(authorization: Authorization, cloudContext: AzureCloudContext)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[WorkbenchEmail]] =
    metrics.incrementCounter("sam/getPetManagedIdentity") >>
      httpClient.expectOptionOr[WorkbenchEmail](
        Request[F](
          method = Method.POST,
          uri = config.samUri
            .withPath(Uri.Path.unsafeFromString(s"/api/azure/v1/user/petManagedIdentity")),
          headers = Headers(authorization, `Content-Type`(MediaType.application.json)),
          entity = cloudContext
        )
      )(onError)

  override def getUserProxy(userEmail: WorkbenchEmail)(implicit ev: Ask[F, TraceId]): F[Option[WorkbenchEmail]] =
    getLeoAuthToken.flatMap { leoToken =>
      metrics.incrementCounter("sam/getUserProxy") >>
        httpClient.expectOptionOr[WorkbenchEmail](
          Request[F](
            method = Method.GET,
            uri = config.samUri
              .withPath(
                Uri.Path
                  .unsafeFromString(s"/api/google/v1/user/proxyGroup/${URLEncoder.encode(userEmail.value, UTF_8.name)}")
              ),
            headers = Headers(leoToken)
          )
        )(onError)
    }

  override def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[String]] =
    for {
      keyOpt <-
        if (config.petCacheEnabled) {
          petKeyCache.cachingF(UserEmailAndProject(userEmail, googleProject))(None)(
            getPetKey(userEmail, googleProject)
          )
        } else {
          getPetKey(userEmail, googleProject)
        }
      token <- keyOpt.traverse(getTokenFromKey)
    } yield token

  override def getCachedArbitraryPetAccessToken(
    userEmail: WorkbenchEmail
  )(implicit ev: Ask[F, TraceId]): F[Option[String]] =
    for {
      keyOpt <-
        if (config.petCacheEnabled) {
          // Cache only by email in the "arbitrary pet" case
          petKeyCache.cachingF(UserEmailAndProject(userEmail, GoogleProject("user-shell-project")))(None)(
            getArbitraryPetKey(userEmail)
          )
        } else {
          getArbitraryPetKey(userEmail)
        }
      token <- keyOpt.traverse(getTokenFromKey)
    } yield token

  override def getUserSubjectId(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[UserSubjectId]] =
    for {
      traceId <- ev.ask
      _ <- metrics.incrementCounter("sam/getSubjectId")
      tokenOpt <- getCachedPetAccessToken(userEmail, googleProject)
      token <- F.fromOption(tokenOpt,
                            AuthProviderException(traceId,
                                                  s"No pet SA found for ${userEmail} in ${googleProject}",
                                                  StatusCodes.Unauthorized
                            )
      )
      userInfo <- getSamUserInfo(token)
    } yield userInfo.map(_.userSubjectId)

  override def getSamUserInfo(token: String)(implicit ev: Ask[F, TraceId]): F[Option[SamUserInfo]] = {
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))
    for {
      resp <- httpClient.expectOptionOr[SamUserInfo](
        Request[F](
          method = Method.GET,
          uri = config.samUri.withPath(Uri.Path.unsafeFromString(s"/register/user/v2/self/info")),
          headers = Headers(authHeader)
        )
      )(onError)
    } yield resp
  }

  override def getLeoAuthToken: F[Authorization] = cloudAuthTokenProvider.getAuthToken

  override def isGroupMembersOrAdmin(groupName: GroupName, workbenchEmail: WorkbenchEmail)(implicit
    ev: Ask[F, TraceId]
  ): F[Boolean] =
    for {
      leoAuth <- getLeoAuthToken
      members <- httpClient.expectOr[List[WorkbenchEmail]](
        Request[F](
          method = Method.GET,
          uri = config.samUri.withPath(Uri.Path.unsafeFromString(s"/api/groups/v1/${groupName.asString}/member")),
          headers = Headers(leoAuth)
        )
      )(onError)
      res <-
        if (members.contains(workbenchEmail))
          F.pure(true)
        else
          for {
            admins <- httpClient.expectOr[List[WorkbenchEmail]](
              Request[F](
                method = Method.GET,
                uri = config.samUri.withPath(Uri.Path.unsafeFromString(s"/api/groups/v1/${groupName.asString}/admin")),
                headers = Headers(leoAuth)
              )
            )(onError)
          } yield admins.contains(workbenchEmail)
    } yield res

  override def isAdminUser(userInfo: UserInfo)(implicit
    ev: Ask[F, TraceId]
  ): F[Boolean] = {
    // Sam's admin endpoints are protected so only admins can access them. Non-admin users get a
    // 403 response. We don't actually care about the content we get in response to our request,
    // we only care about the status code.
    // 200 -> This is an admin user
    // 403 -> The request "succeeded" in telling us this is not an admin user
    // other -> The request failed
    val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, userInfo.accessToken.token))
    for {
      status <- httpClient.status(
        Request[F](
          method = Method.GET,
          uri =
            config.samUri.withPath(Uri.Path.unsafeFromString(s"/api/admin/v1/user/email/${userInfo.userEmail.value}")),
          headers = Headers(authHeader)
        )
      )
      traceId <- ev.ask
      isAdmin <- status match {
        case Status.Ok        => F.pure(true)
        case Status.Forbidden => F.pure(false)
        case _                => F.raiseError(AuthProviderException(traceId, "", status.code))
      }
    } yield isAdmin
  }

  override def getAzureActionManagedIdentity(authHeader: Authorization,
                                             resource: SamResourceId.PrivateAzureStorageAccountSamResourceId,
                                             action: PrivateAzureStorageAccountAction
  )(implicit ev: Ask[F, TraceId]): F[Option[String]] =
    for {
      _ <- metrics.incrementCounter("sam/getActionManagedIdentity")
      resp <- httpClient.expectOptionOr[GetActionManagedIdentityResponse](
        Request[F](
          method = Method.GET,
          uri = config.samUri.withPath(
            Uri.Path.unsafeFromString(
              s"/api/azure/v1/actionManagedIdentity/${resource.resourceType.asString}/${resource.resourceId}/${action.asString}"
            )
          ),
          headers = Headers(authHeader)
        )
      )(onError)
    } yield resp.map(_.objectId)

  private def getPetKey(userEmail: WorkbenchEmail, googleProject: GoogleProject)(implicit
    ev: Ask[F, TraceId]
  ): F[Option[Json]] =
    for {
      leoAuth <- getLeoAuthToken
      _ <- metrics.incrementCounter("sam/getPetServiceAccount")
      // fetch user's pet SA key with leo's authorization token
      userPetKey <- httpClient.expectOptionOr[Json](
        Request[F](
          method = Method.GET,
          uri = config.samUri.withPath(
            Uri.Path.unsafeFromString(
              s"/api/google/v1/petServiceAccount/${googleProject.value}/${URLEncoder.encode(userEmail.value, UTF_8.name)}"
            )
          ),
          headers = Headers(leoAuth)
        )
      )(onError)
    } yield userPetKey

  private def getArbitraryPetKey(userEmail: WorkbenchEmail)(implicit ev: Ask[F, TraceId]): F[Option[Json]] =
    for {
      leoAuth <- getLeoAuthToken
      _ <- metrics.incrementCounter("sam/getArbitraryPetServiceAccount")
      // fetch user's pet SA key with leo's authorization token
      userPetKey <- httpClient.expectOptionOr[Json](
        Request[F](
          method = Method.GET,
          uri = config.samUri.withPath(
            Uri.Path.unsafeFromString(
              s"/api/google/v1/petServiceAccount/${URLEncoder.encode(userEmail.value, UTF_8.name)}/key"
            )
          ),
          headers = Headers(leoAuth)
        )
      )(onError)
    } yield userPetKey

  private def getTokenFromKey(key: Json): F[String] = {
    val keyStream = new ByteArrayInputStream(key.toString().getBytes)
    F.blocking(ServiceAccountCredentials.fromStream(keyStream).createScoped(saScopes.asJava))
      .map(_.refreshAccessToken.getTokenValue)
  }

  private def onError(response: Response[F])(implicit ev: Ask[F, TraceId]): F[Throwable] =
    for {
      traceId <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      loggingCtx = Map("traceId" -> traceId.asString)
      _ <- logger.error(loggingCtx)(s"Sam call failed: $body")
      _ <- metrics.incrementCounter("sam/errorResponse")
    } yield AuthProviderException(traceId, body, response.status.code)
}

object HttpSamDAO {
  def apply[F[_]: Async](
    httpClient: Client[F],
    config: HttpSamDaoConfig,
    petKeyCache: Cache[F, UserEmailAndProject, Option[Json]],
    cloudAuthTokenProvider: CloudAuthTokenProvider[F]
  )(implicit logger: StructuredLogger[F], metrics: OpenTelemetryMetrics[F]): HttpSamDAO[F] =
    new HttpSamDAO[F](httpClient, config, petKeyCache, cloudAuthTokenProvider)

  implicit val samRoleEncoder: Encoder[SamRole] = Encoder.encodeString.contramap(_.asString)
  implicit val projectActionEncoder: Encoder[ProjectAction] = Encoder.encodeString.contramap(_.asString)
  implicit val runtimeActionEncoder: Encoder[RuntimeAction] = Encoder.encodeString.contramap(_.asString)
  implicit val persistentDiskActionEncoder: Encoder[PersistentDiskAction] = Encoder.encodeString.contramap(_.asString)
  implicit val appActionEncoder: Encoder[AppAction] = Encoder.encodeString.contramap(_.asString)
  implicit val wsmAppActionEncoder: Encoder[WsmResourceAction] = Encoder.encodeString.contramap(_.asString)
  implicit val azurRuntimeActionEncoder: Encoder[WorkspaceAction] = Encoder.encodeString.contramap(_.asString)
  implicit val policyDataEncoder: Encoder[SamPolicyData] =
    Encoder.forProduct3("memberEmails", "actions", "roles")(x => (x.memberEmails, List.empty[String], x.roles))
  implicit val samPolicyNameKeyEncoder: KeyEncoder[SamPolicyName] = new KeyEncoder[SamPolicyName] {
    override def apply(p: SamPolicyName): String = p.toString
  }
  implicit val samResourceTypeEncoder: Encoder[SamResourceType] = Encoder.encodeString.contramap(_.asString)
  implicit val samResourceIdEncoder: Encoder[SamResourceId] = Encoder.encodeString.contramap(_.resourceId)

  implicit val samResourceEncoder: Encoder[SerializableSamResource] =
    Encoder.forProduct2("resourceTypeName", "resourceId")(x => (x.resourceTypeName, x.resourceId))

  implicit def createSamResourceRequestEncoder[R: Encoder]: Encoder[CreateSamResourceRequest[R]] =
    Encoder.forProduct5("resourceId", "policies", "authDomain", "returnResource", "parent")(x =>
      (x.samResourceId, x.policies, List.empty[String], x.returnResource, x.parent)
    )

  implicit val getPetManagedIdentityEncoder: Encoder[AzureCloudContext] =
    Encoder.forProduct3("tenantId", "subscriptionId", "managedResourceGroupName")(x =>
      (x.tenantId.value, x.subscriptionId.value, x.managedResourceGroupName.value)
    )

  implicit val samPolicyNameDecoder: Decoder[SamPolicyName] =
    Decoder.decodeString.map(s => SamPolicyName.stringToSamPolicyName.getOrElse(s, SamPolicyName.Other(s)))
  implicit val userSubjectIdDecoder: Decoder[UserSubjectId] =
    Decoder.decodeString.map(UserSubjectId.apply)
  implicit val samPolicyEmailDecoder: Decoder[SamPolicyEmail] = Decoder[WorkbenchEmail].map(SamPolicyEmail)
  implicit val projectActionDecoder: Decoder[ProjectAction] =
    Decoder.decodeString.map(x => ProjectAction.stringToAction.getOrElse(x, ProjectAction.Other(x)))
  implicit val runtimeActionDecoder: Decoder[RuntimeAction] =
    Decoder.decodeString.emap(x => RuntimeAction.stringToAction.get(x).toRight(s"Unknown runtime action: $x"))
  implicit val persistentDiskActionDecoder: Decoder[PersistentDiskAction] =
    Decoder.decodeString.emap(x =>
      PersistentDiskAction.stringToAction.get(x).toRight(s"Unknown persistent disk action: $x")
    )
  implicit val appActionDecoder: Decoder[AppAction] =
    Decoder.decodeString.emap(x => AppAction.stringToAction.get(x).toRight(s"Unknown app action: $x"))
  implicit val wsmApplicationActionDecoder: Decoder[WsmResourceAction] =
    Decoder.decodeString.emap(x =>
      WsmResourceAction.stringToAction.get(x).toRight(s"Unknown wsm application action: $x")
    )
  implicit val samRoleDecoder: Decoder[SamRole] =
    Decoder.decodeString.map(x => SamRole.stringToRole.getOrElse(x, SamRole.Other(x)))
  implicit val controlledResourceActionDecoder: Decoder[WorkspaceAction] =
    Decoder.decodeString.emap(x =>
      WorkspaceAction.stringToAction.get(x).toRight(s"Unknown controlledResource action: $x")
    )
  implicit val samPolicyDataDecoder: Decoder[SamPolicyData] = Decoder.instance { x =>
    for {
      memberEmails <- x.downField("memberEmails").as[List[WorkbenchEmail]]
      roles <- x.downField("roles").as[List[SamRole]]
    } yield SamPolicyData(memberEmails, roles)
  }
  implicit val syncStatusDecoder: Decoder[SyncStatusResponse] = Decoder.instance { x =>
    for {
      lastSyncDate <- x.downField("lastSyncDate").as[String]
      email <- x.downField("email").as[SamPolicyEmail]
    } yield SyncStatusResponse(lastSyncDate, email)
  }

  /**
   * Decodes the `roles` field to a SamRoleAction object.
   * @deprecated Prefer SamResourceRoles and samResourceRolesDecoder.
   */
  @Deprecated
  implicit val samRoleActionDecoder: Decoder[SamRoleAction] = Decoder.forProduct1("roles")(SamRoleAction.apply)

  /** Decodes the `roles` field to a SamResourceRoles object. */
  implicit val samResourceRolesDecoder: Decoder[SamResourceRoles] = Decoder.forProduct1("roles")(SamResourceRoles.apply)

  /**
   * Decodes an item from Sam's resource list endpoint to a `ListResourceResponse`.
   * @deprecated Prefer ListResourceRolesItem and listResourceRolesItemDecoder.
   */
  @Deprecated
  implicit def listResourceResponseDecoder[R: Decoder]: Decoder[ListResourceResponse[R]] = Decoder.instance { x =>
    for {
      resourceId <- x.downField("resourceId").as[R]
      // these three places can have duplicated SamPolicyNames
      direct <- x.downField("direct").as[SamRoleAction]
      inherited <- x.downField("inherited").as[SamRoleAction]
      public <- x.downField("public").as[SamRoleAction]
    } yield ListResourceResponse(resourceId, (direct.roles ++ inherited.roles ++ public.roles).toSet)
  }

  /** Decodes an item from Sam's resource list endpoint to a `ListResourceRolesItem`. Ignores any extra-role actions. */
  implicit def listResourceRolesItemDecoder[R: Decoder]: Decoder[ListResourceRolesItem[R]] = Decoder.instance { x =>
    for {
      resourceId <- x.downField("resourceId").as[R]
      // these three places can have duplicated SamResourceRoles
      direct <- x.downField("direct").as[SamResourceRoles]
      inherited <- x.downField("inherited").as[SamResourceRoles]
      public <- x.downField("public").as[SamResourceRoles]
    } yield ListResourceRolesItem[R](resourceId, (direct.roles ++ inherited.roles ++ public.roles).toSet)
  }

  val subsystemStatusDecoder: Decoder[SubsystemStatus] = Decoder.instance { c =>
    for {
      ok <- c.downField("ok").as[Boolean]
      messages <- c.downField("messages").as[Option[List[String]]]
    } yield SubsystemStatus(ok, messages)
  }
  implicit val systemsDecoder: Decoder[Map[Subsystem, SubsystemStatus]] = Decoder
    .decodeMap[Subsystem, SubsystemStatus](KeyDecoder.decodeKeyString.map(Subsystems.withName), subsystemStatusDecoder)
  implicit val statusCheckResponseDecoder: Decoder[StatusCheckResponse] = Decoder.instance { c =>
    for {
      ok <- c.downField("ok").as[Boolean]
      systems <- c.downField("systems").as[Map[Subsystem, SubsystemStatus]]
    } yield StatusCheckResponse(ok, systems)
  }
  implicit val registerInfoResponseDecoder: Decoder[RegisterInfoResponse] =
    Decoder.forProduct1("enabled")(RegisterInfoResponse.apply)
  implicit val samUserInfoDecoder: Decoder[SamUserInfo] =
    Decoder.forProduct3("userSubjectId", "userEmail", "enabled")(SamUserInfo.apply)

  implicit val getActionManagedIdentityResponseDecoder: Decoder[GetActionManagedIdentityResponse] = Decoder.instance {
    c =>
      for {
        objectId <- c.downField("objectId").as[String]
      } yield GetActionManagedIdentityResponse(objectId)
  }
}

final case class CreateSamResourceRequest[R](samResourceId: R,
                                             policies: Map[SamPolicyName, SamPolicyData],
                                             parent: SerializableSamResource,
                                             authDomain: List[String] = List.empty,
                                             returnResource: Boolean = false
)

final case class SyncStatusResponse(lastSyncDate: String, email: SamPolicyEmail)

/**
 * An item in the list returned by the list resources endpoint, with a resource ID and a set of SamPolicyNames.
 * @deprecated ListResourceRolesItem.
 */
@Deprecated
final case class ListResourceResponse[R](samResourceId: R, samPolicyNames: Set[SamPolicyName])

/** An item in the list returned by the list resources endpoint, with a resource ID and a set of SamRoles. */
final case class ListResourceRolesItem[R](samResourceId: R, samRoles: Set[SamRole])

final case class HttpSamDaoConfig(samUri: Uri,
                                  petCacheEnabled: Boolean,
                                  petCacheExpiryTime: FiniteDuration,
                                  petCacheMaxSize: Int
)

final case class UserEmailAndProject(userEmail: WorkbenchEmail, googleProject: GoogleProject)

final case class SerializableSamResource(resourceTypeName: SamResourceType, resourceId: SamResourceId)

/** @deprecated Prefer SamResourceRoles. */
@Deprecated
final case class SamRoleAction(roles: List[SamPolicyName])

/** Holds a list of roles. Replicates `SamRoleAction` but uses SamRole, which is appropriate to model Sam resource roles. */
final case class SamResourceRoles(roles: List[SamRole])

final case class SamUserInfo(userSubjectId: UserSubjectId, userEmail: WorkbenchEmail, enabled: Boolean)

final case object NotFoundException extends NoStackTrace

final case class AuthProviderException(traceId: TraceId, msg: String, code: StatusCode)
    extends LeoException(message = s"AuthProvider error: $msg", statusCode = code, traceId = Some(traceId))
    with NoStackTrace

final case class RegisterInfoResponse(enabled: Boolean)

final case class GetActionManagedIdentityResponse(objectId: String)
