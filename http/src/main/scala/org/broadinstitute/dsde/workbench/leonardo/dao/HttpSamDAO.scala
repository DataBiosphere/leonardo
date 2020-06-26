package org.broadinstitute.dsde.workbench.leonardo
package dao

import java.io.ByteArrayInputStream
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

import _root_.io.circe.{Decoder, Json, KeyDecoder}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.model.StatusCode._
import cats.effect.implicits._
import cats.effect.{Blocker, ContextShift, Effect, Resource}
import cats.implicits._
import cats.mtl.ApplicativeAsk
import com.google.api.services.plus.PlusScopes
import com.google.api.services.storage.StorageScopes
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import io.chrisdavenport.log4cats.Logger
import org.broadinstitute.dsde.workbench.google2.credentialResource
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.SamResource.{PersistentDiskSamResource, RuntimeSamResource}
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpSamDAO._
import org.broadinstitute.dsde.workbench.leonardo.model._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.{TraceId, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.util.health.Subsystems.Subsystem
import org.broadinstitute.dsde.workbench.util.health.{StatusCheckResponse, SubsystemStatus, Subsystems}
import org.http4s._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.Authorization

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

class HttpSamDAO[F[_]: Effect](httpClient: Client[F], config: HttpSamDaoConfig, blocker: Blocker)(
  implicit logger: Logger[F],
  cs: ContextShift[F]
) extends SamDAO[F]
    with Http4sClientDsl[F] {
  private val saScopes = Seq(PlusScopes.USERINFO_EMAIL, PlusScopes.USERINFO_PROFILE, StorageScopes.DEVSTORAGE_READ_ONLY)

  private[leonardo] val petTokenCache: LoadingCache[UserEmailAndProject, Option[String]] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(config.petCacheExpiryTime.toMinutes, TimeUnit.MINUTES)
    .maximumSize(config.petCacheMaxSize)
    .build(
      new CacheLoader[UserEmailAndProject, Option[String]] {
        def load(userEmailAndProject: UserEmailAndProject): Option[String] = {
          implicit val traceId = ApplicativeAsk.const[F, TraceId](TraceId(UUID.randomUUID()))
          getPetAccessToken(userEmailAndProject.userEmail, userEmailAndProject.googleProject).toIO
            .unsafeRunSync()
        }
      }
    )

  def getStatus(implicit ev: ApplicativeAsk[F, TraceId]): F[StatusCheckResponse] =
    httpClient.expectOr[StatusCheckResponse](
      Request[F](
        method = Method.GET,
        uri = config.samUri.withPath(s"/status")
      )
    )(onError)

  def hasResourcePermission(resource: SamResource, action: String, authHeader: Authorization)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Boolean] =
    for {
      res <- httpClient.expectOr[Boolean](
        Request[F](
          method = Method.GET,
          uri = config.samUri
            .withPath(s"/api/resources/v1/${resource.resourceType.asString}/${resource.resourceId}/action/${action}"),
          headers = Headers.of(authHeader)
        )
      )(onError)
    } yield res

  def getListOfResourcePermissions(resource: SamResource, authHeader: Authorization)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[List[String]] =
    httpClient.expectOr[List[String]](
      Request[F](
        method = Method.GET,
        uri =
          config.samUri.withPath(s"/api/resources/v1/${resource.resourceType.asString}/${resource.resourceId}/actions"),
        headers = Headers.of(authHeader)
      )
    )(onError)

  def getResourcePolicies[A](
    authHeader: Authorization,
    resourceType: SamResourceType
  )(implicit decoder: EntityDecoder[F, List[A]], ev: ApplicativeAsk[F, TraceId]): F[List[A]] =
    httpClient.expectOr[List[A]](
      Request[F](
        method = Method.GET,
        uri = config.samUri.withPath(s"/api/resources/v1/${resourceType.asString}"),
        headers = Headers.of(authHeader)
      )
    )(onError)

  def createResource(resource: SamResource, creatorEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Unit] =
    for {
      traceId <- ev.ask
      token <- getCachedPetAccessToken(creatorEmail, googleProject).flatMap(
        _.fold(
          Effect[F].raiseError[String](
            AuthProviderException(traceId,
                                  s"No pet SA found for ${creatorEmail} in ${googleProject}",
                                  StatusCodes.Unauthorized)
          )
        )(s => Effect[F].pure(s))
      )
      authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))
      _ <- logger.info(
        s"${traceId} | creating ${resource.resourceType.asString} resource in sam for ${googleProject}/${resource.resourceId}"
      )
      _ <- httpClient
        .run(
          Request[F](
            method = Method.POST,
            uri = config.samUri
              .withPath(s"/api/resources/v1/${resource.resourceType.asString}/${resource.resourceId}"),
            headers = Headers.of(authHeader)
          )
        )
        .use { resp =>
          if (resp.status.isSuccess)
            Effect[F].unit
          else
            onError(resp).flatMap(Effect[F].raiseError[Unit])
        }
    } yield ()

  def deleteResource(resource: SamResource,
                     userEmail: WorkbenchEmail,
                     creatorEmail: WorkbenchEmail,
                     googleProject: GoogleProject)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      traceId <- ev.ask
      token <- getCachedPetAccessToken(creatorEmail, googleProject).flatMap(
        _.fold(
          Effect[F].raiseError[String](
            AuthProviderException(traceId,
                                  s"No pet SA found for ${creatorEmail} in ${googleProject}",
                                  StatusCodes.Unauthorized)
          )
        )(s => Effect[F].pure(s))
      )
      authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))
      _ <- httpClient
        .run(
          Request[F](
            method = Method.DELETE,
            uri = config.samUri
              .withPath(s"/api/resources/v1/${resource.resourceType.asString}/${resource.resourceId}"),
            headers = Headers.of(authHeader)
          )
        )
        .use { resp =>
          resp.status match {
            case Status.NotFound =>
              logger.info(
                s"Fail to delete ${googleProject}/${resource.resourceId} because ${resource.resourceType.asString} doesn't exist in SAM"
              )
            case s if (s.isSuccess) => Effect[F].unit
            case _                  => onError(resp).flatMap(Effect[F].raiseError[Unit])
          }
        }
    } yield ()

  def getPetServiceAccount(authorization: Authorization, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[WorkbenchEmail]] =
    httpClient.expectOptionOr[WorkbenchEmail](
      Request[F](
        method = Method.GET,
        uri = config.samUri.withPath(s"/api/google/v1/user/petServiceAccount/${googleProject.value}"),
        headers = Headers.of(authorization)
      )
    )(onError)

  def getUserProxy(userEmail: WorkbenchEmail)(implicit ev: ApplicativeAsk[F, TraceId]): F[Option[WorkbenchEmail]] =
    getAccessTokenUsingLeoJson.use { leoToken =>
      val authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, leoToken))
      httpClient.expectOptionOr[WorkbenchEmail](
        Request[F](
          method = Method.GET,
          uri =
            config.samUri.withPath(s"/api/google/v1/user/proxyGroup/${URLEncoder.encode(userEmail.value, UTF_8.name)}"),
          headers = Headers.of(authHeader)
        )
      )(onError)
    }

  def getCachedPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[String]] =
    if (config.petCacheEnabled) {
      blocker.blockOn(Effect[F].delay(petTokenCache.get(UserEmailAndProject(userEmail, googleProject))))
    } else {
      getPetAccessToken(userEmail, googleProject)
    }

  private def getAccessTokenUsingLeoJson: Resource[F, String] =
    for {
      credential <- credentialResource(
        config.serviceAccountProviderConfig.leoServiceAccountJsonFile.toAbsolutePath.toString
      )
      scopedCredential = credential.createScoped(saScopes.asJava)
      _ <- Resource.liftF(Effect[F].delay(scopedCredential.refresh))
    } yield scopedCredential.getAccessToken.getTokenValue

  private def getPetAccessToken(userEmail: WorkbenchEmail, googleProject: GoogleProject)(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[Option[String]] =
    getAccessTokenUsingLeoJson.use { leoToken =>
      val leoAuth = Authorization(Credentials.Token(AuthScheme.Bearer, leoToken))
      for {
        // fetch user's pet SA key with leo's authorization token
        userPetKey <- httpClient.expectOptionOr[Json](
          Request[F](
            method = Method.GET,
            uri = config.samUri.withPath(
              s"/api/google/v1/petServiceAccount/${googleProject.value}/${URLEncoder.encode(userEmail.value, UTF_8.name)}"
            ),
            headers = Headers.of(leoAuth)
          )
        )(onError)
        token <- userPetKey.traverse { key =>
          val keyStream = new ByteArrayInputStream(key.toString().getBytes)
          Effect[F]
            .delay(ServiceAccountCredentials.fromStream(keyStream).createScoped(saScopes.asJava))
            .map(_.refreshAccessToken.getTokenValue)
        }
      } yield token
    }

  private def onError(response: Response[F])(implicit ev: ApplicativeAsk[F, TraceId]): F[Throwable] =
    for {
      traceId <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(s"${traceId} | Sam call failed: $body")
    } yield AuthProviderException(traceId, body, response.status.code)
}

object HttpSamDAO {
  def apply[F[_]: Effect](httpClient: Client[F],
                          config: HttpSamDaoConfig,
                          blocker: Blocker)(implicit logger: Logger[F], contextShift: ContextShift[F]): HttpSamDAO[F] =
    new HttpSamDAO[F](httpClient, config, blocker)

  implicit val accessPolicyNameDecoder: Decoder[AccessPolicyName] =
    Decoder.decodeString.map(s => AccessPolicyName.stringToAccessPolicyName.getOrElse(s, AccessPolicyName.Other(s)))
  implicit val samClusterPolicyDecoder: Decoder[SamRuntimePolicy] = Decoder.instance { c =>
    for {
      policyName <- c.downField("accessPolicyName").as[AccessPolicyName]
      runtimeSamResource <- c.downField("resourceId").as[RuntimeSamResource]
    } yield SamRuntimePolicy(policyName, runtimeSamResource)
  }
  implicit val samProjectPolicyDecoder: Decoder[SamProjectPolicy] = Decoder.instance { c =>
    for {
      policyName <- c.downField("accessPolicyName").as[AccessPolicyName]
      googleProject <- c.downField("resourceId").as[GoogleProject]
    } yield SamProjectPolicy(policyName, googleProject)
  }
  // TODO: consolidate these 3 access policy decoders into one
  implicit val samPersistentDiskPolicyDecoder: Decoder[SamPersistentDiskPolicy] = Decoder.instance { c =>
    for {
      policyName <- c.downField("accessPolicyName").as[AccessPolicyName]
      diskInternalId <- c.downField("resourceId").as[PersistentDiskSamResource]
    } yield SamPersistentDiskPolicy(policyName, diskInternalId)
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
}

final case class HttpSamDaoConfig(samUri: Uri,
                                  petCacheEnabled: Boolean,
                                  petCacheExpiryTime: FiniteDuration,
                                  petCacheMaxSize: Int,
                                  serviceAccountProviderConfig: ServiceAccountProviderConfig)

final case class UserEmailAndProject(userEmail: WorkbenchEmail, googleProject: GoogleProject)

final case object NotFoundException extends NoStackTrace
final case class AuthProviderException(traceId: TraceId, msg: String, code: StatusCode)
    extends LeoException(message = s"${traceId} | AuthProvider error: $msg", statusCode = code)
