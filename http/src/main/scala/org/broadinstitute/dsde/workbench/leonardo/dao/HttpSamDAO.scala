package org.broadinstitute.dsde.workbench.leonardo
package dao

import java.io.ByteArrayInputStream
import java.util.UUID
import java.util.concurrent.TimeUnit

import _root_.io.circe.{Decoder, Json, KeyDecoder}
import ca.mrvisser.sealerate
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

  def hasResourcePermission(resourceId: String,
                            action: String,
                            resourceTypeName: ResourceTypeName,
                            authHeader: Authorization)(implicit ev: ApplicativeAsk[F, TraceId]): F[Boolean] =
    for {
      res <- httpClient.expectOr[Boolean](
        Request[F](
          method = Method.GET,
          uri = config.samUri
            .withPath(s"/api/resources/v1/${resourceTypeName.toString}/${resourceId}/action/${action}"),
          headers = Headers.of(authHeader)
        )
      )(onError)
    } yield res

  def getResourcePolicies[A](
    authHeader: Authorization,
    resourseTypeName: ResourceTypeName
  )(implicit decoder: EntityDecoder[F, List[A]], ev: ApplicativeAsk[F, TraceId]): F[List[A]] =
    httpClient.expectOr[List[A]](
      Request[F](
        method = Method.GET,
        uri = config.samUri.withPath(s"/api/resources/v1/${resourseTypeName.toString}"),
        headers = Headers.of(authHeader)
      )
    )(onError)

  //Notifications that Leo has created/destroyed clusters. Allows the auth provider to register things.
  /**
   * Leo calls this method to notify the auth provider that a new notebook cluster has been created.
   * The returned future should complete once the provider has finished doing any associated work.
   * Returning a failed Future will prevent the cluster from being created, and will call notifyClusterDeleted for the same cluster.
   * Leo will wait, so be timely!
   *
   * @param internalId     The internal ID for the cluster (i.e. used for Sam resources)
   * @param creatorEmail     The email address of the user in question
   * @param googleProject The Google project the cluster was created in
   * @param runtimeName   The user-provided name of the Dataproc cluster
   * @return A Future that will complete when the auth provider has finished doing its business.
   */
  def createClusterResource(internalId: RuntimeInternalId,
                            creatorEmail: WorkbenchEmail,
                            googleProject: GoogleProject,
                            runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      traceId <- ev.ask
      token <- getCachedPetAccessToken(creatorEmail, googleProject).flatMap(
        _.fold(
          Effect[F].raiseError[String](
            AuthProviderException(traceId, s"No pet SA found for ${creatorEmail} in ${googleProject}")
          )
        )(s => Effect[F].pure(s))
      )
      authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))
      _ <- logger.info(
        s"${traceId} | creating notebook-cluster resource in sam for ${googleProject}/${runtimeName}/${internalId}"
      )
      _ <- httpClient.fetch[Unit](
        Request[F](
          method = Method.POST,
          uri = config.samUri
            .withPath(s"/api/resources/v1/${ResourceTypeName.NotebookCluster.toString}/${internalId.asString}"),
          headers = Headers.of(authHeader)
        )
      ) { resp =>
        if (resp.status.isSuccess)
          Effect[F].unit
        else
          onError(resp).flatMap(Effect[F].raiseError)
      }
    } yield ()

  /**
   * Leo calls this method to notify the auth provider that a notebook cluster has been deleted.
   * The returned future should complete once the provider has finished doing any associated work.
   * Leo will wait, so be timely!
   *
   * @param internalId     The internal ID for the cluster (i.e. used for Sam resources)
   * @param userEmail        The email address of the user in question
   * @param creatorEmail     The email address of the creator of the cluster
   * @param googleProject    The Google project the cluster was created in
   * @param runtimeName      The user-provided name of the Dataproc cluster
   * @return A Future that will complete when the auth provider has finished doing its business.
   */
  def deleteClusterResource(internalId: RuntimeInternalId,
                            userEmail: WorkbenchEmail,
                            creatorEmail: WorkbenchEmail,
                            googleProject: GoogleProject,
                            runtimeName: RuntimeName)(implicit ev: ApplicativeAsk[F, TraceId]): F[Unit] =
    for {
      traceId <- ev.ask
      token <- getCachedPetAccessToken(creatorEmail, googleProject).flatMap(
        _.fold(
          Effect[F].raiseError[String](
            AuthProviderException(traceId, s"No pet SA found for ${creatorEmail} in ${googleProject}")
          )
        )(s => Effect[F].pure(s))
      )
      authHeader = Authorization(Credentials.Token(AuthScheme.Bearer, token))
      _ <- httpClient.fetch[Unit](
        Request[F](
          method = Method.DELETE,
          uri = config.samUri
            .withPath(s"/api/resources/v1/${ResourceTypeName.NotebookCluster.toString}/${internalId.asString}"),
          headers = Headers.of(authHeader)
        )
      ) { resp =>
        resp.status match {
          case Status.NotFound =>
            logger.info(s"Fail to delete ${googleProject}/${internalId} because cluster doesn't exist in SAM")
          case s if (s.isSuccess) => Effect[F].unit
          case _                  => onError(resp).flatMap(Effect[F].raiseError)
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
          uri = config.samUri.withPath(s"/api/google/v1/user/proxyGroup/${userEmail.value}"),
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
            uri = config.samUri.withPath(s"/api/google/v1/petServiceAccount/${googleProject.value}/${userEmail.value}"),
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
      body <- response.bodyAsText(Charset.`UTF-8`).compile.foldMonoid
      _ <- logger.error(s"${traceId} | Sam call failed: $body")
    } yield AuthProviderException(traceId, body)
}

object HttpSamDAO {
  def apply[F[_]: Effect](httpClient: Client[F],
                          config: HttpSamDaoConfig,
                          blocker: Blocker)(implicit logger: Logger[F], contextShift: ContextShift[F]): HttpSamDAO[F] =
    new HttpSamDAO[F](httpClient, config, blocker)

  implicit val accessPolicyNameDecoder: Decoder[AccessPolicyName] =
    Decoder.decodeString.map(s => AccessPolicyName.stringToAccessPolicyName.getOrElse(s, AccessPolicyName.Other(s)))
  implicit val samResourcePolicyDecoder: Decoder[SamNotebookClusterPolicy] = Decoder.instance { c =>
    for {
      policyName <- c.downField("accessPolicyName").as[AccessPolicyName]
      runtimeInternalId <- c.downField("resourceId").as[RuntimeInternalId]
    } yield SamNotebookClusterPolicy(policyName, runtimeInternalId)
  }
  implicit val samProjectPolicyDecoder: Decoder[SamProjectPolicy] = Decoder.instance { c =>
    for {
      policyName <- c.downField("accessPolicyName").as[AccessPolicyName]
      project <- c.downField("resourceId").as[GoogleProject]
    } yield SamProjectPolicy(policyName, project)
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
sealed trait AccessPolicyName extends Serializable with Product
object AccessPolicyName {
  final case object Creator extends AccessPolicyName {
    override def toString = "creator"
  }
  final case object Owner extends AccessPolicyName {
    override def toString = "owner"
  }
  final case class Other(asString: String) extends AccessPolicyName {
    override def toString = asString
  }

  val stringToAccessPolicyName: Map[String, AccessPolicyName] =
    sealerate.collect[AccessPolicyName].map(p => (p.toString, p)).toMap

}
final case class SamNotebookClusterPolicy(accessPolicyName: AccessPolicyName, internalId: RuntimeInternalId)
final case class SamProjectPolicy(accessPolicyName: AccessPolicyName, googleProject: GoogleProject)
final case class UserEmailAndProject(userEmail: WorkbenchEmail, googleProject: GoogleProject)

sealed abstract class ResourceTypeName extends Product with Serializable {
  type ResourcePolicy
}
object ResourceTypeName {
  final case object NotebookCluster extends ResourceTypeName {
    override def toString: String = "notebook-cluster"
    override type ResourcePolicy = SamNotebookClusterPolicy
  }
  final case object BillingProject extends ResourceTypeName {
    override def toString: String = "billing-project"
    override type ResourcePolicy = SamProjectPolicy
  }
}

final case object NotFoundException extends NoStackTrace
final case class AuthProviderException(traceId: TraceId, msg: String)
    extends LeoException(message = s"${traceId} | AuthProvider error: $msg")
