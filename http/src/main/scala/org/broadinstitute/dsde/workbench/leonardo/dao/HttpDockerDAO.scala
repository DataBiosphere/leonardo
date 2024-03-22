package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.Concurrent
import cats.mtl.Ask
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import org.broadinstitute.dsde.workbench.leonardo.ContainerRegistry._
import org.broadinstitute.dsde.workbench.leonardo.RuntimeImageType.{Jupyter, RStudio}
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpDockerDAO._
import org.broadinstitute.dsde.workbench.leonardo.dao.ImageVersion.{Sha, Tag}
import org.broadinstitute.dsde.workbench.leonardo.model.{InvalidImage, LeoException}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.http4s._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.client.middleware.FollowRedirect
import org.http4s.headers.{Accept, Authorization}
import org.typelevel.log4cats.Logger

import java.nio.file.Paths
import java.time.Instant

/**
 * Talks to Docker remote APIs to retrieve manifest information in order to try and figure out
 * what tool it's running.
 *
 * This article was used as a guide: https://ops.tips/blog/inspecting-docker-image-without-pull/
 *
 * Currently supports:
 * - Jupyter or RStudio images
 * - Dockerhub, GCR, GAR or GHCR repos
 * - Tagged or untagged images
 * Does not support:
 * - Private images
 * - SHA specifiers (e.g. myrepo/myimage@sha256:...)
 *
 * Note: this class uses the `Concurrent` typeclass to support following redirects.
 */
class HttpDockerDAO[F[_]] private (httpClient: Client[F])(implicit logger: Logger[F], F: Concurrent[F])
    extends DockerDAO[F]
    with Http4sClientDsl[F] {

  override def detectTool(image: ContainerImage, petTokenOpt: Option[String], now: Instant)(implicit
    ev: Ask[F, TraceId]
  ): F[RuntimeImage] =
    for {
      traceId <- ev.ask

      parsed <- parseImage(image)

      tokenOpt <- getToken(parsed, petTokenOpt)

      digest <- parsed.imageVersion match {
        case Tag(_) =>
          getManifestConfig(parsed, tokenOpt)
            .map(_.digest)
            .adaptError { case e: org.http4s.InvalidMessageBodyFailure =>
              InvalidImage(traceId, image, Some(e))
            }
        case Sha(digest) => F.pure(digest)
      }

      containerConfig <- getContainerConfig(parsed, digest, tokenOpt)
        .adaptError { case e: org.http4s.InvalidMessageBodyFailure =>
          InvalidImage(traceId, image, Some(e))
        }

      envSet = containerConfig.config.env.toSet
      tool = clusterToolEnv
        .find { case (_, env_var) =>
          envSet.exists(_.key == env_var)
        }
        .map(_._1)
      homeDirectory = envSet.collectFirst { case env if env.key == "HOME" => Paths.get(env.value) }
      res <- F.fromEither(tool.toRight(InvalidImage(traceId, image, None)))
    } yield RuntimeImage(res, image.imageUrl, homeDirectory, now)

  // curl -L "https://us.gcr.io/v2/anvil-gcr-public/anvil-rstudio-base/blobs/sha256:aaf072362a3bfa231f444af7a05aa24dd83f6d94ba56b3d6d0b365748deac30a" | jq -r '.container_config'
  private[dao] def getContainerConfig(parsedImage: ParsedImage, digest: String, tokenOpt: Option[Token])(implicit
    ev: Ask[F, TraceId]
  ): F[ContainerConfigResponse] =
    FollowRedirect(3)(httpClient).expectOr[ContainerConfigResponse](
      Request[F](
        method = Method.GET,
        uri = parsedImage.blobUri(digest),
        headers = headers(tokenOpt)
      )
    )(onError)

  // curl --header "Accept: application/vnd.docker.distribution.manifest.v2+json" --header "Authorization: Bearer $TOKEN" --header "Accept: application/json" "https://registry-1.docker.io/v2/library/nginx/manifests/latest"
  private[dao] def getManifestConfig(parsedImage: ParsedImage, tokenOpt: Option[Token])(implicit
    ev: Ask[F, TraceId]
  ): F[ManifestConfig] =
    httpClient.expectOr[ManifestConfig](
      Request[F](
        method = Method.GET,
        uri = parsedImage.manifestUri,
        headers = headers(tokenOpt)
      )
    )(onError)

  // curl --silent "https://auth.docker.io/token?scope=repository%3Alibrary/nginx%3Apull&service=registry.docker.io" | jq '.token'
  private[dao] def getToken(parsedImage: ParsedImage, petTokenOpt: Option[String])(implicit
    ev: Ask[F, TraceId]
  ): F[Option[Token]] =
    parsedImage.registry match {
      // If it's a GCR or GAR repo, use the pet token
      case ContainerRegistry.GCR | ContainerRegistry.GAR => F.pure(petTokenOpt.map(Token))
      // If it's a Dockerhub repo, need to request a token from Dockerhub
      case ContainerRegistry.DockerHub =>
        httpClient.expectOptionOr[Token](
          Request[F](
            method = Method.GET,
            uri = dockerHubAuthUri
              .withPath(Uri.Path.unsafeFromString("/token"))
              .withQueryParam("scope", s"repository:${parsedImage.imageName}:pull")
              .withQueryParam("service", "registry.docker.io"),
            // must be Accept: application/json not Accept: application/vnd.docker.distribution.manifest.v2+json
            headers = Headers(Accept.parse("application/json").toOption.get)
          )
        )(onError)
      // If it's GHCR, request a noop token from GitHub
      case ContainerRegistry.GHCR =>
        httpClient.expectOptionOr[Token](
          Request[F](
            method = Method.GET,
            uri = ghcrAuthUri
              .withPath(Uri.Path.unsafeFromString("/token"))
              .withQueryParam("scope", s"repository:${parsedImage.imageName}:pull"),
            // must be Accept: application/json not Accept: application/vnd.docker.distribution.manifest.v2+json
            headers = Headers(Accept.parse("application/json").toOption.get)
          )
        )(onError)
    }

  private def onError(response: Response[F])(implicit ev: Ask[F, TraceId]): F[Throwable] =
    for {
      traceId <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(s"${traceId} | Docker call failed: $body")
    } yield DockerImageException(traceId, body)

  private def headers(tokenOpt: Option[Token]): Headers =
    Headers(
      Accept.parse("application/vnd.docker.distribution.manifest.v2+json").toOption.get
    ) ++
      tokenOpt.fold(Headers.empty)(t => Headers(Authorization(Credentials.Token(AuthScheme.Bearer, t.token))))

  private[dao] def parseImage(image: ContainerImage)(implicit ev: Ask[F, TraceId]): F[ParsedImage] =
    image.imageUrl match {
      case GCR.regex(registry, imageName, tagOpt, shaOpt) =>
        val version = Option(tagOpt)
          .map(Tag)
          .orElse(Option(shaOpt).map(Sha))
        for {
          traceId <- ev.ask
          res <- version.fold(F.raiseError[ParsedImage](ImageParseException(traceId, image)))(i =>
            F.pure(ParsedImage(GCR, Uri.unsafeFromString(s"https://$registry/v2"), imageName, i))
          )
        } yield res
      case GAR.regex(registry, imageName, tagOpt, shaOpt) =>
        val version = Option(tagOpt)
          .map(Tag)
          .orElse(Option(shaOpt).map(Sha))
        for {
          traceId <- ev.ask
          res <- version.fold(F.raiseError[ParsedImage](ImageParseException(traceId, image)))(i =>
            F.pure(ParsedImage(GAR, Uri.unsafeFromString(s"https://$registry/v2"), imageName, i))
          )
        } yield res
      case DockerHub.regex(imageName, tagOpt, shaOpt) =>
        val identifier = Option(tagOpt)
          .map(Tag)
          .orElse(Option(shaOpt).map(Sha))
          .getOrElse(Tag("latest"))
        F.pure(ParsedImage(DockerHub, dockerHubRegistryUri, imageName, identifier))
      case GHCR.regex(registry, imageName, tagOpt, shaOpt) =>
        val identifier = Option(tagOpt)
          .map(Tag)
          .orElse(Option(shaOpt).map(Sha))
          .getOrElse(Tag("latest"))
        F.pure(ParsedImage(GHCR, Uri.unsafeFromString(s"https://$registry/v2"), imageName, identifier))
      case _ =>
        for {
          traceId <- ev.ask
          _ <- logger.error(s"${traceId} | Unable to parse ${image.registry.toString} image ${image.imageUrl}")
          res <- F.raiseError[ParsedImage](ImageParseException(traceId, image))
        } yield res
    }
}

object HttpDockerDAO {
  val dockerHubAuthUri = Uri.unsafeFromString("https://auth.docker.io")
  val ghcrAuthUri = Uri.unsafeFromString("https://ghcr.io")
  val dockerHubRegistryUri = Uri.unsafeFromString("https://registry-1.docker.io")
  val clusterToolEnv = Map(Jupyter -> "JUPYTER_HOME", RStudio -> "RSTUDIO_HOME")

  def apply[F[_]: Concurrent](httpClient: Client[F])(implicit logger: Logger[F]): HttpDockerDAO[F] =
    new HttpDockerDAO[F](httpClient)

  // Decoders
  implicit val tokenDecoder: Decoder[Token] = Decoder.instance { d =>
    for {
      token <- d.downField("token").as[String]
    } yield Token(token)
  }
  implicit val manifestConfigDecoder: Decoder[ManifestConfig] = Decoder.instance { d =>
    val cursor = d.downField("config")
    for {
      mediaType <- cursor.get[String]("mediaType")
      size <- cursor.get[Int]("size")
      digest <- cursor.get[String]("digest")
    } yield ManifestConfig(mediaType, size, digest)
  }

  implicit val envDecoder: Decoder[Env] = Decoder.decodeString.emap { s =>
    val res = for {
      splitted <- Either.catchNonFatal(s.split("="))
      first <- Either.catchNonFatal(splitted(0))
      second <- Either.catchNonFatal(if (splitted.length > 1) splitted(1) else "")
    } yield Env(first, second)
    res.leftMap(_.getMessage)
  }
  implicit val containerConfigDecoder: Decoder[ContainerConfig] = Decoder.forProduct1("Env")(ContainerConfig.apply)
  implicit val containerConfigResponseDecoder: Decoder[ContainerConfigResponse] = Decoder.instance { d =>
    for {
      envOpt <- d.downField("container_config").as[Option[ContainerConfig]]
      config <- envOpt
        .fold[Decoder.Result[ContainerConfig]](d.downField("config").as[ContainerConfig])(_.asRight[DecodingFailure])
    } yield ContainerConfigResponse(config)
  }
}

// Image parsing models
sealed trait ImageVersion extends Product with Serializable {
  def toString: String
}
object ImageVersion {
  final case class Tag(tag: String) extends ImageVersion {
    override def toString = tag
  }
  final case class Sha(sha: String) extends ImageVersion {
    override def toString = sha
  }
}
final case class ParsedImage(registry: ContainerRegistry,
                             registryUri: Uri,
                             imageName: String,
                             imageVersion: ImageVersion
) {
  def manifestUri: Uri =
    registryUri.withPath(Uri.Path.unsafeFromString(s"/v2/${imageName}/manifests/${imageVersion.toString}"))
  def blobUri(digest: String): Uri =
    registryUri.withPath(Uri.Path.unsafeFromString(s"/v2/${imageName}/blobs/${digest}"))
}

// API response models
final case class Token(token: String) extends AnyVal
final case class ManifestConfig(mediaType: String, size: Int, digest: String)
final case class Env(key: String, value: String)
final case class ContainerConfig(env: List[Env]) extends AnyVal
final case class ContainerConfigResponse(config: ContainerConfig)

// Exceptions
final case class DockerImageException(traceId: TraceId, msg: String)
    extends LeoException(message = s"Error occurred during Docker image auto-detection: $msg", traceId = Some(traceId))

final case class ImageParseException(traceId: TraceId, image: ContainerImage)
    extends LeoException(message = s"Unable to parse ${image.registry} image ${image.imageUrl}",
                         traceId = Some(traceId)
    )
