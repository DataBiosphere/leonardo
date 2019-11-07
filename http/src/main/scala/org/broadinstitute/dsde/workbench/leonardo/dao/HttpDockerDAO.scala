package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.Concurrent
import cats.implicits._
import cats.mtl.ApplicativeAsk
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpDockerDAO._
import org.broadinstitute.dsde.workbench.leonardo.dao.ImageIdentifier.{Sha, Tag}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterTool.{Jupyter, RStudio}
import org.broadinstitute.dsde.workbench.leonardo.model.ContainerRegistry.{DockerHub, GCR}
import org.broadinstitute.dsde.workbench.leonardo.model.{ClusterTool, ContainerImage, ContainerRegistry, LeoException}
import org.broadinstitute.dsde.workbench.model.TraceId
import org.http4s._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.client.middleware.FollowRedirect
import org.http4s.headers.Authorization

class HttpDockerDAO[F[_]: Concurrent] private (httpClient: Client[F])(implicit logger: Logger[F])
    extends DockerDAO[F]
    with Http4sClientDsl[F] {

  // TODO test with private GCR image
  override def detectTool(image: ContainerImage)(implicit ev: ApplicativeAsk[F, TraceId]): F[Option[ClusterTool]] =
    for {
      parsed <- parseImage(image)
      tokenOpt <- getToken(parsed)
      digest <- parsed.imageIdentifier match {
        case Tag(_)      => getManifestConfig(parsed, tokenOpt).map(_.digest)
        case Sha(digest) => Concurrent[F].pure(digest)
      }
      containerConfig <- getContainerConfig(parsed, digest, tokenOpt)
      envSet = containerConfig.env.toSet
      tool = clusterToolEnv
        .find {
          case (_, v) =>
            envSet.exists(_.startsWith(v))
        }
        .map(_._1)
    } yield tool

  //curl -L "https://us.gcr.io/v2/anvil-gcr-public/anvil-rstudio-base/blobs/sha256:aaf072362a3bfa231f444af7a05aa24dd83f6d94ba56b3d6d0b365748deac30a" | jq -r '.container_config'
  private[dao] def getContainerConfig(parsedImage: ParsedImage, digest: String, tokenOpt: Option[Token])(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[ContainerConfig] =
    FollowRedirect(3)(httpClient).expectOr[ContainerConfig](
      Request[F](
        method = Method.GET,
        uri = parsedImage.blobUri(digest),
        headers = headers(tokenOpt)
      )
    )(onError)

  //curl --header "Accept: application/vnd.docker.distribution.manifest.v2+json" --header "Authorization: Bearer $TOKEN" --header "Accept: application/json" "https://registry-1.docker.io/v2/library/nginx/manifests/latest"
  private[dao] def getManifestConfig(parsedImage: ParsedImage, tokenOpt: Option[Token])(
    implicit ev: ApplicativeAsk[F, TraceId]
  ): F[ManifestConfig] =
    httpClient.expectOr[ManifestConfig](
      Request[F](
        method = Method.GET,
        uri = parsedImage.manifestUri,
        headers = headers(tokenOpt)
      )
    )(onError)

  //curl --silent "https://auth.docker.io/token?scope=repository%3Alibrary/nginx%3Apull&service=registry.docker.io" | jq '.token'
  private[dao] def getToken(parsedImage: ParsedImage)(implicit ev: ApplicativeAsk[F, TraceId]): F[Option[Token]] =
    parsedImage.registry match {
      case GCR => Concurrent[F].pure(None)
      case DockerHub =>
        httpClient.expectOptionOr[Token](
          Request[F](
            method = Method.GET,
            uri = dockerHubAuthUri
              .withPath("/token")
              .withQueryParam("scope", s"repository:${parsedImage.imageName}:pull")
              .withQueryParam("service", "registry.docker.io"),
            headers = Headers.of(acceptHeader)
          )
        )(onError)
    }

  private def onError(response: Response[F])(implicit ev: ApplicativeAsk[F, TraceId]): F[Throwable] =
    for {
      traceId <- ev.ask
      body <- response.bodyAsText(Charset.`UTF-8`).compile.foldMonoid
      _ <- logger.error(s"${traceId} | Docker call failed: $body")
    } yield DockerImageException(traceId, body)

  private def headers(tokenOpt: Option[Token]): Headers =
    Headers.of(acceptHeader) ++
      tokenOpt.fold(Headers.empty)(t => Headers.of(Authorization(Credentials.Token(AuthScheme.Bearer, t.token))))

  private[dao] def parseImage(image: ContainerImage): F[ParsedImage] =
    image.imageUrl match {
      case GCR.regex(registry, imageName, tagOpt, shaOpt) =>
        val identifier = Option(tagOpt)
          .map(Tag)
          .orElse(Option(shaOpt).map(Sha))
        identifier.fold(Concurrent[F].raiseError[ParsedImage](ImageParseException(image)))(
          i => Concurrent[F].pure(ParsedImage(GCR, Uri.unsafeFromString(s"https://$registry/v2"), imageName, i))
        )
      case DockerHub.regex(imageName, tagOpt, shaOpt) =>
        val identifier = Option(tagOpt)
          .map(Tag)
          .orElse(Option(shaOpt).map(Sha))
          .getOrElse(Tag("latest"))

        Concurrent[F].pure(ParsedImage(DockerHub, dockerHubRegistryUri, imageName, identifier))
      case _ => Concurrent[F].raiseError(ImageParseException(image))
    }

}

object HttpDockerDAO {
  val dockerHubAuthUri = Uri.unsafeFromString("https://auth.docker.io")
  val dockerHubRegistryUri = Uri.unsafeFromString("https://registry-1.docker.io")
  val acceptHeader = Header("Accept", "application/vnd.docker.distribution.manifest.v2+json")
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
  implicit val containerConfigDecoder: Decoder[ContainerConfig] = Decoder.instance { d =>
    val cursor = d.downField("container_config")
    for {
      image <- cursor.get[String]("Image")
      env <- cursor.get[List[String]]("Env")
    } yield ContainerConfig(image, env)
  }
}

// Image parsing models
sealed trait ImageIdentifier extends Product with Serializable {
  def toString: String
}
object ImageIdentifier {
  final case class Tag(tag: String) extends ImageIdentifier {
    override def toString = tag
  }
  final case class Sha(sha: String) extends ImageIdentifier {
    override def toString = sha
  }
}
final case class ParsedImage(registry: ContainerRegistry,
                             registryUri: Uri,
                             imageName: String,
                             imageIdentifier: ImageIdentifier) {
  def manifestUri: Uri =
    registryUri.withPath(s"/v2/${imageName}/manifests/${imageIdentifier.toString}")
  def blobUri(digest: String): Uri =
    registryUri.withPath(s"/v2/${imageName}/blobs/${digest}")
}

// API response models
final case class Token(token: String)
final case class ManifestConfig(mediaType: String, size: Int, digest: String)
final case class ContainerConfig(image: String, env: List[String])

// Exceptions
final case class DockerImageException(traceId: TraceId, msg: String)
    extends LeoException(message = s"${traceId} | Docker validation error: $msg")

final case class ImageParseException(image: ContainerImage)
    extends LeoException(message = s"Error parsing ${image.registry.toString} image ${image.imageUrl}")
