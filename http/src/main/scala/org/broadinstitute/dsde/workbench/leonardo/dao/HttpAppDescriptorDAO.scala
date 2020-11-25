package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.Effect
import cats.implicits._
import cats.mtl.Ask
import io.chrisdavenport.log4cats.StructuredLogger
import io.circe._
import io.circe.yaml.parser
import org.broadinstitute.dsde.workbench.leonardo.model.LeoException
import org.broadinstitute.dsde.workbench.model.TraceId
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpAppDescriptorDAO._
import org.http4s.{Method, Request, Response, Uri}
import org.http4s.client.Client

class HttpAppDescriptorDAO[F[_]](httpClient: Client[F])(implicit logger: StructuredLogger[F], F: Effect[F])
    extends AppDescriptorDAO[F] {
  override def getDescriptor(path: String)(implicit ev: Ask[F, TraceId]): F[AppDescriptor] =
    for {
      yamlStr <- httpClient.expectOr[String](
        Request[F](
          method = Method.GET,
          uri = Uri.unsafeFromString(path)
        )
      )(onError(path))

      json <- F.fromEither(parser.parse(yamlStr))
      res <- F.fromEither(json.as[AppDescriptor])
    } yield res

  private def onError(path: String)(response: Response[F])(implicit ev: Ask[F, TraceId]): F[Throwable] =
    for {
      traceId <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(s"${traceId} | Docker call failed: $body")
    } yield AppDescriptorException(traceId, path, body)
}
object HttpAppDescriptorDAO {
  implicit val appDescriptorDecoder: Decoder[AppDescriptor] = Decoder.instance { d =>
    for {
      name <- d.downField("name").as[String]
      author <- d.downField("author").as[String]
      description <- d.downField("description").as[String]
      version <- d.downField("version").as[String]
      services <- d.downField("services").as[Map[String, CustomAppService]]
    } yield AppDescriptor(name, author, description, version, services)
  }

  implicit val customAppServiceDecoder: Decoder[CustomAppService] = Decoder.instance { d =>
    for {
      image <- d.downField("image").as[ContainerImage]
      port <- d.downField("port").as[Option[Int]]
      command <- d.downField("command").as[Option[List[String]]]
      args <- d.downField("args").as[Option[List[String]]]
      pdMountPath <- d.downField("pdMountPath").as[String]
      pdAccessMode <- d.downField("pdAccessMode").as[String]
    } yield CustomAppService(image,
                             port.getOrElse(80),
                             command.getOrElse(List.empty),
                             args.getOrElse(List.empty),
                             pdMountPath,
                             pdAccessMode)
  }
}
final case class AppDescriptor(name: String,
                               author: String,
                               description: String,
                               version: String,
                               services: Map[String, CustomAppService])

final case class CustomAppService(image: ContainerImage,
                                  port: Int,
                                  command: List[String],
                                  args: List[String],
                                  pdMountPath: String,
                                  pdAccessMode: String)

final case class AppDescriptorException(traceId: TraceId, path: String, msg: String)
    extends LeoException(message = s"${traceId} | Error occurred fetching app descriptor from path $path: $msg")
