package org.broadinstitute.dsde.workbench.leonardo
package dao

import cats.effect.Concurrent
import cats.implicits._
import cats.mtl.Ask
import org.typelevel.log4cats.StructuredLogger
import io.circe.Decoder
import io.circe.yaml.parser
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.dao.HttpAppDescriptorDAO._
import org.http4s.{Method, Request, Response, Uri}
import org.http4s.client.Client

class HttpAppDescriptorDAO[F[_]](httpClient: Client[F])(implicit logger: StructuredLogger[F], F: Concurrent[F])
    extends AppDescriptorDAO[F] {
  override def getDescriptor(path: Uri)(implicit ev: Ask[F, AppContext]): F[AppDescriptor] =
    for {
      ctx <- ev.ask
      _ <- logger.info(ctx.loggingCtx)(s"Attempting to retrieve descriptor $path")
      yamlStr <- resolveUri(path.toString())
      _ <- logger.info(ctx.loggingCtx)(s"Attempting to parse descriptor for $path as a yaml string into JSON...")
      json <- F.fromEither(parser.parse(yamlStr))
      _ <- logger.info(ctx.loggingCtx)(s"Attempting to parse json for $path into AppDescriptor model...")
      res <- F.fromEither(json.as[AppDescriptor])
      _ <- logger.info(ctx.loggingCtx)(s"Succesfully processed descriptor at $path")
    } yield res

  private[dao] def resolveUri(path: String)(implicit ev: Ask[F, AppContext]): F[String] =
    for {
      resp <- httpClient.expectOr[String](
        Request[F](
          method = Method.GET,
          uri = Uri.unsafeFromString(path)
        )
      )(onError(path))
    } yield resp

  private def onError(path: String)(response: Response[F])(implicit ev: Ask[F, AppContext]): F[Throwable] =
    for {
      ctx <- ev.ask
      body <- response.bodyText.compile.foldMonoid
      _ <- logger.error(ctx.loggingCtx)(s"call failed: $body")
    } yield AppDescriptorException(ctx.traceId, path, body)
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
      port <- d.downField("port").as[Int]
      baseUrl <- d.downField("baseUrl").as[Option[String]]
      command <- d.downField("command").as[Option[List[String]]]
      args <- d.downField("args").as[Option[List[String]]]
      pdMountPath <- d.downField("pdMountPath").as[Option[String]]
      pdAccessMode <- d.downField("pdAccessMode").as[Option[String]]
      environment <- d.downField("environment").as[Option[Map[String, String]]]
    } yield CustomAppService(
      image,
      port,
      baseUrl.getOrElse("/"),
      command.getOrElse(List.empty),
      args.getOrElse(List.empty),
      pdMountPath.getOrElse("/data"),
      // Note, reconsider if we ever support different disk modes
      pdAccessMode.getOrElse("ReadWriteOnce"),
      environment.getOrElse(Map.empty)
    )
  }
}
