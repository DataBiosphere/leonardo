package org.broadinstitute.dsde.workbench.leonardo.util

import java.nio.file.Path
import cats.effect._
import fs2._
import fs2.io.file.Files
import org.broadinstitute.dsde.workbench.leonardo.RuntimeResource
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterResourcesConfig

object TemplateHelper {

  def templateFile[F[_]: Sync: Files](replacementMap: Map[String, String], path: Path): Stream[F, Byte] =
    fileStream(path)
      .through(text.utf8.decode)
      .through(text.lines)
      .map(template(replacementMap))
      .intersperse("\n")
      .through(text.utf8.encode)

  def templateResource[F[_]: Sync](replacementMap: Map[String, String],
                                   clusterResource: RuntimeResource): Stream[F, Byte] =
    resourceStream(clusterResource)
      .through(text.utf8.decode)
      .through(text.lines)
      .map(template(replacementMap))
      .intersperse("\n")
      .through(text.utf8.encode)

  def fileStream[F[_]: Sync: Files](path: Path): Stream[F, Byte] =
    Files[F].readAll(fs2.io.file.Path.fromNioPath(path))

  def resourceStream[F[_]: Sync](clusterResource: RuntimeResource): Stream[F, Byte] = {
    val inputStream =
      Sync[F].delay(
        getClass.getClassLoader.getResourceAsStream(s"${ClusterResourcesConfig.basePath}/${clusterResource.asString}")
      )
    io.readInputStream[F](inputStream, 4096)
  }

  private def template(replacementMap: Map[String, String])(str: String): String =
    replacementMap.foldLeft(str)((a, b) => a.replace("$(" + b._1 + ")", "\"" + b._2 + "\""))

}
