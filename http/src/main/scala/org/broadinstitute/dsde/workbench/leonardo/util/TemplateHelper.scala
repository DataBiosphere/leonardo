package org.broadinstitute.dsde.workbench.leonardo.util

import java.io.File

import cats.effect._
import fs2._
import org.broadinstitute.dsde.workbench.leonardo.RuntimeResource
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterResourcesConfig

object TemplateHelper {

  def templateFile[F[_]: ContextShift: Sync](replacementMap: Map[String, String],
                                             file: File,
                                             blocker: Blocker): Stream[F, Byte] =
    fileStream(file, blocker)
      .through(text.utf8Decode)
      .through(text.lines)
      .map(template(replacementMap))
      .intersperse("\n")
      .through(text.utf8Encode)

  def templateResource[F[_]: ContextShift: Sync](replacementMap: Map[String, String],
                                                 clusterResource: RuntimeResource,
                                                 blocker: Blocker): Stream[F, Byte] =
    resourceStream(clusterResource, blocker)
      .through(text.utf8Decode)
      .through(text.lines)
      .map(template(replacementMap))
      .intersperse("\n")
      .through(text.utf8Encode)

  def fileStream[F[_]: ContextShift: Sync](file: File, blocker: Blocker): Stream[F, Byte] =
    io.file.readAll[F](file.toPath, blocker, 4096)

  def resourceStream[F[_]: ContextShift: Sync](clusterResource: RuntimeResource, blocker: Blocker): Stream[F, Byte] = {
    val inputStream =
      Sync[F].delay(
        getClass.getClassLoader.getResourceAsStream(s"${ClusterResourcesConfig.basePath}/${clusterResource.asString}")
      )
    io.readInputStream[F](inputStream, 4096, blocker)
  }

  private def template(replacementMap: Map[String, String])(str: String): String =
    replacementMap.foldLeft(str)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", "\"" + b._2 + "\""))

}
