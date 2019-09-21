package org.broadinstitute.dsde.workbench.leonardo.util

import java.io.File

import cats.effect._
import fs2._
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterResourcesConfig
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterResource

import scala.concurrent.ExecutionContext

object TemplateHelper {

  def readFile[F[_]: ContextShift: Sync](file: File, blockingEc: ExecutionContext): F[Array[Byte]] = {
    fileStream(file, blockingEc)
      .compile.to[Array]
  }

  def templatefile[F[_]: ContextShift: Sync](replacementMap: Map[String, String], file: File, blockingEc: ExecutionContext): F[Array[Byte]] = {
    fileStream(file, blockingEc)
      .through(text.utf8Decode)
      .map(template(replacementMap))
      .through(text.utf8Encode)
      .compile.to[Array]
  }

  def readResource[F[_]: ContextShift: Sync](clusterResource: ClusterResource, blockingEc: ExecutionContext): F[Array[Byte]] = {
    resourceStream(clusterResource, blockingEc)
      .compile.to[Array]
  }

  def templateResource[F[_]: ContextShift: Sync](replacementMap: Map[String, String], clusterResource: ClusterResource, blockingEc: ExecutionContext): F[Array[Byte]] = {
    resourceStream(clusterResource, blockingEc)
      .through(text.utf8Decode)
      .map(template(replacementMap))
      .through(text.utf8Encode)
      .compile.to[Array]
  }

  private def template(replacementMap: Map[String, String])(str: String): String = {
    replacementMap.foldLeft(str)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", "\"" + b._2 + "\""))
  }

  private def fileStream[F[_]: ContextShift: Sync](file: File, blockingEc: ExecutionContext): Stream[F, Byte] = {
    io.file.readAll[F](file.toPath, blockingEc, 4096)
  }

  private def resourceStream[F[_]: ContextShift: Sync](clusterResource: ClusterResource, blockingEc: ExecutionContext): Stream[F, Byte] = {
    val inputStream = Sync[F].delay(getClass().getResourceAsStream(s"${ClusterResourcesConfig.basePath}/${clusterResource.value}"))
    io.readInputStream[F](inputStream, 4096, blockingEc)
  }

}