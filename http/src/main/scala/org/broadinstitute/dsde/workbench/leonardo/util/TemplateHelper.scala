package org.broadinstitute.dsde.workbench.leonardo.util

import java.io.File

import cats.effect.IO
import org.broadinstitute.dsde.workbench.leonardo.config.ClusterResourcesConfig
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterResource

import scala.io.Source
import cats.effect._
import cats.implicits._
import fs2._
import fs2.io._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.google.GcsObjectName

import scala.concurrent.ExecutionContext


object TemplateHelper {


  case class GcsResource(gcsBlobName: GcsBlobName, content: Array[Byte])


  def readFile[F[_]: ContextShift: Sync](file: File, blockingEc: ExecutionContext): F[GcsResource] = {
    fileStream(file, blockingEc)
      .compile.to[Array]
      .map { data => GcsResource(GcsBlobName(file.getName), data) }
  }

  def templatefile[F[_]: ContextShift: Sync](replacementMap: Map[String, String], file: File, blockingEc: ExecutionContext): F[GcsResource] = {
    fileStream(file, blockingEc)
      .through(text.utf8Decode)
      .map(template(replacementMap))
      .through(text.utf8Encode)
      .compile.to[Array]
      .map { data => GcsResource(GcsBlobName(file.getName), data) }
  }

  def readResource[F[_]: ContextShift: Sync](clusterResource: ClusterResource, blockingEc: ExecutionContext): F[GcsResource] = {
    resourceStream(clusterResource, blockingEc)
      .compile.to[Array]
      .map { data => GcsResource(GcsBlobName(clusterResource.value), data) }
  }

  def templateResource[F[_]: ContextShift: Sync](replacementMap: Map[String, String], clusterResource: ClusterResource, blockingEc: ExecutionContext): F[GcsResource] = {
    resourceStream(clusterResource, blockingEc)
      .through(text.utf8Decode)
      .map(template(replacementMap))
      .through(text.utf8Encode)
      .compile.to[Array]
      .map { data => GcsResource(GcsBlobName(clusterResource.value), data) }
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


  //
  //  private def getFileContent(file: File, blockingEc: ExecutionContext): Stream[IO, String] = {
  //    io.file.readAll[IO](file.toPath, blockingEc, 4096)
  //      .through(text.utf8Decode)
  //      .through(text.lines)
  //  }
  //
  //
  //  private def getResourceContent(clusterResource: ClusterResource, blockingEc: ExecutionContext): Stream[IO, String] = {
  //    val inputStream = IO(getClass().getResourceAsStream(s"${ClusterResourcesConfig.basePath}/${clusterResource.value}"))
  //    io.readInputStream[IO](inputStream, 4096, blockingEc)
  //      .through(text.utf8Decode)
  //      .through(text.lines)
  //  }
  //
  //  private def template(replacementMap: Map[String, String]): Pipe[IO, String, String] = in =>
  //    in.map { line =>
  //      replacementMap.foldLeft(line)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", "\"" + b._2 + "\""))
  //    }



  //  private def resourceContent(blockingEc: ExecutionContext): Pipe[IO, ClusterResource, (GcsObjectName, Array[Byte])] = in =>
  //    in.evalMap { f =>
  //      io.file.readAll(f.toPath, blockingEc, 4096).compile.toList
  //    }
  //
  //
  //  // Process a string using map of replacement values. Each value in the replacement map replaces its key in the string.
  //  private[service] def template(raw: String, replacementMap: Map[String, String]): String = {
  //    replacementMap.foldLeft(raw)((a, b) => a.replaceAllLiterally("$(" + b._1 + ")", "\"" + b._2 + "\""))
  //  }
  //
  //  private[service] def templateFile(file: File, replacementMap: Map[String, String]): String = {
  //    val raw = Source.fromFile(file).mkString
  //    template(raw, replacementMap)
  //  }
  //
  //  private[service] def templateResource(resource: ClusterResource, replacementMap: Map[String, String]): String = {
  //    val raw = Source.fromResource(s"${ClusterResourcesConfig.basePath}/${resource.value}").mkString
  //    template(raw, replacementMap)
  //  }

}