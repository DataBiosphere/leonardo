package org.broadinstitute.dsde.workbench.leonardo.notebooks

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.{Cookie, HttpCookiePair, Referer}
import akka.http.scaladsl.unmarshalling.Unmarshal
import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.leonardo.notebooks.WelderJsonCodec._
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, _}
import org.broadinstitute.dsde.workbench.service.RestClient

/**
 * Welder API service client.
 */
object Welder extends RestClient with LazyLogging {

  val localSafeModeBaseDirectory = "safe"
  val localBaseDirectory = "edit"

  private val url = LeonardoConfig.Leonardo.apiUrl

  private val refererUrl = ProxyRedirectClient.baseUri.renderString

  case class Metadata(syncMode: String,
                      syncStatus: Option[String],
                      lastLockedBy: Option[String],
                      storageLink: Map[String, String]
  )

  def welderBasePath(googleProject: GoogleProject, clusterName: RuntimeName): String =
    s"${url}proxy/${googleProject.value}/${clusterName.asString}/welder"

  def getWelderStatus(cluster: ClusterCopy)(implicit token: AuthToken): IO[StatusResponse] = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName)
    val referer = Referer(Uri(refererUrl))
    logger.info(s"Get welder status: GET $path/status")

    for {
      response <- IO(getRequest(path + "/status", httpHeaders = List(referer)))
      bodyString <- IO.fromFuture(IO(Unmarshal(response.entity).to[String]))
      json <- IO.fromEither(io.circe.parser.parse(bodyString))
      body <- IO.fromEither(json.as[StatusResponse])
    } yield body
  }

  def postStorageLink(cluster: ClusterCopy, cloudStoragePath: GcsPath)(implicit token: AuthToken): String = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName) + "/storageLinks"
    val referer = Referer(Uri(refererUrl))

    val payload = Map(
      "localBaseDirectory" -> localBaseDirectory,
      "localSafeModeBaseDirectory" -> localSafeModeBaseDirectory,
      "cloudStorageDirectory" -> s"gs://${cloudStoragePath.bucketName.value}",
      "pattern" -> ".*.ipynb"
    )

    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))

    logger.info(s"Calling Welder storage links: POST on $path with payload $payload")

    postRequest(path, payload, httpHeaders = List(cookie, referer))
  }

  def localize(cluster: ClusterCopy, cloudStoragePath: GcsPath, isEditMode: Boolean)(implicit
    token: AuthToken
  ): String = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName) + "/objects"
    val referer = Referer(Uri(refererUrl))

    val payload = Map(
      "action" -> "localize",
      "entries" -> Array(
        Map(
          "sourceUri" -> cloudStoragePath.toUri,
          "localDestinationPath" -> getLocalPath(cloudStoragePath, isEditMode)
        )
      )
    )

    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))

    logger.info(s"Calling Welder localize: POST on $path with payload ${payload.toString()}")
    postRequest(path, payload, httpHeaders = List(cookie, referer))
  }

  def getMetadata(cluster: ClusterCopy, cloudStoragePath: GcsPath, isEditMode: Boolean)(implicit
    token: AuthToken
  ): Metadata = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName) + "/objects/metadata"
    val referer = Referer(Uri(refererUrl))

    val payload = Map(
      "localPath" -> getLocalPath(cloudStoragePath, isEditMode)
    )

    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))

    logger.info(s"Calling check metadata on a file: POST on $path with payload ${payload.toString()}")
    parseMetadataResponse(postRequest(path, payload, httpHeaders = List(cookie, referer)))
  }

  def parseMetadataResponse(response: String): Metadata =
    mapper.readValue(response, classOf[Metadata])

  def getLocalPath(cloudStoragePath: GcsPath, isEditMode: Boolean): String =
    (if (isEditMode) {
       localBaseDirectory
     } else {
       localSafeModeBaseDirectory
     }) + "/" + cloudStoragePath.objectName.value
}

object WelderJsonCodec {
  implicit val statusResponseDecoder: Decoder[StatusResponse] =
    Decoder.forProduct1("gitHeadCommit")(StatusResponse.apply)
}

final case class StatusResponse(gitHeadCommit: String)
