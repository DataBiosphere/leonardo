package org.broadinstitute.dsde.workbench.leonardo.notebooks

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.{Cookie, HttpCookiePair}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.{GoogleProject, _}
import org.broadinstitute.dsde.workbench.service.RestClient


/**
  * Welder API service client.
  */
object Welder extends RestClient with LazyLogging {

  val localSafeModeBaseDirectory = "safe"
  val localBaseDirectory = "edit"

  private val url = LeonardoConfig.Leonardo.apiUrl

  case class Metadata(syncMode: String, syncStatus: Option[String], lastLockedBy: Option[String], storageLink: Map[String,String])

  def welderBasePath(googleProject: GoogleProject, clusterName: ClusterName): String = {
    s"${url}proxy/${googleProject.value}/${clusterName.string}/welder"
  }

  def getWelderStatus(cluster: ClusterFixture)(implicit token: AuthToken): HttpResponse = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName)
    logger.info(s"Get welder status: GET $path/status")

    val rawResponse = getRequest(path + "/status")
    rawResponse
  }

  def postStorageLink(cluster: ClusterFixture, cloudStoragePath: GcsPath)(implicit token: AuthToken): String = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName) + "/storageLinks"

    val payload = Map(
      "localBaseDirectory" -> localBaseDirectory,
      "localSafeModeBaseDirectory" -> localSafeModeBaseDirectory,
      "cloudStorageDirectory" -> s"gs://${cloudStoragePath.bucketName.value}",
      "pattern" -> ".*.ipynb"
    )

    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))

    logger.info(s"Calling Welder storage links: POST on $path with payload $payload")

    postRequest(path, payload, httpHeaders = List(cookie))
  }

def localize(cluster: ClusterFixture, cloudStoragePath: GcsPath, isEditMode: Boolean)(implicit token: AuthToken): String = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName) + "/objects"

    val payload = Map(
      "action" -> "localize",
      "entries" -> Array(Map(
        "sourceUri" -> cloudStoragePath.toUri,
        "localDestinationPath" -> getLocalPath(cloudStoragePath, isEditMode)
      ))
    )

    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))

    logger.info(s"Calling Welder localize: POST on $path with payload ${payload.toString()}")
    postRequest(path, payload, httpHeaders = List(cookie))
  }

  def getMetadata(cluster: ClusterFixture, cloudStoragePath: GcsPath, isEditMode: Boolean)(implicit token: AuthToken): Metadata = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName) + "/objects/metadata"

    val payload = Map(
      "localPath" -> getLocalPath(cloudStoragePath, isEditMode)
    )

    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))

    logger.info(s"Calling check metadata on a file: POST on $path with payload ${payload.toString()}")
    parseMetadataResponse(postRequest(path, payload, httpHeaders = List(cookie)))
  }

  def parseMetadataResponse(response: String): Metadata = {
    mapper.readValue(response, classOf[Metadata])
  }

  def getLocalPath(cloudStoragePath: GcsPath, isEditMode: Boolean): String = {
    (if (isEditMode) {
      localBaseDirectory
    } else {
      localSafeModeBaseDirectory
    }) + "/" + cloudStoragePath.objectName.value
  }
}