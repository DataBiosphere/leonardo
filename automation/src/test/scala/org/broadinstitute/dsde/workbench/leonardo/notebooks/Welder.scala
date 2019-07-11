package org.broadinstitute.dsde.workbench.leonardo.notebooks

import akka.http.scaladsl.model.HttpResponse
import org.broadinstitute.dsde.workbench.service.RestClient
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.leonardo._
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.auth.AuthToken
import akka.http.scaladsl.model.headers.{Authorization, Cookie, HttpCookiePair, OAuth2BearerToken}

import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * Leonardo API service client.
  */
object Welder extends RestClient with LazyLogging {

  val localSafeModeBaseDirectory = "safe"
  val localBaseDirectory = "edit"

  private val url = LeonardoConfig.Leonardo.apiUrl

  case class Metadata(syncMode: String, syncStatus: String, lastLockedBy: String, storageLink: Map[String,String])

  def welderBasePath(googleProject: GoogleProject, clusterName: ClusterName): String = {
    s"${url}proxy/${googleProject.value}/${clusterName.string}/welder"
  }

  def getWelderStatus(cluster: Cluster)(implicit token: AuthToken): HttpResponse = {
    println("printing cluster in getWelderStatus")
    println(cluster)
    val path = welderBasePath(cluster.googleProject, cluster.clusterName)
    logger.info(s"Get welder status: GET $path/status")

    val rawResponse = getRequest(path + "/status")
    rawResponse
  }

  def postStorageLink(cluster: Cluster, cloudStoragePath: GcsPath)(implicit token: AuthToken): String = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName) + "/storageLinks"

    val payload = Map(
      "localBaseDirectory" -> localBaseDirectory,
      "localSafeModeBaseDirectory" -> localSafeModeBaseDirectory,
      "cloudStorageDirectory" -> s"gs://${cloudStoragePath.bucketName.value}",
      "pattern" -> ".*.ipynb"
    )

    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))

    logger.info(s"Making Welder storage links entry: POST on $path with payload $payload")

    postRequest(path, payload, httpHeaders = List(cookie))
  }

def localize(cluster: Cluster, cloudStoragePath: GcsPath, isEditMode: Boolean)(implicit token: AuthToken): String = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName) + "/objects"

    val payload = Map(
      "action" -> "localize",
      "entries" -> Array(Map(
        "sourceUri" -> cloudStoragePath.toUri,
        "localDestinationPath" -> getLocalPath(cloudStoragePath, isEditMode)
      ))
    )

    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))


    logger.info(s"Making Welder localize: POST on $path with payload ${payload.toString()}")
    postRequest(path, payload, httpHeaders = List(cookie))
  }

  def getMetadata(cluster: Cluster, cloudStoragePath: GcsPath, isEditMode: Boolean)(implicit token: AuthToken): Metadata = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName) + "/objects/metadata"

    val payload = Map(
      "localPath" -> getLocalPath(cloudStoragePath, isEditMode)
    )

    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))

    logger.info(s"Making Welder localize: POST on $path with payload ${payload.toString()}")
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