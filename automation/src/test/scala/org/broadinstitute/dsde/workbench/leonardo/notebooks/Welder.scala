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

  def welderBasePath(googleProject: GoogleProject, clusterName: ClusterName): String = {
    s"${url}/proxy/${googleProject.value}/${clusterName.string}/welder"
//    s"${url}/proxy/gpalloc-dev-master-3qqssch/automation-test-a3gbuiq6z/welder"
//    s"http://10.1.3.12:8080"
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
      "pattern" -> "*"
    )

    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))

    logger.info(s"Welder status is making storage links entry: POST on $path with payload $payload")
    val rawResponse = postRequest(path, payload, httpHeaders = List(cookie))
   rawResponse
  }

//  def localize(cluster: Cluster, cloudStoragePath: GcsPath, isEditMode: Boolean)(implicit token: AuthToken): String = {
//    val path = welderBasePath(cluster.googleProject, cluster.clusterName) + "/objects"
def localize(cluster: Cluster, cloudStoragePath: GcsPath, isEditMode: Boolean)(implicit token: AuthToken): String = {
    val path = welderBasePath(cluster.googleProject, cluster.clusterName) + "/objects"

    val payload = Map(
      "action" -> "localize",
      "entries" -> Array(Map(
        "sourceUri" -> cloudStoragePath.toUri,
        "localDestinationPath" -> getLocalPath(cloudStoragePath, isEditMode)
      ))
    )
    //    val payload = s"{\"localBaseDirectory\": $localBaseDirectory, \"localSafeModeBaseDirectory\": $localSafeModeBaseDirectory, \"cloudStorageDirectory\": ${cloudStorageDirectory.objectName.toString}, \"pattern\": \"*\" }"
    val cookie = Cookie(HttpCookiePair("LeoToken", token.value))


    logger.info(s"Welder status is localizing: POST on $path with payload $payload")
    val rawResponse = postRequest(path, payload, httpHeaders = List(cookie))
    println("=======localize resp:")
    println(rawResponse)
    rawResponse
  }

  def getLocalPath(cloudStoragePath: GcsPath, isEditMode: Boolean): String = {
    (if (isEditMode) {
      localBaseDirectory
    } else {
      localSafeModeBaseDirectory
    }) + "/" + cloudStoragePath.objectName.value
  }
}