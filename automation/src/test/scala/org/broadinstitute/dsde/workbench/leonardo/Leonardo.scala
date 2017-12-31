package org.broadinstitute.dsde.workbench.leonardo

import java.net.URL
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.headers.{Cookie, HttpCookiePair}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.api.WorkbenchClient
import org.broadinstitute.dsde.workbench.config.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.openqa.selenium.WebDriver
import org.broadinstitute.dsde.workbench.google.gcs.GcsPath

/**
  * Leonardo API service client.
  */
object Leonardo extends WorkbenchClient with LazyLogging {

  private val url = LeonardoConfig.Leonardo.apiUrl

  object test {
    def ping()(implicit token: AuthToken): String = {
      logger.info(s"Pinging: GET /ping")
      parseResponse(getRequest(url + "ping"))
    }
  }

  object cluster {

    // TODO: custom JSON deserializer
    // the default doesn't handle some fields correctly so here they're strings
    private case class ClusterKluge(clusterName: ClusterName,
                                    googleId: UUID,
                                    googleProject: String,
                                    serviceAccountInfo: Map[String, String],
                                    machineConfig: Map[String, String],
                                    clusterUrl: URL,
                                    operationName: OperationName,
                                    status: String,
                                    hostIp: Option[IP],
                                    creator: String,
                                    createdDate: String,
                                    destroyedDate: Option[String],
                                    labels: LabelMap,
                                    jupyterExtensionUri: Option[String]) {

      def toCluster = Cluster(clusterName,
        googleId,
        GoogleProject(googleProject),
        ServiceAccountInfo(serviceAccountInfo),
        MachineConfig(machineConfig),
        clusterUrl,
        operationName,
        ClusterStatus.withName(status),
        hostIp,
        WorkbenchEmail(creator),
        Instant.parse(createdDate),
        destroyedDate map Instant.parse,
        labels,
        jupyterExtensionUri map (GcsPath.parse(_).right.get))
    }

    def handleClusterResponse(response: String): Cluster = {
      mapper.readValue(response, classOf[ClusterKluge]).toCluster
    }

    def handleClusterSeqResponse(response: String): List[Cluster] = {
      // this does not work, due to type erasure
      // mapper.readValue(response, classOf[List[ClusterKluge]])

      mapper.readValue(response, classOf[List[_]]).map { clusterAsAny =>
        val clusterAsJson = mapper.writeValueAsString(clusterAsAny)
        mapper.readValue(clusterAsJson, classOf[ClusterKluge]).toCluster
      }
    }

    def clusterPath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"api/cluster/${googleProject.value}/${clusterName.string}"

    def list()(implicit token: AuthToken): Seq[Cluster] = {
      logger.info(s"Listing all active clusters: GET /api/clusters")
      handleClusterSeqResponse(parseResponse(getRequest(url + "api/clusters")))
    }

    def listIncludingDeleted()(implicit token: AuthToken): Seq[Cluster] = {
      logger.info(s"Listing all clusters including deleted: GET /api/clusters?includeDeleted=true")
      handleClusterSeqResponse(parseResponse(getRequest(url + "api/clusters?includeDeleted=true")))
    }

    def create(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest)(implicit token: AuthToken): Cluster = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Create cluster: PUT /$path")
      handleClusterResponse(putRequest(url + path, clusterRequest))
    }

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Cluster = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Get details for cluster: GET /$path")
      handleClusterResponse(parseResponse(getRequest(url + path)))
    }

    def delete(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Delete cluster: DELETE /$path")
      deleteRequest(url + path)
    }

  }

  object notebooks {
    def handleContentItemResponse(response: String): ContentItem = {
      mapper.readValue(response, classOf[ContentItem])
    }

    def notebooksPath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"notebooks/${googleProject.value}/${clusterName.string}"

    def contentsPath(googleProject: GoogleProject, clusterName: ClusterName, contentPath: String): String =
      s"${notebooksPath(googleProject, clusterName)}/api/contents/$contentPath?content=1"

    def localizePath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"${notebooksPath(googleProject, clusterName)}/api/localize"

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver): NotebooksListPage = {
      val path = notebooksPath(googleProject, clusterName)
      logger.info(s"Get notebook: GET /$path")
      new NotebooksListPage(url + path)
    }

    def localize(googleProject: GoogleProject, clusterName: ClusterName, locMap: Map[String, String])(implicit token: AuthToken): String = {
      val path = localizePath(googleProject, clusterName)
      logger.info(s"Localize notebook files: POST /$path")
      val cookie = Cookie(HttpCookiePair("FCtoken", token.value))
      postRequest(url + path, locMap, httpHeaders = List(cookie))
    }

    def getContentItem(googleProject: GoogleProject, clusterName: ClusterName, contentPath: String)(implicit token: AuthToken): ContentItem = {
      val path = contentsPath(googleProject, clusterName, contentPath)
      logger.info(s"Get notebook contents: GET /$path")
      val cookie = Cookie(HttpCookiePair("FCtoken", token.value))
      handleContentItemResponse(parseResponse(getRequest(url + path, httpHeaders = List(cookie))))
    }
  }
}
