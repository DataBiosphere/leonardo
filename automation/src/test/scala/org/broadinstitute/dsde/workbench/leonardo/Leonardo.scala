package org.broadinstitute.dsde.workbench.leonardo

import java.net.URL
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.headers.{Authorization, Cookie, HttpCookiePair, OAuth2BearerToken}
import akka.Done
import akka.http.scaladsl.server.Route
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.DeserializationFeature
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.ResourceFile
import org.broadinstitute.dsde.workbench.service.RestClient
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.config.LeoAuthToken
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google._
import org.openqa.selenium.WebDriver

import scala.concurrent.Future
import scala.io.Source

/**
  * Leonardo API service client.
  */
object Leonardo extends RestClient with LazyLogging {

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
    @JsonIgnoreProperties(ignoreUnknown = true)
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
                                    jupyterExtensionUri: Option[String],
                                    jupyterUserScriptUri: Option[String],
                                    stagingBucket: String,
                                    errors:List[ClusterError]) {

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
        jupyterExtensionUri map (parseGcsPath(_).right.get),
        jupyterUserScriptUri map (parseGcsPath(_).right.get),
        Some(GcsBucketName(stagingBucket)),
        errors
      )
    }

    def handleClusterResponse(response: String): Cluster = {
      // TODO: the Leo API returns instances which are not recognized by this JSON parser.
      // Ingoring unknown properties to work around it.
      // ClusterKluge will be removed anyway in https://github.com/DataBiosphere/leonardo/pull/236
      val newMapper = mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      newMapper.readValue(response, classOf[ClusterKluge]).toCluster
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
      val path = "api/clusters?includeDeleted=true"
      // logger.info(s"Listing all clusters including deleted: GET /$path")
      handleClusterSeqResponse(parseResponse(getRequest(s"$url/$path")))
    }

    def create(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest)(implicit token: AuthToken): Cluster = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Create cluster: PUT /$path")
      handleClusterResponse(putRequest(url + path, clusterRequest))
    }

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Cluster = {
      val path = clusterPath(googleProject, clusterName)
      val cluster = handleClusterResponse(parseResponse(getRequest(url + path)))
      logger.info(s"GET /$path. Response: $cluster")
      cluster
    }

    def delete(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Delete cluster: DELETE /$path")
      deleteRequest(url + path)
    }

    def stop(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
      val path = clusterPath(googleProject, clusterName) + "/stop"
      logger.info(s"Stopping cluster: POST /$path")
      postRequest(url + path)
    }

    def start(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
      val path = clusterPath(googleProject, clusterName) + "/start"
      logger.info(s"Starting cluster: POST /$path")
      postRequest(url + path)
    }
  }

  object notebooks {
    def handleContentItemResponse(response: String): ContentItem = {
      mapper.readValue(response, classOf[ContentItem])
    }

    def notebooksPath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"notebooks/${googleProject.value}/${clusterName.string}"

    def contentsPath(googleProject: GoogleProject, clusterName: ClusterName, contentPath: String): String =
      s"${notebooksPath(googleProject, clusterName)}/api/contents/$contentPath"

    def localizePath(googleProject: GoogleProject, clusterName: ClusterName, async: Boolean = false): String =
      s"${notebooksPath(googleProject, clusterName)}/api/localize${if (async) "?async=true" else ""}"

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver): NotebooksListPage = {
      val path = notebooksPath(googleProject, clusterName)
      logger.info(s"Get notebook: GET /$path")
      new NotebooksListPage(url + path)
    }

    def getApi(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
      val path = notebooksPath(googleProject, clusterName)
      logger.info(s"Get notebook: GET /$path")
      parseResponse(getRequest(url + path))
    }

    def localize(googleProject: GoogleProject, clusterName: ClusterName, locMap: Map[String, String], async: Boolean = false)(implicit token: AuthToken): String = {
      val path = localizePath(googleProject, clusterName, async)
      logger.info(s"Localize notebook files: POST /$path")
      val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
      postRequest(url + path, locMap, httpHeaders = List(cookie))
    }

    def getContentItem(googleProject: GoogleProject, clusterName: ClusterName, contentPath: String, includeContent: Boolean = true)(implicit token: AuthToken): ContentItem = {
      val path = contentsPath(googleProject, clusterName, contentPath) + (if(includeContent) "?content=1" else "")
      logger.info(s"Get notebook contents: GET /$path")
      val cookie = Cookie(HttpCookiePair("LeoToken", token.value))
      handleContentItemResponse(parseResponse(getRequest(url + path, httpHeaders = List(cookie))))
    }

    def setCookie(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver): String = {
      val path = notebooksPath(googleProject, clusterName) + "/setCookie"
      logger.info(s"Set cookie: GET /$path")
      parseResponse(getRequest(url + path, httpHeaders = List(Authorization(OAuth2BearerToken(token.value)))))

    }
  }

  object lab {
    def labPath(googleProject: GoogleProject, clusterName: ClusterName): String =
      s"notebooks/${googleProject.value}/${clusterName.string}/lab"

    def getApi(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): String = {
      val path = labPath(googleProject, clusterName)
      logger.info(s"Get notebook: GET /$path")
      parseResponse(getRequest(url + path))
    }

    // TODO: add JupyterLab selenium test logic
  }

  object dummyClient {
    import akka.http.scaladsl.Http
    import akka.http.scaladsl.model._
    import akka.http.scaladsl.server.Directives.{get => httpGet, _}

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken, webDriver: WebDriver): DummyClientPage = {
      val localhost = java.net.InetAddress.getLocalHost.getHostName
      val url = s"http://$localhost:9090/${googleProject.value}/${clusterName.string}/client?token=${token.value}"
      logger.info(s"Get dummy client: $url")
      new DummyClientPage(url).open
    }

    def startServer: Future[Http.ServerBinding] = {
      logger.info("Starting local server on port 9090")
      Http().bindAndHandle(route, "0.0.0.0", 9090)
    }

    def stopServer(bindingFuture: Future[Http.ServerBinding]): Future[Done] = {
      logger.info("Stopping local server")
      bindingFuture.flatMap(_.unbind())
    }

    val route: Route =
      path(Segment / Segment / "client") { (googleProject, clusterName) =>
        httpGet {
          parameter('token.as[String]) { token =>
            complete {
              logger.info(s"Serving dummy client for $googleProject/$clusterName")
              HttpEntity(ContentTypes.`text/html(UTF-8)`, getContent(GoogleProject(googleProject), ClusterName(clusterName), LeoAuthToken(token)))
            }
          }
        }
      }

    private def getContent(googleProject: GoogleProject, clusterName: ClusterName, token: LeoAuthToken) = {
      val resourceFile = ResourceFile("dummy-notebook-client.html")
      val raw = Source.fromFile(resourceFile).mkString
      val replacementMap = Map(
        "leoBaseUrl" -> url,
        "googleProject" -> googleProject.value,
        "clusterName" -> clusterName.string,
        "token" -> token.value,
        "googleClientId" -> "some-client"
      )
      replacementMap.foldLeft(raw) { case (source, (key, replacement)) =>
        source.replaceAllLiterally("$("+key+")", s"""'$replacement'""")
      }
    }
  }
}
