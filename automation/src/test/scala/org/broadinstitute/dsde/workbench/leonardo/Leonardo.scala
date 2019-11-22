package org.broadinstitute.dsde.workbench.leonardo

import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.AutomationTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.service.RestClient

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
    def handleClusterResponse(response: String): Cluster = {
      val res = for {
        json <- io.circe.parser.parse(response)
        r <- json.as[Cluster]
      } yield r

      res.getOrElse(throw new Exception("Failed to parse list of clusters response"))
    }

    def handleClusterSeqResponse(response: String): List[Cluster] = {
      val res = for {
        json <- io.circe.parser.parse(response)
        r <- json.as[List[Cluster]]
      } yield r

      res.getOrElse(throw new Exception("Failed to parse list of clusters response"))
    }

    def clusterPath(googleProject: GoogleProject,
                    clusterName: ClusterName,
                    version: Option[ApiVersion] = None): String = {
      val versionPath = version.map(_.toUrlSegment).getOrElse("")
      s"api/cluster${versionPath}/${googleProject.value}/${clusterName.string}"
    }

    def list(googleProject: GoogleProject)(implicit token: AuthToken): Seq[Cluster] = {
      logger.info(s"Listing active clusters in project: GET /api/clusters/${googleProject.value}")
      handleClusterSeqResponse(parseResponse(getRequest(url + "api/clusters")))
    }

    def listIncludingDeleted(googleProject: GoogleProject)(implicit token: AuthToken): Seq[Cluster] = {
      val path = s"api/clusters/${googleProject.value}?includeDeleted=true"
      logger.info(s"Listing clusters including deleted in project: GET /$path")
      handleClusterSeqResponse(parseResponse(getRequest(s"$url/$path")))
    }

    def create(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest)(
      implicit token: AuthToken
    ): Cluster = {
      val path = clusterPath(googleProject, clusterName, Some(ApiVersion.V2))
      logger.info(s"Create cluster: PUT /$path")
      handleClusterResponse(putRequest(url + path, clusterRequest))
    }

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Cluster = {
      val path = clusterPath(googleProject, clusterName)

      val responseString = parseResponse(getRequest(url + path))
      val cluster = handleClusterResponse(responseString)
      logger.info(s"Get cluster: GET /$path. Status = ${cluster.status}")

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

    def update(googleProject: GoogleProject, clusterName: ClusterName, clusterRequest: ClusterRequest)(
      implicit token: AuthToken
    ): Cluster = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Update cluster: PATCH /$path")
      handleClusterResponse(patchRequest(url + path, clusterRequest))
    }
  }
}

object AutomationTestJsonCodec {
  implicit val clusterNameDecoder: Decoder[ClusterName] = Decoder.decodeString.map(ClusterName)
  implicit val clusterStatusDecoder: Decoder[ClusterStatus] =
    Decoder.decodeString.emap(s => ClusterStatus.withNameOpt(s).toRight(s"Invalid cluster status ${s}"))

  implicit val clusterDecoder: Decoder[Cluster] = Decoder.forProduct11(
    "clusterName",
    "googleProject",
    "serviceAccountInfo",
    "machineConfig",
    "status",
    "creator",
    "labels",
    "stagingBucket",
    "errors",
    "dateAccessed",
    "stopAfterCreation"
  )(Cluster.apply)
}

sealed trait ApiVersion {
  def toUrlSegment: String
}

object ApiVersion {
  case object V1 extends ApiVersion {
    override def toString: String = "v1"
    override def toUrlSegment: String = "/v1"
  }

  case object V2 extends ApiVersion {
    override def toString: String = "v2"
    override def toUrlSegment: String = "/v2"
  }

  def fromString(s: String): Option[ApiVersion] = s match {
    case "v1" => Some(V1)
    case "v2" => Some(V2)
    case _    => None
  }
}
