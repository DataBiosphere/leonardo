package org.broadinstitute.dsde.workbench.leonardo

import java.net.URL
import java.time.Instant

import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.AutomationTestJsonCodec._
import org.broadinstitute.dsde.workbench.leonardo.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.JsonCodec._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
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
    def handleClusterResponse(response: String): ClusterCopy = {
      val res = for {
        json <- io.circe.parser.parse(response)
        r <- json.as[ClusterCopy]
      } yield r

      res.fold(e => throw new Exception(s"Failed to parse createCluster response due to ${e.getMessage}"), identity)
    }

    def handleClusterSeqResponse(response: String): List[ClusterCopy] = {
      val res = for {
        json <- io.circe.parser.parse(response)
        r <- json.as[List[ClusterCopy]]
      } yield r

      res.fold(e => throw new Exception(s"Failed to parse list of clusters response ${e}"), identity)
    }

    def handleListRuntimeResponse(response: String): List[ListRuntimeResponseCopy] = {
      val res = for {
        json <- io.circe.parser.parse(response)
        r <- json.as[List[ListRuntimeResponseCopy]]

      } yield r

      res.getOrElse(throw new Exception("Failed to parse list of runtime response"))
    }
    def clusterPath(googleProject: GoogleProject,
                    clusterName: RuntimeName,
                    version: Option[ApiVersion] = None): String = {
      val versionPath = version.map(_.toUrlSegment).getOrElse("")
      s"api/cluster${versionPath}/${googleProject.value}/${clusterName.asString}"
    }

    def runtimePath(googleProject: GoogleProject,
                    runtimeName: RuntimeName,
                    version: Option[ApiVersion] = None): String = {
      val versionPath = version.map(_.toUrlSegment).getOrElse("")
      s"api/google${versionPath}/runtimes/${googleProject.value}/${runtimeName.asString}"
    }

    def list(googleProject: GoogleProject)(implicit token: AuthToken): Seq[ClusterCopy] = {
      logger.info(s"Listing active clusters in project: GET /api/clusters/${googleProject.value}")
      handleClusterSeqResponse(parseResponse(getRequest(url + "api/clusters")))
    }

    def listIncludingDeleted(googleProject: GoogleProject)(implicit token: AuthToken): Seq[ClusterCopy] = {
      val path = s"api/clusters/${googleProject.value}?includeDeleted=true"
      logger.info(s"Listing clusters including deleted in project: GET /$path")
      handleClusterSeqResponse(parseResponse(getRequest(s"$url/$path")))
    }

    def listIncludingDeletedRuntime(
      googleProject: GoogleProject
    )(implicit token: AuthToken): Seq[ListRuntimeResponseCopy] = {
      val path = s"api/google/v1/runtimes/${googleProject.value}?includeDeleted=true"
      logger.info(s"Listing runtimes including deleted in project: GET /$path")
      val parsedRequest = parseResponse(getRequest(s"$url/$path"))
      handleListRuntimeResponse(parsedRequest)
    }

    def create(googleProject: GoogleProject, clusterName: RuntimeName, clusterRequest: ClusterRequest)(
      implicit token: AuthToken
    ): ClusterCopy = {

      val path = clusterPath(googleProject, clusterName, Some(ApiVersion.V2))
      logger.info(s"Create cluster: PUT /$path")
      handleClusterResponse(putRequest(url + path, clusterRequest))

    }

    def createRuntime(googleProject: GoogleProject, runtimeName: RuntimeName, runtimeRequest: RuntimeRequest)(
      implicit token: AuthToken
    ): Unit = {

      val path = runtimePath(googleProject, runtimeName, Some(ApiVersion.V1))
      logger.info(s"Create runtime: POST /$path")

      postRequest(url + path, runtimeRequest)

    }

    def get(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): ClusterCopy = {
      val path = clusterPath(googleProject, clusterName)

      val responseString = parseResponse(getRequest(url + path))
      val cluster = handleClusterResponse(responseString)
      logger.info(s"Get cluster: GET /$path. Status = ${cluster.status}")

      cluster
    }

    def getRuntime(googleProject: GoogleProject,
                   runtimeName: RuntimeName)(implicit token: AuthToken): GetRuntimeResponseCopy = {
      val path = runtimePath(googleProject, runtimeName, Some(ApiVersion.V1))

      val responseString = parseResponse(getRequest(url + path))

      val res = for {
        json <- io.circe.parser.parse(responseString)
        r <- json.as[GetRuntimeResponseCopy]
      } yield r

      res.fold(e => throw e, resp => {
        logger.info(s"Get runtime: GET /$path. Status = ${resp.status}")
        resp
      })
    }

    def delete(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): String = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Delete cluster: DELETE /$path")
      deleteRequest(url + path)
    }

    def deleteRuntime(googleProject: GoogleProject, runtimeName: RuntimeName)(implicit token: AuthToken): String = {
      val path = runtimePath(googleProject, runtimeName, Some(ApiVersion.V1))

      logger.info(s"Delete runtime: DELETE /$path")
      deleteRequest(url + path)
    }

    def stop(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): String = {
      val path = clusterPath(googleProject, clusterName) + "/stop"
      logger.info(s"Stopping cluster: POST /$path")
      postRequest(url + path)
    }

    def stopRuntime(googleProject: GoogleProject, runtimeName: RuntimeName)(implicit token: AuthToken): String = {
      val path = runtimePath(googleProject, runtimeName, Some(ApiVersion.V1)) + "/stop"
      logger.info(s"Stopping runtime: POST /$path")
      postRequest(url + path)
    }

    def start(googleProject: GoogleProject, clusterName: RuntimeName)(implicit token: AuthToken): String = {
      val path = clusterPath(googleProject, clusterName) + "/start"
      logger.info(s"Starting cluster: POST /$path")
      postRequest(url + path)
    }

    def startRuntime(googleProject: GoogleProject, runtimeName: RuntimeName)(implicit token: AuthToken): String = {
      val path = runtimePath(googleProject, runtimeName, Some(ApiVersion.V1)) + "/start"
      logger.info(s"Starting runtime: POST /$path")
      postRequest(url + path)
    }

    def update(googleProject: GoogleProject, clusterName: RuntimeName, clusterRequest: ClusterRequest)(
      implicit token: AuthToken
    ): ClusterCopy = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Update cluster: PATCH /$path")
      handleClusterResponse(patchRequest(url + path, clusterRequest))
    }

    def updateRuntime(googleProject: GoogleProject, runtimeName: RuntimeName, request: UpdateRuntimeRequestCopy)(
      implicit token: AuthToken
    ): Unit = {
      val path = runtimePath(googleProject, runtimeName, Some(ApiVersion.V1))
      logger.info(s"Update runtime: PATCH /$path")
      patchRequest(url + path, request)

    }

  }
}

object AutomationTestJsonCodec {
  implicit val clusterStatusDecoder: Decoder[ClusterStatus] =
    Decoder.decodeString.emap(s => ClusterStatus.withNameOpt(s).toRight(s"Invalid cluster status ${s}"))

  implicit val clusterDecoder: Decoder[ClusterCopy] =
    Decoder.forProduct13[ClusterCopy,
                         RuntimeName,
                         GoogleProject,
                         WorkbenchEmail,
                         RuntimeConfig,
                         ClusterStatus,
                         WorkbenchEmail,
                         LabelMap,
                         Option[GcsBucketName],
                         Option[List[RuntimeError]],
                         Instant,
                         Boolean,
                         Int,
                         Boolean](
      "clusterName",
      "googleProject",
      "googleServiceAccount",
      "machineConfig",
      "status",
      "creator",
      "labels",
      "stagingBucket",
      "errors",
      "dateAccessed",
      "stopAfterCreation",
      "autopauseThreshold",
      "patchInProgress"
    ) { (cn, gp, sa, mc, status, c, l, sb, e, da, sc, at, pip) =>
      ClusterCopy(cn, gp, sa, mc, status, c, l, sb, e.getOrElse(List.empty), da, sc, at, pip)
    }

  implicit val getRuntimeResponseCopyDecoder: Decoder[GetRuntimeResponseCopy] = Decoder.forProduct15[
    GetRuntimeResponseCopy,
    RuntimeName,
    GoogleProject,
    WorkbenchEmail,
    AuditInfo,
    Option[AsyncRuntimeFields],
    RuntimeConfig,
    URL,
    ClusterStatus,
    LabelMap,
    Option[GcsPath],
    Option[UserScriptPath],
    Option[UserScriptPath],
    Option[List[RuntimeError]],
    Option[UserJupyterExtensionConfig],
    Int
  ](
    "runtimeName",
    "googleProject",
    "serviceAccount",
    "auditInfo",
    "asyncRuntimeFields",
    "runtimeConfig",
    "proxyUrl",
    "status",
    "labels",
    "jupyterExtensionUri",
    "jupyterUserScriptUri",
    "jupyterStartUserScriptUri",
    "errors",
    "userJupyterExtensionConfig",
    "autopauseThreshold"
  ) { (rn, gp, sa, ai, arf, rc, pu, status, l, jeu, jusu, jsusu, e, ujec, at) =>
    GetRuntimeResponseCopy(rn, gp, sa, ai, arf, rc, pu, status, l, jusu, jsusu, e.getOrElse(List.empty), ujec, at)
  }

  implicit val listRuntimeResponseCopyDecoder: Decoder[ListRuntimeResponseCopy] = Decoder.forProduct9(
    "id",
    "runtimeName",
    "googleProject",
    "auditInfo",
    "runtimeConfig",
    "proxyUrl",
    "status",
    "labels",
    "patchInProgress"
  )(ListRuntimeResponseCopy.apply)
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
