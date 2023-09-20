package org.broadinstitute.dsde.workbench.leonardo

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
import org.broadinstitute.dsde.workbench.leonardo.ApiJsonDecoder.getRuntimeResponseCopyDecoder

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

    def runtimePath(googleProject: GoogleProject,
                    runtimeName: RuntimeName,
                    version: Option[ApiVersion] = None
    ): String = {
      val versionPath = version.map(_.toUrlSegment).getOrElse("")
      s"api/google${versionPath}/runtimes/${googleProject.value}/${runtimeName.asString}"
    }

    def listIncludingDeletedRuntime(
      googleProject: GoogleProject
    )(implicit token: AuthToken): Seq[ListRuntimeResponseCopy] = {
      val path = s"api/google/v1/runtimes/${googleProject.value}?includeDeleted=true"
      logger.info(s"Listing runtimes including deleted in project: GET /$path")
      val parsedRequest = parseResponse(getRequest(s"$url/$path"))
      handleListRuntimeResponse(parsedRequest)
    }

    def getRuntime(googleProject: GoogleProject, runtimeName: RuntimeName)(implicit
      token: AuthToken
    ): GetRuntimeResponseCopy = {
      val path = runtimePath(googleProject, runtimeName, Some(ApiVersion.V1))

      val responseString = parseResponse(getRequest(url + path))

      val res = for {
        json <- io.circe.parser.parse(responseString)
        r <- json.as[GetRuntimeResponseCopy]
      } yield r

      res.fold(e => throw e,
               resp => {
                 logger.info(s"Get runtime: GET /$path. Status = ${resp.status}")
                 resp
               }
      )
    }

    def deleteRuntime(googleProject: GoogleProject, runtimeName: RuntimeName)(implicit token: AuthToken): String = {
      val path = runtimePath(googleProject, runtimeName, Some(ApiVersion.V1))

      logger.info(s"Delete runtime: DELETE /$path")
      deleteRequest(url + path)
    }

    def stopRuntime(googleProject: GoogleProject, runtimeName: RuntimeName)(implicit token: AuthToken): String = {
      val path = runtimePath(googleProject, runtimeName, Some(ApiVersion.V1)) + "/stop"
      logger.info(s"Stopping runtime: POST /$path")
      postRequest(url + path)
    }

    def startRuntime(googleProject: GoogleProject, runtimeName: RuntimeName)(implicit token: AuthToken): String = {
      val path = runtimePath(googleProject, runtimeName, Some(ApiVersion.V1)) + "/start"
      logger.info(s"Starting runtime: POST /$path")
      postRequest(url + path)
    }
  }
}

object AutomationTestJsonCodec {
  implicit val clusterStatusDecoder: Decoder[ClusterStatus] =
    Decoder.decodeString.map(s => ClusterStatus.withNameIgnoreCase(s))

  implicit val clusterDecoder: Decoder[ClusterCopy] =
    Decoder.forProduct12[
      ClusterCopy,
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
      Int,
      Boolean
    ](
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
      "autopauseThreshold",
      "patchInProgress"
    ) { (cn, gp, sa, mc, status, c, l, sb, e, da, at, pip) =>
      ClusterCopy(cn, gp, sa, mc, status, c, l, sb, e.getOrElse(List.empty), da, at, pip)
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
