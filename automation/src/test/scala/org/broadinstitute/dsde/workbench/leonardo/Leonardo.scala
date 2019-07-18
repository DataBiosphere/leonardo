package org.broadinstitute.dsde.workbench.leonardo

import java.net.URL
import java.time.Instant
import java.util.UUID

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.DeserializationFeature
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.workbench.auth.AuthToken
import org.broadinstitute.dsde.workbench.leonardo.StringValueClass.LabelMap
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
                                    errors: List[ClusterError],
                                    dateAccessed: String,
                                    defaultClientId: Option[String],
                                    stopAfterCreation: Option[Boolean],
                                    scopes: Set[String]) {

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
        Option(stagingBucket).map(GcsBucketName),
        errors,
        Instant.parse(dateAccessed),
        defaultClientId,
        stopAfterCreation.getOrElse(false),
        scopes
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

    def clusterPath(googleProject: GoogleProject,
                    clusterName: ClusterName): String = {
      s"api/cluster/v2/${googleProject.value}/${clusterName.string}"
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

    def create(googleProject: GoogleProject,
               clusterName: ClusterName,
               clusterRequest: ClusterRequest)
              (implicit token: AuthToken): Cluster = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Create cluster: PUT /$path")
      handleClusterResponse(putRequest(url + path, clusterRequest))
    }

    def get(googleProject: GoogleProject, clusterName: ClusterName)(implicit token: AuthToken): Cluster = {
      val path = clusterPath(googleProject, clusterName)

      val cluster = handleClusterResponse(parseResponse(getRequest(url + path)))
      logger.info(s"Get cluster: GET /$path. Response: $cluster")

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

    def update(googleProject: GoogleProject,
               clusterName: ClusterName,
               clusterRequest: ClusterRequest)(implicit token: AuthToken): Cluster = {
      val path = clusterPath(googleProject, clusterName)
      logger.info(s"Update cluster: PATCH /$path")
      handleClusterResponse(patchRequest(url + path, clusterRequest))
    }
  }
}
