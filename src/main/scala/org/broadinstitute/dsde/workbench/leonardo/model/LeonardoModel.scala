package org.broadinstitute.dsde.workbench.leonardo.model

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.{GoogleBucket, GoogleProject, GoogleServiceAccount}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, JsonFormat}
import scala.language.implicitConversions

// maybe we want to get fancy later
object ModelTypes {
  type GoogleProject = String
  type GoogleServiceAccount = String
  type GoogleBucket = String
}

object ClusterStatus extends Enumeration {
  type ClusterStatus = Value
  val Unknown, Creating, Deleting, Deleted = Value
  val activeStatuses = Seq(Unknown, Creating)

  class StatusValue(status: Value) {
    def isActive:Boolean = activeStatuses contains status
  }
  implicit def enumConvert(status: Value): StatusValue = new StatusValue(status)

  def withNameOpt(s: String): Option[Value] = values.find(_.toString == s)
}

object Cluster {
  def apply(clusterRequest: ClusterRequest, clusterResponse: ClusterResponse): Cluster = Cluster(
    clusterName = clusterResponse.clusterName,
    googleId = UUID.fromString(clusterResponse.googleId),
    googleProject = clusterResponse.googleProject,
    googleServiceAccount = clusterRequest.serviceAccount,
    googleBucket = clusterRequest.bucketPath,
    clusterUrl = getClusterUrl(clusterResponse.googleProject, clusterResponse.clusterName),
    operationName = clusterResponse.operationName,
    status = ClusterStatus.Creating,
    hostIp = None,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = clusterRequest.labels)

  def getClusterUrl(googleProject: String, clusterName: String): String = {
    val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load())
    val dataprocConfig = config.as[DataprocConfig]("dataproc")
    dataprocConfig.clusterUrlBase + googleProject + "/" + clusterName
  }
}

case class Cluster(clusterName: String,
                   googleId: UUID,
                   googleProject: GoogleProject,
                   googleServiceAccount: GoogleServiceAccount,
                   googleBucket: GoogleBucket,
                   clusterUrl: String,
                   operationName: String,
                   status: ClusterStatus,
                   hostIp: Option[String],
                   createdDate: Instant,
                   destroyedDate: Option[Instant],
                   labels: Map[String, String])

case class ClusterRequest(bucketPath: GoogleBucket,
                          serviceAccount: String,
                          labels: Map[String, String])

case class ClusterResponse(clusterName: String,
                           googleProject: GoogleProject,
                           googleId: String,
                           status: String,
                           description: String,
                           operationName: String)

case class ClusterInitValues(clusterName: String,
                             googleProject: GoogleProject,
                             jupyterDockerImage: String,
                             proxyDockerImage: String,
                             jupyterServerCrt: String,
                             jupyterServerKey: String,
                             rootCaPem: String,
                             jupyterDockerCompose: String,
                             jupyterServerName: String,
                             proxyServerName: String)


object LeonardoJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  // needed for Cluster
  implicit object UUIDFormat extends JsonFormat[UUID] {
    def write(obj: UUID) = JsString(obj.toString)

    def read(json: JsValue): UUID = json match {
      case JsString(uuid) => UUID.fromString(uuid)
      case other => throw DeserializationException("Expected UUID, got: " + other)
    }
  }

  // needed for Cluster
  implicit object InstantFormat extends JsonFormat[Instant] {
    def write(obj: Instant) = JsString(obj.toString)

    def read(json: JsValue): Instant = json match {
      case JsString(instant) => Instant.parse(instant)
      case other => throw DeserializationException("Expected Instant, got: " + other)
    }
  }

  // needed for Cluster
  implicit object ClusterStatusFormat extends JsonFormat[ClusterStatus] {
    def write(obj: ClusterStatus) = JsString(obj.toString)

    def read(json: JsValue): ClusterStatus = json match {
      case JsString(status) => ClusterStatus.withName(status)
      case other => throw DeserializationException("Expected ClusterStatus, got: " + other)
    }
  }

  implicit val clusterFormat = jsonFormat12(Cluster.apply)
  implicit val clusterRequestFormat = jsonFormat3(ClusterRequest)
  implicit val clusterResponseFormat = jsonFormat6(ClusterResponse)
  implicit val clusterMetadataFormat = jsonFormat10(ClusterInitValues)
}