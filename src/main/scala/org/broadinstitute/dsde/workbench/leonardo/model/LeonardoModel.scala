package org.broadinstitute.dsde.workbench.leonardo.model

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.{GoogleBucket, GoogleProject, GoogleServiceAccount, LeonardoUser}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, JsonFormat}

// maybe we want to get fancy later
object ModelTypes {
  type GoogleProject = String
  type GoogleServiceAccount = String
  type GoogleBucket = String
  type LeonardoUser = String // TODO: email or google subject ID ?
}

object Cluster {
  def apply(clusterRequest: ClusterRequest, clusterResponse: ClusterResponse): Cluster = Cluster(
    clusterId = UUID.fromString(clusterResponse.clusterId),
    clusterName = clusterResponse.clusterName,
    googleProject = clusterResponse.googleProject,
    googleServiceAccount = clusterRequest.serviceAccount,
    googleBucket = clusterRequest.bucketPath,
    operationName = clusterResponse.operationName,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = clusterRequest.labels)
}

case class Cluster(clusterId: UUID,
                   clusterName: String,
                   googleProject: GoogleProject,
                   googleServiceAccount: GoogleServiceAccount,
                   googleBucket: GoogleBucket,
                   operationName: String,
                   createdDate: Instant,
                   destroyedDate: Option[Instant],
                   labels: Map[String, String])

case class ClusterRequest(bucketPath: GoogleBucket,
                          serviceAccount: String,
                          labels: Map[String, String])

case class ClusterResponse(clusterName: String,
                           googleProject: GoogleProject,
                           clusterId: String,
                           status: String,
                           description: String,
                           operationName: String)

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

  implicit val clusterFormat = jsonFormat9(Cluster.apply)
  implicit val clusterRequestFormat = jsonFormat3(ClusterRequest)
  implicit val clusterResponseFormat = jsonFormat6(ClusterResponse)
}