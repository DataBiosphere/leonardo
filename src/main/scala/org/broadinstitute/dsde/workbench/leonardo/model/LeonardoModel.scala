package org.broadinstitute.dsde.workbench.leonardo.model

import java.net.URL
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.DataprocConfig
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.TypedString.LabelMap
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, JsonFormat}

sealed trait TypedString extends Any
case class ClusterName(s: String) extends AnyVal with TypedString
case class GoogleProject(s: String) extends AnyVal with TypedString
case class GoogleServiceAccount(s: String) extends AnyVal with TypedString
case class GoogleBucket(s: String) extends AnyVal with TypedString
case class OperationName(s: String) extends AnyVal with TypedString
case class IP(s: String) extends AnyVal with TypedString

object TypedString {
  type LabelMap = Map[String, String]
}

object ClusterStatus extends Enumeration {
  type ClusterStatus = Value
  val Unknown, Creating = Value
}

object Cluster {
  def apply(clusterRequest: ClusterRequest, clusterResponse: ClusterResponse): Cluster = Cluster(
    clusterName = clusterResponse.clusterName,
    googleId = clusterResponse.googleId,
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

  def getClusterUrl(googleProject: GoogleProject, clusterName: ClusterName): URL = {
    val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load())
    val dataprocConfig = config.as[DataprocConfig]("dataproc")
    new URL(dataprocConfig.clusterUrlBase + googleProject.s + "/" + clusterName.s)
  }
}

case class Cluster(clusterName: ClusterName,
                   googleId: UUID,
                   googleProject: GoogleProject,
                   googleServiceAccount: GoogleServiceAccount,
                   googleBucket: GoogleBucket,
                   clusterUrl: URL,
                   operationName: OperationName,
                   status: ClusterStatus,
                   hostIp: Option[IP],
                   createdDate: Instant,
                   destroyedDate: Option[Instant],
                   labels: LabelMap)

case class ClusterRequest(bucketPath: GoogleBucket,
                          serviceAccount: GoogleServiceAccount,
                          labels: LabelMap)

case class ClusterResponse(clusterName: ClusterName,
                           googleProject: GoogleProject,
                           googleId: UUID,
                           status: String,
                           description: String,
                           operationName: OperationName)

object LeonardoJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit object UUIDFormat extends JsonFormat[UUID] {
    def write(obj: UUID) = JsString(obj.toString)

    def read(json: JsValue): UUID = json match {
      case JsString(uuid) => UUID.fromString(uuid)
      case other => throw DeserializationException("Expected UUID, got: " + other)
    }
  }

  implicit object InstantFormat extends JsonFormat[Instant] {
    def write(obj: Instant) = JsString(obj.toString)

    def read(json: JsValue): Instant = json match {
      case JsString(instant) => Instant.parse(instant)
      case other => throw DeserializationException("Expected Instant, got: " + other)
    }
  }

  implicit object ClusterStatusFormat extends JsonFormat[ClusterStatus] {
    def write(obj: ClusterStatus) = JsString(obj.toString)

    def read(json: JsValue): ClusterStatus = json match {
      case JsString(status) => ClusterStatus.withName(status)
      case other => throw DeserializationException("Expected ClusterStatus, got: " + other)
    }
  }

  implicit object URLFormat extends JsonFormat[URL] {
    def write(obj: URL) = JsString(obj.toString)

    def read(json: JsValue): URL = json match {
      case JsString(url) => new URL(url)
      case other => throw DeserializationException("Expected URL, got: " + other)
    }
  }

  case class TypedStringFormat[T <: TypedString](create: String => T) extends JsonFormat[T] {
    def write(obj: T): JsValue = JsString(obj.toString)

    def read(json: JsValue): T = json match {
      case JsString(value) => create(value)
      case other => throw DeserializationException("Expected TypedString, got: " + other)
    }
  }

  implicit val googleProjectFormat = TypedStringFormat(GoogleProject)
  implicit val googleServiceAccountFormat = TypedStringFormat(GoogleServiceAccount)
  implicit val googleBucketFormat = TypedStringFormat(GoogleBucket)
  implicit val clusterNameFormat = TypedStringFormat(ClusterName)
  implicit val operationNameFormat = TypedStringFormat(OperationName)
  implicit val ipFormat = TypedStringFormat(IP)

  implicit val clusterFormat = jsonFormat12(Cluster.apply)
  implicit val clusterRequestFormat = jsonFormat3(ClusterRequest)
  implicit val clusterResponseFormat = jsonFormat6(ClusterResponse)
}