package org.broadinstitute.dsde.workbench.leonardo.model

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.leonardo.config.{DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.ModelTypes.{GoogleBucket, GoogleBucketUri, GoogleProject, GoogleServiceAccount}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, JsonFormat}

import scala.language.implicitConversions

// maybe we want to get fancy later
object ModelTypes {
  type GoogleProject = String
  type GoogleServiceAccount = String
  type GoogleBucket = String
  type GoogleBucketUri = String
}

object GoogleBucketUri {
  def apply (bucketName: String, fileName: String): GoogleBucketUri = {
    s"gs://${bucketName}/${fileName}"
  }
}

object ClusterStatus extends Enumeration {
  type ClusterStatus = Value
  //NOTE: Remember to update the definition of this enum in Swagger when you add new ones
  val Unknown, Creating, Running, Updating, Error, Deleting, Deleted = Value
  val activeStatuses = Set(Unknown, Creating, Running, Updating)
  val monitoredStatuses = Set(Unknown, Creating, Updating, Deleting)

  class StatusValue(status: ClusterStatus) {
    def isActive: Boolean = activeStatuses contains status
    def isMonitored: Boolean = monitoredStatuses contains status
  }
  implicit def enumConvert(status: ClusterStatus): StatusValue = new StatusValue(status)

  def withNameOpt(s: String): Option[ClusterStatus] = values.find(_.toString == s)

  def withNameIgnoreCase(str: String): ClusterStatus = {
    values.find(_.toString.equalsIgnoreCase(str)).getOrElse(throw new IllegalArgumentException(s"Unknown cluster status: $str"))
  }
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
    labels = clusterRequest.labels,
    jupyterExtensionUri = clusterRequest.jupyterExtensionUri)

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
                   labels: Map[String, String],
                   jupyterExtensionUri: Option[GoogleBucketUri])

case class ClusterRequest(bucketPath: GoogleBucket,
                          serviceAccount: String,
                          labels: Map[String, String],
                          jupyterExtensionUri: Option[GoogleBucketUri])

case class ClusterResponse(clusterName: String,
                           googleProject: GoogleProject,
                           googleId: String,
                           status: String,
                           description: String,
                           operationName: String)

case class ClusterErrorDetails(code: Int, message: Option[String])

object ClusterInitValues {
  def apply(googleProject: GoogleProject, clusterName: String, bucketName: String, dataprocConfig: DataprocConfig, clusterRequest: ClusterRequest): ClusterInitValues =
    ClusterInitValues(
      googleProject,
      clusterName,
      dataprocConfig.dataprocDockerImage,
      dataprocConfig.jupyterProxyDockerImage,
      GoogleBucketUri(bucketName, dataprocConfig.jupyterServerCrtName),
      GoogleBucketUri(bucketName, dataprocConfig.jupyterServerKeyName),
      GoogleBucketUri(bucketName, dataprocConfig.jupyterRootCaPemName),
      GoogleBucketUri(bucketName, dataprocConfig.clusterDockerComposeName),
      GoogleBucketUri(bucketName, dataprocConfig.jupyterProxySiteConfName),
      dataprocConfig.jupyterServerName,
      dataprocConfig.proxyServerName,
      GoogleBucketUri(bucketName, dataprocConfig.jupyterInstallExtensionScript),
      clusterRequest.jupyterExtensionUri.getOrElse(""),
      GoogleBucketUri(bucketName, dataprocConfig.userServiceAccountCredentials)
    )

}

case class ClusterInitValues(googleProject: GoogleProject,
                             clusterName: String,
                             jupyterDockerImage: String,
                             proxyDockerImage: String,
                             jupyterServerCrt: GoogleBucketUri,
                             jupyterServerKey: GoogleBucketUri,
                             rootCaPem: GoogleBucketUri,
                             jupyterDockerCompose: GoogleBucketUri,
                             jupyterProxySiteConf: GoogleBucketUri,
                             jupyterServerName: String,
                             proxyServerName: String,
                             jupyterInstallExtensionScript: String,
                             jupyterExtensionUri: GoogleBucketUri,
                             userServiceAccountCredentialsUri: GoogleBucketUri)


object FirewallRuleRequest {
  def apply(googleProject: GoogleProject, dataprocConfig: DataprocConfig, proxyConfig: ProxyConfig): FirewallRuleRequest =
    FirewallRuleRequest(name = dataprocConfig.clusterFirewallRuleName,
      googleProject = googleProject,
      targetTags = List(dataprocConfig.clusterNetworkTag),
      port = proxyConfig.jupyterPort.toString,
      protocol = proxyConfig.jupyterProtocol
    )
}

case class FirewallRuleRequest(name: String,
                               googleProject: GoogleProject,
                               targetTags: List[String] = List.empty,
                               port: String, // could be a number "80" or range "5000-6000"
                               protocol: String
                              )


case class StorageObjectResponse(name: String,
                                 bucketName: String,
                                 timeCreated: String)


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

  implicit val clusterFormat = jsonFormat13(Cluster.apply)
  implicit val clusterRequestFormat = jsonFormat4(ClusterRequest)
  implicit val clusterResponseFormat = jsonFormat6(ClusterResponse)
  implicit val clusterInitValuesFormat = jsonFormat14(ClusterInitValues.apply)
  implicit val firewallRuleRequestFormat = jsonFormat5(FirewallRuleRequest.apply)
  implicit val storageObjectResponseFormat = jsonFormat3(StorageObjectResponse)

}
