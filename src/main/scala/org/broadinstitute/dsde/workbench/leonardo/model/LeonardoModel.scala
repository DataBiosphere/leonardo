package org.broadinstitute.dsde.workbench.leonardo.model

import java.net.URL
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import org.broadinstitute.dsde.workbench.google.gcs.{GcsBucketName, GcsPath, GcsRelativePath}
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterResourcesConfig, DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.model.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.StringValueClass.LabelMap
import org.broadinstitute.dsde.workbench.model.WorkbenchUserServiceAccountEmail
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import spray.json.{DefaultJsonProtocol, DeserializationException, JsString, JsValue, JsonFormat, SerializationException}

import scala.language.implicitConversions

// this needs to be a Universal Trait to enable mixin with Value Classes
// it only serves as a marker for StringValueClassFormat
sealed trait StringValueClass extends Any
case class GoogleProject(string: String) extends AnyVal with StringValueClass
case class GoogleServiceAccount(string: String) extends AnyVal with StringValueClass
case class IP(string: String) extends AnyVal with StringValueClass
case class ZoneUri(string: String) extends AnyVal with StringValueClass

// productPrefix makes toString = e.g. "Cluster (clustername)"

case class ClusterName(string: String) extends AnyVal with StringValueClass {
  override def productPrefix: String = "Cluster "
}

case class OperationName(string: String) extends AnyVal with StringValueClass {
  override def productPrefix: String = "Operation "
}

case class FirewallRuleName(string: String) extends AnyVal with StringValueClass {
  override def productPrefix: String = "Firewall Rule "
}

case class InstanceName(string: String) extends AnyVal with StringValueClass {
  override def productPrefix: String = "Instance "
}

object StringValueClass {
  type LabelMap = Map[String, String]
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
  def create(clusterRequest: ClusterRequest, clusterName: ClusterName, googleProject: GoogleProject, googleId: UUID, operationName: OperationName, serviceAccount: WorkbenchUserServiceAccountEmail): Cluster = Cluster(
    clusterName = clusterName,
    googleId = googleId,
    googleProject = googleProject,
    googleServiceAccount = serviceAccount,
    googleBucket = clusterRequest.bucketPath,
    clusterUrl = getClusterUrl(googleProject, clusterName),
    operationName = operationName,
    status = ClusterStatus.Creating,
    hostIp = None,
    createdDate = Instant.now(),
    destroyedDate = None,
    labels = clusterRequest.labels,
    jupyterExtensionUri = clusterRequest.jupyterExtensionUri)

  def getClusterUrl(googleProject: GoogleProject, clusterName: ClusterName): URL = {
    val config = ConfigFactory.parseResources("leonardo.conf").withFallback(ConfigFactory.load())
    val dataprocConfig = config.as[DataprocConfig]("dataproc")
    new URL(dataprocConfig.clusterUrlBase + googleProject.string + "/" + clusterName.string)
  }
}

case class DefaultLabels(clusterName: ClusterName,
                         googleProject: GoogleProject,
                         googleBucket: GcsBucketName,
                         serviceAccount: WorkbenchUserServiceAccountEmail,
                         notebookExtension: Option[GcsPath])

case class Cluster(clusterName: ClusterName,
                   googleId: UUID,
                   googleProject: GoogleProject,
                   googleServiceAccount: WorkbenchUserServiceAccountEmail,
                   googleBucket: GcsBucketName,
                   clusterUrl: URL,
                   operationName: OperationName,
                   status: ClusterStatus,
                   hostIp: Option[IP],
                   createdDate: Instant,
                   destroyedDate: Option[Instant],
                   labels: LabelMap,
                   jupyterExtensionUri: Option[GcsPath]) {
  def projectNameString: String = s"${googleProject.string}/${clusterName.string}"
}

case class ClusterRequest(bucketPath: GcsBucketName,
                          labels: LabelMap,
                          jupyterExtensionUri: Option[GcsPath])

case class ClusterErrorDetails(code: Int, message: Option[String])

object ClusterInitValues {
  def apply(googleProject: GoogleProject, clusterName: ClusterName, bucketName: GcsBucketName, clusterRequest: ClusterRequest, dataprocConfig: DataprocConfig,
            clusterResourcesConfig: ClusterResourcesConfig, proxyConfig: ProxyConfig): ClusterInitValues =
    ClusterInitValues(
      googleProject.string,
      clusterName.string,
      dataprocConfig.dataprocDockerImage,
      proxyConfig.jupyterProxyDockerImage,
      GcsPath(bucketName, GcsRelativePath(clusterResourcesConfig.jupyterServerCrt)).toUri,
      GcsPath(bucketName, GcsRelativePath(clusterResourcesConfig.jupyterServerKey)).toUri,
      GcsPath(bucketName, GcsRelativePath(clusterResourcesConfig.jupyterRootCaPem)).toUri,
      GcsPath(bucketName, GcsRelativePath(clusterResourcesConfig.clusterDockerCompose)).toUri,
      GcsPath(bucketName, GcsRelativePath(clusterResourcesConfig.jupyterProxySiteConf)).toUri,
      dataprocConfig.jupyterServerName,
      proxyConfig.proxyServerName,
      GcsPath(bucketName, GcsRelativePath(clusterResourcesConfig.jupyterInstallExtensionScript)).toUri,
      clusterRequest.jupyterExtensionUri.map(_.toUri).getOrElse("")
    )
}

// see https://broadinstitute.atlassian.net/browse/GAWB-2619 for why these are Strings rather than value classes

case class ClusterInitValues(googleProject: String,
                             clusterName: String,
                             jupyterDockerImage: String,
                             proxyDockerImage: String,
                             jupyterServerCrt: String,
                             jupyterServerKey: String,
                             rootCaPem: String,
                             jupyterDockerCompose: String,
                             jupyterProxySiteConf: String,
                             jupyterServerName: String,
                             proxyServerName: String,
                             jupyterInstallExtensionScript: String,
                             jupyterExtensionUri: String)


object FirewallRuleRequest {
  def apply(googleProject: GoogleProject, proxyConfig: ProxyConfig): FirewallRuleRequest =
    FirewallRuleRequest(
      name = FirewallRuleName(proxyConfig.firewallRuleName),
      googleProject = googleProject,
      targetTags = List(proxyConfig.networkTag),
      port = proxyConfig.jupyterPort,
      protocol = proxyConfig.jupyterProtocol
    )
}

case class FirewallRuleRequest(name: FirewallRuleName,
                               googleProject: GoogleProject,
                               targetTags: List[String] = List.empty,
                               port: Int,
                               protocol: String
                              )

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

  implicit object GcsBucketNameFormat extends JsonFormat[GcsBucketName] {
    def write(obj: GcsBucketName) = JsString(obj.name)

    def read(json: JsValue): GcsBucketName = json match {
      case JsString(bucketName) => GcsBucketName(bucketName)
      case other => throw DeserializationException("Expected GcsBucketName, got: " + other)
    }
  }

  implicit object GcsPathFormat extends JsonFormat[GcsPath] {
    def write(obj: GcsPath) = JsString(obj.toUri)

    def read(json: JsValue): GcsPath = json match {
      case JsString(path) => GcsPath.parse(path) match {
        case Right(gcsPath) => gcsPath
        case Left(gcsParseError) => throw DeserializationException(gcsParseError.message)
      }
      case other => throw DeserializationException("Expected GcsPath, got: " + other)
    }
  }

  case class StringValueClassFormat[T <: StringValueClass](apply: String => T, unapply: T => Option[String]) extends JsonFormat[T] {
    def write(obj: T): JsValue = unapply(obj) match {
      case Some(s) => JsString(s)
      case other => throw new SerializationException("Expected String, got: " + other)
    }

    def read(json: JsValue): T = json match {
      case JsString(value) => apply(value)
      case other => throw DeserializationException("Expected StringValueClass, got: " + other)
    }
  }

  implicit val googleProjectFormat = StringValueClassFormat(GoogleProject.apply, GoogleProject.unapply)
  implicit val clusterNameFormat = StringValueClassFormat(ClusterName, ClusterName.unapply)
  implicit val operationNameFormat = StringValueClassFormat(OperationName, OperationName.unapply)
  implicit val ipFormat = StringValueClassFormat(IP, IP.unapply)
  implicit val firewallRuleNameFormat = StringValueClassFormat(FirewallRuleName, FirewallRuleName.unapply)

  implicit val clusterFormat = jsonFormat13(Cluster.apply)
  implicit val clusterRequestFormat = jsonFormat3(ClusterRequest)
  implicit val clusterInitValuesFormat = jsonFormat14(ClusterInitValues.apply)
  implicit val defaultLabelsFormat = jsonFormat5(DefaultLabels.apply)
}
