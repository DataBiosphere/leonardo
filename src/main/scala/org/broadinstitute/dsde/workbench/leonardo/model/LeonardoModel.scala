package org.broadinstitute.dsde.workbench.leonardo.model

import java.net.URL
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import cats.Semigroup
import cats.implicits._
import org.broadinstitute.dsde.workbench.leonardo.config.{ClusterDefaultsConfig, ClusterFilesConfig, ClusterResourcesConfig, DataprocConfig, ProxyConfig}
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster._
import org.broadinstitute.dsde.workbench.leonardo.model.google.ClusterStatus.ClusterStatus
import org.broadinstitute.dsde.workbench.leonardo.model.google.DataprocJsonSupport._
import org.broadinstitute.dsde.workbench.leonardo.model.google._
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model.google.GoogleModelJsonSupport._
import org.broadinstitute.dsde.workbench.model.google._
import org.broadinstitute.dsde.workbench.model._
import spray.json._

// Create cluster API request
case class ClusterRequest(labels: LabelMap = Map(),
                          jupyterExtensionUri: Option[GcsPath] = None,
                          machineConfig: Option[MachineConfig] = None)

// A resource that is required by a cluster
case class ClusterResource(value: String) extends ValueObject

// Information about service accounts used by the cluster
case class ServiceAccountInfo(clusterServiceAccount: Option[WorkbenchEmail],
                              notebookServiceAccount: Option[WorkbenchEmail])

// The cluster itself
case class Cluster(clusterName: ClusterName,
                   googleId: UUID,
                   googleProject: GoogleProject,
                   serviceAccountInfo: ServiceAccountInfo,
                   machineConfig: MachineConfig,
                   clusterUrl: URL,
                   operationName: OperationName,
                   status: ClusterStatus,
                   hostIp: Option[IP],
                   creator: WorkbenchEmail,
                   createdDate: Instant,
                   destroyedDate: Option[Instant],
                   labels: LabelMap,
                   jupyterExtensionUri: Option[GcsPath]) {
  def projectNameString: String = s"${googleProject.value}/${clusterName.value}"
}
object Cluster {
  type LabelMap = Map[String, String]

  def create(clusterRequest: ClusterRequest,
             userEmail: WorkbenchEmail,
             clusterName: ClusterName,
             googleProject: GoogleProject,
             operation: Operation,
             serviceAccountInfo: ServiceAccountInfo,
             machineConfig: MachineConfig,
             clusterUrlBase: String): Cluster = {
    Cluster(
        clusterName = clusterName,
        googleId = operation.uuid,
        googleProject = googleProject,
        serviceAccountInfo = serviceAccountInfo,
        machineConfig = machineConfig,
        clusterUrl = getClusterUrl(clusterUrlBase, googleProject, clusterName),
        operationName = operation.name,
        status = ClusterStatus.Creating,
        hostIp = None,
        userEmail,
        createdDate = Instant.now(),
        destroyedDate = None,
        labels = clusterRequest.labels,
        jupyterExtensionUri = clusterRequest.jupyterExtensionUri
      )
  }

  def createDummyForDeletion(clusterRequest: ClusterRequest,
                             userEmail: WorkbenchEmail,
                             clusterName: ClusterName,
                             googleProject: GoogleProject,
                             serviceAccountInfo: ServiceAccountInfo): Cluster = {
    Cluster(
      clusterName = clusterName,
      googleId = UUID.randomUUID,
      googleProject = googleProject,
      serviceAccountInfo = serviceAccountInfo,
      machineConfig = MachineConfigOps.create(clusterRequest.machineConfig, ClusterDefaultsConfig(0, "", 0, "", 0, 0, 0)),
      clusterUrl = getClusterUrl("https://dummy-cluster/", googleProject, clusterName),
      operationName = OperationName("dummy-operation"),
      status = ClusterStatus.Creating,
      hostIp = None,
      userEmail,
      createdDate = Instant.now(),
      destroyedDate = None,
      labels = clusterRequest.labels,
      jupyterExtensionUri = clusterRequest.jupyterExtensionUri
    )
  }

  def getClusterUrl(clusterUrlBase: String, googleProject: GoogleProject, clusterName: ClusterName): URL = {
    new URL(clusterUrlBase + googleProject.value + "/" + clusterName.value)
  }
}

// Default cluster labels
case class DefaultLabels(clusterName: ClusterName,
                         googleProject: GoogleProject,
                         creator: WorkbenchEmail,
                         clusterServiceAccount: Option[WorkbenchEmail],
                         notebookServiceAccount: Option[WorkbenchEmail],
                         notebookExtension: Option[GcsPath])

// Provides ways of combining MachineConfigs with Leo defaults
object MachineConfigOps {
  case class NegativeIntegerArgumentInClusterRequestException()
    extends LeoException(s"Your cluster request should not have negative integer values. Please revise your request and submit again.", StatusCodes.BadRequest)

  case class OneWorkerSpecifiedInClusterRequestException()
    extends LeoException("Google Dataproc does not support clusters with 1 non-preemptible worker. Must be 0, 2 or more.")

  private implicit val machineConfigSemigroup = new Semigroup[MachineConfig] {
    def combine(defined: MachineConfig, default: MachineConfig): MachineConfig = {
      val minimumDiskSize = 100
      defined.numberOfWorkers match {
        case None | Some(0) => MachineConfig(Some(0), defined.masterMachineType.orElse(default.masterMachineType),
          checkNegativeValue(defined.masterDiskSize.orElse(default.masterDiskSize)).map(s => math.max(minimumDiskSize, s)))
        case Some(numWorkers) if numWorkers == 1 => throw OneWorkerSpecifiedInClusterRequestException()
        case numWorkers => MachineConfig(checkNegativeValue(numWorkers),
          defined.masterMachineType.orElse(default.masterMachineType),
          checkNegativeValue(defined.masterDiskSize.orElse(default.masterDiskSize)).map(s => math.max(minimumDiskSize, s)),
          defined.workerMachineType.orElse(default.workerMachineType),
          checkNegativeValue(defined.workerDiskSize.orElse(default.workerDiskSize)).map(s => math.max(minimumDiskSize, s)),
          checkNegativeValue(defined.numberOfWorkerLocalSSDs.orElse(default.numberOfWorkerLocalSSDs)),
          checkNegativeValue(defined.numberOfPreemptibleWorkers.orElse(default.numberOfPreemptibleWorkers)))
      }
    }
  }

  private def checkNegativeValue(value: Option[Int]): Option[Int] = {
    value.map(v => if (v < 0) throw NegativeIntegerArgumentInClusterRequestException() else v)
  }

  def create(definedMachineConfig: Option[MachineConfig], defaultMachineConfig: ClusterDefaultsConfig): MachineConfig = {
    definedMachineConfig.getOrElse(MachineConfig()) |+| MachineConfigOps.createFromDefaults(defaultMachineConfig)
  }

  def createFromDefaults(clusterDefaultsConfig: ClusterDefaultsConfig): MachineConfig = MachineConfig(
    Some(clusterDefaultsConfig.numberOfWorkers),
    Some(clusterDefaultsConfig.masterMachineType),
    Some(clusterDefaultsConfig.masterDiskSize),
    Some(clusterDefaultsConfig.workerMachineType),
    Some(clusterDefaultsConfig.workerDiskSize),
    Some(clusterDefaultsConfig.numberOfWorkerLocalSSDs),
    Some(clusterDefaultsConfig.numberOfPreemptibleWorkers)
  )
}

// Fields that must be templated into cluster resources (e.g. the init script).
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
                             jupyterExtensionUri: String,
                             jupyterServiceAccountCredentials: String,
                             jupyterCustomJsUri: String,
                             jupyterGoogleSignInJsUri: String,
                             userEmailLoginHint: String)

object ClusterInitValues {
  val serviceAccountCredentialsFilename = "service-account-credentials.json"

  def apply(googleProject: GoogleProject, clusterName: ClusterName, initBucketName: GcsBucketName, clusterRequest: ClusterRequest, dataprocConfig: DataprocConfig,
            clusterFilesConfig: ClusterFilesConfig, clusterResourcesConfig: ClusterResourcesConfig, proxyConfig: ProxyConfig,
            serviceAccountKey: Option[ServiceAccountKey], userEmailLoginHint: WorkbenchEmail): ClusterInitValues =
    ClusterInitValues(
      googleProject.value,
      clusterName.value,
      dataprocConfig.dataprocDockerImage,
      proxyConfig.jupyterProxyDockerImage,
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterServerCrt.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterServerKey.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterFilesConfig.jupyterRootCaPem.getName)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.clusterDockerCompose.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterProxySiteConf.value)).toUri,
      dataprocConfig.jupyterServerName,
      proxyConfig.proxyServerName,
      clusterRequest.jupyterExtensionUri.map(_.toUri).getOrElse(""),
      serviceAccountKey.map(_ => GcsPath(initBucketName, GcsObjectName(serviceAccountCredentialsFilename)).toUri).getOrElse(""),
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterCustomJs.value)).toUri,
      GcsPath(initBucketName, GcsObjectName(clusterResourcesConfig.jupyterGoogleSignInJs.value)).toUri,
      userEmailLoginHint.value
    )
}

object LeonardoJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit val ClusterRequestFormat = jsonFormat3(ClusterRequest)

  implicit val ClusterResourceFormat = ValueObjectFormat(ClusterResource)

  implicit val ServiceAccountInfoFormat = jsonFormat2(ServiceAccountInfo)

  implicit val ClusterFormat = jsonFormat14(Cluster.apply)

  implicit val DefaultLabelsFormat = jsonFormat6(DefaultLabels.apply)

  implicit val ClusterInitValuesFormat = jsonFormat16(ClusterInitValues.apply)

  implicit object URLFormat extends JsonFormat[URL] {
    def write(obj: URL) = JsString(obj.toString)

    def read(json: JsValue): URL = json match {
      case JsString(url) => new URL(url)
      case other => throw DeserializationException("Expected URL, got: " + other)
    }
  }
}
